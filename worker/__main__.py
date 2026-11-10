"""``python -m worker``: run the worker simulator.

Every option can be given as a CLI flag or an environment variable (flags win), so scenarios can
be switched by restarting the container with different env vars or arguments.
"""

import argparse
import asyncio
import logging
import os
import random
import signal

from app.messaging.broker import connect
from app.messaging.topology import declare_topology
from worker.simulator import WorkerConfig, WorkerSimulator

log = logging.getLogger("worker")


def _env_bool(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    env = os.environ.get
    p = argparse.ArgumentParser(prog="worker", description=__doc__)
    p.add_argument(
        "--amqp-url",
        default=env("WORKER_AMQP_URL")
        or env("CP_AMQP_URL", "amqp://control_plane:control_plane@localhost:5672/"),
    )
    p.add_argument(
        "--fail-rate",
        type=float,
        default=float(env("WORKER_FAIL_RATE", "0")),
        help="probability (0-1) that a task fails [WORKER_FAIL_RATE]",
    )
    p.add_argument(
        "--min-delay-ms",
        type=int,
        default=int(env("WORKER_MIN_DELAY_MS", "500")),
        help="minimum delay before each progress update [WORKER_MIN_DELAY_MS]",
    )
    p.add_argument(
        "--max-delay-ms",
        type=int,
        default=int(env("WORKER_MAX_DELAY_MS", "2000")),
        help="maximum delay before each progress update [WORKER_MAX_DELAY_MS]",
    )
    p.add_argument(
        "--duplicate-rate",
        type=float,
        default=float(env("WORKER_DUPLICATE_RATE", "0")),
        help="probability (0-1) that an update is published twice [WORKER_DUPLICATE_RATE]",
    )
    p.add_argument(
        "--reorder",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("WORKER_REORDER"),
        help="publish the terminal update before in_progress [WORKER_REORDER]",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=int(env("WORKER_CONCURRENCY", "4")),
        help="tasks processed in parallel (AMQP prefetch) [WORKER_CONCURRENCY]",
    )
    p.add_argument("--seed", type=int, default=None, help="random seed for reproducible runs")
    return p.parse_args(argv)


async def run(args: argparse.Namespace) -> None:
    config = WorkerConfig(
        fail_rate=args.fail_rate,
        min_delay_ms=args.min_delay_ms,
        max_delay_ms=args.max_delay_ms,
        duplicate_rate=args.duplicate_rate,
        reorder=args.reorder,
    )
    log.info("starting worker with %s", config)
    connection = await connect(args.amqp_url)
    async with connection:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        await channel.set_qos(prefetch_count=args.concurrency)
        topology = await declare_topology(channel)
        simulator = WorkerSimulator(
            config,
            topology.updates_exchange,
            random.Random(args.seed),  # noqa: S311  # nosec B311
        )
        await topology.tasks_queue.consume(simulator.handle)

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await stop.wait()
        log.info("worker shutting down")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
