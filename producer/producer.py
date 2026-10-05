"""Continuously generates transaction events.
Phase 3: prints events to the terminal. Phase 4: sends them to Kafka."""
import argparse
import json
import logging
import time

from config import settings
from producer.generator import next_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("producer")


def main() -> None:
    parser = argparse.ArgumentParser(description="StreamFlow transaction producer")
    parser.add_argument("--count", type=int, default=0, help="events to send (0 = run forever)")
    parser.add_argument("--rate", type=float, default=settings.PRODUCER_EVENTS_PER_SECOND,
                        help="events per second")
    parser.add_argument("--bad-rate", type=float, default=settings.PRODUCER_BAD_RECORD_RATE,
                        help="fraction of deliberately bad events (0 to 1)")
    args = parser.parse_args()

    delay = 1.0 / args.rate
    sent = 0
    previous = None
    logger.info("Producer started: rate=%s/s, bad_rate=%s", args.rate, args.bad_rate)

    try:
        while args.count == 0 or sent < args.count:
            event = next_event(args.bad_rate, previous)
            previous = event
            payload = json.dumps(event)   # dict -> JSON string

            print(payload)                # Phase 4 replaces this with a Kafka send

            sent += 1
            time.sleep(delay)
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        logger.info("Total events produced: %d", sent)


if __name__ == "__main__":
    main()