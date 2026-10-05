"""Continuously generates transaction events and sends them to Kafka."""
import argparse
import json
import logging
import time

from confluent_kafka import Producer

from config import settings
from producer.generator import next_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("producer")

stats = {"delivered": 0, "failed": 0}


def delivery_report(err, msg) -> None:
    """Called by the Kafka client when the broker confirms (or rejects) a message."""
    if err is not None:
        stats["failed"] += 1
        logger.error("Delivery failed: %s", err)
    else:
        stats["delivered"] += 1
        logger.debug("Delivered to %s [partition %d] @ offset %d",
                     msg.topic(), msg.partition(), msg.offset())


def main() -> None:
    parser = argparse.ArgumentParser(description="StreamFlow transaction producer")
    parser.add_argument("--count", type=int, default=0, help="events to send (0 = run forever)")
    parser.add_argument("--rate", type=float, default=settings.PRODUCER_EVENTS_PER_SECOND,
                        help="events per second")
    parser.add_argument("--bad-rate", type=float, default=settings.PRODUCER_BAD_RECORD_RATE,
                        help="fraction of deliberately bad events (0 to 1)")
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",           # wait for the broker to confirm the write
    })

    delay = 1.0 / args.rate
    sent = 0
    previous = None
    logger.info("Producer started: topic=%s, rate=%s/s, bad_rate=%s",
                settings.KAFKA_TOPIC, args.rate, args.bad_rate)

    try:
        while args.count == 0 or sent < args.count:
            event = next_event(args.bad_rate, previous)
            previous = event

            producer.produce(
                topic=settings.KAFKA_TOPIC,
                key=event["customer_id"],             # None is allowed (corrupted events)
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)   # serve delivery callbacks

            sent += 1
            if sent % 20 == 0:
                logger.info("Sent %d events", sent)
            time.sleep(delay)
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        producer.flush(10)     # wait up to 10s for unsent messages
        logger.info("Produced=%d delivered=%d failed=%d",
                    sent, stats["delivered"], stats["failed"])


if __name__ == "__main__":
    main()