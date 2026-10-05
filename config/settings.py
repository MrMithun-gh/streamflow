"""Central place for configuration. Everything comes from environment variables."""
import os

from dotenv import load_dotenv

load_dotenv()  # reads .env in the current directory into os.environ

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

PRODUCER_EVENTS_PER_SECOND = float(os.getenv("PRODUCER_EVENTS_PER_SECOND", "2"))
PRODUCER_BAD_RECORD_RATE = float(os.getenv("PRODUCER_BAD_RECORD_RATE", "0.05"))