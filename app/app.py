import json
import logging
import os
import sys
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer, Producer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from src.scorer import make_pred

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("/app/logs/service.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Set kafka configuration file
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")


class ProcessingService:
    def __init__(self):
        self.consumer_config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "ml-scorer",
            "auto.offset.reset": "earliest",
        }
        self.producer_config = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer(self.producer_config)

    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                # Десериализация JSON
                data = json.loads(msg.value().decode("utf-8"))

                # Извлекаем ID и данные
                transaction_id = data["transaction_id"]
                input_df = pd.DataFrame([data["data"]])

                # Препроцессинг и предсказание
                proba, fraud = make_pred(input_df, "kafka_stream")

                out_msg = {
                    "transaction_id": transaction_id,
                    "score": float(proba[0]),
                    "fraud_flag": int(fraud[0]),
                }

                # Отправка результата в топик
                self.producer.produce(SCORING_TOPIC, value=json.dumps(out_msg).encode("utf-8"))
                self.producer.flush()
            except Exception as e:
                logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    logger.info("Starting Kafka ML scoring service...")
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
