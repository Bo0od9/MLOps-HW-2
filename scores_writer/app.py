import json
import logging
import os

import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("scores-writer")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")

# Postgres
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "fraud")
PG_USER = os.getenv("PG_USER", "fraud")
PG_PASSWORD = os.getenv("PG_PASSWORD", "fraud")


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def main():
    # Kafka consumer
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "scores-writer",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_SCORING_TOPIC])
    logger.info(f"Scores-writer started, listening topic {KAFKA_SCORING_TOPIC}")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Kafka error: {msg.error()}")
            continue

        try:
            payload = json.loads(msg.value().decode("utf-8"))
            transaction_id = payload["transaction_id"]
            score = float(payload["score"])
            fraud_flag = int(payload["fraud_flag"])

            conn = get_pg_conn()
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO scoring_results (transaction_id, score, fraud_flag)
                        VALUES (%s, %s, %s)
                        """,
                        (transaction_id, score, fraud_flag),
                    )
            conn.close()
            logger.info(f"Inserted transaction {transaction_id}")

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)


if __name__ == "__main__":
    main()
