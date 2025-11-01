import json
import os
import uuid

import pandas as pd
import streamlit as st
from kafka import KafkaProducer
from sqlalchemy import create_engine, text

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions"),
}

# Конфигурация Postgres
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("PG_DB", "fraud")
PG_USER = os.getenv("PG_USER", "fraud")
PG_PASSWORD = os.getenv("PG_PASSWORD", "fraud")
PG_PORT = int(os.getenv("PG_PORT", "5432"))


@st.cache_resource
def get_engine():
    conn_str = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(conn_str)


engine = get_engine()


def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None


def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT",
        )

        # Генерация уникальных ID для всех транзакций
        df["transaction_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

        progress_bar = st.progress(0)
        total_rows = len(df)

        for idx, row in df.iterrows():
            # Отправляем данные вместе с ID
            producer.send(
                topic, value={"transaction_id": row["transaction_id"], "data": row.drop("transaction_id").to_dict()}
            )
            progress_bar.progress((idx + 1) / total_rows)

        producer.flush()

        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False


def load_last_fraud(n: int = 10) -> pd.DataFrame:
    """Забирает из postgres транзакции с fraud_flag = 1"""
    try:
        query = text(
            """
            SELECT transaction_id, score, fraud_flag, created_at
            FROM scoring_results
            WHERE fraud_flag = 1
            ORDER BY created_at DESC
            LIMIT :limit;
        """
        )
        return pd.read_sql(query, engine, params={"limit": n})
    except Exception as e:
        st.error(f"Ошибка чтения из Postgres: {e}")
        return pd.DataFrame()


def load_last_scores(n: int = 100) -> pd.DataFrame:
    """Забирает из Postgres последние n скоров (для гистограммы)"""
    try:
        query = text(
            """
            SELECT score, created_at
            FROM scoring_results
            ORDER BY created_at DESC
            LIMIT :limit;
        """
        )
        return pd.read_sql(query, engine, params={"limit": n})
    except Exception as e:
        st.error(f"Ошибка чтения из Postgres: {e}")
        return pd.DataFrame()


def load_class_distribution() -> pd.DataFrame:
    """Возвращает количество транзакций по классам"""
    try:
        query = text(
            """
            SELECT fraud_flag, COUNT(*) AS count
            FROM scoring_results
            GROUP BY fraud_flag
            ORDER BY fraud_flag;
        """
        )
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Ошибка при получении распределения классов: {e}")
        return pd.DataFrame()


# Инициализация состояния
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Интерфейс
st.title("📤 Отправка данных в Kafka")

# Блок загрузки файлов
uploaded_file = st.file_uploader("Загрузите CSV файл с транзакциями", type=["csv"])

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "Загружен",
        "df": load_file(uploaded_file),
    }
    st.success(f"Файл {uploaded_file.name} успешно загружен!")

# Список загруженных файлов
if st.session_state.uploaded_files:
    st.subheader("🗂 Список загруженных файлов")

    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([4, 2, 2])

        with cols[0]:
            st.markdown(f"**Файл:** `{file_name}`")
            st.markdown(f"**Статус:** `{file_data['status']}`")

        with cols[2]:
            if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                if file_data["df"] is not None:
                    with st.spinner("Отправка..."):
                        success = send_to_kafka(
                            file_data["df"], KAFKA_CONFIG["topic"], KAFKA_CONFIG["bootstrap_servers"]
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()
                else:
                    st.error("Файл не содержит данных")

st.subheader("Просмотр результатов скоринга")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("Показать последние 10 фродовых транзакций"):
        df_fraud = load_last_fraud(10)
        if df_fraud.empty:
            st.info("Фродовых транзакций пока нет")
        else:
            st.dataframe(df_fraud)

with col2:
    if st.button("Показать гистограмму скоров (100 последних транзакций)"):
        df_scores = load_last_scores(100)
        if df_scores.empty:
            st.info("Скорингов пока нет.")
        else:
            st.bar_chart(df_scores["score"])

with col3:
    if st.button("Показать распределение классов"):
        df_classes = load_class_distribution()
        if df_classes.empty:
            st.info("Нет данных для построения распределения.")
        else:
            st.bar_chart(df_classes.set_index("fraud_flag"))
