# Fraud Detection Service (MlOps and ML System Design HW 2)

## Структура проекта

```
HW-2/
├── app/
│   └── app.py              # Основной сервис: слушает Kafka-топик "transactions", выполняет препроцессинг и скоринг
├── src/
│   ├── preprocess.py       # Препроцессинг входных данных
│   └── scorer.py           # Загрузка модели CatBoost и выполнение инференса
├── configs/
│   └── config.yaml         # Конфигурации
├── models/
│   └── model.cbm           # Предобученная CatBoost модель
├── scores_writer/
│   └── app.py              # Сервис БД: читает топик "scoring" и сохраняет результаты в PostgreSQL
├── interface/
│   ├── app.py              # Streamlit UI для загрузки CSV, отправки данных в Kafka и просмотра результатов
│   └── Dockerfile
├── requirements.txt
├── Dockerfile              # Dockerfile для сервиса скоринга
├── docker-compose.yaml     # Полная сборка всех контейнеров
└── README.md
```

---

## Что делает сервис

1. **Streamlit-интерфейс**

   * Пользователь загружает CSV-файл с транзакциями.
   * Данные отправляются в Kafka-топик `transactions`.
   * Отображает:

     * последние 10 фродовых транзакций;
     * гистограмму распределения последних 100 скоров;
     * распределение классов (`fraud_flag` = 0 / 1).

2. **ML-сервис (fraud_detector)**

   * Слушает топик `transactions`.
   * Препроцессит и скорит данные из топика.
   * Записывает результаты (`score`, `fraud_flag`) в топик `scoring`.

3. **Scores-writer**

   * Подписан на топик `scoring`.
   * Сохраняет результаты скоринга в PostgreSQL.

---

## Запуск через Docker

### Собрать и запустить контейнеры

```bash
docker-compose up --build
```

### Открыть интерфейс

**[Streamlit-UI: localhost:8501](http://localhost:8501)**

**[Kafka-UI: localhost:8080](http://localhost:8080)**

## Требования к файлу test.csv

* Должны быть **те же колонки**, что в соревновании [Kaggle](https://www.kaggle.com/competitions/teta-ml-1-2025/overview)
* Формат CSV, разделитель запятая
* Можно взять файл из `/notebooks/data`
