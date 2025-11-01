FROM python:3.12-slim

WORKDIR /app

# создаем нужные паки
RUN mkdir -p /app/logs && \
    touch /app/logs/service.log && \
    chmod -R 777 /app/logs  # Права на запись для всех пользователей

# установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY . .

# запуск
CMD ["python", "-u", "app/app.py"]
