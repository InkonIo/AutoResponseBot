FROM python:3.11-slim

WORKDIR /app

# Установка зависимостей для компиляции
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем requirements и устанавливаем
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY bot.py .

# Запуск
CMD ["python", "bot.py"]