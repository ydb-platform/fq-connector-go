# Используем официальный образ PostgreSQL
FROM postgres

# Копируем скрипт для загрузки данных в контейнер
COPY load_data.sql /docker-entrypoint-initdb.d/

# Этот скрипт будет автоматически выполнен при запуске контейнера