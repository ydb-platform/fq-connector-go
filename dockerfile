FROM postgres:latest

ENV POSTGRES_PASSWORD=mysecretpassword

ENV POSTGRES_DB=users

COPY load_data.sql /docker-entrypoint-initdb.d/

CMD ["postgres"]