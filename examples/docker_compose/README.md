# YDB Federated Query dockerized setup

Run at least three containers (the YDB itself, the connector and at least one external datasource - PostgreSQL in this case):

```bash
docker compose up -d ydb fq-connector-go postgresql
```

Enter the PostgreSQL container:
```bash
docker compose exec -it postgresql psql -d fq -U admin
```

Initialize PostgreSQL with some table:
```
CREATE TABLE example (
    id int,
    col_01_int int,
    col_02_text text
);

INSERT INTO example (id, col_01_int, col_02_text) VALUES 
    (1, 10, 'a'), 
    (2, 20, 'b'), 
    (3, 30, 'c'),
    (4, NULL, NULL);
```

Visit http://localhost:8765 in your browser. Click: `Databases` -> `/local`, and you'll see a query editor. Enter the following queries:

```sql
CREATE OBJECT postgresql_datasource_user_password (TYPE SECRET) WITH (value = "password");
```

```sql
CREATE EXTERNAL DATA SOURCE postgresql_datasource WITH (
    SOURCE_TYPE="PostgreSQL",
    LOCATION="postgresql:5432",
    DATABASE_NAME="fq",
    AUTH_METHOD="BASIC",
    LOGIN="admin",
    PASSWORD_SECRET_NAME="postgresql_datasource_user_password",
    PROTOCOL="NATIVE",
    USE_TLS="FALSE",
    SCHEMA="public"
);
```

Finally, you'll be able to query the external table:
```sql
SELECT * FROM postgresql_datasource.example;
```