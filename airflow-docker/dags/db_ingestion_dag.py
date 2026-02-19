from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="db_ingestion_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["+a"],
) as dag:

    load_products = SQLExecuteQueryOperator(
        task_id="load_products",
        conn_id="postgres-app",
        sql="""
        TRUNCATE TABLE products CASCADE;

        COPY products(
            id,
            title,
            price,
            description,
            category,
            image,
            rating_rate,
            rating_count
        )
        FROM '/opt/airflow/data/landing/products.csv'
        DELIMITER ','
        CSV HEADER;
        """,
    )

    load_carts = SQLExecuteQueryOperator(
        task_id="load_carts",
        conn_id="postgres-app",
        sql="""
        CREATE TEMP TABLE carts_stg (
            id INTEGER,
            userId INTEGER,
            date TEXT,
            products TEXT,
            __v INTEGER
        );

        COPY carts_stg
        FROM '/opt/airflow/data/landing/carts.csv'
        DELIMITER ','
        CSV HEADER
        QUOTE '"';

        TRUNCATE TABLE carts CASCADE;

        INSERT INTO carts (id, user_id, date)
        SELECT
            id,
            userId,
            date::timestamptz
        FROM carts_stg;
        """,
    )

    load_users = SQLExecuteQueryOperator(
        task_id="load_users",
        conn_id="postgres-app",
        sql="""
        CREATE TEMP TABLE users_stg (
            id INTEGER,
            email TEXT,
            username TEXT,
            password TEXT,
            phone TEXT,
            __v INTEGER,
            lat TEXT,
            long TEXT,
            city TEXT,
            street TEXT,
            number INTEGER,
            zipcode TEXT,
            firstname TEXT,
            lastname TEXT
        );

        COPY users_stg
        FROM '/opt/airflow/data/landing/users.csv'
        DELIMITER ','
        CSV HEADER
        QUOTE '"';

        TRUNCATE TABLE users CASCADE;

        INSERT INTO users (
            id,
            email,
            username,
            firstname,
            lastname,
            city,
            street,
            number,
            zipcode,
            phone
        )
        SELECT
            id,
            email,
            username,
            firstname,
            lastname,
            city,
            street,
            number,
            zipcode,
            phone
        FROM users_stg;
        """,
    )

    load_products >> load_users >> load_carts
