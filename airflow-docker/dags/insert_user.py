from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id="insert_user_postgres_app",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["+a"],
) as dag:

    insert_user = SQLExecuteQueryOperator(
        task_id="insert_user",
        conn_id="postgres-app",   # mudou aqui
        sql="""
        INSERT INTO users (
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
        VALUES (
            'john.doe@email.com',
            'johndoe',
            'John',
            'Doe',
            'Sao Paulo',
            'Av Paulista',
            1000,
            '01310-100',
            '+5511999999999'
        )
        ON CONFLICT (email) DO NOTHING;
        """,
    )