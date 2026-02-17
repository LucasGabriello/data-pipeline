from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.create_csv import create_csv

with DAG(
    dag_id="csv_ingestion_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["api", "+a"],
) as dag:

    get_products = PythonOperator(
        task_id="get_products",
        python_callable=create_csv,
        op_kwargs={
            "endpoint": "https://fakestoreapi.com/products",
            "name": "products",
        },
    )

    get_categories = PythonOperator(
        task_id="get_categories",
        python_callable=create_csv,
        op_kwargs={
            "endpoint": "https://fakestoreapi.com/products/categories",
            "name": "categories",
        },
    )

    get_carts = PythonOperator(
        task_id="get_carts",
        python_callable=create_csv,
        op_kwargs={
            "endpoint": "https://fakestoreapi.com/carts",
            "name": "carts",
        },
    )

    get_users = PythonOperator(
        task_id="get_users",
        python_callable=create_csv,
        op_kwargs={
            "endpoint": "https://fakestoreapi.com/users",
            "name": "users",
        },
    )

    get_products >> get_categories >> get_carts >> get_users