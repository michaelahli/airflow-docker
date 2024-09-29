from airflow import DAG
from airflow.providers.apache.hive.operators.hive import HiveOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 27),
    'retries': 1,
}

with DAG(
    dag_id='check_and_insert_customerss',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    check_table_task = HiveOperator(
        task_id='check_customers_table',
        hql="SHOW TABLES;",
        hive_cli_conn_id='hive_operator_conn',
    )
    
    insert_task = HiveOperator(
        task_id='insert_customers_data',
        hql="""
        INSERT INTO customers VALUES 
        (642, 'HDPoKpdL', 'HDPoKpdL@example.com', 'US'), 
        (4754, 'wtUssKpK', 'wtUssKpK@example.com', 'FR'),
        (817, 'rNCoQPXO', 'rNCoQPXO@example.com', 'IN'), 
        (2924, 'wlyJcUoB', 'wlyJcUoB@example.com', 'UK'), 
        (6980, 'qEzTzMaA', 'qEzTzMaA@example.com', 'CN');
        """,
        hive_cli_conn_id='hive_operator_conn',
    )

    check_table_task >> insert_task
