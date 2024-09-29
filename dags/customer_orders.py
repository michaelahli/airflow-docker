from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from datetime import datetime
import random
import string
import logging

# Helper functions to generate random data
def generate_random_customer():
    customer_id = random.randint(1, 10000)
    name = ''.join(random.choices(string.ascii_letters, k=8))
    email = f'{name}@example.com'
    country = random.choice(['US', 'UK', 'FR', 'DE', 'IN', 'CN'])
    return customer_id, name, email, country

def generate_random_order(customer_id):
    order_id = random.randint(1, 10000)
    order_date = datetime.now().date()
    total = round(random.uniform(10.0, 500.0), 2)
    return order_id, customer_id, order_date, total

def generate_random_order_item(order_id):
    order_item_id = random.randint(1, 10000)
    product_id = random.randint(1, 1000)
    quantity = random.randint(1, 10)
    price = round(random.uniform(5.0, 100.0), 2)
    return order_item_id, order_id, product_id, quantity, price

# Helper function to format data into HiveQL values
def format_values(data):
    formatted_rows = []
    for row in data:
        formatted_row = []
        for value in row:
            if isinstance(value, str):
                formatted_row.append(f"'{value}'")  # Add single quotes around string values
            else:
                formatted_row.append(str(value))  # Leave other values as-is
        formatted_rows.append(f"({', '.join(formatted_row)})")
    return ', '.join(formatted_rows)

# Python functions to handle tasks
def create_customers(**context):
    customers = [generate_random_customer() for _ in range(5)]
    context['ti'].xcom_push(key='customers', value=customers)

def create_orders(**context):
    customers = context['ti'].xcom_pull(key='customers', task_ids='create_customers')
    if customers:
        orders = [generate_random_order(customer[0]) for customer in customers]
        logging.info(f'Generated orders: {orders}')
        context['ti'].xcom_push(key='orders', value=orders)
    else:
        logging.error('No customers found, unable to create orders')
        raise ValueError('No customers found in XCom')

def create_order_items(**context):
    orders = context['ti'].xcom_pull(key='orders', task_ids='create_orders')
    if orders:
        order_items = [generate_random_order_item(order[0]) for order in orders]
        logging.info(f'Generated order items: {order_items}')
        context['ti'].xcom_push(key='order_items', value=order_items)
    else:
        logging.error('No orders found, unable to create order items')
        raise ValueError('No orders found in XCom')

def verify_uniqueness(**context):
    customers = context['ti'].xcom_pull(key='customers', task_ids='create_customers')
    orders = context['ti'].xcom_pull(key='orders', task_ids='create_orders')
    order_items = context['ti'].xcom_pull(key='order_items', task_ids='create_order_items')
    
    if len(customers) != len(set([customer[0] for customer in customers])):
        raise ValueError('Duplicate customer IDs found!')
    if len(orders) != len(set([order[0] for order in orders])):
        raise ValueError('Duplicate order IDs found!')
    if len(order_items) != len(set([item[0] for item in order_items])):
        raise ValueError('Duplicate order item IDs found!')

def prepare_hive_queries(**context):
    customers = context['ti'].xcom_pull(key='customers', task_ids='create_customers')
    orders = context['ti'].xcom_pull(key='orders', task_ids='create_orders')
    order_items = context['ti'].xcom_pull(key='order_items', task_ids='create_order_items')

    customers_query = f"INSERT INTO customers VALUES {format_values(customers)};"
    orders_query = f"INSERT INTO orders VALUES {format_values(orders)};"
    order_items_query = f"INSERT INTO order_items VALUES {format_values(order_items)};"

    # Push the formatted queries to XCom
    context['ti'].xcom_push(key='customers_query', value=customers_query)
    context['ti'].xcom_push(key='orders_query', value=orders_query)
    context['ti'].xcom_push(key='order_items_query', value=order_items_query)

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 27),
    'retries': 1,
}

# Define the DAG
with DAG(
    'generate_and_insert_data_to_hive',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    create_customers_task = PythonOperator(
        task_id='create_customers',
        python_callable=create_customers
    )

    create_orders_task = PythonOperator(
        task_id='create_orders',
        python_callable=create_orders,
        provide_context=True
    )

    create_order_items_task = PythonOperator(
        task_id='create_order_items',
        python_callable=create_order_items,
        provide_context=True
    )

    verify_uniqueness_task = PythonOperator(
        task_id='verify_uniqueness',
        python_callable=verify_uniqueness,
        provide_context=True
    )

    prepare_hive_queries_task = PythonOperator(
        task_id='prepare_hive_queries',
        python_callable=prepare_hive_queries,
        provide_context=True
    )

    insert_customers_hive_task = HiveOperator(
        task_id='insert_customers_hive',
        hql="{{ ti.xcom_pull(key='customers_query', task_ids='prepare_hive_queries') }}",
        hive_cli_conn_id='hive_operator_conn'
    )

    insert_orders_hive_task = HiveOperator(
        task_id='insert_orders_hive',
        hql="{{ ti.xcom_pull(key='orders_query', task_ids='prepare_hive_queries') }}",
        hive_cli_conn_id='hive_operator_conn'
    )

    insert_order_items_hive_task = HiveOperator(
        task_id='insert_order_items_hive',
        hql="{{ ti.xcom_pull(key='order_items_query', task_ids='prepare_hive_queries') }}",
        hive_cli_conn_id='hive_operator_conn'
    )

    # Task dependencies
    create_customers_task >> create_orders_task >> create_order_items_task
    create_order_items_task >> verify_uniqueness_task
    verify_uniqueness_task >> prepare_hive_queries_task
    prepare_hive_queries_task >> [insert_customers_hive_task, insert_orders_hive_task, insert_order_items_hive_task]
