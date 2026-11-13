from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
import os

def run_bronze_layer():
    os.makedirs('data', exist_ok=True)
    np.random.seed(42)
    n_customers = 500

    customers_data = {
        'customer_id': range(1001, 1001 + n_customers),
        'signup_date': [datetime(2025, 1, 1) + timedelta(days=int(np.random.randint(0, 300))) for _ in range(n_customers)],
        'region': np.random.choice(['Cairo', 'Alexandria', 'Giza', 'Mansoura', 'Aswan'], n_customers)
    }
    df_customers = pd.DataFrame(customers_data)

    n_orders = 2000
    orders_data = {
        'order_id': range(50001, 50001 + n_orders),
        'customer_id': np.random.choice(df_customers['customer_id'], n_orders),
        'order_date': [datetime(2025, 6, 1) + timedelta(days=int(np.random.randint(0, 150))) for _ in range(n_orders)],
        'order_amount': np.random.exponential(scale=150.0, size=n_orders).round(2)
    }
    df_orders = pd.DataFrame(orders_data)

    df_customers.to_csv('data/customers_bronze.csv', index=False)
    df_orders.to_csv('data/orders_bronze.csv', index=False)
    print("Bronze Layer Executed Successfully.")

def run_silver_layer():
    df_orders = pd.read_csv('data/orders_bronze.csv')
    df_orders_clean = df_orders.drop_duplicates().dropna()
    df_orders_clean = df_orders_clean[df_orders_clean['order_amount'] > 0]
    df_orders_clean.to_csv('data/orders_silver.csv', index=False)
    print("Silver Layer Executed Successfully.")

def run_gold_layer():
    df_orders_clean = pd.read_csv('data/orders_silver.csv')
    df_orders_clean['order_date'] = pd.to_datetime(df_orders_clean['order_date'])
    snapshot_date = df_orders_clean['order_date'].max() + timedelta(days=1)

    rfm = df_orders_clean.groupby('customer_id').agg({
        'order_date': lambda x: (snapshot_date - pd.to_datetime(x).max()).days,
        'order_id': 'count',
        'order_amount': 'sum'
    }).reset_index()

    rfm.columns = ['customer_id', 'Recency', 'Frequency', 'Monetary']

    def rfm_segment(row):
        if row['Monetary'] > 500 and row['Frequency'] > 5:
            return 'VIP Customer'
        elif row['Recency'] > 90:
            return 'At-Risk (Churned)'
        else:
            return 'Regular Customer'

    rfm['Customer_Segment'] = rfm.apply(rfm_segment, axis=1)
    rfm.to_csv('data/customers_rfm_gold.csv', index=False)
    print("Gold Layer Executed Successfully.")

default_args = {
    'owner': 'aya_elsaify',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_customer_pipeline_dag',
    default_args=default_args,
    description='A professional E-commerce ETL and RFM pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='bronze_ingestion_task',
        python_callable=run_bronze_layer,
    )

    t2 = PythonOperator(
        task_id='silver_cleaning_task',
        python_callable=run_silver_layer,
    )

    t3 = PythonOperator(
        task_id='gold_rfm_segmentation_task',
        python_callable=run_gold_layer,
    )

    t1 >> t2 >> t3