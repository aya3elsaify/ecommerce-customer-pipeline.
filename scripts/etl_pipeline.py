import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

print("--- [INFO] E-Commerce Data Engineering Pipeline Initiated ---")

# التأكد من وجود مجلد البيانات
os.makedirs('data', exist_ok=True)

# 1. محاكاة واستخراج البيانات الخام (Bronze Layer)
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

# حفظ طبقة Bronze
df_customers.to_csv('data/customers_bronze.csv', index=False)
df_orders.to_csv('data/orders_bronze.csv', index=False)
print("[OK] Bronze Layer: Raw data generated successfully.")

# 2. تنظيف ومعالجة البيانات (Silver Layer)
df_orders_clean = df_orders.drop_duplicates().dropna()
df_orders_clean = df_orders_clean[df_orders_clean['order_amount'] > 0]
df_orders_clean['order_date'] = pd.to_datetime(df_orders_clean['order_date'])

df_orders_clean.to_csv('data/orders_silver.csv', index=False)
print("[OK] Silver Layer: Data cleaned and validated successfully.")

# 3. التحليل المتقدم وتجزئة العملاء (Gold Layer - RFM Analysis)
snapshot_date = df_orders_clean['order_date'].max() + timedelta(days=1)

rfm = df_orders_clean.groupby('customer_id').agg({
    'order_date': lambda x: (snapshot_date - x.max()).days, # Recency
    'order_id': 'count',                                   # Frequency
    'order_amount': 'sum'                                  # Monetary
}).reset_index()

rfm.columns = ['customer_id', 'Recency', 'Frequency', 'Monetary']

# تصنيف العملاء بناءً على الـ RFM
def rfm_segment(row):
    if row['Monetary'] > 500 and row['Frequency'] > 5:
        return 'VIP Customer'
    elif row['Recency'] > 90:
        return 'At-Risk (Churned)'
    else:
        return 'Regular Customer'

rfm['Customer_Segment'] = rfm.apply(rfm_segment, axis=1)

# حفظ طبقة الـ Gold النهائية
rfm.to_csv('data/customers_rfm_gold.csv', index=False)
print("[OK] Gold Layer: RFM Customer Segmentation completed successfully.")
print("--- [INFO] Pipeline Execution Finished Successfully ---")