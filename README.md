# E-Commerce 360° Customer Segmentation & Analytics Pipeline (Airflow Orchestrated)

## 📌 Project Overview
An automated, production-ready Data Engineering pipeline orchestrated using **Apache Airflow**. It automates raw data ingestion, cleaning, and advanced RFM (Recency, Frequency, Monetary) customer segmentation across Bronze, Silver, and Gold architectural layers.

## 🛠️ Tech Stack
- **Orchestration:** Apache Airflow (DAGs, PythonOperators)
- **Language:** Python
- **Libraries:** Pandas, NumPy
- **Architecture:** Multi-Layer ETL Pipeline (Bronze -> Silver -> Gold)
- **Version Control:** Git & GitHub

## 🚀 Airflow Pipeline Workflow
1. **Bronze Ingestion Task (`bronze_ingestion_task`):** Simulates and ingests raw operational data.
2. **Silver Cleaning Task (`silver_cleaning_task`):** Cleans, validates, and filters data anomalies.
3. **Gold RFM Task (`gold_rfm_segmentation_task`):** Executes RFM modeling and categorizes customers into strategic segments (VIP, Regular, At-Risk).