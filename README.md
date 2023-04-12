# SGX Stock Data Pipeline

### Objective
The high-level objective of the data pipeline is to process financial data to support the firm’s 
investment process. The data pipeline begins with the ingestion of data from various sources, 
followed by the transformation of that information into actionable insights, and ultimately 
displaying them to investment professionals’ use to monetize ideas.

### Tools & Technologies
- Cloud - Google Cloud Platform
- Storage - [**BigQuery**](https://cloud.google.com/bigquery)
- Orchestration - [**Airflow**](https://airflow.apache.org)
- Language - [**Python**](https://www.python.org)

### Consumers
- Portfolio Managers
- Quantitative Developers

### Data
- STI Components Daily OHLCV
- STI Components Company Financials
- STI Components Company Dividends
- MarketWatch News Headlines
- Portfolio Positions CSV File

### Data Pipeline Architecture
![Pipeline](https://github.com/calvenjs/SGXStockDataPipeline/blob/main/images/pipeline_architecture.JPG)

### Data Warehouse Architecture
![Warehouse](https://github.com/calvenjs/SGXStockDataPipeline/blob/main/images/warehouse_architecture.JPG)

### Airflow DAG
![Airflow Dependency](https://github.com/calvenjs/SGXStockDataPipeline/blob/main/images/airflow_dag.JPG)

### Setup
[Setup Full Guide](https://docs.google.com/document/d/16P-mJLdfNc8EPg8126i3VaNIoV9kEAhqdbCzCAXj6LU/edit)
