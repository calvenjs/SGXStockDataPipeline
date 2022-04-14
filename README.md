# SGX Stock Data Pipeline
This repo shows our project on STI constituents data pipeline.

### Objective
The project will extract data from interal sources provided in a flat file and external sources like YahooFinance API and scrapes news from MarketWatch. Then, the data would be processed by batch and stored to the data warehouse periodically (daily). The batch job will also apply transformations, and create the desired tables to load into the central repository for users to access.

### Consumers
- Portfolio Managers
- Quantitative Developers

### Tools & Technologies
- Cloud: Google Cloud Platform
- Orchestrator: Apache Airflow
- Data Warehouse: Google BigQuery
- Language: Python

### Architecture
![Pipeline](https://github.com/calvenjs/SGXStockDataPipeline/blob/main/images/pipeline_architecture.JPG)


### Airflow DAG
![Airflow Dependency](https://github.com/calvenjs/SGXStockDataPipeline/blob/main/images/airflow_dag.JPG)

## Setup
