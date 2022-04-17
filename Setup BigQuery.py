from google.cloud import bigquery
import os
import json

#Get Project ID
openfile=open('key.json')
jsondata=json.load(openfile)
openfile.close()
project_id = jsondata['project_id']

# Construct a BigQuery client object.
credentials_path = 'key.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
client = bigquery.Client()

# TODO(developer): Set dataset_id to the ID of the dataset to create.
# dataset_id = "{}.your_dataset".format(client.project)


def setupDataset():
    '''
    Sets up the dataset
    '''
    dataset = ["Accounting", "Accounting_Staging", "Market", "Market_Raw", "Market_Staging", "Reference", "Reference_Staging"]

    for d in dataset:
        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(project_id  + "." + d)
        # TODO(developer): Specify the geographic location where the dataset should reside.
        dataset.location = "asia-southeast1"
        # Send the dataset to the API for creation, with an explicit timeout.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

def setupTable():
    '''
    Sets up the respective tables
    '''
    #Setup Dividend Table
    schema = [
        bigquery.SchemaField("EX_Date", "TIMESTAMP"),
        bigquery.SchemaField("Stock", "STRING"),
        bigquery.SchemaField("Ticker", "STRING"),
        bigquery.SchemaField("Dividends_Per_Share", "FLOAT"),	
    ]
    table = bigquery.Table(project_id + "." + "Market.Dividend", schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

    #Setup Stock Table
    schema = [
        bigquery.SchemaField("Date", "TIMESTAMP"),
        bigquery.SchemaField("Stock", "STRING"),
        bigquery.SchemaField("Ticker", "STRING"),
        bigquery.SchemaField("Open", "FLOAT"),
        bigquery.SchemaField("High", "FLOAT"),
        bigquery.SchemaField("Low", "FLOAT"),
        bigquery.SchemaField("Close", "FLOAT"),
        bigquery.SchemaField("Adj_Close", "FLOAT"),
        bigquery.SchemaField("Volume", "INTEGER"),
        bigquery.SchemaField("MA_5days", "FLOAT"),
        bigquery.SchemaField("Signal", "STRING")
    ]
    table = bigquery.Table(project_id + "." + "Market.StockPrice", schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    #Setup Position Table
    schema = [
        bigquery.SchemaField("Date", "TIMESTAMP"),
        bigquery.SchemaField("Stock", "STRING"),
        bigquery.SchemaField("Ticker", "STRING"),
        bigquery.SchemaField("Avg_Price", "FLOAT"),
        bigquery.SchemaField("Share", "FLOAT"),
        bigquery.SchemaField("Cost", "FLOAT"),
        bigquery.SchemaField("Value", "FLOAT"),
        bigquery.SchemaField("Return", "FLOAT"),
	bigquery.SchemaField("Type", "STRING"),
    ]
    table = bigquery.Table(project_id + "." + "Accounting.Position", schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    ) 
    #Setup Financial Data
    schema = [
        bigquery.SchemaField("Company_Name", "STRING"),
        bigquery.SchemaField("Ticker", "STRING"),
        bigquery.SchemaField("Gross_Profits", "FLOAT"),
        bigquery.SchemaField("Total_Debt", "FLOAT"),
        bigquery.SchemaField("Total_Cashflow", "FLOAT"),
        bigquery.SchemaField("Total_Revenue", "FLOAT"),
        bigquery.SchemaField("Net_Income", "FLOAT"),
        bigquery.SchemaField("Return_On_Equity", "FLOAT"),
        bigquery.SchemaField("Book_per_Share", "FLOAT"),
        bigquery.SchemaField("Date", "TIMESTAMP")
    ]
    table = bigquery.Table(project_id + "." + "Market.Financials", schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    #Setup Financial_News Data
    schema = [
            bigquery.SchemaField("Headline", "STRING"),
            bigquery.SchemaField("Date", "TIMESTAMP"),
            bigquery.SchemaField("URL", "STRING"),
            bigquery.SchemaField("Sentiment", "FLOAT"),
    ]
    table = bigquery.Table(project_id + "." + "Reference.Financial news", schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

setupDataset()
setupTable()
  



	


