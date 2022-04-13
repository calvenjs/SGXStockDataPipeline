import datetime
import yfinance as yf
import pandas as pd
from google.cloud import bigquery
import os

# Get company_info for all the STI components and store them into dataframes
def financials_extract(ti):

    # Get tickers for STIcomponets using xcom
    df = ti.xcom_pull(key='STIcomponents', task_ids=['STIExtract'])[0]
    df = pd.DataFrame(eval(df))

    STI_companies = df['Ticker'].tolist()
    # STI_companies = STI_companies[:3] # testing
    company_info = []

    for i in range(len(STI_companies)):
        company = yf.Ticker(STI_companies[i])

        if company.info.get('regularMarketPrice') != None:
            company_info.append(company.info)

    ti.xcom_push(key = 'company info', value = company_info)

# Extract key information or fields from company_info and convert into json format
def financials_transform(ti):
    company_info = ti.xcom_pull(key = 'company info', task_ids=['financialsExtract'])
    numOfCompany = len(company_info[0])

    # Key information to be used
    dic = {'Company_Name': [company_info[0][0]['shortName']], 'Ticker': [company_info[0][0]['symbol']], 'Gross_Profits': [company_info[0][0]['grossProfits']],
           'Total_Debt': [company_info[0][0]['totalDebt']], 'Total_Cashflow': [company_info[0][0]['operatingCashflow']],
           'Total_Revenue': [company_info[0][0]['totalRevenue']], 'Net_Income': [company_info[0][0]['netIncomeToCommon']],
           'Return_On_Equity': [company_info[0][0]['returnOnEquity']], 'Book_per_Share': [company_info[0][0]['bookValue']]}
    
    # Convert into dataframe
    df = pd.DataFrame(dic)
    
    for i in range(1, numOfCompany):
        company = company_info[0][i]
        newDic = {'Company_Name': [company['shortName']], 'Ticker': [company['symbol']], 'Gross_Profits': [company['grossProfits']],
                  'Total_Debt': [company['totalDebt']], 'Total_Cashflow': [company['operatingCashflow']],
                  'Total_Revenue': [company['totalRevenue']], 'Net_Income': [company['netIncomeToCommon']],
                  'Return_On_Equity': [company['returnOnEquity']], 'Book_per_Share': [company['bookValue']]}

        newRow = pd.DataFrame(newDic)
        df = pd.concat([df, newRow], ignore_index=True)

    df = df.fillna(0)
    df['Date'] = df['Company_Name'].apply(lambda x: datetime.datetime.now(datetime.timezone.utc))
    df = df.to_json(orient='records')
    ti.xcom_push(key="processed_user_info", value=df)

# Create financials table in the staging area if not exist, and push the info for each company in the json into the table
def financials_staging(ti):
    results = ti.xcom_pull(task_ids=["financialsTransform"], key="processed_user_info")
    df = pd.DataFrame(eval(results[0]))
    df['Date'] = df['Date'].apply(lambda x: datetime.datetime.fromtimestamp(int(x)/1000))

<<<<<<< Updated upstream
    # Connect to staging area, create staging table if not exist
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
=======
    #Get Project ID
    openfile=open('testkey.json')
    jsondata=json.load(openfile)
    openfile.close()

    #Setup
    credentials_path = 'testkey.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
>>>>>>> Stashed changes
    client = bigquery.Client()

    staging_table_id = jsondata['project_id'] + ".Market_Staging.Financials_Staging"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Company_Name", "STRING"),
            bigquery.SchemaField("Ticker", "STRING"),
            bigquery.SchemaField("Gross_Profits", "FLOAT"),
            bigquery.SchemaField("Total_Debt", "FLOAT"),
            bigquery.SchemaField("Total_Cashflow", "FLOAT"),
            bigquery.SchemaField("Total_Revenue", "FLOAT"),
            bigquery.SchemaField("Net_Income", "FLOAT"),
            bigquery.SchemaField("Return_On_Equity", "FLOAT"),
            bigquery.SchemaField("Book_per_Share", "FLOAT"),
            bigquery.SchemaField("Date", "TIMESTAMP"),
        ]
    )
    load_job = client.load_table_from_dataframe(
        df,
        staging_table_id,
        job_config=job_config,
    )

    load_job.result()  # Waits for the job to complete.

    print('Successfully loaded company financials')

<<<<<<< Updated upstream
# Load information in the staging table into the storage table
=======
>>>>>>> Stashed changes
def financials_load():
    #Get Project ID
    openfile=open('testkey.json')
    jsondata=json.load(openfile)
    openfile.close()
    
    project_id = jsondata['project_id']
    staging_table_id = '`' + project_id + ".Market_Staging.Financials_Staging`"
    actual_table_id = "`" + project_id + ".Market.Financials`"

    #Setup Big Query
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client()
    
    query = f"""
    INSERT INTO {actual_table_id}
    SELECT DISTINCT * FROM  {staging_table_id};
    Delete {staging_table_id} where True;
    """
<<<<<<< Updated upstream

    query_job = client.query(query)
=======
    query_job = client.query(query)
    print('Successfully loaded Financials Details')
>>>>>>> Stashed changes
