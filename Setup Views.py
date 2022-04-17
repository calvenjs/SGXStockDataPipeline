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


def setupviewDataset():
    '''
    Sets up the views dataset
    '''
    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(project_id  + ".Views" )
    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = "asia-southeast1"
    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

def setupViews():
    '''
    Sets up the respective views
    '''
    #Signalbuy
    view_id = project_id + ".Views." + "SignalBuy"
    source_id = "`" + project_id + ".Market.StockPrice`"
    view = bigquery.Table(view_id)
    view.view_query = f"SELECT s.Date, S.Stock, S.Ticker, Round(S.Close,2) as Close_Price_SGD, S.Signal FROM {source_id} as s where signal = 'Buy' and CAST(DATE as Date) = CURRENT_DATE()"
    view = client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")
    
    #SignalSell
    view_id = project_id + ".Views." + "SignalSell"
    stock_source_id = "`" + project_id + ".Market.StockPrice`"
    market_source_id = "`" + project_id + ".Accounting.Position`"
    view = bigquery.Table(view_id)
    view.view_query = f"""
    SELECT p.Date,  p.Ticker, p.Stock, p.Avg_Price, p.Share, p.Cost, p.Value, p.Return, sp.MA_5days, sp.signal FROM {stock_source_id} sp inner join {market_source_id} p
    on p.Ticker = sp.Ticker where sp.signal = "Sell" and CAST(sp.DATE as Date) = CURRENT_DATE()
    """
    view = client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")

    #FinancialNews
    view_id = project_id + ".Views." + "FinancialNews"
    source_id = "`" + project_id + ".Reference.Financial news`"
    view = bigquery.Table(view_id)
    view.view_query = f"SELECT * from {source_id}"
    view = client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")

    #Profit
    view_id = project_id + ".Views." + "Profit"
    source_id = "`" + project_id + ".Accounting.Position`"
    view = bigquery.Table(view_id)
    view.view_query = f"SELECT sum(Return) as Profit FROM {source_id}"
    view = client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")

    #Portfolio
    view_id = project_id + ".Views." + "Portfolio"
    source_id = "`" + project_id + ".Accounting.Position`"
    view = bigquery.Table(view_id)
    view.view_query = f"SELECT * FROM {source_id}"
    view = client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")

    #Latest Financials
    view_id = project_id + ".Views." + "LatestFinancials"
    position_source_id = "`" + project_id + ".Accounting.Position`"
    financials_source_id = "`" + project_id + ".Market.Financials`"
    view = bigquery.Table(view_id)
    view.view_query = f"""
    SELECT f.Date, f.Ticker, f.Gross_Profits	,f.Total_Debt, f.Total_Cashflow, f.Total_Revenue, f.Net_Income, f.Return_On_Equity, f.Book_per_Share, FROM 
    {financials_source_id} f INNER JOIN 
    (SELECT max(Date) as MaxDate, Company_Name, Ticker, Gross_Profits, Total_Debt, Total_Cashflow, Total_Revenue, Net_Income, Return_On_Equity, Book_per_Share
    FROM  {financials_source_id}
    GROUP BY Company_Name, Ticker,Gross_Profits,Total_Debt, Total_Cashflow, Total_Revenue, Net_Income, Return_On_Equity, Book_per_Share) g 
    on f.Ticker = g.Ticker and f.Date = g.MaxDate
    INNER JOIN {position_source_id} h on f.Ticker = h.Ticker
    """
    view = client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")

    #Dividend
    view_id = project_id + ".Views." + "Dividend"
    position_source_id = "`" + project_id + ".Accounting.Position`"
    dividend_source_id = "`" + project_id + ".Market.Dividend`"
    view = bigquery.Table(view_id)
    view.view_query = f"""
    SELECT  p.Ticker, p.Stock, Round(sum(d.Dividends_Per_Share*p.Share),2) as DividendEarned 
    FROM  
    {position_source_id} p INNER JOIN {dividend_source_id} d 
    ON p.Ticker = d.Ticker 
    where d.EX_Date  < p.Date and p.Type = "Long"
    GROUP BY  p.Ticker, p.Stock
    """
    view = client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")


setupviewDataset()
setupViews()


