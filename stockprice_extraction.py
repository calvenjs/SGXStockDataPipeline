#from datetime import datetime, timedelta  
#import datetime as dt
#from functools import reduce
import pandas as pd
import yfinance as yf
from google.cloud import bigquery
import os



credentials_path = 'C:/Users/gratz/OneDrive/Desktop/SGXStockDataPipeline/key.json'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path


tickers = ['C31.SI','C52.SI','O39.SI','U96.SI','N2IU.SI','T39.SI','S63.SI','S58.SI','U11.SI'
           ,'C6L.SI','Y92.SI','S68.SI','V03.SI','U14.SI','C61U.SI','Z74.SI','J37.SI','C07.SI'
           ,'F34.SI','C38U.SI','BN4.SI','M44U.SI','G13.SI','A17U.SI','BS6.SI','D05.SI','C09.SI'
           ,'H78.SI','J36.SI','D01.SI'] # STI Top 30 Components

stock = ['C31.SI','ComfortDelGro Corporation Limited','Oversea-Chinese Banking Corporation Limited','Sembcorp Industries Ltd','Mapletree Commercial Trust','Singapore Press Holdings Limited','Singapore Technologies Engineering Ltd','SATS Ltd.','United Overseas Bank Limited'
           ,'Singapore Airlines Limited','Thai Beverage Public Company Limited','Singapore Exchange Limited','Venture Corporation Limited','UOL Group Limited','C61U.SI','Singapore Telecommunications Limited','J37.SI','Jardine Cycle & Carriage Limited'
           ,'Wilmar International Limited','CapitaLand Integrated Commercial Trust','Keppel Corporation Limited','Mapletree Logistics Trust','Genting Singapore Limited','Ascendas Real Estate Investment Trust','Yangzijiang Shipbuilding (Holdings) Ltd.','DBS Group Holdings Ltd','City Developments Limited'
           ,'Hongkong Land Holdings Limited','Jardine Matheson Holdings Limited','Dairy Farm International Holdings Limited']



def fetch_sgx_function(): # <-- Remember to include "**kwargs" in all the defined functions 
    print('1 Fetching stock prices and remove duplicates...')
    ohlcv_daily = pd.DataFrame(columns=["Date", 'Stock', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])
    i = 0
    for ticker in tickers:
        prices = yf.download(ticker, period = '1d').iloc[: , :6].dropna(axis=0, how='any')
        prices = prices.loc[~prices.index.duplicated(keep='last')]
        prices = prices.reset_index()
        prices.insert(loc = 1, column = 'Ticker', value = ticker)
        prices.insert(loc = 1, column = 'Stock', value = stock[i])
        i += 1
        data = pd.DataFrame(prices)
        ohlcv_daily = ohlcv_daily.append(data, ignore_index=True)
    ohlcv_daily = ohlcv_daily.rename(columns={'Adj Close': 'Adj_Close'})
    return ohlcv_daily  # <-- This list is the output of the fetch_prices_function and the input for the functions below

    
def fetch_sgx_dividend(): # <-- Remember to include "**kwargs" in all the defined functions 
    dividend_quarterly = pd.DataFrame(columns=["Date", 'Stock', 'Ticker', 'Dividends'])   
    i = 0
    for ticker in tickers:
        dividend = yf.Ticker(ticker).dividends
        if len(dividend) > 0:
            dividend = pd.DataFrame({'Date':dividend.index, 'Dividends':dividend.values})
            dividend = dividend[(dividend['Date'] > "2022-01-01") & (dividend['Date'] < "2022-04-01")]
            dividend.insert(loc = 1, column = 'Ticker', value = ticker)
            dividend.insert(loc = 1, column = 'Stock', value = stock[i])
            dividend_quarterly  =  dividend_quarterly.append(dividend, ignore_index=True)
        i += 1
    print(dividend_quarterly)
    dividend_quarterly.to_csv("aa.csv")
    return dividend_quarterly 
    
        
 
def stocks_plot_function(**kwargs): 
    print('2 Pulling stocks_prices to concatenate sub-lists to create a combined dataset + write to CSV file...')
    ti = kwargs['Ticker']
    stocks_prices = ti.xcom_pull(task_ids='fetch_prices_task') # <-- xcom_pull is used to pull the stocks_prices list generated above
    stock_plots_data = pd.concat(stocks_prices, ignore_index = True)
    stock_plots_data.to_csv('/Users/yuting/airflow/stocks_plots_data.csv', index = False)
    
    print('DF Shape: ', stock_plots_data.shape)
    print(stock_plots_data.head(5))
    print('Completed \n\n')


def stocks_table_function(**kwargs):
    print('3 Creating aggregated dataframe with stock stats for last available date + write to CSV file...')
    ti = kwargs['ti']
    stocks_prices = ti.xcom_pull(task_ids='fetch_prices_task') # <-- xcom_pull is used to pull the stocks_prices list generated above
    stocks_adj_close = []
    for i in range(0, len(stocks_prices)):
        adj_price= stocks_prices[i][['Date','Adj Close']]
        adj_price.set_index('Date', inplace = True)
        adj_price.columns = [tickers[i]]
        stocks_adj_close.append(adj_price)

    stocks_adj_close = reduce(lambda left,right: pd.merge(left, right, left_index = True, right_index = True ,how='outer'), stocks_adj_close)
    stocks_adj_close.sort_index(ascending = False, inplace = True)
    stocks_adj_close.index = pd.to_datetime(stocks_adj_close.index).date

    stocks_adj_close_f = stocks_adj_close.iloc[0] # <- creates a copy of the full df including last row only
    stocks_adj_close_f = stocks_adj_close_f.reset_index() # <- removing the index transforms the pd.Series into pd.DataFrame
    stocks_adj_close_f.insert(loc = 1, column = 'Date', value = stocks_adj_close_f.columns[1])
    stocks_adj_close_f.columns = ['Symbol', 'Date' , 'Adj. Price']
    stocks_adj_close_f.set_index('Symbol', inplace = True)

    #######################################


df = fetch_sgx_function()
print(df)



client = bigquery.Client()
table_id = "bustling-brand-344211.IS3107Project.SGX_Daily "

job_config = bigquery.LoadJobConfig(schema=[
    bigquery.SchemaField("Stock", "STRING"),
    bigquery.SchemaField("Ticker", "STRING"),
])

job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)

job.result()

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)

