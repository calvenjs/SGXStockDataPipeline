import pandas as pd
import requests

def STIextraction(ti):
    '''
    Retrieves the STI ETF constituents.

    Input: URL 
    Output: STI ETF constituents which include ticker symbol and stock name. This will be pushed to xcom and stored in a CSV. 
    '''
    #Retrieve STI Index (^STI) components
    try:
  
        url = 'https://sginvestors.io/analysts/sti-straits-times-index-constituents-target-price'
        r = requests.get(url, headers ={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'})
        payload=pd.read_html(r.text)
        df = payload[0]
        df = df[['Straits Times Index STI Constituent']]
        df = df.dropna()
        
        tickers = []
        companyNames = []
        #Store STI Components in List
        for index, row in df.iterrows():
            tickers.append(row['Straits Times Index STI Constituent'].split('(')[1].split(')')[0][4:] + '.SI')
            companyNames.append(row['Straits Times Index STI Constituent'].split('(')[0])
            
        #Convert List to Dataframe
        df = pd.DataFrame({'Ticker':tickers, 'Company name': companyNames})
        df.to_csv("STI_components.csv", index=False)
        df = df.to_json(orient='records')
        #Push STI components to pipeline
        ti.xcom_push(key='STIcomponents', value=df)
    except:
        df = pd.read_csv("STI_components.csv")
        df = df.to_json(orient='records')
        ti.xcom_push(key='STIcomponents', value=df)
        
