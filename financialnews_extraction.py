import bs4
import requests
import pandas as pd
from datetime import datetime, timedelta
import json

from google.cloud import bigquery
import os

import nltk
# nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
sid = SentimentIntensityAnalyzer()

def getBacklog():
    url = "https://www.marketwatch.com/investing/index/sti?countrycode=sg"
    headlines = []
    dates = []
    links = []
    sentiment = []

    result = requests.get(url, "html.parser")
    soup = bs4.BeautifulSoup(result.text, "html.parser")
    text = soup.find_all(class_= "element element--article")
    
    for row in text:
        try:
            headline = row.h3.text.strip()
            sentiment_score = sid.polarity_scores(headline)['compound']
            
        except:
            print("headline missing")
            continue
            
        try:
            date = row.find(class_ = "article__timestamp")["data-est"]
        except:
            print("date missing")
            continue
            
        try:
            link = row.find(class_ = "link")["href"]
        except:
            print("link missing")
            continue

        headlines.append(headline)
        sentiment.append(sentiment_score)
        dates.append(datetime.strptime(date[:10],'%Y-%m-%d'))
        links.append(link)

    df = pd.DataFrame({"Headline": headlines, "Date": dates, "URL": links, "Sentiment": sentiment})
    # print(df)

    credentials_path = 'C:/Users/hewtu/Documents/GitHub/SGXStockDataPipeline/key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path

    client = bigquery.Client()
    table_id = "bustling-brand-344211.IS3107Project.News Sentiment"  

    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("Headline", "STRING"),
        bigquery.SchemaField("Date", "TIMESTAMP"),
        bigquery.SchemaField("URL", "STRING"),
        bigquery.SchemaField("Sentiment", "FLOAT"),
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
    
getBacklog()

def extract(ti):
    url = "https://www.marketwatch.com/investing/index/sti?countrycode=sg"

    result = requests.get(url, "html.parser")
    soup = bs4.BeautifulSoup(result.text, "html.parser")
    text = soup.find_all(class_= "element element--article")

    headline_data_link = {"Headline": [], "Date": [], "Link": [], "Sentiment": []}

    for row in text:
        try:
            date = row.find(class_ = "article__timestamp")["data-est"] 
            
        except:
            print('date mising')
            continue

        if datetime.strptime(date[:10],'%Y-%m-%d') == datetime(2022,3,19): #datetime.today() - timedelta(1):
            try:
                headline_data_link['Date'].append(date)
                headline = row.h3.text.strip()
                headline_data_link['Headline'].append(headline)

            except:
                print('headline missing')
            
            try:
                link = row.find(class_ = "link")["href"]
                headline_data_link['Link'].append(link)
            except:
                print('link missing')

    ti.xcom_push(key = 'headline_date_link', value = headline_data_link)

def transform(ti):
    headline_date_link = ti.xcom_pull(key='headline_date_link', task_ids = ['financialNewsExtraction'])[0]

    headlines = headline_date_link['Headline']

    if len(headlines) == 0:
        ti.xcom_push(key = 'no articles', value = True)
        return 
    
    scores = []
    for headline in headlines:
        score = sid.polarity_scores(headline)['compound']
        scores.append(score)

    ti.xcom_push(key = 'scores', value = scores)

def load(ti):
    if ti.xcom_pull(key = 'no articles', task_ids = ['financialNewsExtraction']):
        print('No articles for the day')
        return 
    headline_date_link = ti.xcom_pull(key='headline_date_link', task_ids = ['financialNewsExtraction'])[0]
    scores = ti.xcom_pull(key='scores', task_ids = ['financialNewsTransformation'])[0]
    headline_date_link['Sentiment'].extend(scores)
    df = pd.DataFrame(headline_date_link)

    # print(df)
    # print(headline_date_link)
    
    credentials_path = 'C:/Users/hewtu/Documents/GitHub/SGXStockDataPipeline/key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path

    client = bigquery.Client()
    table_id = "bustling-brand-344211.IS3107Project.News Sentiment"  

    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("Headline", "STRING"),
        bigquery.SchemaField("Date", "TIMESTAMP"),
        bigquery.SchemaField("URL", "STRING"),
        bigquery.SchemaField("Sentiment", "FLOAT"),
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