import bs4
import requests
import pandas as pd
from datetime import datetime, timedelta

from google.cloud import bigquery
import os

import nltk
# nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
sid = SentimentIntensityAnalyzer()

def financialnews_backlog():
    url = "https://www.marketwatch.com/investing/index/sti?countrycode=sg"
    headlines = []
    dates = []
    links = []
    sentiment = []

    result = requests.get(url, "html.parser")
    soup = bs4.BeautifulSoup(result.text, "html.parser")
    text = soup.find_all(class_= "element element--article")[0]
    
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

    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    client = bigquery.Client()
    table_id = "bustling-brand-344211.Reference.Financial news"  


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

    print('Successfully loaded stock prices')

    
def financialnews_extract(ti):
    url = "https://www.marketwatch.com/investing/index/sti?countrycode=sg"

    result = requests.get(url, "html.parser")
    soup = bs4.BeautifulSoup(result.text, "html.parser")
    text = soup.find_all(class_= "element element--article")

    headline_data_link = {"Headline": [], "Date": [], "URL": [], "Sentiment": []}

    for row in text:
        try:
            date = row.find(class_ = "article__timestamp")["data-est"] 
            
        except:
            print('date mising')
            continue

        article_date = datetime.strptime(date[:10],'%Y-%m-%d')
        yesterday = datetime.today() - timedelta(1)
        print(yesterday.date())
        print(article_date.date())

        if article_date.date() == yesterday.date():
            print('same date')
            try:
                headline_data_link['Date'].append(date)
                headline = row.h3.text.strip()
                headline_data_link['Headline'].append(headline)

            except:
                print('headline missing')
            
            try:
                link = row.find(class_ = "link")["href"]
                headline_data_link['URL'].append(link)
            except:
                print('link missing')

    ti.xcom_push(key = 'headline_date_link', value = headline_data_link)

def financialnews_transform(ti):
    headline_date_link = ti.xcom_pull(key='headline_date_link', task_ids = ['financialNewsExtract'])[0]

    headlines = headline_date_link['Headline']

    if len(headlines) == 0:
        ti.xcom_push(key = 'no articles', value = True)
        return 
    ti.xcom_push(key = 'no articles', value = False)
    scores = []
    for headline in headlines:
        score = sid.polarity_scores(headline)['compound']
        scores.append(score)

    ti.xcom_push(key = 'scores', value = scores)

def financialnews_load(ti):
    no_articles = ti.xcom_pull(key = 'no articles', task_ids = ['financialNewsTransform'])[0]
    
    if no_articles:
        print('No articles for the day')
        return 

    headline_date_link = ti.xcom_pull(key='headline_date_link', task_ids = ['financialNewsExtract'])[0]
    scores = ti.xcom_pull(key='scores', task_ids = ['financialNewsTransform'])[0]
    scores = [float(x) for x in scores]
    headline_date_link['Sentiment'].extend(scores)
    df = pd.DataFrame(headline_date_link)

    df['Date'] = pd.to_datetime(df['Date'])
    
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path

    client = bigquery.Client()
    table_id = "bustling-brand-344211.Reference.Financial news"  

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

    print('Successfully loaded news headlines')
    