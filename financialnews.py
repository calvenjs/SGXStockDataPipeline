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

def financialnews_backlog():
    # url that we are extracting from 
    url = "https://www.marketwatch.com/investing/index/sti?countrycode=sg"
    
    # initialising lists which will be converted into columns in a pandas dataframe later
    headlines = []
    dates = []
    links = []
    sentiment = []

    # using web scraping to extract the section of the website that contains the news headline
    result = requests.get(url, "html.parser")
    soup = bs4.BeautifulSoup(result.text, "html.parser")
    text = soup.find_all(class_= "element element--article")[0]  # this contains a list of multiple articles
    
    # for loop to iterate through all the articles 
    for row in text:

        # try except blocks in order to prevent missing data being input into the data warehouse
        try:

            # obtaining news headline
            headline = row.h3.text.strip()

            # using NLTK's VADER sentiment analysis library to obtain sentiment score
            sentiment_score = sid.polarity_scores(headline)['compound']
            
        except:
            print("headline missing")
            continue
            
        try:

            # obtaining date of article 
            date = row.find(class_ = "article__timestamp")["data-est"]
        except:
            print("date missing")
            continue
            
        try:

            # obtaining URL of article
            link = row.find(class_ = "link")["href"]

        except:
            print("link missing")
            continue

        # appending various data into the lists initialised earlier 
        headlines.append(headline)
        sentiment.append(sentiment_score)
        dates.append(datetime.strptime(date[:10],'%Y-%m-%d'))  # reformatting the date 
        links.append(link)

    # making new dataframe based on the data 
    df = pd.DataFrame({"Headline": headlines, "Date": dates, "URL": links, "Sentiment": sentiment})

    # setting up credentials for Google BigQuery
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    client = bigquery.Client()
    table_id = "bustling-brand-344211.Reference.Financial news"  

    # defining the bigquery table schema
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("Headline", "STRING"),
        bigquery.SchemaField("Date", "TIMESTAMP"),
        bigquery.SchemaField("URL", "STRING"),
        bigquery.SchemaField("Sentiment", "FLOAT"),
    ])

    # code to load dataframe into Google BigQuery 
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
 
    job.result()

    print('Successfully loaded stock prices')

    
def financialnews_extract(ti):
    # url that we are extracting from 
    url = "https://www.marketwatch.com/investing/index/sti?countrycode=sg"

    # using web scraping to extract the section of the website that contains the news headline
    result = requests.get(url, "html.parser")
    soup = bs4.BeautifulSoup(result.text, "html.parser")
    text = soup.find_all(class_= "element element--article")  # this contains a list of multiple articles

    # setting up dictionary which will be converted into a dataframe later
    headline_data_link = {"Headline": [], "Date": [], "URL": [], "Sentiment": []}

    # for loop to iterate through all the articles 
    for row in text:

        # try except blocks in order to prevent missing data being input into the data warehouse
        try:
            # obtaining date of article
            date = row.find(class_ = "article__timestamp")["data-est"] 
            
        except:
            print('date mising')
            continue

        # reformatting date of article 
        article_date = datetime.strptime(date[:10],'%Y-%m-%d')

        # obtaining yesterday's date
        yesterday = datetime.today() - timedelta(1)
        # yesterday = datetime(2022, 4, 1)  # for testing

        # if article date is the same as yesterday's date, 
        if article_date.date() == yesterday.date(): 

            # extract headline and URL of the article, and add them to the dictionary defined earlier
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

    # push dictionary to xcom 
    ti.xcom_push(key = 'headline_date_link', value = headline_data_link)

def financialnews_transform(ti):

    # retrieve data from xcom 
    headline_date_link = ti.xcom_pull(key='headline_date_link', task_ids = ['financialNewsExtract'])[0]

    headlines = headline_date_link['Headline']

    # if there are no headlines, it means that there are no articles for the day, indicate this in xcom, and stop the transformation process 
    if len(headlines) == 0:
        ti.xcom_push(key = 'no articles', value = True)
        return 

    # if there are deadlines, it means articles are extracted for the day. indicate this in xcom 
    ti.xcom_push(key = 'no articles', value = False)
    scores = []

    # for each headline extracted, 
    for headline in headlines:

        # obtain the sentiment score using NLTK 
        score = sid.polarity_scores(headline)['compound']
        scores.append(score)

    # push the sentiment scores to xcom
    ti.xcom_push(key = 'scores', value = scores)


def financialnews_staging(ti):

    # check if there are articles extracted for the day 
    no_articles = ti.xcom_pull(key = 'no articles', task_ids = ['financialNewsTransform'])[0]
    
    # if there are no articles extracted, print this to the console
    if no_articles:
        print('No articles for the day')
        return 

    # retrieve data from xcom from previous financialNewsExtract and financialNewsTransform
    headline_date_link = ti.xcom_pull(key='headline_date_link', task_ids = ['financialNewsExtract'])[0]
    scores = ti.xcom_pull(key='scores', task_ids = ['financialNewsTransform'])[0]

    # convert scores to float data type
    scores = [float(x) for x in scores]

    # create dataframe of data 
    headline_date_link['Sentiment'].extend(scores)
    df = pd.DataFrame(headline_date_link)

    # converting date column to datetime data type
    df['Date'] = pd.to_datetime(df['Date'])
    
    # setting up credentials for Google BigQuery
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path

    client = bigquery.Client()

    #Get Project ID
    openfile=open('key.json')
    jsondata=json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']
    staging_table_id = project_id + ".Reference_Staging.Financial news staging"

    # defining the bigquery table schema
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("Headline", "STRING"),
        bigquery.SchemaField("Date", "TIMESTAMP"),
        bigquery.SchemaField("URL", "STRING"),
        bigquery.SchemaField("Sentiment", "FLOAT"),
    ])

    # code to load dataframe into Google BigQuery 
    job = client.load_table_from_dataframe(
        df, staging_table_id, job_config=job_config
    )

    job.result()

    print('Successfully loaded news headlines')
    
def financialnews_load():
    #Get Project ID
    openfile=open('key.json')
    jsondata=json.load(openfile)
    openfile.close()
    project_id = jsondata['project_id']
    staging_table_id = '`' + project_id + ".Reference_Staging.Financial news staging`"
    actual_table_id = "`" + project_id + ".Reference.Financial news`"
    
    # setting up credentials for Google BigQuery
    credentials_path = 'key.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= credentials_path
    client = bigquery.Client()  

    query = """
    INSERT INTO {actual_table_id}
    SELECT DISTINCT * FROM  {staging_table_id};
    Delete {staging_table_id} where True;
    """
    query_job = client.query(query)

    print('Successfully loaded news headlines')
    

