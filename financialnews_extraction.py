import bs4
import requests
import pandas as pd
import nltk

URL = "https://www.marketwatch.com/investing/index/sti?countrycode=sg"

headlines = []
dates = []
links = []
sentiment = []

nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
sid = SentimentIntensityAnalyzer()


def extract(url):

    result = requests.get(url, "html.parser")
    soup = bs4.BeautifulSoup(result.text, "html.parser")
    text = soup.find_all(class_= "element element--article")
    
    for row in text:
        try:
            headline = row.h3.text.strip()
            headlines.append(headline)
            
            sentiment_score = sid.polarity_scores(headline)['compound']
            sentiment.append(sentiment_score)
            
            
        except:
            print("headline missing")
            headlines.append(None)
            sentiment.append(None)
        
        try:
            date = row.find(class_ = "article__timestamp")["data-est"]
            dates.append(date)
        except:
            print("date missing")
            dates.append(None)
            
        try:
            link = row.find(class_ = "link")["href"]
            links.append(link)
        except:
            print("link missing")
            links.append(None)


extract(URL)


df = pd.DataFrame({"Headline": headlines, "Dates": dates, "Links": links, "Sentiment score": sentiment})

for index, row in df.iterrows():
    print(row['Headline'])
    print(row['Sentiment score'])
    print()