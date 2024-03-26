import time
import logging
import requests
import argparse
import numpy as np
import unicodedata
import datetime as dt
from bs4 import BeautifulSoup as bs

from stocks import stocks, stocks_test
from mail import Mail

logging.basicConfig(filename='bourso_news_logs.log', filemode='w', encoding='utf-8', level=logging.DEBUG)


def extract_datetime(st) -> dt.date:
    date_, time_ = st[0].text.split("."), st[1].text.split(":")
    return dt.datetime(int(date_[2]), int(date_[1]), int(date_[0]), int(time_[0]), int(time_[1]))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n_hours", type=int, default=14)
    ap.add_argument("--test", type=bool, default=False)
    argv = ap.parse_args()

    if argv.test:
            stocks = stocks_test
    recency_duration = argv.n_hours * 3600
    news_list = []
    current_time = time.strftime("%H:%M:%S", time.localtime())

    with open('/Users/bobstomach/Documents/Dev/bourse/recent_news.txt', 'a') as f:
        f.write(f"{dt.date.today().isoformat()} - {current_time}")
        f.write('\n')

    for ticker, name in stocks.items():
        try:
            url = f"https://www.boursorama.com/cours/actualites/1rP{ticker}/"
            if ticker in ["ALCLS", "ALGEN"]:
                url = f"https://www.boursorama.com/cours/actualites/1rEP{ticker}/"
            r = requests.get(url)
            soup = bs(r.text, 'html.parser')
            news = soup.find_all(attrs={"class": "c-list-details-news__author"})
            if news:
                last_news = news[0]
            if last_news:
                date_time = last_news.find_all(attrs={"class": "c-source__time"})
                last_news_title = soup.find_all(attrs={"class": "c-list-details-news__title"})
                if last_news_title:
                    last_news_title = last_news_title[0].text[1:-1]
                    last_news_title = unicodedata.normalize('NFKD', last_news_title).encode('ascii', 'ignore').decode()
            else:
                continue
            date_time = extract_datetime(date_time)
            
            delay = (dt.datetime.now() - date_time).days * 24 * 3600 + (dt.datetime.now() - date_time).seconds
            if delay < recency_duration:
                print(f"Recent news for {name} ({ticker})")
                with open('/Users/bobstomach/Documents/Dev/bourse/recent_news.txt', 'a') as f:
                    f.write(f"Recent news for {name} ({ticker}): {last_news_title}")
                    f.write('\n')
                news_list.append((ticker, name, last_news_title))
            time.sleep(10 * np.random.random(1)[0])
            
        except Exception:
            print(f"Unable to crawl stock {ticker}-{name}")

    logging.debug(f"news_list\n: {news_list}")
    mail = Mail()
    text = "\n".join([
        f"{len(stocks)} actions crawlees\n",
        "\n\n".join([el[0]+" "+el[1]+" "+el[2] for el in news_list])
    ])
    mail.send(["emmanuel.n.pasquet@gmail.com"],
              f"Recent news boursorama {dt.date.today().isoformat()} - {current_time}",
              text)