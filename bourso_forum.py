import os
import re
import time
import logging
import argparse
import requests
import unicodedata
import numpy as np
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup as bs

from mail import Mail
from logger import log
from stocks import stocks, stocks_test


DIR_PATH = "/Users/bobstomach/Documents/Dev/boursobot/"
NOW = dt.datetime.now()
TODAY = NOW.date()
CURRENT_YEAR, CURRENT_MONTH = TODAY.year, TODAY.month
CURRENT_HOUR, CURRENT_MINUTE = NOW.hour, NOW.minute
TOPICS_HTML_CLASS_NAME = "c-table__cell c-table__cell--v-medium c-table__cell--dotted c-table__cell--wrap"
LAST_ANSWER_HTML_CLASS_NAME = "c-table__cell c-table__cell--v-medium c-table__cell--dotted c-table__cell--wrap c-my-list__title u-text-left"
ANSWERS_NUMBER_HTML_CLASS_NAME = "c-table__cell c-table__cell--v-medium c-table__cell--dotted c-table__cell--wrap u-text-right c-table__comments"
CLOSE_HTML_CLASS_NAME = "c-instrument c-instrument--last"
PRE_OPEN_HTML_CLASS_NAME = "c-faceplate__indicative-value"
MONTHS_CONVERTER = {
    "janv": 1,
    "févr": 2,
    "mars": 3,
    "avr": 4,
    "mai": 5,
    "juin": 6,
    "juil": 7,
    "août": 8,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "déc": 12
}
N_POSTS_MULTIPLIER = 1.1
PRE_OUV_MULTIPLIER_HIGH = 1.1
PRE_OUV_MULTIPLIER_LOW = 0.9
LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING
}
PREOPEN_HOUR = 8


def download_soup(ticker):
    url = f"https://www.boursorama.com/bourse/forum/1rP{ticker}/"
    if ticker in ["ALCLS", "ALGEN"]:
        url = f"https://www.boursorama.com/bourse/forum/1rEP{ticker}/"
    r = requests.get(url)
    soup = bs(r.text, 'html.parser')
    return soup

def get_datetime_from_topic(topic):
    ls = topic.text.split("•")
    logging.debug(ls)
    if len(ls) <= 1:
        raise Exception("Date format in bourso topic not supported")
    if len(ls) >= 3:
        ls = ls[:3]
    if len(ls) == 2:
        time_ = ls[1].strip()
        hour, min = int(time_.split(":")[0]), int(time_.split(":")[1])
        return dt.datetime(TODAY.year, TODAY.month, TODAY.day, hour, min, 0)
    if len(ls) == 3:
        date_, time_ = ls[1].strip().split(), ls[2].strip()
        day_of_month, month, year = int(date_[0]), MONTHS_CONVERTER[date_[1].split(".")[0]], int(date_[2])
        hour, min = int(time_.split(":")[0].strip()), int(time_.split(":")[1].strip())
        return dt.datetime(year, month, day_of_month, hour, min, 0)

def get_datetime_from_last_answer(last_answer):
    ls = last_answer.text.split(" ")
    logging.debug(ls)
    if ls[0][-4:] == "par\n":
        logging.debug("auj")
        day_of_month, month, time_ = TODAY.day, TODAY.month, ls[0][:-4]
    elif ls[2][-4:] == "par\n":
        logging.debug("jour précédent")
        day_of_month, month, time_ = int(ls[0]), MONTHS_CONVERTER[ls[1].split(".")[0]], ls[2][:-4]
    else:
        raise NotImplementedError(f"this last answer's date and time case is not covered : {ls}")
    hours, min = int(time_.split(":")[0].strip()), int(time_.split(":")[1].strip())
    year = CURRENT_YEAR
    if month > CURRENT_MONTH:
        year -= 1
    return dt.datetime(year, month, day_of_month, hours, min, 0)

def extract_topics(soup):
    topics = soup.find_all(attrs={"class": TOPICS_HTML_CLASS_NAME})
    subject_datetimes_list = [get_datetime_from_topic(topic) for topic in topics]
    return subject_datetimes_list

def extract_last_answers_of_topics(soup):
    last_answers = soup.find_all(attrs={"class": LAST_ANSWER_HTML_CLASS_NAME})
    last_answer_datetimes_list = [get_datetime_from_last_answer(last_answer) for last_answer in last_answers]
    return last_answer_datetimes_list

def extract_answers_numbers_of_topics(soup):
    answers_numbers = soup.find_all(attrs={"class": ANSWERS_NUMBER_HTML_CLASS_NAME})
    answers_number_list = [int(re.findall("[0-9]+", answers_number.text)[0]) for answers_number in answers_numbers]
    return answers_number_list

def extract_all(soup, name):
    subject_datetimes_list = extract_topics(soup)
    last_answer_datetimes_list = extract_last_answers_of_topics(soup)
    answers_number_list = extract_answers_numbers_of_topics(soup)
    if not (len(answers_number_list) == len(subject_datetimes_list)):
        raise Exception(f"Distinct number of elements found when parsing {name}'s forum")
    if not (len(answers_number_list) == len(last_answer_datetimes_list)):
        raise Exception(f"Distinct number of elements found when parsing {name}'s forum")
    return subject_datetimes_list, last_answer_datetimes_list, answers_number_list

def lists_to_df(topic_datetimes_list, answers_number_list, last_answer_datetimes_list):
    df = pd.DataFrame({
        "topic_datetime": topic_datetimes_list,
        "last_answer_datetime": last_answer_datetimes_list,
        "n_answers": answers_number_list,
    })
    df["topic_year"] = df.topic_datetime.dt.year
    df["topic_month"] = df.topic_datetime.dt.month
    df["topic_day"] = df.topic_datetime.dt.day
    df["topic_hour"] = df.topic_datetime.dt.hour
    df["topic_minute"] = df.topic_datetime.dt.minute
    df["topic_date"] = df.topic_datetime.dt.date
    df["is_topic_created_today"] = (df.topic_date == TODAY)
    df["last_answer_year"] = df.last_answer_datetime.dt.year
    df["last_answer_month"] = df.last_answer_datetime.dt.month
    df["last_answer_day"] = df.last_answer_datetime.dt.day
    df["last_answer_hour"] = df.last_answer_datetime.dt.hour
    df["last_answer_minute"] = df.last_answer_datetime.dt.minute
    df["last_answer_date"] = df.last_answer_datetime.dt.date
    df["is_topic_answered_today"] = (df.last_answer_date == TODAY)
    return df

def get_test_dir_suffix(test):
    return {True: "_test", False: ""}.get(test)

def bourso_forum_posts_count_filename(ticker, test=False):
    test_dir_suffix = get_test_dir_suffix(test)
    path = os.path.join(DIR_PATH, f"data{test_dir_suffix}/csv/boursorama_forum_posts_count/boursorama_forum_posts_count_{ticker}")
    logging.debug(f"bourso_forum_posts_count_filename for {ticker} is {path}")
    return path

@log
def load_forum_history(ticker, test=False):
    filename = bourso_forum_posts_count_filename(ticker, test)
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        return pd.DataFrame()

@log
def compute_store_results(df, ticker, test=False):
    res = pd.concat([
            load_forum_history(ticker, test),
            pd.DataFrame({
                "date": [TODAY],
                "hour": [CURRENT_HOUR],
                "minute": [CURRENT_MINUTE],
                "n_new_topics": [df.query("is_topic_created_today").shape[0]],
                "n_new_topics_answers": [df.query("is_topic_created_today").n_answers.sum()],
                "n_topics_answered_today": [df.query("is_topic_answered_today").shape[0]],
                "n_posts": [df.n_answers.sum()],
            })
        ])
    res["date"] = pd.to_datetime(res["date"])
    res.drop_duplicates(subset=["date", "hour"], keep='first', inplace=True)
    res.to_csv(bourso_forum_posts_count_filename(ticker, test), index=False)

@log
def count_posts_for_timeslot(ticker):
    dg = load_forum_history(ticker)
    dg["date"] = pd.to_datetime(dg["date"])
    dg = dg[dg["hour"] == CURRENT_HOUR]
    n_posts_avg = dg[dg["date"] >= pd.to_datetime((TODAY - dt.timedelta(days=60)))]["n_posts"].mean()
    logging.debug(f"n_posts_avg - {n_posts_avg}")
    n_posts = dg.loc[dg["date"] >= pd.to_datetime(TODAY), "n_posts"].mean() #should be one single value. "mean" used for stability
    logging.debug(f"n_posts_avg - {n_posts_avg}")
    return n_posts_avg, n_posts

@log
def find_close_preouv(soup, ticker):
    if CURRENT_HOUR == PREOPEN_HOUR:
        try:
            close = float(soup.find_all(attrs={"class": CLOSE_HTML_CLASS_NAME})[0].text)
            pre_ouv = float(soup.find_all(attrs={"class": PRE_OPEN_HTML_CLASS_NAME})[0].text)
            return close, pre_ouv
        except Exception:
            logging.warning(f"Failed find_close_preouv on stock {ticker}")
    logging.debug(f"sending nans for {ticker}")
    return np.nan, np.nan

def bourso_preopen_filename(ticker, test=False):
    test_dir_suffix = get_test_dir_suffix(test)
    path = os.path.join(DIR_PATH, f"data{test_dir_suffix}/csv/boursorama_preopen/boursorama_preopen_{ticker}.csv")
    logging.debug(f"bourso_preopen_filename for {ticker} is {path}")
    return path

@log
def load_preopen_history(ticker, test=False):
    filename = bourso_preopen_filename(ticker, test)
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        return pd.DataFrame()

@log
def compute_store_preopen_results(ticker, close, pre_ouv, test=False):
    if CURRENT_HOUR == PREOPEN_HOUR:
        res = pd.concat([
                load_preopen_history(ticker, test),
                pd.DataFrame({
                    "date": [TODAY],
                    "hour": [CURRENT_HOUR],
                    "minute": [CURRENT_MINUTE],
                    "previous_close_value": [close],
                    "preopen_value": [pre_ouv],
                })
            ])
        res["date"] = pd.to_datetime(res["date"])
        logging.debug("drop_duplicates")
        res.drop_duplicates(subset=["date", "hour", "minute"], keep='first', inplace=True)
        logging.debug("to_csv")
        res.to_csv(bourso_preopen_filename(ticker), index=False)
    else:
        pass

@log
def send_mail_forum(stocks, alert_list):
    mail = Mail()
    text = "\n".join([
        f"{len(stocks)} actions crawlees",
        *[f"{el[0]} {el[1]}: {el[2]} posts instead of about {el[3]}" for el in alert_list]
    ])
    mail.send(
        ["emmanuel.n.pasquet@gmail.com"],
        f"Forums agites {NOW.isoformat()[:16].replace('T', ' ')}",
        unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode()
    )

@log
def send_mail_preouv(stocks, alert_list):
    if CURRENT_HOUR == PREOPEN_HOUR:
        mail = Mail()
        text = "\n".join([
            f"{len(stocks)} actions crawlees",
            *[f"{el[0]} {el[1]}: facteur {el[3]/el[2] - 1:.1%} entre pre ouv et veille" for el in alert_list]
        ])
        mail.send(
            ["emmanuel.n.pasquet@gmail.com"],
            f"Alertes pre ouvertures {NOW.isoformat()[:16].replace('T', ' ')}",
            unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode()
        )
    else:
        pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", type=bool, default=False)
    ap.add_argument("--loglevel", type=str, default="info")
    argv = ap.parse_args()

    loglevel = LOG_LEVELS.get(argv.loglevel)
    print(loglevel)
    logging.basicConfig(filename=os.path.join(DIR_PATH, f'logs/bourso_forum_logs_{NOW}.log'), filemode='w', encoding='utf-8', level=loglevel)
    if argv.test:
        stocks = stocks_test
    logging.debug(f"loglevel is {loglevel}")
    logging.debug(f"is test mode? {argv.test}")


    alert_list_forum = []
    alert_list_preouv = []
    for ticker, name in stocks.items():
        try:
            logging.debug(f"processing {ticker}")

            # Part 1 : download and store data
            # Part 1.1 : forum messages
            soup = download_soup(ticker)
            subject_datetimes_list, last_answer_datetimes_list, answers_number_list = extract_all(soup, name)
            df = lists_to_df(subject_datetimes_list, answers_number_list, last_answer_datetimes_list)
            compute_store_results(df, ticker, argv.test)
            # Part 1.2 : pre ouv value
            close, pre_ouv = find_close_preouv(soup, ticker)
            compute_store_preopen_results(ticker, close, pre_ouv, argv.test)

            # Part 2 : process and alert
            # Part 2.1 : forum
            n_posts_avg, n_posts = count_posts_for_timeslot(ticker)
            if (n_posts_avg > 0) and (n_posts > N_POSTS_MULTIPLIER * n_posts_avg):
                alert_list_forum.append((ticker, name, n_posts, n_posts_avg))
            # Part 2.2 : pre ouv
            if (pre_ouv/close > PRE_OUV_MULTIPLIER_HIGH) or (pre_ouv/close < PRE_OUV_MULTIPLIER_LOW):
                alert_list_preouv.append((ticker, name, close, pre_ouv))

        except Exception:
            logging.debug(f"Failed processing stock {ticker}")
        
        time.sleep(12 * np.random.random(1)[0])

    logging.debug(f"alert_list_forum\n: {alert_list_forum}")
    logging.debug(f"alert_list_preouv\n: {alert_list_preouv}")
    send_mail_forum(stocks, alert_list_forum)
    send_mail_preouv(stocks, alert_list_forum)
