from db import DBhelper
from gAPI import GoogleAds
from basic.date import datetime_to_str, curdate
from basic.decorator import timing
import pandas as pd
from definitions import ROOT_DIR
## fetch keywords yesterday + today
@timing
def fetch_new_gtrend_keywords():
    query = f"""
            SELECT DISTINCT
                keyword AS keyword
            FROM
                dione.google_trend_keyword
            WHERE
                keyword NOT IN                 
                (SELECT 
                    keyword_ask
                FROM
                    roas_report.google_ads_metrics_history)
            """
    print(query)
    data = DBhelper("dione").ExecuteSelect(query)
    keyword_list = list(set([d[0] for d in data]))
    return keyword_list

## fetch keywords yesterday + today
@timing
def fetch_new_gconsole_keywords():
    query = f"""
            SELECT DISTINCT
                query AS query
            FROM
                google_search_console_query
            WHERE
                query NOT IN 
                (SELECT 
                    keyword_ask
                FROM
                    google_ads_metrics_history)
            """
    print(query)
    data = DBhelper("roas_report").ExecuteSelect(query)
    keyword_list = list(set([d[0] for d in data]))
    return keyword_list


def save_init_monthly_keyword_metrics(n=300, save=True):
    keyword_list_gtrend = fetch_new_gtrend_keywords()
    keyword_list_gconsole = fetch_new_gconsole_keywords()
    keyword_list = list(set(keyword_list_gtrend + keyword_list_gconsole))[:n]
    df_monthly_keyword_metrics = GoogleAds()._generate_12month_keyword_metrics(keyword_list)
    df_monthly_keyword_metrics = add_unavailable(keyword_list, df_monthly_keyword_metrics)
    if save:
        ## generate sql script by DataFrame
        query_history = DBhelper.generate_insertDup_SQLquery(df_monthly_keyword_metrics, 'google_ads_metrics_history',
                                                        list(df_monthly_keyword_metrics.columns))
        ## save to metrics tables
        DBhelper('roas_report').ExecuteUpdate(query_history, df_monthly_keyword_metrics.to_dict('records'))
    return keyword_list, df_monthly_keyword_metrics

## add unavailable data
def add_unavailable(keyword_list, df_monthly_keyword_metrics):
    add_dict = {}
    today = curdate(utc=8)
    keyword_ask_list = set(df_monthly_keyword_metrics['keyword_ask'])
    for i,keyword in enumerate(keyword_list):
        ## adding those keywords not in google keyword_metrics to prevent repetitively asking
        if keyword not in keyword_ask_list:
            add_dict[i] = {'keyword_ask':keyword, 'keyword_join':keyword.replace(' ',''), 'keyword_google':keyword,
                           'competition_level':'', 'competition_value':0, 'low_price':0, 'high_price':0,
                           'avg_monthly_traffic':0, 'monthly_traffic':0, 'year':today.year, 'month':today.month,
                           'date': datetime_to_str(today), 'google_available':0}
    df_add = pd.DataFrame.from_dict(add_dict, "index")
    df = df_monthly_keyword_metrics.append(df_add).fillna(1)
    return df


def save_latest_month_keyword_metrics(n=50, save=True):
    keyword_list = fetch_update_keywords(n=n)
    df_monthly_keyword_metrics = GoogleAds()._generate_12month_keyword_metrics(keyword_list)
    if df_monthly_keyword_metrics.shape[0] == 0:
        print("Today's budget is ran out")
        return keyword_list, []
    else:
        ## use update on duplicate key (not updating if existing)
        if save:
            query = DBhelper.generate_insertDup_SQLquery(df_monthly_keyword_metrics, 'google_ads_metrics_history',
                                                            ['monthly_traffic'])
            ## save to metrics tables
            DBhelper('roas_report').ExecuteUpdate(query, df_monthly_keyword_metrics.to_dict('records'))
        return keyword_list, df_monthly_keyword_metrics


def fetch_update_keywords(n=50):
    today = curdate(utc=8)
    year = today.year
    month = today.month
    # query = f"""
    #         SELECT
    #             keyword_ask
    #         FROM
    #             roas_report.google_ads_metrics_history
    #         WHERE
    #             year = {year} AND month = {month-2} AND google_available = 1
    #                 AND keyword_ask NOT IN (SELECT
    #                     keyword_ask
    #                 FROM
    #                     google_ads_metrics_history
    #                 WHERE
    #                     year = {year} AND month = {month-1} AND google_available = 1)
    #         ORDER BY monthly_traffic DESC LIMIT {n}
    #         """
    query = f"""
            SELECT 
                distinct keyword_ask
            FROM
                roas_report.google_ads_metrics_history
            WHERE
                date < date_add((curdate()), INTERVAL -3 MONTH) AND google_available = 1
                    AND keyword_ask NOT IN (SELECT 
                        keyword_ask
                    FROM
                        google_ads_metrics_history
                    WHERE
                        year = {year} AND month = {month-2} AND google_available = 1)
            ORDER BY monthly_traffic DESC LIMIT {n}
            """
    print(query)
    data = DBhelper("roas_report").ExecuteSelect(query)
    keyword_list = [d[0] for d in data]
    return keyword_list


## update rate: twice per day (13:30, 23:30)
if __name__ == '__main__':
    ################ init table, google_ads_metrics_history ###################
    keyword_list, df = save_init_monthly_keyword_metrics(n=100, save=True) #100
    ################ update latest month into table, google_ads_metrics_history ###################
    keyword_list_update, df_update = save_latest_month_keyword_metrics(n=260, save=True) #250

