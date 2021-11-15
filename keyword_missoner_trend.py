import numpy as np
import pandas as pd
import datetime
import time
import socket
import re
from basic.decorator import timing
from jieba_based.utility import Composer_jieba
from db.mysqlhelper import MySqlHelper
from media.Media import Media
from basic.date import get_hour, date2int, get_today, get_yesterday, check_is_UTC0, datetime_to_str, date2int
from keyword_cross_hot import update_cross_keywords

def update_keyword_trend():
    web_id_all = fetch_missoner_web_id()
    is_UTC0 = check_is_UTC0()
    hour_now = get_hour(is_UTC0=is_UTC0)
    # hour_now = 12
    for web_id in web_id_all:
        data = fetch_keyword_pageviews_by_hour(web_id, is_UTC0=is_UTC0)
        data_trend = calculate_diff_trend(data, is_UTC0=is_UTC0)
        data_save = {}
        for i,d in enumerate(data):
            keyword, pageviews, date, hour = d
            trend = data_trend[keyword]
            if hour==hour_now:
                data_save[i] = {'web_id':web_id, 'keyword':keyword, 'pageviews':pageviews,
                                'trend':trend , 'date':date, 'hour':hour}
        df_trend = pd.DataFrame.from_dict(data_save, "index")
        if np.array(df_trend).size == 0:
            print(f"no available data")
            continue
        query = MySqlHelper.generate_update_SQLquery(df_trend, 'missoner_keyword_trend')
        trend_list_dict = df_trend.to_dict('records')
        MySqlHelper('dione', is_ssh=False).ExecuteUpdate(query, trend_list_dict)
    return df_trend

def fetch_keyword_pageviews_by_hour(web_id, is_UTC0=False):
    # is_UTC0 = check_is_UTC0()
    today = date2int(get_today(is_UTC0=is_UTC0))
    hour_now = get_hour(is_UTC0=is_UTC0)
    # hour_now = 12
    hour_last = hour_now - 1
    query = f"SELECT keyword, pageviews, date, hour FROM missoner_keyword_trend where web_id='{web_id}' and date='{today}' and hour between {hour_last} and {hour_now}"
    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    # columns = ['keyword', 'pageviews', 'hour']
    # df_pageviews = pd.DataFrame(data=data, columns=columns)
    return data

def calculate_diff_trend(data, is_UTC0=False):
    # is_UTC0 = check_is_UTC0()
    hour_now = get_hour(is_UTC0=is_UTC0)
    data_trend = {}
    # hour_last = hour_now - 1
    for d in data:
        keyword, pageviews, date, hour = d
        if keyword not in data_trend.keys(): ##
            data_trend[keyword] = pageviews
        else:
            if hour == hour_now: ## init pageviews is hour_last, trend = (data_2-data_1)/data_2*100
                data_trend[keyword] = (pageviews - data_trend[keyword])/pageviews*100
            else:
                data_trend[keyword] = -(pageviews - data_trend[keyword])/data_trend[keyword]*100
    return data_trend

@timing
def fetch_missoner_web_id():
    query = "SELECT web_id FROM web_id_table where missoner_keyword_enable=1"
    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    web_id_all = [d[0] for d in data]
    return web_id_all

## calculate trend
if __name__ == '__main__':

    df_trend = update_keyword_trend()

    # web_id_all = fetch_missoner_web_id()
    # web_id_all = ['cnews']
    # is_UTC0 = check_is_UTC0()
    # hour_now = get_hour(is_UTC0=is_UTC0)
    # # hour_now = 12
    # for web_id in web_id_all:
    #     data = fetch_keyword_pageviews_by_hour(web_id, is_UTC0=is_UTC0)
    #     data_trend = calculate_diff_trend(data, is_UTC0=is_UTC0)
    #     data_save = {}
    #     for i,d in enumerate(data):
    #         keyword, pageviews, date, hour = d
    #         trend = data_trend[keyword]
    #         if hour==hour_now:
    #             data_save[i] = {'web_id':web_id, 'keyword':keyword, 'pageviews':pageviews,
    #                             'trend':trend , 'date':date, 'hour':hour}
    #     df_trend = pd.DataFrame.from_dict(data_save, "index")
    #     if np.array(df_trend).size == 0:
    #         print(f"no ")
    #         continue
    #     query = MySqlHelper.generate_update_SQLquery(df_trend, 'missoner_keyword_trend')
    #     trend_list_dict = df_trend.to_dict('records')
    #     MySqlHelper('dione', is_ssh=False).ExecuteUpdate(query, trend_list_dict)



    # data_trend, i = {}, 0
    # is_UTC0 = check_is_UTC0()
    # hour_now = get_hour(is_UTC0=is_UTC0)
    # hour_last = hour_now - 1
    # for d in data:
    #     keyword, pageviews, hour = d
    #     if keyword not in data_trend.keys(): ##
    #         data_trend[keyword] = pageviews
    #     else:
    #         if hour == hour_now: ## init pageviews is hour_last
    #             data_trend[keyword] = (pageviews - data_trend[keyword])/pageviews*100
    #         else:
    #             data_trend[keyword] = -(pageviews - data_trend[keyword])/data_trend[keyword]*100


