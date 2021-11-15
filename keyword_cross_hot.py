import time
import jieba
import jieba.analyse
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
from basic.date import get_hour, date2int, get_today, get_yesterday

@timing
def fetch_cross_hot_keyword(date_int):
    query = f"""
            SELECT 
                k.keyword,
                k.pageviews,
                k.external_source_count,
                k.internal_source_count,
                COUNT(ka.article_id) as mentionedArticles,
                k.landings,
                k.exits,
                k.bounce,
                k.timeOnPage
            FROM
                (SELECT 
                    keyword,
                    SUM(pageviews) AS pageviews,
                    SUM(external_source_count) AS external_source_count,
                    SUM(internal_source_count) AS internal_source_count,
                    SUM(landings) AS landings,
                    SUM(exits) AS exits,
                    SUM(bounce) AS bounce,
                    SUM(timeOnPage) AS timeOnPage
                FROM
                    missoner_keyword
                WHERE
                    date = {date_int}
                GROUP BY keyword
                ORDER BY pageviews DESC
                LIMIT 100) AS k
                    INNER JOIN
                missoner_keyword_article ka ON k.keyword = ka.keyword
            GROUP BY k.keyword
            """
    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    df_hot_keyword = pd.DataFrame(data, columns=['keyword', 'pageviews', 'external_source_count',
                                                 'internal_source_count', 'mentionedArticles',
                                                 'landings', 'exits', 'bounce', 'timeOnPage'])
    df_hot_keyword['date'] = [date_int] * df_hot_keyword.shape[0]
    return df_hot_keyword


def update_cross_keywords(date_int):
    df_hot_keyword = fetch_cross_hot_keyword(date_int)
    hot_keyword_list_dict = df_hot_keyword.to_dict('records')
    query = """
            REPLACE INTO missoner_keyword_crossHot
            (keyword, pageviews, external_source_count, internal_source_count, mentionedArticles, landings, exits, bounce, timeOnPage, date)
            VALUES 
            (:keyword, :pageviews, :external_source_count, :internal_source_count, :mentionedArticles, :landings, :exits, :bounce, :timeOnPage, :date)
            """
    MySqlHelper('dione', is_ssh=False).ExecuteUpdate(query, hot_keyword_list_dict)
    return df_hot_keyword

def save_trend_table(df_hot_keyword, hour):
    hot_keyword_list_dict = df_hot_keyword.to_dict('records')
    data_trend = {}
    for i,data in enumerate(hot_keyword_list_dict):
        data_trend[i] = {'web_id':'crossHot', 'keyword':data['keyword'], 'pageviews':data['pageviews'],
                        'date':data['date'], 'hour':hour}
    df_trend = pd.DataFrame.from_dict(data_trend, "index")
    query = MySqlHelper.generate_update_SQLquery(df_trend, 'missoner_keyword_trend')
    trend_list_dict = df_trend.to_dict('records')
    MySqlHelper('dione', is_ssh=False).ExecuteUpdate(query, trend_list_dict)
    return df_trend



## update cross-web_id popular keyword statistics every hour
if __name__ == '__main__':
    ## set is in UTC+0 or UTC+8
    is_UTC0 = True
    # df_hot_keyword = fetch_cross_hot_keyword(date_int=20211103)
    df_hot_keyword = update_cross_keywords(date_int=20211101)
    # hot_keyword_list_dict = df_hot_keyword.to_dict('records')
    df_trend = save_trend_table(df_hot_keyword, 15)

    # query = """
    #         REPLACE INTO missoner_keyword_crossHot
    #         (keyword, pageviews, external_source_count, internal_source_count, landings, exits, bounce, timeOnPage, date)
    #         VALUES
    #         (:keyword, :pageviews, :external_source_count, :internal_source_count, :landings, :exits, :bounce, :timeOnPage, :date)
    #         """
    # MySqlHelper('dione', is_ssh=False).ExecuteUpdate(query, hot_keyword_list_dict)