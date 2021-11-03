import re
import json
import requests
import pandas as pd
import numpy as np
from basic.date import to_datetime, datetime_to_str
from pytrends.request import TrendReq
from basic.date import get_today
from db.mysqlhelper import MySqlHelper
from sqlalchemy import text


# from bs4 import BeautifulSoup
import datetime

# url = 'https://trends.google.com.tw/trends/api/dailytrends?hl=zh-TW&tz=-480&ed=20211010&geo=TW&ns=15'
# resp = requests.get(url)
# a = resp.text
# df2 = pd.DataFrame(json.loads(re.sub(r'\)\]\}\',\n', '', resp.text))['default']['trendingSearchesDays'][0]['trendingSearches'])

class gtrend:
    def __init__(self, language='zh-TW', tz=-480, ns=15, geo='TW', client='firefox', timeframe_custom='2021-09-16 2021-10-25'):
        self.GENERAL_URL = 'https://trends.google.com/trends/api/explore'
        self.url_relatedsearch = 'https://trends.google.com.tw/trends/api/widgetdata/relatedsearches'
        self.url_dailytrend = 'https://trends.google.com.tw/trends/api/dailytrends'
        # 'https://www.google.com/complete/search?q=di&cp=2&client=gws-wiz&xssi=t&hl=zh-TW&authuser=0&psi=JK5mYfbPIqaJr7wPyLyXiAs.1634119204880&dpr=1'
        self.url_autocomplete = 'https://www.google.com/complete/search'
        self.timeframe = ['now 1-H', 'now 4-H', 'now 1-d', 'now 7-d', 'today 1-m', 'today 3-m', 'today 12-m', 'today 5-y']
        self.timeframe_custom = timeframe_custom
        self.language = language
        self.tz = tz
        self.ns = ns
        self.geo = geo
        self.client = client
        self.TrendReq = TrendReq()
        self.token_payload = {}

    def insert_gtrend_keywords(self, date_start=None, date_end=None, filter_repeat=True):
        if date_end == None:
            date_end = datetime_to_str(get_today())
        if date_start == None:
            date_start = datetime_to_str(to_datetime(date_end) - datetime.timedelta(days=29))
        df_30day = self.fetch_keyword(date_start, date_end, filter_repeat=filter_repeat)
        df_30day_list_of_dict = df_30day.to_dict('records')
        # query = "INSERT IGNORE INTO google_trend_keyword (keyword, relatedQueries, traffic, date) VALUES (:keyword, :relatedQueries, :traffic, :date)"
        query = "INSERT INTO google_trend_keyword (keyword, relatedQueries, traffic, date) VALUES (:keyword, :relatedQueries, :traffic, :date)"
        MySqlHelper('dione').ExecuteUpdate(query, df_30day_list_of_dict)
        # MySqlHelper('dione').ExecuteInsert('google_trend_keyword', df_30day_list_of_dict)
        return df_30day

    def update_latest_two_days(self, is_ssh=False):
        date_end = datetime_to_str(get_today())
        date_start = datetime_to_str(to_datetime(date_end) - datetime.timedelta(days=1))
        df_2day = self.fetch_keyword(date_start, date_end, filter_repeat=True)
        df_2day_list_of_dict = df_2day.to_dict('records')
        query = text("REPLACE INTO google_trend_keyword (keyword, relatedQueries, traffic, date) VALUES (:keyword, :relatedQueries, :traffic, :date)")
        # query = "INSERT IGNORE INTO google_trend_keyword (keyword, relatedQueries, traffic, date) VALUES (:keyword, :relatedQueries, :traffic, :date)"
        MySqlHelper('dione', is_ssh=is_ssh).ExecuteUpdate(query, df_2day_list_of_dict)
        return df_2day



    # get only latest one month
    def fetch_keyword(self, date_start, date_end, filter_repeat=True, is_select=True):
        date_start = to_datetime(date_start)
        date_end = to_datetime(date_end)
        # url_nodate = self.url_dailytrend + f'?hl={self.language}&tz={self.tz}&ns={self.ns}&geo={self.geo}'
        url_nodate = f'{self.url_dailytrend}?hl={self.language}&tz={self.tz}&ns={self.ns}&geo={self.geo}'
        list_df = []
        df = pd.DataFrame()
        for date in pd.date_range(start=date_start, end=date_end):
            date_str = datetime_to_str(date, pattern='%Y%m%d')
            url = url_nodate + f'&ed={date_str}'
            print(url)
            response = requests.get(url)
            print(response)
            dfi = pd.DataFrame(json.loads(re.sub(r'\)\]\}\',\n', '', response.text))['default']['trendingSearchesDays'][0]['trendingSearches'])
            dfi['date'] = datetime_to_str(date, pattern='%Y-%m-%d')
            dfi['traffic'] = [self.traffic_to_num(traffic) for traffic in dfi['formattedTraffic']]
            df = df.append(dfi)
            # list_df.append(dfi)
        # df = pd.concat(list_df, ignore_index=True)
        df['title'] = df['title'].apply(lambda x: x['query'])

        if is_select:
            df = self.get_relatedQueries(df)
            df = df.drop(columns=['formattedTraffic', 'image', 'articles', 'shareUrl'])
            df = df[['title', 'relatedQueries', 'traffic', 'date']]
            df = df.rename(columns={'title': 'keyword'})
            if filter_repeat:
                # df = self.remove_repeat(df)
                df = df.drop_duplicates(subset=['keyword', 'traffic', 'date'])
        else:
            df = df.drop(columns=['formattedTraffic'])
            df = df[['title', 'traffic', 'date', 'relatedQueries', 'image', 'articles', 'shareUrl']]
            df = df.rename(columns={'title': 'keyword'})
            if filter_repeat:
                df = df.drop_duplicates(subset=['keyword', 'traffic', 'date'])
        print(df)
        return df
    # relative values
    def get_avg_traffic(self, keyword_list):
        pytrend = TrendReq(hl=self.language, tz=360)
        pytrend.build_payload(
            kw_list=keyword_list,
            timeframe='today 1-m',
            geo=self.geo,
            gprop='')
        df = pytrend.interest_over_time() # max:5
        df = df.drop(columns=['isPartial'])
        avg_traffic = np.mean(np.array(df), axis=0)
        return avg_traffic

    def traffic_to_num(self, traffic):
        if traffic.find('萬+') != -1:
            num = int(traffic.replace('萬+','0000'))
        # elif traffic.find('+') == 1:
        else:
            num = int(traffic.replace('+', ''))
        return num

    def autocomplete(self, keyword):
        url = f'{self.url_autocomplete}?q={keyword}&client={self.client}&hl={self.language}'
        response = requests.get(url)
        suggestions = json.loads(re.sub(r'\)\]\}\',\n', '', response.text))[1]
        suggestions = [suggestion for suggestion in suggestions if suggestion!=keyword]
        return suggestions

    def keyword_composed(self, keyword):
        criteria = self.is_contains_chinese(keyword)
        if criteria:
        # if keyword[-1] != ' ':
            keyword += '/'
            suggestions = self.autocomplete(keyword)
        else: # all is english
            keyword += ' '
            suggestions = self.autocomplete(keyword)
        return suggestions

    def is_contains_chinese(self, str):
        for _char in str:
            if '\u4e00' <= _char <= '\u9fa5':
                return True
        return False

    def get_relatedQueries(self, df):
        related_query = df['relatedQueries']
        queries = []
        for q in related_query:
            sub_query = []
            for sub_q in q:
                sub_query += [sub_q['query']]
            queries += [','.join(sub_query)]
        df['relatedQueries'] = queries
        return df
    ## timeframe range limit, ex: 2021-10-22 2021-10-25 (4 days)
    def fetch_yt_keyword(self, gprop='youtube', cat=0, timeframe='today 1-m'):
        """Create the payload for related queries, interest over time and interest by region"""
        if gprop not in ['', 'images', 'news', 'youtube', 'froogle']:
            raise ValueError('gprop must be empty (to indicate web), images, news, youtube, or froogle')
        self.token_payload = {
            'hl': self.language,
            'tz': self.tz,
            'req': {'comparisonItem': [{'geo':self.geo, 'time':timeframe}], 'category': cat, 'property': gprop}
        }

        self.token_payload['req'] = json.dumps(self.token_payload['req'])
        widget_dicts_list = self.TrendReq._get_data(
            url=self.GENERAL_URL,
            method='get',
            params=self.token_payload,
            trim_chars=4,
        )['widgets']
        response_all = {}
        dict_name = ['SearchTopic', 'SearchQuery']
        for i, widget_dicts in enumerate(widget_dicts_list):
            url = self.build_request_url(self.url_relatedsearch, ['hl', 'tz', 'req', 'token'],
                                         [self.language, self.tz, widget_dicts['request'], widget_dicts['token']])
            response = requests.get(url)
            df_hot = pd.DataFrame(json.loads(re.sub(r'\)\]\}\',\n', '', response.text))['default']['rankedList'][0]['rankedKeyword'])
            df_up = pd.DataFrame(json.loads(re.sub(r'\)\]\}\',\n', '', response.text))['default']['rankedList'][1]['rankedKeyword'])
            response_all[dict_name[i]] = [df_hot, df_up]
        return response_all

    def build_request_url(self, url, params_name_list, params_value_list):
        url += '?'
        for name, value in zip(params_name_list,params_value_list):
            url += f'{name}={value}&'
        return url[:-1]


    def remove_repeat(self, df, columns=['keyword', 'traffic', 'date']):
        series_join = (df['keyword'] + ',' + df['traffic'].astype('string') + ',' + df['date']).unique()
        data_list = []
        for s in series_join:
            data_list += [s.split(',')]
        data_array = np.array(data_list)
        df_unique = pd.DataFrame(data_array, columns=['keyword', 'traffic', 'date'])
        df_unique['traffic'] = df_unique['traffic'].astype('int')
        return df_unique

if __name__ == '__main__':


    gtrend = gtrend()
    response_all = gtrend.fetch_yt_keyword(timeframe='2021-10-22 2021-10-22')
    # date_end = datetime_to_str(get_today())
    # date_start = datetime_to_str(to_datetime(date_end) - datetime.timedelta(days=29))
    # df_30day = gtrend().fetch_keyword(date_start='2021-10-18', date_end='2021-10-18', filter_repeat=False)
    # df_30day_list_of_dict = df_30day.to_dict('records')
    #
    # series_join = (df_30day['keyword']+','+df_30day['traffic'].astype('string')+','+df_30day['date']).unique()
    # data_list = []
    # for s in series_join:
    #     data_list += [s.split(',')]
    # data_array = np.array(data_list)
    # df_unique = pd.DataFrame(data_array, columns=['keyword', 'traffic', 'date'])
    # df_unique['traffic'] = df_unique['traffic'].astype('int')
    #
    # df_30day_list_of_dict = df_30day.to_dict('records')
    # MySqlHelper('rheacache-db0').ExecuteInsert('google_trend_keyword', df_30day_list_of_dict)



    # query = text("REPLACE INTO google_trend_keyword (keyword, traffic, date) VALUES (:keyword, :traffic, :date)")
    # MySqlHelper('rheacache-db0').ExecuteUpdate(query, df_30day_list_of_dict)


    # df = gtrend().fetch_keyword('2021-9-21','2021-10-20')

    # gtrend().insert_gtrend_keywords(filter_repeat=True)

    # traffic = '20萬+'
    # n = gtrend().traffic_to_num(traffic)
    #
    # suggestions = gtrend().autocomplete('龍龍')
    # composed = gtrend().keyword_composed('龍龍')
    #
    # # values_s = gtrend().get_avg_traffic(suggestions)
    # # values_c = gtrend().get_avg_traffic(composed)
    #
    # pytrend = TrendReq(hl='zh-TW', tz=360, timeout=(20, 15))
    # pytrend.build_payload(
    #     kw_list=composed[0:5],
    #     timeframe='today 1-m',
    #     geo='TW',
    #     gprop='')
    # df = pytrend.interest_over_time()
    # df = df.drop(columns=['isPartial'])
    # avg_traffic = np.mean(np.array(df), axis=0)