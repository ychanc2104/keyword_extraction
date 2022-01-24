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
from jieba_based.jieba_utils import Composer_jieba
import datetime

# url = 'https://trends.google.com.tw/trends/api/dailytrends?hl=zh-TW&tz=-480&ed=20211010&geo=TW&ns=15'
# resp = requests.get(url)
# a = resp.text
# df2 = pd.DataFrame(json.loads(re.sub(r'\)\]\}\',\n', '', resp.text))['default']['trendingSearchesDays'][0]['trendingSearches'])

class GoogleTrend:
    def __init__(self, language='zh-TW', tz=-480, ns=15, geo='TW', client='firefox', timeframe_custom='2021-09-16 2021-10-25'):
        self.GENERAL_URL = 'https://trends.google.com/trends/api/explore'
        self.url_multiline = 'https://trends.google.com.tw/trends/api/widgetdata/multiline'
        self.url_comparedgeo = 'https://trends.google.com.tw/trends/api/widgetdata/comparedgeo'
        self.url_relatedsearch = 'https://trends.google.com.tw/trends/api/widgetdata/relatedsearches'
        self.url_list = [self.url_multiline, self.url_comparedgeo, self.url_relatedsearch, self.url_relatedsearch] ## order by widgets
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

    ## max day of google trend is 30 days
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

    ## for routine
    def update_latest_two_days(self, is_ssh=False):
        date_end = datetime_to_str(get_today())
        date_start = datetime_to_str(to_datetime(date_end) - datetime.timedelta(days=1))
        df_2day = self.fetch_keyword(date_start, date_end, filter_repeat=True)
        # query = text("REPLACE INTO google_trend_keyword (keyword, relatedQueries, traffic, date) VALUES (:keyword, :relatedQueries, :traffic, :date)")
        query = MySqlHelper.generate_insertDup_SQLquery(df_2day, 'google_trend_keyword', ['relatedQueries', 'traffic'])
        MySqlHelper('dione', is_ssh=is_ssh).ExecuteUpdate(query, df_2day.to_dict('records'))
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
    def fetch_keyword_explore(self, keyword, gprop='', category=0, timeframe='today 1-m'):
        """
        get popular queries in 'web', 'images', 'news', 'youtube', 'froogle'
        Parameters
        ----------
        keyword: '' for popular queries
        gprop: ''(web), 'images', 'news', 'youtube', 'froogle'
        category: 0 to fetch token
        timeframe: 'today 1-m', 'today 12-m', '2021-10-22 2021-10-22'

        Returns:
            four dict, 'MultiLine', 'ComparedGEO', 'SearchTopic' and 'SearchQuery'
        -------

        """
        """Create the payload for related queries, interest over time and interest by region"""
        if gprop not in ['', 'images', 'news', 'youtube', 'froogle']:
            raise ValueError('gprop must be empty (to indicate web), images, news, youtube, or froogle')
        self.token_payload = {
            'hl': self.language,
            'tz': self.tz,
            'req': {'comparisonItem': [{'keyword':keyword, 'geo':self.geo, 'time':timeframe}], 'category': category, 'property': gprop}
        }
        self.token_payload['req'] = json.dumps(self.token_payload['req']) ## dict to string
        widget_dicts_list = self.TrendReq._get_data(
            url=self.GENERAL_URL,
            method='get',
            params=self.token_payload,
            trim_chars=4,
        )['widgets'] ## get four objects, 1.multiline, 2.comparedgeo, 3.related topics, 4.related queries
        self.response_list = []
        response_all = {}
        dict_name = ['MultiLine', 'ComparedGEO', 'SearchTopic', 'SearchQuery']
        for i, widget_dicts in enumerate(widget_dicts_list):
            print(f"finish {i}")
            url = self.build_request_url(self.url_list[i], ['hl', 'tz', 'req', 'token'],
                                         [self.language, self.tz, widget_dicts['request'], widget_dicts['token']])
            response = requests.get(url)
            self.response_list += [response]
            if dict_name[i] == 'MultiLine':
                df_multiline = pd.DataFrame(json.loads(re.sub(r'\)\]\}\',\n', '', response.text))['default']['timelineData'])
                # response_all[dict_name[i]] = self._reformat_value(df_multiline)
                response_all[dict_name[i]] = self._reformat_cols(df_multiline, cols=['value', 'hasData', 'formattedValue'])
            elif dict_name[i] == 'ComparedGEO':
                df_geo = pd.DataFrame(json.loads(re.sub(r'\)\]\}\',\n', '', response.text))['default']['geoMapData'])
                # response_all[dict_name[i]] = self._reformat_value(df_geo)
                response_all[dict_name[i]] = self._reformat_cols(df_geo, cols=['value', 'hasData', 'formattedValue'])

            else:
                df_hot = pd.DataFrame(json.loads(re.sub(r'\)\]\}\',\n', '', response.text))['default']['rankedList'][0]['rankedKeyword']) #popular topics or queries
                df_up  = pd.DataFrame(json.loads(re.sub(r'\)\]\}\',\n', '', response.text))['default']['rankedList'][1]['rankedKeyword']) #increasing topics or queries
                if dict_name[i] == 'SearchTopic':
                    if df_hot.shape[0]==0 or df_up.shape[0]==0:
                        response_all[dict_name[i]] = {'hot': df_hot, 'up': df_up}
                    else:
                        response_all[dict_name[i]] = {'hot': self._reshape_topic(df_hot), 'up': self._reshape_topic(df_up)}
                else:
                    # if df_hot.shape[0]==0 or df_up.shape[0]==0:
                    #     continue
                    # else:
                    print(df_hot)
                    print(df_up)
                    response_all[dict_name[i]] = {'hot': df_hot, 'up': df_up}
        return response_all

    ## timeframe range limit, ex: 2021-10-22 2021-10-25 (4 days)
    def fetch_keyword_list_explore(self, keyword_list, gprop='', category=0, timeframe='today 1-m'):
        """
        get popular queries in 'web', 'images', 'news', 'youtube', 'froogle'
        Parameters
        ----------
        keyword_list: ['a', 'b', ...]
        gprop: ''(web), 'images', 'news', 'youtube', 'froogle'
        category: 0 to fetch token
        timeframe: 'today 1-m', 'today 12-m', '2021-10-22 2021-10-22'

        Returns:
            two dict, 'MultiLine', 'ComparedGEO'
        -------

        """
        """Create the payload for related queries, interest over time and interest by region"""
        if gprop not in ['', 'images', 'news', 'youtube', 'froogle']:
            raise ValueError('gprop must be empty (to indicate web), images, news, youtube, or froogle')
        payload_keyword_list = []
        for keyword in keyword_list:
            payload_keyword_list += [{'keyword':keyword, 'geo':self.geo, 'time':timeframe}]
        self.token_payload = {
            'hl': self.language,
            'tz': self.tz,
            'req': {'comparisonItem': payload_keyword_list, 'category': category, 'property': gprop}
        }
        self.token_payload['req'] = json.dumps(self.token_payload['req']) ## dict to string
        self.widget_dicts_list = self.TrendReq._get_data(
            url=self.GENERAL_URL,
            method='get',
            params=self.token_payload,
            trim_chars=4,
        )['widgets'] ## get four objects, 1.multiline, 2.comparedgeo, 3.related topics, 4.related queries

        response_all = {}
        dict_name = ['MultiLine', 'ComparedGEO'] ## MultiLine * 1, ComparedGEO * 1,
        for i in range(2):
            print(f"finish {i}")
            widget_dicts = self.widget_dicts_list[i]
            req = widget_dicts['request']
            url = self.build_request_url(self.url_list[i], ['hl', 'tz', 'req', 'token'],
                                         [self.language, self.tz, req, widget_dicts['token']])
            response = requests.get(url)
            self.response = response
            if dict_name[i] == 'MultiLine':
                df_multiline = pd.DataFrame(json.loads(re.sub(r'\)\]\}\',\n', '', response.text))['default']['timelineData'])
                response_all[dict_name[i]] = self._reformat_cols_list(df_multiline, cols=['value', 'hasData', 'formattedValue'], names=keyword_list, cols_drop=['value', 'hasData', 'formattedValue'])
            elif dict_name[i] == 'ComparedGEO':
                df_geo = pd.DataFrame(json.loads(re.sub(r'\)\]\}\',\n', '', response.text))['default']['geoMapData'])
                response_all[dict_name[i]] = self._reformat_cols_list(df_geo, cols=['value', 'hasData', 'formattedValue'], names=keyword_list, cols_drop=['value', 'hasData', 'formattedValue'])
        return response_all

    @staticmethod
    def _reshape_topic(df):
        mid = [text['mid'] for text in df.topic.values]
        title_tw = [Composer_jieba().zwcn2tw(text['title']) for text in df.topic.values]
        type = [Composer_jieba().zwcn2tw(text['type']) for text in df.topic.values]
        df['title'] = title_tw
        df['mid'] = mid
        df['type'] = type
        df = df.drop(columns=['topic'], inplace=False)[['title', 'type', 'formattedValue']]
        return df

    @staticmethod
    def _reformat_value(df):
        df['value'] = [value[0] for value in df.value.values]
        return df

    @staticmethod
    def _reformat_cols(df, cols):
        for col in cols:
            df[col] = [value[0] for value in df[col].values]
        return df

    @staticmethod
    def _reformat_cols_list(df, cols, names, cols_drop):
        for col in cols:
            for i,name in enumerate(names):
                col_new = f"{col}_{name}"
                df[col_new] = [value[i] for value in df[col].values]
        return df.drop(columns=cols_drop, inplace=False)

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

    @staticmethod
    def save_multi_df(name_list, df_list):
        for name,df in zip(name_list, df_list):
            df.to_excel(f"{name}.xlsx")

if __name__ == '__main__':


    gtrend = GoogleTrend()

    #
    response_all = gtrend.fetch_keyword_explore(keyword='NFT', gprop='',timeframe='2021-10-01 2022-01-05')
    df_multiline = response_all['MultiLine']
    df_geo = response_all['ComparedGEO']
    df_topic_hot, df_topic_up = response_all['SearchTopic']['hot'], response_all['SearchTopic']['up']
    df_query_hot, df_query_up = response_all['SearchQuery']['hot'], response_all['SearchQuery']['up']
    GoogleTrend.save_multi_df(['multiline' ,'geo', 'topic_hot', 'topic_up', 'query_hot', 'query_up'], [df_multiline, df_geo, df_topic_hot, df_topic_up, df_query_hot, df_query_up])
    # a= GoogleTrend._reformat_cols(df_multiline, cols=['value', 'hasData', 'formattedValue'])
    # response_list = gtrend.fetch_keyword_list_explore(keyword_list=['藻 礁','公投 綁 大選','萊 豬','核 四'], gprop='',timeframe='2021-10-01 2021-12-10')
    #
    # response_list = gtrend.fetch_keyword_list_explore(keyword_list=['疫情', '元 宇宙'], gprop='',timeframe='2021-11-01 2022-01-03')

    # gtrend.save_multi_df(name_list=['trend','geo','topic_hot','topic_up','query_hot','query_up'],
    #                      df_list=[df_multiline, df_geo, df_topic_hot, df_topic_up, df_query_hot, df_query_up])
    # keyword_list = ['藻 礁', '公投 綁 大選', '萊 豬', '核 四']
    # response = response_list[0]
    # df_multiline_2 = pd.DataFrame(json.loads(re.sub(r'\)\]\}\',\n', '', response.text))['default']['timelineData'])
    # # # df_multiline_3 = GoogleTrend._reformat_value(df_multiline)
    #
    # df_multiline_3 = GoogleTrend._reformat_cols_list(df_multiline_2, cols=['value', 'hasData', 'formattedValue'], names=keyword_list, cols_drop=['value', 'hasData', 'formattedValue'])
    # #

    # df = response_all['MultiLine']
    # df['value'] = [value[0] for value in df.value.values]

    # df = response_all['SearchTopic']['hot']
    # mid = [text['mid'] for text in df.topic.values]
    # title_tw = [Composer_jieba().zwcn2tw(text['title']) for text in df.topic.values]
    # type = [text['type'] for text in df.topic.values]
    #
    # df['title'] = title_tw
    # df['mid'] = mid
    # df['type'] = type
    # df = df.drop(columns=['topic'], inplace=False)[['title', 'type', 'formattedValue']]

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