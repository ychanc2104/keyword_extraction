from db.mysqlhelper import MySqlHelper
from basic.decorator import timing
from sqlalchemy.sql import text
import datetime
import numpy as np
import pandas as pd

from basic.date import get_today, datetime_to_str, to_datetime, date2int

class Media:
    def __init__(self): ##
        # self.web_id = web_id
        self.date_min = '2020-12-01'
        # delete edge435
        self.web_id_all = ['edh', 'bnext', 'sportz', 'awater', 'arielhsutw', 'xuite', 'ctnews', 'pixnet', 'babyhome', 'mirrormedia', 'lordcat', 'tomorrowsci', 'upmedia', 'healthbw']

    @staticmethod
    @timing
    def fetch_web_id(date_start=None, date_end=None, use_old=True): # default 30 days
        if use_old:
            web_id_all = ['edh', 'bnext', 'sportz', 'awater', 'arielhsutw', 'xuite', 'ctnews', 'pixnet', 'babyhome', 'mirrormedia', 'lordcat', 'tomorrowsci', 'upmedia', 'healthbw']
        else:
            if date_end == None:
                date_end = datetime_to_str(get_today())
            if date_start == None:
                date_start = datetime_to_str(to_datetime(date_end) - datetime.timedelta(days=30))
            query = f"SELECT web_id, update_time FROM article_click_count WHERE update_time BETWEEN '{date_start}' AND '{date_end}'"
            data = MySqlHelper('dione').ExecuteSelect(query)
            web_id_all = list(set([d[0] for d in data]))
        return web_id_all

    ## fetch hot article from RDS dione.article_click_count and get details from dione.article_list
    @timing
    def fetch_hot_articles(self, web_id, n=50, date=None, is_UTC0=False): # default get today's popular articles
        # query = f"SELECT web_id, article_id, clickCountOfMonth, update_time FROM article_click_count WHERE web_id='{web_id}' AND clickCountOfMonth != 0 ORDER BY clickCountOfMonth DESC limit {n}"
        # query = f"SELECT subscriber_browse_record.web_id, subscriber_browse_record.uuid, subscriber_browse_record.article_id, article_list.title, article_list.content FROM subscriber_browse_record inner Join article_list on subscriber_browse_record.article_id=article_list.signature"
        if (date == None):
            date_int = date2int(get_today(is_UTC0=is_UTC0))
        else:
            date_int = date2int(date)
        query = f"""
                    SELECT 
                        h.web_id, h.article_id, l.title, l.content, l.keywords, h.source_domain, 
                        SUM(h.pageviews) as pageviews, SUM(h.landings) as landings, SUM(h.exits) as exits,
                        SUM(h.bounce) as bounce, SUM(h.timeOnPage) as timeOnPage, h.date
                    FROM
                        report_hour h
                            INNER JOIN
                        article_list l ON h.article_id = l.signature
                            AND h.web_id = '{web_id}'
                            AND h.date = '{date_int}'
                            AND l.web_id = '{web_id}'
                    GROUP BY h.article_id, source_domain
                    ORDER BY pageviews DESC LIMIT {n}
                """
        print(query)
        data = MySqlHelper('dione').ExecuteSelect(query)
        columns = ['web_id', 'article_id', 'title', 'content', 'keywords', 'source_domain', 'pageviews', 'landings', 'exits', 'bounce', 'timeOnPage', 'date']
        df_hot = pd.DataFrame(data=data, columns=columns)

        self.query_article_click_count = query
        self.sql_article_click_count = data
        return df_hot

    @timing
    def fetch_article_list(self, web_id, date_start=None, date_end=None):
        if date_end == None:
            date_end = datetime_to_str(get_today())
        if date_start == None:
            date_start = datetime_to_str(to_datetime(date_end) - datetime.timedelta(days=30))
        query = f"SELECT signature, title, content, keywords, ML_category_result, url, published_time FROM article_list WHERE web_id='{web_id}' AND published_time BETWEEN '{date_start}' AND '{date_end}'"
        data = MySqlHelper('dione').ExecuteSelect(query)
        columns = ['article_id', 'title', 'content', 'keywords', 'ML_category_result', 'url', 'published_time']
        df = pd.DataFrame(data=data, columns=columns)
        self.query_article_list = query
        self.sql_article_list = data
        return df

    @timing
    def search_article(self, article_id, online=True, article_pool=None):
        if online:
            query = f"SELECT title, content, keywords, ML_category_result, url, published_time FROM article_list WHERE signature='{article_id}'"
            data = MySqlHelper('dione').ExecuteSelect(query)
            columns = ['title', 'content', 'keywords', 'ML_category_result', 'url', 'published_time']
            df = pd.DataFrame(data=data, columns=columns)
            return df
        else: # offline, article_pool is also from article_list (type: DataFrame)
            article_pool.query(f"")

    def collect_df(self, *args, columns):
        args_list = []
        for arg in args:
            args_list += [list(arg)]
        data_matrix = np.array(args_list).T
        df = pd.DataFrame(data=data_matrix, columns=columns)
        return df

    def clean_keyword(self, keyword_list, stopwords):
        data_clean = [word for word in keyword_list if word != ' ']
        data_remove_stopword = [word for word in data_clean if word not in stopwords]
        return data_remove_stopword


    def clean_df_search(self, *args, df_search, columns_add, columns_drop, columns_rearrange):
        for i, arg in enumerate(args):
            df_search[columns_add[i]] = arg
        df_search = df_search.drop(columns=columns_drop)
        df_search = df_search[columns_rearrange]
        return df_search

if __name__ == '__main__':
    web_id = 'mirrormedia'
    n = 10000

    web_ids = Media().fetch_web_id(date_start='2021-1-1', date_end='2021-10-14')
    # web_ids2 = Media().fetch_web_id()
    # df = Media().search_article('ctnews_20211001000007-260404')
    # df_all = Media().fetch_hot_articles('ctnews')

    # query = f"SELECT web_id, article_id, clickCountOfMonth, update_time FROM article_click_count WHERE web_id='{web_id}' AND clickCountOfMonth != 0 ORDER BY clickCountOfMonth DESC limit {n}"
    # data = MySqlHelper('dione').ExecuteSelect(query)
    # df1 = pd.DataFrame(data=data, columns=['web_id', 'article_id', 'clickCountOfMonth', 'update_time'])
    # df2 = df1.sort_values(by='article_id')
    #
    # article_id_all = list(df2['article_id'])
    # clicks = list(df2['clickCountOfMonth'])
    #
    # df_all2 = Media().fetch_article_list(web_id)
    # df = df_all2.query(f'{article_id_all} in article_id')
    # df.sort_values(by='article_id')
    # df['clickCountOfMonth'] = clicks


    # df_test = Media().fetch_hot_articles(web_id, n)


