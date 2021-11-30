import pandas as pd
import datetime
from db.mysqlhelper import MySqlHelper
from media.Media import Media
from basic.date import get_date_shift, datetime_to_str, get_yesterday, to_datetime, get_today
from basic.decorator import timing
from jieba_based.jieba_utils import Composer_jieba
from keyword_usertag_report import keyword_usertag_report, delete_expired_rows
import jieba.analyse
import numpy as np
import time



def clean_keyword_list(keyword_list, stopwords, stopwords_usertag):
    keyword_list = Composer_jieba().clean_keyword(keyword_list, stopwords)  ## remove stopwords
    keyword_list = Composer_jieba().clean_keyword(keyword_list, stopwords_usertag)  ## remove stopwords, only for usertag
    keyword_list = Composer_jieba().filter_quantifier(keyword_list)  ## remove number+quantifier, ex: 5.1萬
    keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9]{2}")  ## remove 2 digit number
    keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9.]*")  ## remove floating
    keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[a-z]{1,4}|[A-Z]{2}")  ## remove 1-4 lowercase letter and 2 Upper
    keyword_list = [keyword for keyword in keyword_list if keyword != ''] ## remove blank
    return keyword_list

@timing
def fetch_usertag_web_id():
    query = "SELECT web_id FROM web_id_table where usertag_keyword_enable=1"
    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    web_id_all = [d[0] for d in data]
    return web_id_all

@timing
def fetch_browse_record_yesterday_join(web_id, is_df=False, is_UTC0=False):
    date_start = get_yesterday(is_UTC0=is_UTC0)
    date_end = get_today(is_UTC0=is_UTC0) - datetime.timedelta(seconds=1)
    query = \
        f"""
            SELECT 
            s.uuid,
            t.code,
            t.registation_id AS token,
            s.article_id,
            l.title,
            l.content,
            l.keywords
        FROM
            subscriber_browse_record s
                INNER JOIN
            article_list l ON s.article_id = l.signature                
                AND s.web_id = '{web_id}'                
                AND s.click_time BETWEEN '{date_start}' AND '{date_end}'
                AND l.web_id = '{web_id}'
                INNER JOIN         
            token_index t ON t.uuid = s.uuid
                AND t.invalid = 0
                AND t.web_id = '{web_id}'            
        """
    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    if is_df:
        df = pd.DataFrame(data, columns=['web_id', 'uuid', 'token', 'article_id', 'title', 'content', 'keywords'])
        return df
    else:
        return data

@timing
def fetch_browse_record_join(web_id, date, is_df=False):
    date_start = to_datetime(date)
    date_end = date_start - datetime.timedelta(days=-1, seconds=1)
    query = \
        f"""
            SELECT 
            s.uuid,
            t.code,
            t.registation_id AS token,
            t.cert_web_id,
            s.article_id,
            l.title,
            l.content,
            l.keywords
        FROM
            subscriber_browse_record s
                INNER JOIN
            article_list l ON s.article_id = l.signature                
                AND s.web_id = '{web_id}'                
                AND s.click_time BETWEEN '{date_start}' AND '{date_end}'
                AND l.web_id = '{web_id}'
                INNER JOIN         
            token_index t ON t.uuid = s.uuid
                AND t.invalid = 0
                AND t.web_id = '{web_id}'            
        """
    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    if is_df:
        df = pd.DataFrame(data, columns=['web_id', 'uuid', 'token', 'cert_web_id', 'article_id', 'title', 'content', 'keywords'])
        return df
    else:
        return data


# def delete_expired_rows(web_id, table='usertag', is_UTC0=False, jump2gcp=True):
#     date_now = datetime_to_str(get_today(is_UTC0=is_UTC0))
#     query = f"DELETE FROM {table} where expired_date<'{date_now}' and web_id='{web_id}'"
#     print(query)
#     MySqlHelper('missioner', is_ssh=jump2gcp).ExecuteUpdate(query)

if __name__ == '__main__':
    ## set is in UTC+0 or UTC+8
    is_UTC0 = True
    jump2gcp = True
    date = get_yesterday(is_UTC0=is_UTC0) ## compute all browsing record yesterday ad 3:10 o'clock
    date = '2021-11-29'
    ## set up config (add word, user_dict.txt ...)
    jieba_base = Composer_jieba()
    all_hashtag = jieba_base.set_config()
    stopwords = jieba_base.get_stopword_list()
    stopwords_usertag = jieba_base.read_file('./jieba_based/stop_words_usertag.txt')
    web_id_all = fetch_usertag_web_id()
    web_id_all = ['managertoday']
    ## get expired_date
    expired_date = get_date_shift(date_ref=date, days=-4, to_str=True, is_UTC0=is_UTC0) ## set to today + 3 (yesterday+4), preserve 4 days
    t_start_outloop = time.time()
    for web_id in web_id_all:
        ## fetch subscribed browse record
        # data = fetch_browse_record_yesterday_join(web_id, is_df=False, is_UTC0=is_UTC0)
        data = fetch_browse_record_join(web_id, date=date, is_df=False)
        if len(data) == 0:
            print('no valid data in dione.subscriber_browse_record')
            continue
        ## build usertag DataFrame
        t_start_inloop = time.time()
        data_save = {}
        j=0
        for i, d in enumerate(data):
            uuid, code, token, cert_web_id, article_id, title, content, keywords = d
            news = title + ' ' + content
            ## pattern for removing https
            news_clean = jieba_base.filter_str(news, pattern="https:\/\/([0-9a-zA-Z.\/]*)")
            ## pattern for removing symbol, -,+~.
            news_clean = jieba_base.filter_symbol(news_clean)
            if (keywords == '') | (keywords == '_'):
                keyword_list = jieba.analyse.extract_tags(news_clean, topK=8)
                keyword_list = clean_keyword_list(keyword_list, stopwords, stopwords_usertag)
                keywords = ','.join(keyword_list)  ## add keywords
                is_cut = 1
            else:
                keyword_list = [k.strip() for k in keywords.split(',')]
                keyword_list = clean_keyword_list(keyword_list, stopwords, stopwords_usertag)
                is_cut = 0
            for keyword in keyword_list:
                data_save[j] = {'web_id':web_id, 'uuid':uuid, 'code':code, 'token':token, 'cert_web_id':cert_web_id,
                                'news':news_clean, 'keywords':keywords, 'usertag':keyword, 'article_id': article_id,
                                'expired_date':expired_date, 'is_cut': is_cut}
                j += 1
            print(f'finish built {i}, article_id: {article_id}')
        ## build DataFrame
        df_map = pd.DataFrame.from_dict(data_save, "index")
        t_end = time.time()
        spent_time = t_end - t_start_inloop
        print(f'build df loop spent time: {spent_time}s')
        ## filter nonsense data
        df_map = df_map[df_map.usertag != '']
        # ## save to db
        df_map_save = df_map.drop(columns=['news', 'keywords']).drop_duplicates()
        usertag_list_dict = df_map_save.to_dict('records')
        query = MySqlHelper.generate_update_SQLquery(df_map_save, 'usertag')
        MySqlHelper('missioner', is_ssh=jump2gcp).ExecuteUpdate(query, usertag_list_dict)
        ## delete expired data
        delete_expired_rows(web_id, table='usertag', is_UTC0=is_UTC0, jump2gcp=jump2gcp)

        ### prepare keyword_usertag_report
        df_freq_token = keyword_usertag_report(web_id, usertag_table='usertag', report_table='usertag_report', is_UTC0=is_UTC0, jump2gcp=jump2gcp)
    t_end_program = time.time()
    spent_time_program = t_end_program - t_start_outloop
    print(f'One round spent: {spent_time_program} s')





