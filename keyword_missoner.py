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



def clean_keyword_list(keyword_list, stopwords, stopwords_missoner):
    keyword_list = Composer_jieba().clean_keyword(keyword_list, stopwords)  ## remove stopwords
    keyword_list = Composer_jieba().clean_keyword(keyword_list, stopwords_missoner)  ## remove stopwords
    keyword_list = Composer_jieba().filter_quantifier(keyword_list)  ## remove number+quantifier, ex: 5.1萬
    keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9]{2}")  ## remove 2 digit number
    keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9.]*")  ## remove floating
    keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[a-z]{1,4}|[A-Z]{2}")  ## remove 1-4 lowercase letter and 2 Upper
    keyword_list = [keyword for keyword in keyword_list if keyword != ''] ## remove blank
    return keyword_list


@timing
def test_speed():
    query = f"""
            SELECT 
                h.web_id, l.title, l.content, h.source_domain, h.pageviews, h.landings,
                h.exits, h.bounce, h.timeOnPage, h.date, h.hour
            FROM
                report_hour h
                    INNER JOIN
                article_list l ON h.article_id = l.signature
                    AND h.web_id = 'ctnews'
                    AND h.date = 20211025
                    AND l.web_id = 'ctnews'
            """
    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    return data


@timing
def fetch_missoner_web_id():
    query = "SELECT web_id FROM web_id_table where missoner_keyword_enable=1"
    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    web_id_all = [d[0] for d in data]
    return web_id_all

@timing
def fetch_source_domain_mapping(web_id):
    query = f"SELECT domain FROM source_domain_mapping where web_id='{web_id}'"
    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    source_domain_mapping = [d[0] for d in data]
    return source_domain_mapping

@timing
def fetch_hot_articles(web_id, n=50, date=None, is_UTC0=False): # default get today's popular articles
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
    return df_hot

## process one day if assign date, default is today
@timing
def build_keyword_article(date=None, is_UTC0=False, n=10000):
    if (date == None):
        date_int = date2int(get_today(is_UTC0=is_UTC0))
    else:
        date_int = date2int(date)
    ## set up config (add word, user_dict.txt ...)
    jieba_base = Composer_jieba()
    all_hashtag = jieba_base.set_config() ## add all user dictionary (add_words, google_trend, all_hashtag)
    stopwords = jieba_base.get_stopword_list()
    stopwords_missoner = jieba_base.read_file('./jieba_based/stop_words_missoner.txt')
    ## set up media
    media = Media()
    web_id_all = fetch_missoner_web_id()
    # web_id_all = ['ctnews']
    for web_id in web_id_all:
        ## fetch source domain mapping
        source_domain_mapping = fetch_source_domain_mapping(web_id)
        ## fetch user_based popular article
        df_hot = media.fetch_hot_articles(web_id, n, date=date, is_UTC0=is_UTC0)
        if df_hot.size == 0:
            print('no valid data in dione.report_hour')
            continue
        dict_keyword_article = {}
        i = 0
        keyword_dict = {}
        for index, row in df_hot.iterrows():
            ## process keyword ##
            keywords = row['keywords']
            news = row['title'] + ' ' + row['content']
            news_clean = jieba_base.filter_str(news, pattern="https:\/\/([0-9a-zA-Z.\/]*)")  ## pattern for https
            news_clean = jieba_base.filter_symbol(news_clean)
            if (keywords == '') | (keywords == '_'):
                keyword_list = jieba.analyse.extract_tags(news_clean, topK=8)
                keyword_list = clean_keyword_list(keyword_list, stopwords, stopwords_missoner)
                keywords = ','.join(keyword_list)  ## add keywords
                is_cut = 1
            else:
                keyword_list = [k.strip() for k in keywords.split(',')]
                keyword_list = clean_keyword_list(keyword_list, stopwords, stopwords_missoner)
                is_cut = 0
            params = np.array(row[['pageviews', 'landings', 'exits', 'bounce', 'timeOnPage']]).astype('int')
            ## build dictionary loop ##
            for keyword in keyword_list:
                ## keyword and articles mapping
                dict_keyword_article[i] = {'web_id': web_id, 'article_id': row['article_id'], 'keyword': keyword, 'is_cut': is_cut}
                i += 1
                ## save each keyword from a article ##
                if keyword not in keyword_dict.keys():
                    ## process internal and external source loop and save to popular keyword dict
                    if row['source_domain'] in source_domain_mapping: # internal case
                        keyword_dict[keyword] = np.append(params, [0, row['pageviews'], is_cut])
                    else: # external case
                        keyword_dict[keyword] = np.append(params, [row['pageviews'], 0, is_cut])
                else:
                    ## process internal and external source loop and add to popular keyword dict
                    if row['source_domain'] in source_domain_mapping: # internal case
                        ## add to internal source count
                        keyword_dict[keyword][:-1] += np.append(params, [0, row['pageviews']])
                    else: # external case
                        ## add to external source count
                        keyword_dict[keyword][:-1] += np.append(params, [row['pageviews'], 0])
            print(f"index: {index}, keywords: {keywords}")
        date = date_int
        data_save = {}
        i = 0
        for key, value in keyword_dict.items():
            data_save[i] = {'web_id': web_id, 'keyword': key, 'pageviews': value[0], 'external_source_count': value[5],
                            'internal_source_count': value[6], 'landings': value[1], 'exits': value[2],
                            'bounce': value[3], 'timeOnPage': value[4], 'is_cut': value[7], 'date': date}
            print(f'{data_save[i]}')
            i += 1
        ## build DataFrame
        df_keyword = pd.DataFrame.from_dict(data_save, "index")
        ## select enough number of keywords
        pageviews_array = np.array(df_keyword['pageviews']).astype('int')
        mean_pageviews = np.mean(pageviews_array)
        df_select = df_keyword.query(f"pageviews > {mean_pageviews}")

        ## save keyword statistics to db
        df_select_list_dict = df_select.to_dict('records')
        query = """
        REPLACE INTO missoner_keyword 
        (web_id, keyword, pageviews, landings, exits, bounce, timeOnPage, external_source_count, internal_source_count, is_cut, date)
        VALUES 
        (:web_id, :keyword, :pageviews, :landings, :exits, :bounce, :timeOnPage, :external_source_count, :internal_source_count, :is_cut, :date)
        """
        MySqlHelper('dione', is_ssh=False).ExecuteUpdate(query, df_select_list_dict)
        ## save keywords <=> articles mapping
        df_map = pd.DataFrame.from_dict(dict_keyword_article, "index")
        df_map_list_dict = df_map.to_dict('records')
        query = "REPLACE INTO missoner_keyword_article (web_id, article_id, keyword, is_cut) VALUES (:web_id, :article_id, :keyword, :is_cut)"
        MySqlHelper('dione', is_ssh=False).ExecuteUpdate(query, df_map_list_dict)
    return df_select, df_map


## analyze data yesterday, insert two tables, missoner_keyword and missoner_keyword_article
if __name__ == '__main__':

    t_start = time.time()
    date = None ## None: assign today
    if socket.gethostbyname(socket.gethostname()) == '127.0.1.1':
        is_UTC0 = True
    else:
        is_UTC0 = False
    # date = '2021-11-01'
    hour_now = get_hour(is_UTC0=is_UTC0)
    if (hour_now == 3):
        ## routine
        df_select, df_map = build_keyword_article(date=date, n=5000, is_UTC0=is_UTC0)
        print(f'routine to update every hour, hour: {hour_now}')
        yesterday = get_yesterday(is_UTC0=is_UTC0)
        ## cal at 0,1,2 to confirm data is complete
        df_select_y, df_map_y = build_keyword_article(date=yesterday, n=50000, is_UTC0=is_UTC0)
        print(f"in 3:00 (UTC+8), update yesterday all browse record")
        # print(f"in time range between 0 and 2 (UTC+8), update yesterday all twice(at 0, 1 and 2 o'clock)")
    else:
        df_select, df_map = build_keyword_article(date=date, n=5000, is_UTC0=is_UTC0)
        print(f'routine to update every hour, hour: {hour_now}')
    t_end = time.time()
    t_spent = t_end - t_start
    print(f'finish all routine spent: {t_spent}s')


    # ### directly run
    # is_UTC0 = False
    # n = 5000
    # date = get_today(is_UTC0=is_UTC0)
    # date_int = date2int(date)
    # ## set up config (add word, user_dict.txt ...)
    # jieba_base = Composer_jieba()
    # all_hashtag = jieba_base.set_config() ## add all user dictionary (add_words, google_trend, all_hashtag)
    # stopwords = jieba_base.get_stopword_list()
    # stopwords_missoner = jieba_base.read_file('./jieba_based/stop_words_missoner.txt')
    # ## set up media
    # media = Media()
    # web_id_all = fetch_missoner_web_id()
    # web_id_all = ['cmoney']
    # # n = 500
    # for web_id in web_id_all:
    #     ## fetch source domain mapping
    #     source_domain_mapping = fetch_source_domain_mapping(web_id)
    #     internal_source_count, external_source_count = 0, 0
    #     ## fetch user_based popular article
    #     df_hot = media.fetch_hot_articles(web_id, n, date=date, is_UTC0=is_UTC0)
    #     if df_hot.size == 0:
    #         print('no valid data in dione.report_hour')
    #         continue
    #     dict_keyword_article = {}
    #     i = 0
    #     keyword_dict = {}
    #     for index, row in df_hot.iterrows():
    #         ## process keyword ##
    #         keywords = row['keywords']
    #         news = row['title'] + ' ' + row['content']
    #         news_clean = jieba_base.filter_str(news, pattern="https:\/\/([0-9a-zA-Z.\/]*)")  ## pattern for https
    #         news_clean = jieba_base.filter_symbol(news_clean)
    #         if (keywords == '') | (keywords == '_'):
    #             keyword_list = jieba.analyse.extract_tags(news_clean, topK=8)
    #             keyword_list = clean_keyword_list(keyword_list, stopwords, stopwords_missoner)
    #             keywords = ','.join(keyword_list)  ## add keywords
    #             is_cut = 1
    #         else:
    #             keyword_list = [k.strip() for k in keywords.split(',')]
    #             keyword_list = clean_keyword_list(keyword_list, stopwords, stopwords_missoner)
    #             is_cut = 0
    #         params = np.array(row[['pageviews', 'landings', 'exits', 'bounce', 'timeOnPage']]).astype('int')
    #         ## build dictionary loop ##
    #         for keyword in keyword_list:
    #             ## keyword and articles mapping
    #             dict_keyword_article[i] = {'web_id': web_id, 'article_id': row['article_id'], 'keyword': keyword, 'is_cut': is_cut}
    #             i += 1
    #             ## save each keyword from a article ##
    #             if keyword not in keyword_dict.keys():
    #                 ## process internal and external source loop and save to popular keyword dict
    #                 if row['source_domain'] in source_domain_mapping: # internal case
    #                     keyword_dict[keyword] = np.append(params, [0, row['pageviews'], is_cut])
    #                 else: # external case
    #                     keyword_dict[keyword] = np.append(params, [row['pageviews'], 0, is_cut])
    #             else:
    #                 ## process internal and external source loop and add to popular keyword dict
    #                 if row['source_domain'] in source_domain_mapping: # internal case
    #                     ## add to internal source count
    #                     keyword_dict[keyword][:-1] += np.append(params, [0, row['pageviews']])
    #                 else: # external case
    #                     ## add to external source count
    #                     keyword_dict[keyword][:-1] += np.append(params, [row['pageviews'], 0])
    #         print(f"index: {index}, keywords: {keywords}")
    #     date = date_int
    #     data_save = {}
    #     i = 0
    #     for key, value in keyword_dict.items():
    #         data_save[i] = {'web_id': web_id, 'keyword': key, 'pageviews': value[0], 'external_source_count': value[5],
    #                         'internal_source_count': value[6], 'landings': value[1], 'exits': value[2],
    #                         'bounce': value[3], 'timeOnPage': value[4], 'is_cut': value[7], 'date': date}
    #         print(f'{data_save[i]}')
    #         i += 1
    #     ## build DataFrame
    #     df_keyword = pd.DataFrame.from_dict(data_save, "index")
    #     ## select enough number of keywords
    #     pageviews_array = np.array(df_keyword['pageviews']).astype('int')
    #     mean_pageviews = np.mean(pageviews_array)
    #     df_select = df_keyword.query(f"pageviews > {mean_pageviews}")
    #
    #     ## save keyword statistics to db
    #     df_select_list_dict = df_select.to_dict('records')
    #     query = """
    #     REPLACE INTO missoner_keyword
    #     (web_id, keyword, pageviews, landings, exits, bounce, timeOnPage, external_source_count, internal_source_count, is_cut, date)
    #     VALUES
    #     (:web_id, :keyword, :pageviews, :landings, :exits, :bounce, :timeOnPage, :external_source_count, :internal_source_count, :is_cut, :date)
    #     """
    #     MySqlHelper('dione', is_ssh=False).ExecuteUpdate(query, df_select_list_dict)
    #
    #     ## save keywords <=> articles mapping
    #     df_map = pd.DataFrame.from_dict(dict_keyword_article, "index")
    #     df_map_list_dict = df_map.to_dict('records')
    #     query = "REPLACE INTO missoner_keyword_article (web_id, article_id, keyword, is_cut) VALUES (:web_id, :article_id, :keyword, :is_cut)"
    #     MySqlHelper('dione', is_ssh=False).ExecuteUpdate(query, df_map_list_dict)