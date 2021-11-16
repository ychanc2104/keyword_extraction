import jieba
import jieba.analyse
import numpy as np
import pandas as pd
import time
from basic.decorator import timing
from jieba_based.utility import Composer_jieba
from db.mysqlhelper import MySqlHelper
from media.Media import Media
from basic.date import get_hour, date2int, get_today, get_yesterday, check_is_UTC0


## main, process one day if assign date, default is today
@timing
def update_missoner_three_tables(date=None, is_UTC0=False, n=10000):
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
            # ## process keyword ##
            keywords, keyword_list, is_cut = generate_keyword_list(row, jieba_base, stopwords, stopwords_missoner)
            params = np.array(row[['pageviews', 'landings', 'exits', 'bounce', 'timeOnPage']]).astype('int')
            ## separate keyword_list to build dictionary ##
            for keyword in keyword_list:
                ## keyword and articles mapping, for table, missoner_keyword_article
                dict_keyword_article[i] = {'web_id': web_id, 'article_id': row['article_id'], 'keyword': keyword, 'is_cut': is_cut}
                i += 1
                ## compute pageviews by external and internal sources, for table, missoner_keyword
                keyword_dict = collect_pageviews_by_source(keyword_dict, keyword, row, source_domain_mapping, params, is_cut)
            print(f"index: {index}, keywords: {keywords}")
        date = date_int
        hour = get_hour(is_UTC0=is_UTC0)

        ## build dict for building DataFrame
        data_save, data_trend = {}, {}
        i = 0
        for key, value in keyword_dict.items():
            data_save[i] = {'web_id': web_id, 'keyword': key, 'pageviews': value[0], 'external_source_count': value[5],
                            'internal_source_count': value[6], 'landings': value[1], 'exits': value[2],
                            'bounce': value[3], 'timeOnPage': value[4], 'is_cut': value[7], 'date': date}
            data_trend[i] = {'web_id': web_id, 'keyword': key, 'pageviews': value[0], 'hour':hour, 'date':date}
            print(f'{data_save[i]}')
            i += 1
        ## deal with trend before replace missoner_keyword table
        df_pageviews_last = fetch_now_keywords_by_web_id(web_id, is_UTC0=False)
        df_pageviews_now = pd.DataFrame.from_dict(data_trend, "index")[['keyword', 'pageviews']]
        df_trend = compute_trend_from_df(df_pageviews_last, df_pageviews_now)

        ## build DataFrame
        df_keyword = pd.DataFrame.from_dict(data_save, "index")
        ## merge keyword and trend
        df_keyword = pd.concat([df_keyword.set_index('keyword'), df_trend.set_index('keyword')], axis=1).reset_index(level=0)
        ## select enough number of keywords
        pageviews_array = np.array(df_keyword['pageviews']).astype('int')
        mean_pageviews = np.mean(pageviews_array)
        df_keyword = df_keyword.query(f"pageviews > {mean_pageviews}").fillna(0)
        ## save keyword statistics to table: missoner_keyword
        keyword_list_dict = df_keyword.to_dict('records')
        query_keyword = MySqlHelper.generate_update_SQLquery(df_keyword, 'missoner_keyword')
        MySqlHelper('dione', is_ssh=False).ExecuteUpdate(query_keyword, keyword_list_dict)

        ## save keywords <=> articles mapping, tabel: missoner_keyword_article
        df_keyword_article = pd.DataFrame.from_dict(dict_keyword_article, "index")
        keyword_article_list_dict = df_keyword_article.to_dict('records')
        query_keyword_article = MySqlHelper.generate_update_SQLquery(df_keyword_article, 'missoner_keyword_article')
        MySqlHelper('dione', is_ssh=False).ExecuteUpdate(query_keyword_article, keyword_article_list_dict)

    # ## save cross hot keywords, tabel: missoner_keyword_crossHot (compute after all web_id ran) (without trend)
    ## deal with trend before replace missoner_keyword_crossHot table
    df_keyword_crossHot_last = fetch_now_crossHot_keywords(date_int) ## take keyword in missoner_keyword_crossHot
    df_keyword_crossHot_now = fetch_crossHot_keyword(date_int) ## only take top100 keywords from missoner_keyword
    df_trend_crossHot = compute_trend_from_df(df_keyword_crossHot_last, df_keyword_crossHot_now)
    ## merge keyword and trend
    df_keyword_crossHot = pd.concat([df_keyword_crossHot_now.set_index('keyword'), df_trend_crossHot.set_index('keyword')], axis=1)
    ## recover column keyword and remove nan
    df_keyword_crossHot = df_keyword_crossHot.reset_index(level=0).dropna()

    query_crossHot = MySqlHelper.generate_update_SQLquery(df_keyword_crossHot, 'missoner_keyword_crossHot')
    keyword_crossHot_list_dict = df_keyword_crossHot.to_dict('records')
    MySqlHelper('dione', is_ssh=False).ExecuteUpdate(query_crossHot, keyword_crossHot_list_dict)

    return df_keyword, df_keyword_article, df_keyword_crossHot


def update_crossHot_trend_table(df_hot_keyword, hour):
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

def compute_trend_from_df(df_pageviews_last, df_pageviews_now):
    ## make pageviews to be int32 and math operation by index:keyword
    df_pageviews_last = df_pageviews_last[['keyword', 'pageviews']].set_index('keyword').astype({'pageviews': 'int32'})
    df_pageviews_now = df_pageviews_now[['keyword', 'pageviews']].set_index('keyword').astype({'pageviews': 'int32'})
    df_trend = ((df_pageviews_now - df_pageviews_last)/df_pageviews_now*100).fillna(0).rename(columns = {'pageviews': 'trend'})

    df_trend = pd.concat([df_trend, df_pageviews_last], axis=1).rename(columns = {'pageviews': 'pageviews_last'})
    df_trend['keyword'] = df_trend.index
    return df_trend

@timing
def fetch_crossHot_keyword(date_int):
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
                LIMIT 500) AS k
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

## get latest keyword data
@timing
def fetch_now_crossHot_keywords(date_int):
    # date_int = date2int(get_today(is_UTC0=is_UTC0))
    query = f"SELECT keyword, pageviews FROM missoner_keyword_crossHot WHERE date={date_int}"
    data = MySqlHelper('dione').ExecuteSelect(query)
    df = pd.DataFrame(data, columns=['keyword', 'pageviews'])
    return df

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


## get latest keyword data
@timing
def fetch_now_keywords_by_web_id(web_id, is_UTC0=False):
    date_int = date2int(get_today(is_UTC0=is_UTC0))
    query = f"SELECT keyword, pageviews FROM missoner_keyword WHERE date={date_int} and web_id='{web_id}'"
    data = MySqlHelper('dione').ExecuteSelect(query)
    df = pd.DataFrame(data, columns=['keyword', 'pageviews'])
    return df

## cut keyword list if keywords is empty
def generate_keyword_list(row, jieba_base, stopwords, stopwords_missoner):
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
    return keywords, keyword_list, is_cut

## compute pageviews by external and internal sources
def collect_pageviews_by_source(keyword_dict, keyword, row, source_domain_mapping, params, is_cut):
    ## save each keyword from a article ##
    if keyword not in keyword_dict.keys():
        ## process internal and external source loop and save to popular keyword dict
        if row['source_domain'] in source_domain_mapping:  # internal case
            keyword_dict[keyword] = np.append(params, [0, row['pageviews'], is_cut])
        else:  # external case
            keyword_dict[keyword] = np.append(params, [row['pageviews'], 0, is_cut])
    else:
        ## process internal and external source loop and add to popular keyword dict
        if row['source_domain'] in source_domain_mapping:  # internal case
            ## add to internal source count
            keyword_dict[keyword][:-1] += np.append(params, [0, row['pageviews']])
        else:  # external case
            ## add to external source count
            keyword_dict[keyword][:-1] += np.append(params, [row['pageviews'], 0])
    return keyword_dict



## analyze data yesterday, insert two tables, missoner_keyword and missoner_keyword_article
if __name__ == '__main__':
    t_start = time.time()
    date = None
    # date = '2021-10-30' ## None: assign today
    is_UTC0 = check_is_UTC0()
    hour_now = get_hour(is_UTC0=is_UTC0)
    if (hour_now == 3):
        ## routine
        # update four tables, missoner_keyword, missoner_keyword_article, missoner_keyword_crossHot, missoner_keyword_trend
        df_keyword, df_keyword_article, df_keyword_crossHot = update_missoner_three_tables(date=date, n=5000, is_UTC0=is_UTC0)

        print(f'routine to update every hour, hour: {hour_now}')
        yesterday = get_yesterday(is_UTC0=is_UTC0)
        ## cal at 0,1,2 to confirm data is complete
        df_keyword_y, df_keyword_article_y, df_keyword_crossHot_y = update_missoner_three_tables(date=yesterday, n=50000, is_UTC0=is_UTC0)
        print(f"in 3:00 (UTC+8), update yesterday all browse record")
    else:
        # update four tables, missoner_keyword, missoner_keyword_article, missoner_keyword_crossHot, missoner_keyword_trend
        df_keyword, df_keyword_article, df_keyword_crossHot = update_missoner_three_tables(date=date, n=5000, is_UTC0=is_UTC0)

        print(f'routine to update every hour, hour: {hour_now}')

    t_end = time.time()
    t_spent = t_end - t_start
    print(f'finish all routine spent: {t_spent}s')
