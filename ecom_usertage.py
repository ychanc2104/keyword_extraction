import pandas as pd
import datetime
from db.mysqlhelper import MySqlHelper
from basic.date import get_date_shift, datetime_to_str, get_yesterday, to_datetime, get_today, date_to_timestamp, check_is_UTC0
from basic.decorator import timing
from jieba_based.jieba_utils import Composer_jieba
import jieba.analyse
import numpy as np

@timing
def fetch_ecom_user_record(web_id, date):
    date_start = to_datetime(date)
    date_end = date_start - datetime.timedelta(days=-1, seconds=1)
    date_start_ts, date_end_ts = date_to_timestamp(date_start)*1000, date_to_timestamp(date_end)*1000
    query = f"SELECT uuid,guid,clickItem,session_id FROM cdp_user_event_record WHERE web_id='{web_id}' AND session_id BETWEEN '{date_start_ts}' AND '{date_end_ts}' AND clickItem!='[]'"
    data = MySqlHelper('cdp').ExecuteSelect(query)
    result, i = {}, 0
    for d in data:
        uuid, guid, clickItem = d[:-1]
        product_id_list = eval(clickItem)
        for product_id in product_id_list:
            result[i] = {'uuid':uuid, 'guid':guid, 'product_id':product_id,}
            i += 1
    df_user_record = pd.DataFrame.from_dict(result, "index")
    return df_user_record


@timing
def fetch_ecom_content(web_id, product_id_list):
    query = f"SELECT product_id, title, description FROM report_data.item_list where web_id='{web_id}' and product_id in ("
    for i,product_id in enumerate(product_id_list):
        if i == len(product_id_list) - 1:
            query += f"'{product_id}')"
        else:
            query += f"'{product_id}',"
    print(query)
    data = MySqlHelper('db_webpush-api02').ExecuteSelect(query)
    # df_ecom_content = pd.DataFrame(data, columns=['product_id', 'content', 'url'])
    df_ecom_content = pd.DataFrame(data, columns=['product_id', 'title', 'description'])
    return df_ecom_content

@timing
def fetch_usertag_ecom_web_id():
    query = "SELECT web_id FROM web_push.LineConfigTable where enable=1"
    print(query)
    data = MySqlHelper('new_slave_cloud').ExecuteSelect(query)
    web_id_list = list(set([d[0] for d in data]))
    return web_id_list


if __name__ == '__main__':

    ## set is in UTC+0 or UTC+8
    is_UTC0 = check_is_UTC0()
    date = get_yesterday(is_UTC0=is_UTC0) ## compute all browsing record yesterday ad 3:10 o'clock
    # date = '2021-11-28'
    ## set up config (add word, user_dict.txt ...)
    jieba_base = Composer_jieba()
    all_hashtag = jieba_base.set_config()
    stopwords = jieba_base.get_stopword_list()
    # stopwords_usertag = jieba_base.read_file('./jieba_based/stop_words_usertag.txt')
    web_id_list = fetch_usertag_ecom_web_id()
    web_id_list = ['underwear']
    date = datetime_to_str(get_yesterday())
    date = '2021-11-10'

    for web_id in web_id_list:
        df_user_record = fetch_ecom_user_record(web_id, date) ## fetch usert record at this day
        product_id_unique = list(set(df_user_record['product_id']))
        df_ecom_content = fetch_ecom_content(web_id, product_id_unique)

        data_usertag, i = {}, 0
        data_keywords, j = {}, 0
        ## cut keyword and generate usertag
        for index,row in df_user_record.iterrows():
            uuid, guid, product_id = row
            df_query = df_ecom_content.query(f"product_id=='{product_id}'")
            if df_query.shape[0] == 0:
                print('no valid data in dione.subscriber_browse_record')
                continue
            title, description = np.array(df_query)[0,1:]
            content = title + ' ' + description
            ## pattern for removing symbol, -,+~.
            content_clean = jieba_base.filter_symbol(content)
            keyword_list = jieba.analyse.extract_tags(content_clean, topK=8)
            keyword_list = jieba_base.clean_keyword(keyword_list, stopwords)
            keywords = ','.join(keyword_list)  ## add keywords
            ## for table, usertag_ecom
            for keyword in keyword_list:
                data_usertag[i] = {'web_id': web_id, 'uuid': uuid, 'guid': guid,
                                'title': title, 'description': description, 'keywords': keywords, 'usertag': keyword,
                                'product_id': product_id, 'date':date}
                i += 1
            ## for table, usertag_ecom_product_id
            data_keywords[j] = {'web_id': web_id, 'product_id': product_id, 'keywords': keywords,
                                'title': title, 'description': description}
            j += 1
            print(f'finish built {j}, article_id: {product_id}')
        df_usertag = pd.DataFrame.from_dict(data_usertag, "index")
        ## save to usertag_ecom
        df_usertag_save = df_usertag.drop(columns=['title', 'description', 'keywords']).drop_duplicates()
        usertag_list_dict = df_usertag_save.to_dict('records')
        query_usertag = MySqlHelper.generate_update_SQLquery(df_usertag_save, 'usertag_ecom')
        MySqlHelper('cdp').ExecuteUpdate(query_usertag, usertag_list_dict)
        ## save to usertag_ecom_product_id
        df_keywords = pd.DataFrame.from_dict(data_keywords, "index")
        keywords_list_dict = df_keywords.to_dict('records')
        query_keywords = MySqlHelper.generate_update_SQLquery(df_keywords, 'usertag_ecom_product_id')
        MySqlHelper('cdp').ExecuteUpdate(query_keywords, keywords_list_dict)
