import pandas as pd
import datetime
from jieba_based.jieba_utils import Composer_jieba
import jieba.analyse
import numpy as np
from db import DBhelper
from basic import get_date_shift, datetime_to_str, get_today



def fetch_white_list_keywords():
    query = f"""SELECT name FROM BW_list where property=1"""
    print(query)
    data = DBhelper('missioner', is_ssh=True).ExecuteSelect(query)
    white_list = [d[0] for d in data]
    return white_list


def fetch_ecom_history(web_id,today,yesterday):
    today=str(today)
    yesterday=str(yesterday)
    query=f"""SELECT uuid,timestamp,meta_title FROM tracker.clean_event_load
        where web_id='{web_id}' AND date_time between '{yesterday}' and '{today}'
        """
    data = DBhelper('cdp').ExecuteSelect(query)
    return pd.DataFrame(data,columns=['uuid','timetamp','meta_title'])

def fetch_title_description(web_id):
    query=f"""SELECT product_id,title,description,meta_title FROM item_list WHERE web_id='{web_id}' """
    data = DBhelper('rhea_web_push', is_ssh=True).ExecuteSelect(query)
    return pd.DataFrame(data,columns=['product_id','title','description','meta_title'])

def count_unique(data_dict):
    for key, value in data_dict.items():
        data_dict[key] = len(set(value))
    return data_dict

def fetch_usertag_ecom_webid_and_date():
    query=f"""SELECT web_id,usertag_keyword_expired_day FROM ecom_web_id_table"""
   # print(query)
    data = DBhelper('missioner', is_ssh=True).ExecuteSelect(query)
    web_id_list = [i[0] for i in data]
    expired_date_list = [i[1] for i in data]
    return web_id_list,expired_date_list

def fetch_token(web_id):
    query = f"""
     SELECT registation_id,uuid,os_platform,is_fcm FROM web_gcm_reg WHERE web_id='{web_id}'
        """
    data = DBhelper('cloud_subscribe', is_ssh=True).ExecuteSelect(query)
    data = pd.DataFrame(data, columns=['token','uuid','os_platform','is_fcm'])
    return data

def fetch_usertag(web_id, table='usertag'):
    date_now = datetime_to_str(get_today())
    query = f"SELECT uuid, token, usertag FROM {table} where expired_date>='{date_now}' and web_id='{web_id}'"
    # query = f"SELECT uuid, token, usertag FROM usertag where web_id='{web_id}'"
    print(query)
    data = DBhelper('missioner', is_ssh=True).ExecuteSelect(query)
    df_map_save = pd.DataFrame(data=data, columns=['uuid', 'token', 'usertag'])
    return df_map_save

def datetime_to_str(date, pattern='%Y-%m-%d'):
    datetime_str = datetime.datetime.strftime(date, pattern)
    return datetime_str

def count_unique(data_dict):
    for key, value in data_dict.items():
        data_dict[key] = len(set(value))
    return data_dict

def update_usertag_report(web_id):
    expired_date_s = get_date_shift(days=-4, to_str=True, is_UTC0=False)
    df_map = fetch_usertag(web_id)
    usertag_dict, token_dict, uuid_dict = {}, {}, {}
    usertags, tokens, uuids = list(df_map['usertag']), list(df_map['token']), list(df_map['uuid'])
    L = len(usertags)
    i = 0
    for usertag, token, uuid in zip(usertags, tokens, uuids):
        if usertag not in usertag_dict.keys():  # add a set
            usertag_dict[usertag] = 1
            token_dict[usertag] = [token]
            uuid_dict[usertag] = [uuid]
        else:
            usertag_dict[usertag] += 1
            token_dict[usertag] += [token]
            uuid_dict[usertag] += [uuid]
        i += 1
        if i % 10000 == 0:
            print(f"finish add counting, {i}/{L}")
    token_dict = count_unique(token_dict)
    uuid_dict = count_unique(uuid_dict)
    ## build a dict to save to Dataframe (faster version for adding components)
    data_save = {}
    i = 0
    for usertag, term_freq in usertag_dict.items():
        data_save[i] = {'web_id': web_id, 'usertag': usertag, 'term_freq': term_freq,
                        'token_count': token_dict[usertag], 'uuid_count': uuid_dict[usertag],
                        'expired_date': expired_date_s, 'enable': 1}
        i += 1
    df_freq_token = pd.DataFrame.from_dict(data_save, "index")
    token_count_list = list(df_freq_token.token_count)

    n_row = len(token_count_list)
    # df_freq_token = df_freq_token[df_freq_token.token_count > 5]
    # freq_mean = np.mean(token_count_list)
    if n_row < 500:
        df_freq_token = df_freq_token[df_freq_token.token_count > 2]
        freq_limit = np.mean(token_count_list) - 50
        print(f"only take {n_row} keywords, filter out token_count is greater than {freq_limit}")
    else:
        freq_limit = np.percentile(token_count_list, [100 * (1 - 500 / n_row)])[0]
        print(f"only take top500 keywords, filter out percentile which token_count is greater than {freq_limit}")
    df_freq_token = df_freq_token[df_freq_token.token_count > freq_limit]
    ## convert int to sort
    df_freq_token[['term_freq', 'token_count', 'uuid_count']] = df_freq_token[
        ['term_freq', 'token_count', 'uuid_count']].astype('int')
    # print(df_freq_token)
    DBhelper.ExecuteUpdatebyChunk(df_freq_token, db='missioner', table='usertag_report', chunk_size=100000,
                                     is_ssh=True)




if __name__ == '__main__':
    today = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    #today='2022-09-05'
    yesterday = datetime.datetime.utcnow() + datetime.timedelta(hours=8-24)
    jieba_base = Composer_jieba()
    all_hashtag = jieba_base.set_config()
    stopwords = jieba_base.get_stopword_list()
    stopwords_usertag = jieba_base.read_file('./jieba_based/stop_words_usertag.txt')
    white_list = fetch_white_list_keywords()
    jieba_base.add_words(white_list)
    web_id_list,expired_date_list = fetch_usertag_ecom_webid_and_date()
    for web_id,expired_date_int in zip(web_id_list,expired_date_list):
        #web_id='i3fresh'
        print(f'{web_id}\n')
        expired_date = datetime.date.today() + datetime.timedelta(expired_date_int)
        df_user_record = fetch_ecom_history(web_id, today, yesterday)
        if df_user_record.shape[0] == 0:
            print('no available data in cdp.cdp_user_event_record')
            continue
        df_item_list = fetch_title_description(web_id)
        for i in range(len(df_item_list)):
            if df_item_list.meta_title[i] == '_':
                df_item_list.meta_title[i] = df_item_list.title[i]
        df_item_list = df_item_list.drop_duplicates('meta_title')
        data = df_user_record.merge(df_item_list, on='meta_title', how='left').dropna()
        if not data.values.tolist():
            continue
        token_df = fetch_token(web_id)
        data = data.merge(token_df, on='uuid', how='left').dropna()
        data['code'] = data['os_platform'] + data['is_fcm'].astype('int').astype('str')
        data_usertag, i = {}, 0
        data_keywords, j = {}, 0
        for row in data.iterrows():
            uuid, timetamp, meta_title,product_id ,title, description,token,_,_,code = row[-1]
            date = datetime.datetime.fromtimestamp(int(timetamp) / 1000).strftime("%Y-%m-%d")
            content = title + ' ' + description
            content_clean = jieba_base.preserve_str(content)
            keyword_list = jieba.analyse.extract_tags(content_clean, topK=8)
            keyword_list = jieba_base.clean_keyword(keyword_list, stopwords)
            keywords = ','.join(keyword_list)  ## add keywords
            ## for table, usertag_ecom
            for keyword in keyword_list:
                data_usertag[i] = {'web_id': web_id,'cert_web_id':web_id ,'uuid': uuid,'code':code,'token':token,'usertag': keyword,'article_id':product_id, 'is_cut':'1','expired_date':expired_date}
                i += 1
        df_usertag = pd.DataFrame.from_dict(data_usertag, "index")
       # print(df_usertag)
        DBhelper.ExecuteUpdatebyChunk(df_usertag, db='missioner', table='usertag',chunk_size=100000, is_ssh=True)

        update_usertag_report(web_id)
