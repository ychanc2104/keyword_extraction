import pandas as pd
from db import MySqlHelper, DBhelper
from basic import logging_channels, timing, datetime_to_str, get_yesterday, check_is_UTC0, datetime_range
from jieba_based import Composer_jieba
from log_utils import error_log
from definitions import ROOT_DIR
import jieba.analyse
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
def fetch_browse_record(web_id, datetime_start, datetime_end, is_df=False):
    # date_start = get_yesterday(is_UTC0=is_UTC0)
    # date_end = get_today(is_UTC0=is_UTC0) - datetime.timedelta(seconds=1)
    query = f"""
            SELECT 
                s.uuid,
                s.article_id,
                l.title,
                l.content,
                l.keywords
            FROM
                browse_record s
                    INNER JOIN
                article_list l ON s.article_id = l.signature                
                    AND s.web_id = '{web_id}'                
                    AND s.click_time BETWEEN '{datetime_start}' AND '{datetime_end}'
                    AND l.web_id = '{web_id}'     
            """

    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    if is_df:
        df = pd.DataFrame(data, columns=['uuid', 'article_id', 'title', 'content', 'keywords'])
        df['web_id'] = [web_id] * len(data)
        return df
    else:
        return data

# @timing
# def fetch_deleted_uuid(web_id):
#     query = f"select a.uuid from (SELECT web_id, uuid, sum(num_usertag) as n FROM web_push.usertag_uuid_stat where web_id='{web_id}' group by uuid) a where n >100"
#     print(query)
#     data = MySqlHelper('missioner', is_ssh=True).ExecuteSelect(query)
#     uuid_list = [d[0] for d in data]
#     return uuid_list

## return criteria of date to delete
@timing
def fetch_date_usertag_meet_criteria(web_id, n_limit=100):
    query = f"""
            SELECT 
                uuid, num_usertag, hour, date
            FROM
                usertag_uuid_stat
            WHERE
                uuid IN (SELECT 
                        a.uuid
                    FROM
                        (SELECT 
                            web_id, uuid, SUM(num_usertag) AS n
                        FROM
                            web_push.usertag_uuid_stat
                        WHERE
                            web_id = '{web_id}'
                        GROUP BY uuid) a
                    WHERE
                        n > {n_limit})
                    AND web_id = '{web_id}'
            ORDER BY date DESC, hour DESC
    """

    print(query)
    data = DBhelper('missioner', is_ssh=True).ExecuteSelect(query)
    data_dict = {}
    for d in data: ## order from new to old
        uuid, num_usertag, hour, date = d
        if uuid not in data_dict.keys():
            data_dict[uuid] = {"n": num_usertag, "hour": hour, "date": date}
        else:
            if data_dict[uuid]["n"] >= n_limit: ## meet criteria, delete uuid which date<=date and hour < hour
                continue
            else:
                data_dict[uuid]["n"] += num_usertag
                data_dict[uuid]["hour"] = hour
                data_dict[uuid]["date"] = date
    return data_dict

@timing
def sorting_data_dict(data_dict):
    date_dict = {}
    for uuid in data_dict:
        if data_dict[uuid]['date'] not in date_dict:
            date_dict[data_dict[uuid]['date']] = {}
        if data_dict[uuid]['hour'] not in date_dict[data_dict[uuid]['date']]:
            date_dict[data_dict[uuid]['date']][data_dict[uuid]['hour']] = []
        date_dict[data_dict[uuid]['date']][data_dict[uuid]['hour']].append(uuid)
    return date_dict


## main for deleting uuid base usertag
@logging_channels(["clare_test"])
@timing
def delete_usertag_meet_criteria(web_id, n_limit=50, disconnect=True):
    data_dict = fetch_date_usertag_meet_criteria(web_id, n_limit)
    date_dict = sorting_data_dict(data_dict)
    ## build connect session
    missioner = DBhelper('missioner', is_ssh=True)
    for date_limit in date_dict:
        for hour_limit in date_dict[date_limit]:
            uuidList = date_dict[date_limit][hour_limit]
            #### delete usertag_uuid_stat table
            ## delete same day
            query_stat_1 = f"""delete from usertag_uuid_stat where date='{date_limit}' and uuid IN ('{"','".join(uuidList)}') and web_id='{web_id}' and hour<{hour_limit}"""
            missioner.ExecuteDelete(query_stat_1, disconnect=disconnect)
            ## delete date <= date_limit_2
            query_stat_2 = f"""delete from usertag_uuid_stat where date<'{date_limit}' and uuid IN ('{"','".join(uuidList)}') and web_id='{web_id}'"""
            missioner.ExecuteDelete(query_stat_2, disconnect=disconnect)
            ## delete usertag_uuid table
            query_uuid_1 = f"""delete from usertag_uuid where date='{date_limit}' and uuid IN ('{"','".join(uuidList)}') and web_id='{web_id}' and hour<{hour_limit}"""
            missioner.ExecuteDelete(query_uuid_1, disconnect=disconnect)
            query_uuid_2 = f"""delete from usertag_uuid where date<'{date_limit}' and uuid IN ('{"','".join(uuidList)}') and web_id='{web_id}'"""
            missioner.ExecuteDelete(query_uuid_2, disconnect=disconnect)
    return missioner

@timing
def fetch_usertag_uuid_web_id():
    query = "SELECT web_id FROM web_id_table where usertag_keyword_uuid_enable=1"
    print(query)
    data = DBhelper('dione').ExecuteSelect(query)
    web_id_all = [d[0] for d in data]
    return web_id_all


@timing
def fetch_stat_from_tag_table(web_id, date, hour):
    query = f"""
            SELECT uuid, COUNT(id) as num_usertag FROM web_push.usertag_uuid
            WHERE date = '{date}' AND web_id = '{web_id}' AND hour = {hour}
            GROUP BY uuid
            """
    print(query)
    data = DBhelper('missioner', is_ssh=True).ExecuteSelect(query)
    if len(data)==0:
        return pd.DataFrame(columns=['web_id', 'uuid', 'num_usertag', 'date', 'hour'])
    df_uuid_stat = pd.DataFrame(data, columns=['uuid', 'num_usertag'])
    df_uuid_stat[['web_id', 'date', 'hour']] = [[web_id, date, hour]] * df_uuid_stat.shape[0]
    return df_uuid_stat

@timing
def collect_usertag_uuid_stat(web_id, datetime_list):
    df_uuid_stat_all = pd.DataFrame()
    for datetime in datetime_list:
        date = datetime_to_str(datetime_list[0], pattern='%Y-%m-%d')
        hour = datetime.hour
        df_uuid_stat = fetch_stat_from_tag_table(web_id, date, hour)
        df_uuid_stat_all = pd.concat([df_uuid_stat_all, df_uuid_stat])
    return df_uuid_stat_all


## main for computing uuid base usertag
@logging_channels(["clare_test"])
@timing
def main_keyword_uuid(web_id, date, jieba_base, stopwords, stopwords_usertag):
    ## fetch subscribed browse record
    # size = 13 including date+1 00:00:00
    datetime_list = datetime_range(date, num_days=1, hour_sep=2)
    for i in range(12):  ## every two hours
        hour = 2 * i
        data = fetch_browse_record(web_id, datetime_start=datetime_list[i], datetime_end=datetime_list[i + 1],
                                   is_df=False)
        n_data = len(data)
        print(f"fetch browse_record with datetime range from {datetime_list[i]} to {datetime_list[i + 1]}")
        print(f"browse_record size: {n_data}")
        if n_data == 0:
            print('no valid data in dione.subscriber_browse_record')
            continue
        ## build usertag DataFrame
        data_save = {}
        j = 0
        for i, d in enumerate(data):
            uuid, article_id, title, content, keywords = d
            news = title + ' ' + content
            ## pattern for removing https
            news_clean = jieba_base.filter_str(news, pattern="https:\/\/([0-9a-zA-Z.\/]*)")
            ## pattern for removing symbol, -,+~.
            news_clean = jieba_base.filter_symbol(news_clean)
            if (keywords == '') | (keywords == '_'):
                keyword_list = jieba.analyse.extract_tags(news_clean, topK=20)
                keyword_list = clean_keyword_list(keyword_list, stopwords, stopwords_usertag)[:5]
                is_cut = 1
            else:
                keyword_list = [k.strip() for k in keywords.split(',')]
                keyword_list = clean_keyword_list(keyword_list, stopwords, stopwords_usertag)
                is_cut = 0
            for keyword in keyword_list:
                data_save[j] = {'uuid': uuid, 'usertag': keyword, 'article_id': article_id,
                                'is_cut': is_cut, 'date': date}
                j += 1
            if i % 10000 == 0:
                print(f'finish built {i}/{n_data}')
        ## build DataFrame
        df_usertag = pd.DataFrame.from_dict(data_save, "index").drop_duplicates()
        df_usertag[['web_id', 'hour', 'date']] = [[web_id, hour, date]] * df_usertag.shape[0]
        ## save to db
        df_usertag_save = df_usertag.drop_duplicates(subset=['uuid', 'usertag'])
        # remove length of usertag > SQL table limit
        df_usertag_save = df_usertag_save[df_usertag_save['usertag'].map(len) <= 64]
        query = DBhelper.generate_insertDup_SQLquery(df_usertag_save, 'usertag_uuid', ['web_id', 'date', 'hour', 'article_id'])
        DBhelper.ExecuteUpdatebyChunk(df_usertag_save, 'missioner', query=query, chunk_size=100000, is_ssh=True)

    ## save uuid_stat at a time (all web_id)
    df_uuid_stat_all = collect_usertag_uuid_stat(web_id, datetime_list[:-1]).query(f"num_usertag<32767") ## smallint in SQL
    query_stat = DBhelper.generate_insertDup_SQLquery(df_uuid_stat_all, 'usertag_uuid_stat', ['num_usertag'])
    DBhelper.ExecuteUpdatebyChunk(df_uuid_stat_all, 'missioner', query=query_stat, chunk_size=100000, is_ssh=True)

if __name__ == '__main__':
    ## set is in UTC+0 or UTC+8
    is_UTC0 = check_is_UTC0()
    # jump2gcp = True
    date = datetime_to_str(get_yesterday(is_UTC0=is_UTC0)) ## compute all browsing record yesterday ad 3:10 o'clock
    # date = '2021-12-23'
    ## set up config (add word, user_dict.txt ...)
    jieba_base = Composer_jieba()
    all_hashtag = jieba_base.set_config()
    stopwords = jieba_base.get_stopword_list()
    stopwords_usertag = jieba_base.read_file('./jieba_based/stop_words_usertag.txt')
    web_id_all = fetch_usertag_uuid_web_id()
    # web_id_all = ['nownews', 'ctnews', 'pixnet', 'upmedia', 'cmoney', 'mirrormedia', 'bnetx', 'managertoday', 'btnet']
    # web_id_all = ['btnet']
    t_start_outloop = time.time()
    for web_id in web_id_all:
        main_keyword_uuid(web_id, date, jieba_base, stopwords, stopwords_usertag)
        ## delete uuid_tag exceed criteria(100)
        missioner = delete_usertag_meet_criteria(web_id, n_limit=100, disconnect=False)
        ## close sql session
        missioner.session_close()
    t_end_program = time.time()
    spent_time_program = t_end_program - t_start_outloop
    t_opt_uuid = time.time()
    DBhelper('missioner').ExecuteOptimize('usertag_uuid')
    dt_opt_uuid = time.time() - t_opt_uuid
    t_opt_stat = time.time()
    DBhelper('missioner').ExecuteOptimize('usertag_uuid_stat')
    dt_opt_stat = time.time() - t_opt_stat
    message = f"""
    One round of keyword_usertag_uuid(all web_id) spent: {spent_time_program} s
    all web_id: {','.join(web_id_all)}
    optimize usertag_uuid spent {dt_opt_uuid} s
    optimize usertag_uuid_stat spent {dt_opt_stat} s
    """
    print(message)
    error_log(message, ROOT_DIR=ROOT_DIR, filename='usertag_uuid.log')