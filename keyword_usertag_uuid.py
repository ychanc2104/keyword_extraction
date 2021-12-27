import pandas as pd
import datetime
from db.mysqlhelper import MySqlHelper
from media.Media import Media
from basic.date import get_date_shift, datetime_to_str, get_yesterday, to_datetime, get_today, check_is_UTC0, datetime_range
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
def fetch_browse_record(web_id, datetime_start, datetime_end, is_df=False):
    # date_start = get_yesterday(is_UTC0=is_UTC0)
    # date_end = get_today(is_UTC0=is_UTC0) - datetime.timedelta(seconds=1)
    query = \
        f"""
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

@timing
def fetch_deleted_uuid(web_id):
    query = f"select a.uuid from (SELECT web_id, uuid, sum(num_usertag) as n FROM web_push.usertag_uuid_stat where web_id='{web_id}' group by uuid) a where n >100"
    print(query)
    data = MySqlHelper('missioner', is_ssh=True).ExecuteSelect(query)
    uuid_list = [d[0] for d in data]
    return uuid_list

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
    data = MySqlHelper('missioner', is_ssh=True).ExecuteSelect(query)
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
def delete_usertag_meet_criteria(web_id, n_limit=100):
    data_dict = fetch_date_usertag_meet_criteria(web_id, n_limit)
    uuid_list = data_dict.keys()
    for uuid in uuid_list:
        date_limit, hour_limit = data_dict[uuid]['date'], data_dict[uuid]['hour']
        date_limit_2 = get_date_shift(date_ref=date_limit,days=1,to_str=True)
        #### delete usertag_uuid_stat table
        ## delete same day
        query_stat_1 = f"delete from usertag_uuid_stat where uuid='{uuid}' and date<='{date_limit}' and hour<{hour_limit} and web_id='{web_id}'"
        print(query_stat_1)
        MySqlHelper('missioner', is_ssh=jump2gcp).ExecuteUpdate(query_stat_1)
        ## delete date <= date_limit_2
        query_stat_2 = f"delete from usertag_uuid_stat where uuid='{uuid}' and date<='{date_limit_2}' and web_id='{web_id}'"
        MySqlHelper('missioner', is_ssh=jump2gcp).ExecuteUpdate(query_stat_2)
        ## delete usertag_uuid table
        query_uuid_1 = f"delete from usertag_uuid where uuid='{uuid}' and date<='{date_limit}' and hour<{hour_limit} and web_id='{web_id}'"
        MySqlHelper('missioner', is_ssh=jump2gcp).ExecuteUpdate(query_uuid_1)
        query_uuid_2 = f"delete from usertag_uuid where uuid='{uuid}' and date<='{date_limit_2}' and web_id='{web_id}'"
        MySqlHelper('missioner', is_ssh=jump2gcp).ExecuteUpdate(query_uuid_2)

#

if __name__ == '__main__':
    ## set is in UTC+0 or UTC+8
    is_UTC0 = check_is_UTC0()
    jump2gcp = True
    date = datetime_to_str(get_yesterday(is_UTC0=is_UTC0)) ## compute all browsing record yesterday ad 3:10 o'clock
    # date = '2021-12-23'
    # set up config (add word, user_dict.txt ...)
    jieba_base = Composer_jieba()
    all_hashtag = jieba_base.set_config()
    stopwords = jieba_base.get_stopword_list()
    stopwords_usertag = jieba_base.read_file('./jieba_based/stop_words_usertag.txt')
    web_id_all = ['nownews', 'ctnews', 'pixnet', 'upmedia', 'cmoney', 'mirrormedia', 'bnetx', 'managertoday', 'btnet']
    # web_id_all = ['cmoney']
    t_start_outloop = time.time()
    for web_id in web_id_all:
        ## fetch subscribed browse record
        datetime_list = datetime_range(date, num_days=1, hour_sep=2)
        for i in range(12): ## every two hours
            hour = 2*i
            data = fetch_browse_record(web_id, datetime_start=datetime_list[i], datetime_end=datetime_list[i+1], is_df=False)
            print(f"fetch data with datetime range from {datetime_list[i]} to {datetime_list[i+1]}\n data size: {len(data)}")
            if len(data) == 0:
                print('no valid data in dione.subscriber_browse_record')
                continue
            ## build usertag DataFrame
            t_start_inloop = time.time()
            data_save, data_stat = {}, {}
            j = 0
            for i, d in enumerate(data):
                uuid, article_id, title, content, keywords = d
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
                    # data_save[j] = {'web_id':web_id, 'uuid':uuid,
                    #                 'news':news_clean, 'keywords':keywords, 'usertag':keyword, 'article_id': article_id,
                    #                 'is_cut': is_cut, 'date': date}
                    data_save[j] = {'uuid':uuid,
                                    'usertag':keyword, 'article_id': article_id,
                                    'is_cut': is_cut, 'date': date}
                    j += 1
                data_stat[i] = {'uuid':uuid, 'num_usertag':len(keyword_list)}
                print(f'finish built {i}, article_id: {article_id}')
            ## build DataFrame
            df_usertag = pd.DataFrame.from_dict(data_save, "index").drop_duplicates()
            df_usertag['web_id'] = [web_id] * df_usertag.shape[0]
            df_usertag['hour'] = [hour] * df_usertag.shape[0]
            df_usertag['date'] = [date] * df_usertag.shape[0]

            df_statistics = pd.DataFrame.from_dict(data_stat, "index")
            ## group df_statistics
            df_statistics = df_statistics.groupby(['uuid']).sum().reset_index()
            df_statistics['web_id'] = [web_id] * df_statistics.shape[0]
            df_statistics['hour'] = [hour] * df_statistics.shape[0]
            df_statistics['date'] = [date] * df_statistics.shape[0]

            t_end = time.time()
            spent_time = t_end - t_start_inloop
            print(f'build df loop spent time: {spent_time}s')

            ## save to db
            # df_usertag_save = df_usertag.drop(columns=['news', 'keywords']).drop_duplicates()
            df_usertag_save = df_usertag.drop_duplicates()
            query = MySqlHelper.generate_update_SQLquery(df_usertag_save, 'usertag_uuid')
            MySqlHelper('missioner', is_ssh=jump2gcp).ExecuteUpdate(query, df_usertag_save.to_dict('records'))

            df_statistics_save = df_statistics.query(f"num_usertag<32767") ## smallint in SQL
            query_stat = MySqlHelper.generate_update_SQLquery(df_statistics, 'usertag_uuid_stat')
            MySqlHelper('missioner', is_ssh=jump2gcp).ExecuteUpdate(query_stat, df_statistics.to_dict('records'))
        ## delete uuid_tag exceed criteria(100)
        delete_usertag_meet_criteria(web_id)
    t_end_program = time.time()
    spent_time_program = t_end_program - t_start_outloop
    print(f'One round(all web_id) spent: {spent_time_program} s')
