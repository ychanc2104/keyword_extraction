import pandas as pd
from db import MySqlHelper, DBhelper
from media.Media import Media
from basic.date import get_date_shift, datetime_to_str, get_yesterday, to_datetime, get_today
from basic.decorator import timing
import numpy as np

@timing
def fetch_usertag(web_id, table='usertag'):
    # date_now = datetime_to_str(get_today())
    query = f"SELECT uuid, token, usertag FROM {table} where expired_date>=CURDATE() and web_id='{web_id}'"
    # query = f"SELECT uuid, token, usertag FROM usertag where web_id='{web_id}'"
    print(query)
    data = DBhelper('missioner', is_ssh=True).ExecuteSelect(query)
    df_map_save = pd.DataFrame(data=data, columns=['uuid', 'token', 'usertag'])
    return df_map_save

@timing
def fetch_black_list_keywords(web_id):
    query = f"""SELECT name FROM BW_list where web_id='{web_id}' and property=0"""
    print(query)
    data = DBhelper('missioner', is_ssh=True).ExecuteSelect(query)
    black_list = [d[0] for d in data]
    return black_list

@timing
def fetch_BW_list_keywords(web_id):
    query = f"""SELECT name,property FROM BW_list where web_id='{web_id}'"""
    print(query)
    data = DBhelper('missioner', is_ssh=True).ExecuteSelect(query)
    black_list, white_list = [], []
    for d in data:
        keyword, property = d
        if property==0: ## black
            black_list += [keyword]
        elif property==1:
            white_list += [keyword]
    return black_list, white_list

def delete_expired_rows(web_id, table='usertag', is_UTC0=False, jump2gcp=True):
    # date_now = datetime_to_str(get_today(is_UTC0=is_UTC0))
    query = f"DELETE FROM {table} where expired_date<CURDATE() and web_id='{web_id}'"
    print(query)
    DBhelper('missioner', is_ssh=jump2gcp).ExecuteDelete(query)

def count_unique(data_dict):
    for key, value in data_dict.items():
        data_dict[key] = len(set(value))
    return data_dict


@timing
def keyword_usertag_report(web_id, expired_date=None, usertag_table='usertag', report_table='usertag_report',
                           is_UTC0=False, jump2gcp=True, is_save=True, delete_expired_report=True):
    if expired_date==None:
        expired_date = get_date_shift(days=-4, to_str=True, is_UTC0=is_UTC0)
    # for web_id in web_id_all:
    ### collect report
    df_map = fetch_usertag(web_id, usertag_table) ## fetch all usertags when expired_date >= today
    if df_map.size == 0:
        print('no valid data in missioner.usertag')
    ## count term frequency
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
        if i%10000==0:
            print(f"finish add counting, {i}/{L}")
    token_dict = count_unique(token_dict)
    uuid_dict = count_unique(uuid_dict)
    ## build a dict to save to Dataframe (faster version for adding components)
    data_save = {}
    i = 0
    for usertag, term_freq in usertag_dict.items():
        data_save[i] = {'web_id': web_id, 'usertag': usertag, 'term_freq': term_freq,
                        'token_count': token_dict[usertag], 'uuid_count': uuid_dict[usertag],
                        'expired_date': expired_date, 'enable': 1}
        i += 1
    ## change enable=0 if in black list
    black_list = fetch_black_list_keywords(web_id)
    for i,data in data_save.items():
        usertag = data['usertag']
        if usertag in black_list:
            data_save[i]['enable'] = 0
    ## build Dataframe
    df_freq_token = pd.DataFrame.from_dict(data_save, "index")
    ## remove too small token_count, remove small number first
    token_count_list = list(df_freq_token.token_count)
    n_row = len(token_count_list)
    # df_freq_token = df_freq_token[df_freq_token.token_count > 5]
    # freq_mean = np.mean(token_count_list)
    if n_row < 500:
        df_freq_token = df_freq_token[df_freq_token.token_count > 2]
        freq_limit = np.mean(token_count_list) - 50
        print(f"only take {n_row} keywords, filter out token_count is greater than {freq_limit}")
    else:
        freq_limit = np.percentile(token_count_list, [100 * (1 - 500/n_row)])[0]
        print(f"only take top500 keywords, filter out percentile which token_count is greater than {freq_limit}")
    df_freq_token = df_freq_token[df_freq_token.token_count > freq_limit]
    ## convert int to sort
    df_freq_token[['term_freq', 'token_count', 'uuid_count']] = df_freq_token[
        ['term_freq', 'token_count', 'uuid_count']].astype('int')
    if is_save:
        ## save to db, clean_df(*args, df_search, columns, columns_drop, columns_rearrange)
        # query = f"REPLACE INTO {report_table} (web_id, usertag, term_freq, token_count, uuid_count, expired_date) VALUES (:web_id, :usertag, :term_freq, :token_count, :uuid_count, :expired_date)"
        query = DBhelper.generate_insertDup_SQLquery(df_freq_token, report_table,
                                                     ['term_freq','token_count','uuid_count','expired_date','enable'])
        DBhelper('missioner', is_ssh=jump2gcp).ExecuteUpdate(query, df_freq_token.to_dict('records'))
    if delete_expired_report:
        ## delete expired data
        delete_expired_rows(web_id, table='usertag_report', jump2gcp=jump2gcp)
    return df_freq_token


if __name__ == '__main__':
    web_id = 'upmedia'
    black_list, white_list = fetch_BW_list_keywords(web_id)
    df_freq_token = keyword_usertag_report(web_id, usertag_table='usertag', report_table='usertag_report',
                                           is_save=False, delete_expire=False)
    # web_id_all = Media().fetch_web_id()
    # # web_id_all = ['pixnet']
    # for web_id in web_id_all:
    #     keyword_usertag_report(web_id, usertag_table='usertag', report_table='usertag_report', jump2gcp=True)


    # t_start = time.time()
    # is_ssh = False
    # web_id_all = Media().fetch_web_id()
    # web_id_all = ['pixnet']
    # expired_date = get_date_shift(days=-3, to_str=True) ## set to today + 3
    # for web_id in web_id_all:
    #     ### collect report
    #     df_map = fetch_usertag(web_id)
    #     if df_map.size == 0:
    #         print('no valid data in missioner.usertag')
    #         continue
    #     ## count term frequency
    #     usertag_dict, token_dict, uuid_dict = {}, {}, {}
    #     usertags, tokens, uuids = list(df_map['usertag']), list(df_map['token']), list(df_map['uuid'])
    #     L = len(usertags)
    #     i = 0
    #     for usertag, token, uuid in zip(usertags, tokens, uuids):
    #         if usertag not in usertag_dict.keys():  # add a set
    #             usertag_dict[usertag] = 1
    #             token_dict[usertag] = [token]
    #             uuid_dict[usertag] = [uuid]
    #         else:
    #             usertag_dict[usertag] += 1
    #             token_dict[usertag] += [token]
    #             uuid_dict[usertag] += [uuid]
    #         i += 1
    #         print(f"finish add counting to {usertag}, {i}/{L}")
    #     token_dict = count_unique(token_dict)
    #     uuid_dict = count_unique(uuid_dict)
    #     ## build a dict to save to Dataframe (faster version for adding components)
    #     data_save = {}
    #     i = 0
    #     for usertag, term_freq in usertag_dict.items():
    #         data_save[i] = {'web_id': web_id, 'usertag': usertag, 'term_freq': term_freq, 'token_count': token_dict[usertag],
    #                         'uuid_count': uuid_dict[usertag], 'expired_date': expired_date}
    #         i += 1
    #     ## build Dataframe
    #     df_freq_token = pd.DataFrame.from_dict(data_save, "index")
    #     ## remove too small token_count, remove small number first
    #     token_count_list = list(df_freq_token.token_count)
    #     n_row = len(token_count_list)
    #     # df_freq_token = df_freq_token[df_freq_token.token_count > 5]
    #     # freq_mean = np.mean(token_count_list)
    #     if n_row < 500:
    #         df_freq_token = df_freq_token[df_freq_token.token_count > 2]
    #         freq_limit = np.mean(token_count_list)
    #         print(f"only take {n_row} keywords, filter out token_count is greater than {freq_limit}")
    #     else:
    #         freq_limit = np.percentile(token_count_list, [100*(1 - 500/n_row)])[0]
    #         print(f"only take top500 keywords, filter out percentile which token_count is greater than {freq_limit}")
    #     df_freq_token = df_freq_token[df_freq_token.token_count > freq_limit]
    #     ## convert int to sort
    #     df_freq_token[['term_freq', 'token_count', 'uuid_count']] = df_freq_token[
    #         ['term_freq', 'token_count', 'uuid_count']].astype('int')
    #
    #
    #     ## save to db, clean_df(*args, df_search, columns, columns_drop, columns_rearrange)
    #     usertag_report_list_dict = df_freq_token.to_dict('records')
    #     query = "REPLACE INTO usertag_report (web_id, usertag, term_freq, token_count, uuid_count, expired_date) VALUES (:web_id, :usertag, :term_freq, :token_count, :uuid_count, :expired_date)"
    #     MySqlHelper('missioner', is_ssh=is_ssh).ExecuteUpdate(query, usertag_report_list_dict)
    #     # MySqlHelper('missioner').ExecuteInsert('usertag_report', usertag_report_list_dict)
    #     ## delete expired data
    #     delete_expired_rows(web_id, table='usertag_report')
    #     #
    #     # for usertag in usertags:
    #     #     if usertag not in freq_dict.keys():  # add a set
    #     #         freq_dict[usertag] = 1
    #     #     else:
    #     #         freq_dict[usertag] += 1
    #     # print(f'count usertag: {usertag}')
    #     # ## count unique uuid
    #     # usertag_freq_uuid_toekn_list = []
    #     # ## save to dict
    #     # data_save = {}
    #     # i = 0
    #     # for usertag, term_freq in freq_dict.items():
    #     #     token_count = len(df_map.query(f"usertag=='{usertag}'")['token'].unique())
    #     #     uuid_count = len(df_map.query(f"usertag=='{usertag}'")['uuid'].unique())
    #     #     # usertag_freq_uuid_toekn_list += [[web_id, usertag, term_freq, token_count, uuid_count, expired_date]]
    #     #
    #     #     data_save[i] = {'web_id': web_id, 'usertag': usertag, 'term_freq': term_freq, 'token_count': token_count,
    #     #                     'uuid_count': uuid_count, 'expired_date': expired_date}
    #     #     i += 1
    #     #     print(f'finish {i}, usertag: {usertag}')
    #     # ## build Dataframe
    #     # df_freq_token = pd.DataFrame.from_dict(data_save, "index")
    #     # ## remove too small token_count
    #     # df_freq_token = df_freq_token[df_freq_token.token_count > 5]
    #     #
    #     # ## convert int to sort
    #     # df_freq_token[['term_freq', 'token_count', 'uuid_count']] = df_freq_token[
    #     #     ['term_freq', 'token_count', 'uuid_count']].astype('int')
    #     # ## save to db, clean_df(*args, df_search, columns, columns_drop, columns_rearrange)
    #     # usertag_report_list_dict = df_freq_token.to_dict('records')
    #
    #
    #     # query = "REPLACE INTO usertag_report (web_id, usertag, term_freq, token_count, uuid_count, expired_date) VALUES (:web_id, :usertag, :term_freq, :token_count, :uuid_count, :expired_date)"
    #     # MySqlHelper('missioner', is_ssh=is_ssh).ExecuteUpdate(query, usertag_report_list_dict)
    #     # # MySqlHelper('missioner').ExecuteInsert('usertag_report', usertag_report_list_dict)
    #     # ## delete expired data
    #     # delete_expired_rows(web_id, table='usertag_report')
    #
    # t_end_program = time.time()
    # spent_time_program = t_end_program - t_start
    # print(f'One round spent: {spent_time_program} s')
