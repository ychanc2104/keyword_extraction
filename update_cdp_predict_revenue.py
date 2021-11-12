from db.mysqlhelper import MySqlHelper
from basic.decorator import timing
from predict_hot_item.Ecom import Ecom
import datetime
import numpy as np
import pandas as pd
import socket
from basic.date import get_today, datetime_to_str


def add_title_clean(df):
    df_copy = df.copy()
    titles = df['title']
    titles_clean = [title.replace("'", '"') for title in titles]
    df_copy['title_clean'] = titles_clean
    return df_copy

def save_predict_revenue():
    # date_end = '2021-09-01'; #datetime.datetime.today()
    ecom = Ecom()
    ## if in server use UTC+0
    local_ip = socket.gethostbyname(socket.gethostname())
    if local_ip == '127.0.1.1': # in localhost, UTC+8
        is_UTC0 = False
    else:
        is_UTC0 = True
    date_start = '2020-12-01';  # min date: 2020-12-01
    date_end = datetime_to_str(get_today(is_UTC0=is_UTC0))
    today = date_end
    web_id_to_update = ecom.fetch_all_web_id()
    # web_id_to_update = ['kinaz']
    for web_id in web_id_to_update:
        df, df_title = ecom.fetch_all_seq(web_id, date_start, date_end, use_daily=True)
        if np.array(df).size == 0:
            print(f"this web_id {web_id} without data in cdp")
            continue

        df_clean = add_title_clean(df)  ## replace ' with "
        titles_all = list(set(df['title']))
        n = len(titles_all)
        # dict_map = ecom.fetch_product_id_by_title(titles_all)
        dict_predict = {}
        i = 0
        for title in titles_all:
            revenues = df_title[title]
            days = df_title.index
            t_predict, restored_sig = ecom.ft_extrapolation(days, revenues, day_predict=7, fig=False, n_harm=None)
            revenue_L7 = np.mean(restored_sig[-14:-7])  # last 7-day mean revenue (known)
            revenue_F7 = np.mean(restored_sig[-7:])  # future 7-day mean revenue (predict)
            diff = revenue_F7 - revenue_L7
            title_clean = title.replace("'", '"')
            df_search_id = df_clean.query(f"title_clean=='{title_clean}'")
            if np.array(df_search_id).size == 0:
                product_id = '_' ## not found
            else:
                product_id = df_search_id['product_id'].values[0]

            if revenue_L7 == 0:
                grow_rate = 0
            else:
                grow_rate = diff/revenue_L7*100
            dict_predict[i] = {'web_id':web_id, 'product_id':product_id, 'title':title, 'revenue_L7':revenue_L7,
                               'revenue_F7':revenue_F7, 'grow_rate':grow_rate, 'date':today}
            i += 1
        df_to_insert = pd.DataFrame.from_dict(dict_predict, "index")
        df_to_insert_clean = df_to_insert.drop(index=df_to_insert[df_to_insert['product_id'] == '_'].index, inplace=False)
        data_list_of_dict = df_to_insert_clean.to_dict('records')
        # MySqlHelper('cdp').ExecuteInsert('cdp_predict_revenue', data_list_of_dict)
        query = f"REPLACE INTO cdp.cdp_predict_revenue " \
                f"(web_id,product_id,title,revenue_L7,revenue_F7,grow_rate,date) " \
                f"VALUES " \
                f"(:web_id,:product_id,:title,:revenue_L7,:revenue_F7,:grow_rate,:date)"
        print(query)
        MySqlHelper('cdp').ExecuteUpdate(query, data_list_of_dict)

# update everyday
if __name__ == '__main__':

    save_predict_revenue()

    #
    # # date_end = '2021-09-01'; #datetime.datetime.today()
    # ecom = Ecom()
    # ## if in server use UTC+0
    # local_ip = socket.gethostbyname(socket.gethostname())
    # if local_ip == '127.0.1.1': # in localhost, UTC+8
    #     is_UTC0 = False
    # else:
    #     is_UTC0 = True
    # date_start = '2020-12-01';  # min date: 2020-12-01
    # date_end = datetime_to_str(get_today(is_UTC0=is_UTC0))
    # today = date_end
    # web_id_to_update = ecom.fetch_all_web_id()
    # # web_id_to_update = ['kinaz']
    # for web_id in web_id_to_update:
    #     df, df_title = ecom.fetch_all_seq(web_id, date_start, date_end, use_daily=True)
    #     if np.array(df).size == 0:
    #         print(f"this web_id {web_id} without data in cdp")
    #         continue
    #
    #     df_clean = add_title_clean(df)  ## replace ' with "
    #     titles_all = list(set(df['title']))
    #     n = len(titles_all)
    #     # dict_map = ecom.fetch_product_id_by_title(titles_all)
    #     dict_predict = {}
    #     i = 0
    #     for title in titles_all:
    #         revenues = df_title[title]
    #         days = df_title.index
    #         t_predict, restored_sig = ecom.ft_extrapolation(days, revenues, day_predict=7, fig=False, n_harm=None)
    #         revenue_L7 = np.mean(restored_sig[-14:-7])  # last 7-day mean revenue (known)
    #         revenue_F7 = np.mean(restored_sig[-7:])  # future 7-day mean revenue (predict)
    #         diff = revenue_F7 - revenue_L7
    #         title_clean = title.replace("'", '"')
    #         df_search_id = df_clean.query(f"title_clean=='{title_clean}'")
    #         if np.array(df_search_id).size == 0:
    #             product_id = '_' ## not found
    #         else:
    #             product_id = df_search_id['product_id'].values[0]
    #
    #         if revenue_L7 == 0:
    #             grow_rate = 0
    #         else:
    #             grow_rate = diff/revenue_L7*100
    #         dict_predict[i] = {'web_id':web_id, 'product_id':product_id, 'title':title, 'revenue_L7':revenue_L7,
    #                            'revenue_F7':revenue_F7, 'grow_rate':grow_rate, 'date':today}
    #         i += 1
    #     df_to_insert = pd.DataFrame.from_dict(dict_predict, "index")
    #     df_to_insert_clean = df_to_insert.drop(index=df_to_insert[df_to_insert['product_id'] == '_'].index, inplace=False)
    #     data_list_of_dict = df_to_insert_clean.to_dict('records')
    #     # MySqlHelper('cdp').ExecuteInsert('cdp_predict_revenue', data_list_of_dict)
    #     query = f"REPLACE INTO cdp.cdp_predict_revenue " \
    #             f"(web_id,product_id,title,revenue_L7,revenue_F7,grow_rate,date) " \
    #             f"VALUES " \
    #             f"(:web_id,:product_id,:title,:revenue_L7,:revenue_F7,:grow_rate,:date)"
    #     print(query)
    #     MySqlHelper('cdp').ExecuteUpdate(query, data_list_of_dict)

