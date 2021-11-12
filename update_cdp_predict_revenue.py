from db.mysqlhelper import MySqlHelper
from basic.decorator import timing
from predict_hot_item.Ecom import Ecom
import datetime
import numpy as np
import pandas as pd
import socket
from basic.date import get_today, datetime_to_str



# update everyday
if __name__ == '__main__':
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
        titles_all = list(set(df['title']))
        n = len(titles_all)
        dict_map = ecom.fetch_product_id_by_title(titles_all)
        dict_predict = {}
        i = 0
        for title in titles_all:
            revenues = df_title[title]
            days = df_title.index
            t_predict, restored_sig = ecom.ft_extrapolation(days, revenues, day_predict=7, fig=False, n_harm=None)
            revenue_L7 = np.mean(restored_sig[-14:-7])  # last 7-day mean revenue (known)
            revenue_F7 = np.mean(restored_sig[-7:])  # future 7-day mean revenue (predict)
            diff = revenue_F7 - revenue_L7
            if title in dict_map:
                product_id = dict_map[title]
            else:
                product_id = '_'
            if revenue_L7 == 0:
                grow_rate = 0
            else:
                grow_rate = diff/revenue_L7*100
            dict_predict[i] = {'web_id':web_id, 'product_id':product_id, 'title':title, 'revenue_L7':revenue_L7,
                               'revenue_F7':revenue_F7, 'grow_rate':grow_rate, 'date':today}
            i += 1

        df_to_insert = pd.DataFrame.from_dict(dict_predict, "index")
        data_list_of_dict = df_to_insert.to_dict('records')
        # MySqlHelper('cdp').ExecuteInsert('cdp_predict_revenue', data_list_of_dict)
        query = f"REPLACE INTO cdp.cdp_predict_revenue " \
                f"(web_id,product_id,title,revenue_L7,revenue_F7,grow_rate,date) " \
                f"VALUES " \
                f"(:web_id,:product_id,:title,:revenue_L7,:revenue_F7,:grow_rate,:date)"
        print(query)
        MySqlHelper('cdp').ExecuteUpdate(query, data_list_of_dict)

