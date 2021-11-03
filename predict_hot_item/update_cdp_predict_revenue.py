from db.mysqlhelper import MySqlHelper
from basic.decorator import timing
from predict_hot_item.Ecom import Ecom
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt



# def valid_save_titles(ecom, df_title, titles, day_predict=7, n_harm=None, path_root=''):
#     for title in titles:
#         days = df_title.index
#         revenues = df_title[title]
#         fig = ecom.validate_ft_extrapolation(days, revenues, day_predict=day_predict, fig=True, n_harm=n_harm)
#         save_img(fig, f'{path_root}{title}_{day_predict}.png')
#         plt.close(fig)


# update everyday
if __name__ == '__main__':
    date_start = '2020-12-01'; # min date: 2020-12-01
    # date_end = '2021-09-01'; #datetime.datetime.today()
    ecom = Ecom()
    date_end = ecom._get_today()
    date_yesterday = ecom._get_yesterday()
    web_id_to_update = ecom.fetch_all_web_id(date_start=date_yesterday, use_daily=True)
    # web_id_toupdate = ['kinaz']
    for web_id in web_id_to_update:
        df, df_title = ecom.fetch_all_seq(web_id, date_start, date_end, use_daily=True)
        titles_all = list(set(df['title']))
        n = len(titles_all)
        web_id_all, date_all = [web_id]*n, [date_end]*n
        revenue_L7_all, revenue_F7_all = [], []
        diff_all, grow_rate_all = [], []
        columns = ['web_id', 'title', 'revenue_L7', 'revenue_F7', 'grow_rate', 'date']
        for title in titles_all:
            revenues = df_title[title]
            days = df_title.index
            t_predict, restored_sig = ecom.ft_extrapolation(days, revenues, day_predict=7, fig=False, n_harm=None)
            revenue_L7 = np.mean(restored_sig[-14:-7])  # last 7-day mean revenue (known)
            revenue_F7 = np.mean(restored_sig[-7:])  # future 7-day mean revenue (predict)
            diff = revenue_F7 - revenue_L7
            if revenue_L7 == 0:
                grow_rate = 0
            else:
                grow_rate = diff/revenue_L7*100
            revenue_L7_all += [revenue_L7]
            revenue_F7_all += [revenue_F7]
            diff_all += [diff]
            grow_rate_all += [grow_rate]
        data_to_insert = np.array([web_id_all,titles_all,revenue_L7_all,revenue_F7_all,grow_rate_all,date_all]).T
        df_to_insert = pd.DataFrame(data=data_to_insert, columns=columns)
        data_list_of_dict = df_to_insert.to_dict('records')
        MySqlHelper('cdp').ExecuteInsert('cdp_predict_revenue', data_list_of_dict)
