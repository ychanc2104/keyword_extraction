from db.mysqlhelper import MySqlHelper
from db.mysqlconnector import MysqlConnector
from basic.decorator import timing
from basic.date import to_datetime, get_yesterday
from basic.filter import MA
from sqlalchemy.sql import text
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


class Ecom:
    def __init__(self, web_id='i3fresh'): ##
        self.web_id = web_id
        self.date_min = '2020-12-01'

    @timing
    def insert_daily_report(self, web_id, mode_insert='all', date_start=None, is_reformat_product_id=False):
        date_yesterday = get_yesterday()
        if mode_insert == 'all':
            df = self.sqldate(web_id, self.date_min, date_yesterday, is_reformat_product_id=is_reformat_product_id)
        elif mode_insert == 'one':
            df = self.sqldate(web_id, date_start, date_start, is_reformat_product_id=is_reformat_product_id)
        elif mode_insert == 'assign':
            df = self.sqldate(web_id, date_start, date_yesterday, is_reformat_product_id=is_reformat_product_id)
        elif mode_insert == 'yesterday':
            df = self.sqldate(web_id, date_yesterday, date_yesterday) ## latest data do not require get new product_id

        if np.array(df).size == 0:
            print('data is empty')
            return []
        else:
            ## remove useless column
            df = df.drop(columns=['date_count'])
            # df = df.rename(columns={'transcation':'transaction'}) ## fix typing error in cdp_ad2
            data_list_of_dict = df.to_dict('records')
            # query = """INSERT INTO cdp_ad_daily (web_id,title,transaction,click,revenue,date) VALUES (:web_id,:title,:transcation,:click,:revenue,:date)"""
            # MySqlHelper('cdp').ExecuteUpdate(text(query), data_list_of_dict)
            query = """REPLACE INTO cdp_ad_daily (web_id,product_id,title,transaction,click,revenue,date) 
                                          VALUES (:web_id,:product_id,:title,:transaction,:click,:revenue,:date)"""
            print(query)
            MySqlHelper('cdp').ExecuteUpdate(query, data_list_of_dict)
            # MySqlHelper('cdp').ExecuteInsert('cdp_ad_daily', data_list_of_dict)
            return df

    @timing
    def sqldate(self, web_id, date_start, date_end, use_daily=False, is_reformat_product_id=False):
        if type(date_start) == datetime.datetime:
            date_start = date_start.strftime('%Y-%m-%d')
        if type(date_end) == datetime.datetime:
            date_end = date_end.strftime('%Y-%m-%d')
        if use_daily:
            query = f"SELECT web_id, product_id, title, transaction, click, revenue, date FROM cdp_ad_daily where date BETWEEN '{date_start}' AND '{date_end}' AND web_id='{web_id}'"
        else:
            query = f"SELECT web_id, product_id, title, sum(transcation) as transaction, sum(click) as click, sum(revenue) as revenue, date FROM cdp_ad2 where date BETWEEN '{date_start}' AND '{date_end}' AND web_id='{web_id}' AND revenue>0 AND url!='_' group by title, date"
        print(query)
        data = MySqlHelper('cdp').ExecuteSelect(query)
        data = list(set(data)) ## remove repetitive terms
        if data == []:
            return []
        else:
            self.df = self._date2count(data, date_start, is_reformat_product_id=is_reformat_product_id)
            return self.df

    ## date to number (1-365)
    def _date2count(self, data_list, date_start, is_reformat_product_id=False):
        results_list = []
        date_list = [data[-1] for data in data_list]
        date_list.sort()
        # date_start = date_list[0]
        # date_end = date_list[-1]
        for data in data_list:
            results_list += [list(data)]
            date = results_list[-1][-1]
            count = self._date_count(date, date_ref=date_start)
            # results_list[-1][-1] = count
            results_list[-1] += [count]
        results_matrix = np.array(results_list)
        df = pd.DataFrame(data=results_matrix, columns=['web_id', 'product_id', 'title', 'transaction', 'click', 'revenue', 'date', 'date_count'])
        if is_reformat_product_id:
            df_reformat = self._reformat_product_id(df)
            return df_reformat
        else:
            return df


    def _reformat_product_id(self, df):
        data = np.array(df)
        titles = list(set(df['title']))
        dict_map = self.fetch_product_id_by_title(titles)
        print(f"dict_map is {dict_map}")
        data_dict = {}
        for i, row in enumerate(data):
            web_id, product_id, title, transaction, click, revenue, date, date_count = row
            print(f"title is {title}")
            ## update product_id
            if title in dict_map:
                product_id_new = dict_map[title]  # get product_id
                print(f"found title in mapping dict, new product_id is {product_id_new}")
            else:
                product_id_new = '_'
            data_dict[i] = {'web_id': web_id, 'product_id': product_id_new, 'title': title, 'transaction': transaction,
                            'click': click, 'revenue': revenue, 'date': date, 'date_count': date_count}
        df_reformat = pd.DataFrame.from_dict(data_dict, "index")
        return df_reformat


    def fetch_product_id_by_title(self, titles, ):
        query = f"SELECT title, product_id FROM ecom_table WHERE title in ("
        for i, title in enumerate(titles):
            if title.find("'") != -1:  ## if find ' symbol => use "{title}"
                query += f'"{title}", '
            else:
                query += f"'{title}', "
                if i == len(titles) - 1:
                    if title.find("'") != -1:  ## if find ' symbol => use "{title}"
                        query += f'"{title}")'
                    else:
                        query += f"'{title}')"
        print(query)
        data = MySqlHelper('jupiter_new').ExecuteSelect(query)
        dict_map = {}
        for d in data:
            dict_map[d[0]] = d[1]
        return dict_map

    def ft_extrapolation(self, t, signal, day_predict=7, n_harm=None, fs=1, fig=False, xlabel='Day', ylabel='Revenue', detrend=True):
        n = len(signal)
        if n_harm == None:
            n_harm = int(n)  # number of used harmonics in model
        if detrend:
            p = np.polyfit(t, signal, 1)  # find linear trend in signal
            signal_notrend = signal - p[0] * t  # detrended signal
        else:
            signal_notrend = signal
        signal_ft = np.fft.fft(signal_notrend)  # detrended x in frequency domain
        freq = np.linspace(0, 1/fs, n)  # frequencies
        indexes = list(range(n))
        freq, signal_ft, indexes = self._remove_aliasing(freq, signal_ft, indexes)  # remove aliasing spectrum
        # indexes.sort(key=lambda i: np.absolute(freq[i]))  # sort indexes by frequency, lower -> higher
        indexes.sort(key=lambda i: np.absolute(signal_ft[i]), reverse=True)  # sort by amp of signal_ft, DESC
        t_predict = np.append(t, np.arange(max(t) + 1, max(t)+1 + day_predict))
        restored_sig = np.zeros(t_predict.size)
        for i in indexes[:1 + n_harm]:  # use low frequency
            ampli = np.absolute(signal_ft[i]) / n  # amplitude
            phase = np.angle(signal_ft[i])  # phase
            restored_sig += ampli * np.cos(2 * np.pi * freq[i] * t_predict + phase) # inverse Fourier transform
        if detrend:
            restored_sig = restored_sig + p[0] * t_predict
        if fig:
            fig, ax = plt.subplots()
            ax.plot(t, signal, '-o', color='b')
            ax.plot(t_predict[-day_predict:], restored_sig[-day_predict:], '-*', color='r')
            ax.set_xlabel(xlabel, fontsize=14)
            ax.set_ylabel(ylabel, fontsize=14)
        restored_sig[restored_sig<0] = 0
        self.freq, self.signal_ft, self.phase = freq, signal_ft, np.angle(signal_ft)
        self.day, self.signal = t, signal
        self.day_pred, self.signal_pred = t_predict, restored_sig
        self.day_pred_cut, self.signal_pred_cut = t_predict[-day_predict:], restored_sig[-day_predict:]
        return t_predict, restored_sig

    def validate_ft_extrapolation(self, t, signal, day_predict=30, n_harm=None, fs=1, fig=True, xlabel='Day', ylabel='Revenue', detrend=True):
        t_cut = t[:-day_predict]
        signal_cut = signal[:-day_predict]
        t_predict, restored_sig = self.ft_extrapolation(t_cut, signal_cut, day_predict, n_harm, fs, fig=False, detrend=detrend)
        if fig:
            fig, ax = plt.subplots()
            ax.plot(t, signal, '-o', color='b')
            # ax.plot(t, MA(signal,7), color='black')
            ax.plot(t_predict[-day_predict:], restored_sig[-day_predict:], '-*', color='r')
            ax.plot(t_predict[:-day_predict], restored_sig[:-day_predict], color='g')
            # ax.plot(t_predict, restored_sig, '-*', color='g')
            ax.set_xlabel(xlabel, fontsize=14)
            ax.set_ylabel(ylabel, fontsize=14)
            ax.set_xlim(t_predict[-day_predict-10], t_predict[-1])
        return fig

    def fetch_all_seq(self, web_id, date_start, date_end, use_daily=False):
        df = self.sqldate(web_id, date_start, date_end, use_daily=use_daily)
        if np.array(df).size == 0:
            return [] ,[]
        else:
            df_title = self.collect_df_title(df, date_start, date_end)
            return df, df_title


    def collect_df_title(self, df, date_start, date_end):
        titles = list(set(df['title']))
        revenues_list =[]
        for title in titles:
            days, revenues = self.collect_seq(df, title)
            days, revenues = self._zero_padding(days, revenues, date_start, date_end)
            revenues_list += [revenues.ravel()]
        df_title = pd.DataFrame(data=np.array(revenues_list).T, columns=titles, index=days)
        return df_title

    def collect_seq(self, df, title):
        '''
        :param df: dataframe
        :param attr: column name of db_table, web_id, title, transcation, click, revenue, hour, date
        :return: result: days, revenues; both are int-array(n,)
        '''
        if title.find("'") != -1: ## if find ' symbol => use "{title}"
            data_collect = np.array(df.query(f'title == "{title}"'))
        else:
            data_collect = np.array(df.query(f"title == '{title}'"))
        days = data_collect[:, -1].astype('int')
        revenues = data_collect[:, -3].astype('int')
        return days, revenues

    @timing
    def sqlmonth(self, year, month):
        web_id = self.web_id
        date_start = f'{year}-{month}-01'
        date_end = f'{year}-{month}-{self._day_month(date_start)}'
        print(f'date_start: {date_start}, date_end: {date_end}')
        data = MySqlHelper('cdp').ExecuteSelect(
            # f"-- SELECT web_id, title, transcation, click, revenue, hour, date FROM cdp_ad2 where date BETWEEN '{date_start}' AND '{date_end}' AND web_id='{web_id}' AND revenue!=0")
            f"SELECT web_id, title, sum(transcation) as transaction, sum(click) as click, sum(revenue) as revenue, date FROM cdp_ad2 where date BETWEEN '{date_start}' AND '{date_end}' AND web_id='{web_id}' AND revenue!=0 group by title, date")
        data = list(set(data)) ## remove repetitive terms
        self.df = self._date2count(data)
        return self.df


    @timing
    def fetch_hot(self, web_id, date_start, date_end, n_item=10):
        data = MySqlHelper('cdp').ExecuteSelect(
            f"SELECT title, sum(revenue) as revenue, date FROM cdp_ad2 where date BETWEEN '{date_start}' AND '{date_end}' AND web_id='{web_id}' AND revenue!=0 group by title order by revenue desc limit {n_item}")
        titles = [row['title'] for row in data]
        return titles

    @timing
    def fetch_all_web_id(self):
        query = f"SELECT web_id FROM cdp_management_table"
        data =MySqlHelper('cdp').ExecuteSelect(query)
        web_id_all = list(set([d[0] for d in data]))
        # if date_end == None:
        #     date_end = self._get_yesterday()
        # # query = f"-- SELECT web_id, title, sum(transcation) as transcation, sum(click) as click, sum(revenue) as revenue, date FROM cdp_ad2 where date BETWEEN '{date_start}' AND '{date_end}' AND revenue>0 group by title, date"
        # # query = f"-- SELECT web_id, sum(revenue) as revenue FROM cdp_ad2 where date BETWEEN '{date_start}' AND '{date_end}' AND revenue>0 group by web_id, date"
        # if use_daily:
        #     query = f"SELECT web_id, date FROM cdp_ad_daily where date BETWEEN '{date_start}' AND '{date_end}'"
        # else:
        #     query = f"SELECT web_id, date FROM cdp_ad2 where date BETWEEN '{date_start}' AND '{date_end}' AND revenue>0 group by web_id, date"
        # data =MySqlHelper('cdp').ExecuteSelect(query)
        # web_id_all = list(set([d[0] for d in data]))

        # data = MySqlHelper('cdp').ExecuteSelect(
        #     f"SELECT distinct web_id FROM cdp_ad2 where date BETWEEN '{date_start}' AND '{date_end}'")
        # web_id_all = list(np.array(data).ravel())
        return web_id_all

    def gen_signal(self, freq=[1/60,1/50,1/45,1/40,1/35,1/30], amp=[100,70,60,20,50,100], phase=[0.2,0.5,-0.3,2,1,-1,-1.5], T=300, fs=1):
        t = np.arange(1, T+1)
        signal = np.zeros(len(t))
        for f,a,theta in zip(freq,amp,phase):
            signal += np.cos(2*np.pi*f*t+theta)
        return t, signal

    ## remove half length of FT
    def _remove_aliasing(self, *args):
        output = []
        for arg in args:
            output += [arg[:int(len(arg)/2)]]
        return output

    ## zero-padding to today-1
    def _zero_padding(self, days, revenues, date_start, date_end):
        day_start = self._date_count(date_start, date_ref=date_start)
        day_end = self._date_count(date_end, date_ref=date_start)
        days_full = np.arange(day_start, day_end+1)
        revenues_full = []
        for day in days_full:
            if day in days:
                index = np.where(days == day)
                revenues_full += [revenues[index]]
            else:
                revenues_full += [0]
        days_pad = days_full
        revenues_pad = np.array(revenues_full, dtype=object).astype('int')
        return days_pad, revenues_pad




    def _day_month(self, date):
        '''
        :param date: string or datetime datatype
        :return: How many days in that month
        '''
        if type(date) == str:
            date = datetime.datetime.strptime(date, '%Y-%m-%d')
        year = date.year
        month = date.month
        d = datetime.date(year + int(month / 12), month % 12 + 1, 1) - datetime.timedelta(days=1)
        return d.day

    def _day_year(self, date):
        '''
        :param date: string, datetime or int year datatype
        :return: How many days in that year
        '''
        date = to_datetime(date)
        if type(date) == int:
            date = to_datetime(f'{date}-01-01')
        year = date.year
        d = self._date_count(f'{year}-12-31') - self._date_count(f'{year}-01-01')
        return d

    def _date_count(self, date, date_ref=None):
        '''
        :param date: string or datetime datatype
        :return: x-nd day of date
        '''
        date = to_datetime(date)
        year = date.year
        count = 0
        if date_ref == None:
            ref_add = 0
        else:
            date_ref = to_datetime(date_ref)
            year_range = list(range(date_ref.year, year, 1))
            ref_add = sum([self._day_year(year)+1 for year in year_range]) - self._date_count(date_ref)+1
        for i in range(date.month - 1):
            date_i = datetime.datetime.strptime(f'{year}-{i + 1}-01', '%Y-%m-%d')
            count += self._day_month(date_i)
        count += date.day + ref_add
        return count





if __name__ == '__main__':
    ecom = Ecom()
    date_yesterday = 'ecom._get_yesterday()'
    web_id = 'i3fresh'
    date_start = '2021-11-11'
    date_end = '2021-11-11'
    # df = ecom.sqldate_withid(web_id, date_start, date_end, use_daily=False)
    df = ecom.insert_daily_report(web_id, mode_insert='one', date_start=date_start)

    data_list_of_dict = df.to_dict('records')
    # query = """INSERT INTO cdp_ad_daily (web_id,title,transaction,click,revenue,date) VALUES (:web_id,:title,:transcation,:click,:revenue,:date)"""
    # MySqlHelper('cdp').ExecuteUpdate(text(query), data_list_of_dict)
    query = f"REPLACE INTO cdp.cdp_ad_daily (web_id,product_id,title,transaction,click,revenue,date) VALUES (:web_id,:product_id,:title,:transaction,:click,:revenue,:date)"
    print(query)
    MySqlHelper('cdp').ExecuteUpdate(query, data_list_of_dict)