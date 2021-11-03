from db.mysqlhelper import MySqlHelper
from basic.decorator import timing
from keyword_ad_v2 import Keyword_ad
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from sklearn.decomposition import PCA
from sklearn.mixture import GaussianMixture
from ckiptagger import WS
import jieba

def day_month(date):
    if type(date) == str:
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    year = date.year
    month = date.month
    d = datetime.date(year + int(month/12), month%12+1, 1)-datetime.timedelta(days=1)
    return d.day

def date_count(date):
    if type(date) == str:
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    year = date.year
    count = 0
    for i in range(date.month-1):
        date_i = datetime.datetime.strptime(f'{year}-{i+1}-01', '%Y-%m-%d')
        count += day_month(date_i)
    count += date.day
    return count

## date to number (1-365)
def date2count(data_list):
    results_list = []
    for data in data_list:
        results_list += [list(data)]
        count = date_count(results_list[-1][-1])
        results_list[-1][-1] = count
    results_matrix = np.array(results_list)
    df = pd.DataFrame(data=results_matrix, columns=['web_id', 'title', 'transcation', 'click', 'revenue', 'date'])
    return df

def collect_seq(df, title):
    '''
    :param df: dataframe
    :param attr: column name of db_table, web_id, title, transcation, click, revenue, hour, date
    :return: result: matrix y, days:y[:,0], revenue:y[:,1]
    '''
    # titles_set = set(df['title'])
    # n_day = max(set(df['date'])) - min(set(df['date']))+1
    # x = np.array([min(set(df['date']))+i for i in range(n_day)])
    data_collect = np.array(df.query(f"title == '{title}'"))
    x = data_collect[:, -1]
    y = data_collect[:, -2]
    return np.array([x,y]).T

@timing
def sqlmonth(web_id, year, month):
    date_start = f'{year}-{month}-01'
    date_end = f'{year}-{month}-{day_month(date_start)}'
    print(f'date_start: {date_start}, date_end: {date_end}')
    data = MySqlHelper('cdp').ExecuteSelect(
        # f"-- SELECT web_id, title, transcation, click, revenue, hour, date FROM cdp_ad2 where date BETWEEN '{date_start}' AND '{date_end}' AND web_id='{web_id}' AND revenue!=0")
        f"SELECT web_id, title, sum(transcation) as transcation, sum(click) as click, sum(revenue) as revenue, date FROM cdp_ad where date BETWEEN '{date_start}' AND '{date_end}' AND web_id='{web_id}' AND revenue!=0 group by title, date")
    return data

@timing
def sqldate(web_id, date_start, date_end):
    data = MySqlHelper('cdp').ExecuteSelect(
        f"SELECT web_id, title, sum(transcation) as transcation, sum(click) as click, sum(revenue) as revenue, date FROM cdp_ad where date BETWEEN '{date_start}' AND '{date_end}' AND web_id='{web_id}' AND revenue!=0 group by title, date")
    return data

## zero-padding to today-1
def zero_padding(days, revenues):
    today_count = date_count(datetime.datetime.today())
    days_full = np.arange(1, today_count)
    revenues_full = []
    for day in days_full:
        if day in days:
            index = np.where(days == day)
            revenues_full += [revenues[index]]
        else:
            revenues_full += [0]
    return days_full, np.array(revenues_full)

## get power spectral density
def get_PSD(days, signal):
    n = int(len(signal)/2)
    index = np.argsort(days)
    signal = signal[index]
    fs = 1 ## day
    signal_fft = np.fft.fft(signal)
    phase = np.angle(signal_fft)
    psd = signal_fft*np.conjugate(signal_fft)/(len(signal))
    freq = np.linspace(0, 1/fs, len(psd))
    return freq[0:n], psd[0:n], phase[0:n]

def reduce_dim(matrix, dim=2):
    '''
    :param matrix: (n_samples, n_features)
    :param dim: number of reduced dim
    :return:
    '''
    pca = PCA(n_components=dim)
    pca.fit(matrix)
    matrix_r = pca.transform(matrix)
    return matrix_r, pca

def normailze(matrix):
    mean = np.mean(matrix, axis=0)
    std = np.std(matrix, axis=0, ddof=1)
    matrix_nor = (matrix-mean)/std
    return matrix_nor


if __name__ == '__main__':
    ## fetch data from db
    data = sqldate('i3fresh', '2021-01-01', '2021-08-31')
    data_set = list(set(data))
    df = date2count(data_set)
    titles = np.array(df['title'])
    KW_ad = Keyword_ad()

    ## clean titles
    titles_cn = [KW_ad.remove_en_num(title) for title in titles]
    KW_ad._load_kw_config(in_sub_folder=True)
    KW_ad._load_cut_config(in_sub_folder=True)

    ## 1. use ckip
    # path_data = r'../gitignore/data'
    # ws = WS(path_data)
    # keyword_item = [ws([title])[0] for title in titles_cn]
    ## 2. use jieba extract
    # keyword_item = [KW_ad.analyze_keyword(title,topK=3,allowPOS=()) for title in titles_cn]
    ## 3. use jieba cut
    keyword_item = [list(jieba.cut(title)) for title in titles_cn]
    ## load word2vector model and get each mean vector
    KW_ad.composer_gensim.load_model('../gensim_compose/word2vec.model')
    vector_item = [KW_ad.composer_gensim.mean_word2vector(keyword) for keyword in keyword_item]
    matrix = normailze(np.array(vector_item))
    matrix_r, pca = reduce_dim(matrix, dim=10)
    ## visualization
    fig = plt.figure()
    ax = Axes3D(fig)
    ax.scatter(matrix_r[:,0], matrix_r[:,1], matrix_r[:,2],'o')
    ## get optimal n_components
    BICs = []
    n_clusters = np.arange(10,30)
    for c in n_clusters:
        model = GaussianMixture(n_components=c)
        model.fit(matrix_r)
        label = model.predict(matrix_r)
        BICs += [model.bic(matrix_r)]
    n_components = n_clusters[np.argmin(BICs)]
    model = GaussianMixture(n_components=10, tol=1e-5) ## or use n_components
    ##  fit the model
    model.fit(matrix_r)
    ##  assign a cluster to each example
    label = model.predict(matrix_r)

    ## visualization clustering results
    colors = ['r', 'g', 'b', 'k', 'm', 'y', 'c', 'silver', 'lightcoral', 'maroon', 'sienna', 'bisque', 'tan', 'orange']
    plt.figure()
    for i,l in enumerate(label):
        plt.plot(matrix_r[i,0], matrix_r[i,1], '*', color=colors[l])


