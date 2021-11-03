from basic.decorator import timing
from gensim_compose.embedding import Composer
from db.mysqlhelper import MySqlHelper
from strsimpy.jaro_winkler import JaroWinkler
from strsimpy.longest_common_subsequence import LongestCommonSubsequence
from strsimpy.damerau import Damerau
from basic.date import get_hour, date2int, get_today, get_yesterday, get_date_shift, datetime_to_str
import pandas as pd
from media.Media import Media
import numpy as np
import time

@timing
def fetch_keywords_7day(web_id):
    date_start = date2int(datetime_to_str(get_date_shift(6)))
    date_end = date2int(datetime_to_str(get_today()))
    query = f"SELECT keyword FROM missoner_keyword WHERE web_id='{web_id}' and date BETWEEN {date_start} and {date_end}"
    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    keywords = [d[0] for d in data]
    return list(set(keywords))

@timing
def fetch_keywords_2day(web_id, is_UTC0=False):
    date_start = date2int(datetime_to_str(get_date_shift(1, is_UTC0=is_UTC0)))
    date_end = date2int(datetime_to_str(get_today(is_UTC0=is_UTC0)))
    query = f"SELECT keyword FROM missoner_keyword WHERE web_id='{web_id}' and date BETWEEN {date_start} and {date_end}"
    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    keywords = [d[0] for d in data]
    return list(set(keywords))

@timing
def fetch_keywords(web_id, date):
    date_int = date2int(date)
    query = f"SELECT keyword FROM missoner_keyword WHERE web_id='{web_id}' and date={date_int}"
    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    keywords = [d[0] for d in data]
    return list(set(keywords))

@timing
def insert_keyword_similarity(date):
    composer = Composer()
    composer.load_model(path='./gensim_compose/word2vec_zh_only.model')
    damerau = Damerau()
    ## set up media
    media = Media()
    web_id_all = media.fetch_web_id()
    web_id_all = ['ctnews']
    for web_id in web_id_all:
        keywords = np.array(fetch_keywords(web_id, date))
        similarity_matrix = np.array([[composer.similarity(k1,k2) for k1 in keywords] for k2 in keywords])
        print(f"finish build similarity_matrix")
        damerau_matrix = np.array([[damerau.distance(k1,k2) for k1 in keywords] for k2 in keywords])
        print(f"finish build damerau_matrix")
        dict_save = {}
        i, j = 0, 0
        weight = 2; ## weighting for similarity_matrix
        for similarity_row, damerau_row in zip(similarity_matrix,damerau_matrix):
            keyword = keywords[i]
            n_keyword = len(keyword)
            score_row = weight*similarity_row - damerau_row/n_keyword ## add similarity and damerau, bigger is more similar
            index = np.argsort(score_row)[::-1] ## order ASC
            score_row_sort = np.sort(score_row)[::-1] ## order ASC
            index_select = index[score_row_sort > 0] ## filter out score < 0
            # keyword_related = keywords[index_select][1:11] ## remove self and choose top10
            # scores = score_row[index_select][1:11]
            keyword_related = keywords[index_select][:10] ## choose top10 including self
            scores = score_row[index_select][:10]
            i += 1
            for k_related, score in zip(keyword_related, scores):
                dict_save[j] = {'web_id': web_id, 'keyword': keyword, 'keyword_related': k_related, 'score': score}
                j += 1
                print(f"finish {j}, keyword: {keyword}, keyword_related: {keyword_related}, score: {score}")
        df_save = pd.DataFrame.from_dict(dict_save, "index")
        df_save_list_dict = df_save.to_dict('records')
        query = "REPLACE INTO missoner_keyword_similarity (web_id, keyword, keyword_related, score) VALUES (:web_id, :keyword, :keyword_related, :score)"
        print(query)
        MySqlHelper('dione', is_ssh=False).ExecuteUpdate(query, df_save_list_dict)
        return df_save

if __name__ == '__main__':
    ## set up params
    t_start = time.time()
    is_UTC0 = True
    today = get_today(is_UTC0=is_UTC0)
    yesterday = get_yesterday(is_UTC0=is_UTC0)
    hour_now = get_hour(is_UTC0=is_UTC0)
    if (hour_now >= 4) and (hour_now <= 7):
        ## update yesterday once
        df_save_y = insert_keyword_similarity(yesterday)
        print(f"finish updating yesterday")
        ## routine for today
        df_save = insert_keyword_similarity(today)
        print(f"finish routine everyday")
    else:
        ## routine for today
        df_save = insert_keyword_similarity(today)
        print(f"finish routine everyday")



    #
    # composer = Composer()
    # composer.load_model(path='./gensim_compose/word2vec_zh_only.model')
    # damerau = Damerau()
    # ## set up media
    # media = Media()
    # web_id_all = media.fetch_web_id()
    # web_id_all = ['ctnews']
    # for web_id in web_id_all:
    #     # keywords = np.array(fetch_keywords_7day(web_id))
    #     keywords = np.array(fetch_keywords_2day(web_id, is_UTC0=is_UTC0))
    #     similarity_matrix = np.array([[composer.similarity(k1,k2) for k1 in keywords] for k2 in keywords])
    #     print(f"finish build similarity_matrix")
    #     damerau_matrix = np.array([[damerau.distance(k1,k2) for k1 in keywords] for k2 in keywords])
    #     print(f"finish build damerau_matrix")
    #     dict_save = {}
    #     i, j = 0, 0
    #     weight = 2; ## weighting for similarity_matrix
    #     for similarity_row, damerau_row in zip(similarity_matrix,damerau_matrix):
    #         keyword = keywords[i]
    #         n_keyword = len(keyword)
    #         score_row = weight*similarity_row - damerau_row/n_keyword ## add similarity and damerau, bigger is more similar
    #         index = np.argsort(score_row)[::-1] ## order ASC
    #         score_row_sort = np.sort(score_row)[::-1] ## order ASC
    #         index_select = index[score_row_sort > 0] ## filter out score < 0
    #         # keyword_related = keywords[index_select][1:11] ## remove self and choose top10
    #         # scores = score_row[index_select][1:11]
    #         keyword_related = keywords[index_select][:10] ## choose top10 including self
    #         scores = score_row[index_select][:10]
    #         i += 1
    #         for k_related, score in zip(keyword_related, scores):
    #             dict_save[j] = {'web_id': web_id, 'keyword': keyword, 'keyword_related': k_related, 'score': score}
    #             j += 1
    #             print(f"finish {j}, keyword: {keyword}, keyword_related: {keyword_related}, score: {score}")
    #     df_save = pd.DataFrame.from_dict(dict_save, "index")
    #     df_save_list_dict = df_save.to_dict('records')
    #     query = "REPLACE INTO missoner_keyword_similarity (web_id, keyword, keyword_related, score) VALUES (:web_id, :keyword, :keyword_related, :score)"
    #     print(query)
    #     MySqlHelper('dione', is_ssh=False).ExecuteUpdate(query, df_save_list_dict)
    # t_end = time.time()
    # t_spent = t_end - t_start
    # print(f'finish all routine spent: {t_spent}s')
