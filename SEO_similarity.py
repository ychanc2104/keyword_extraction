from db.mysqlhelper import MySqlHelper
from jieba_based.jieba_utils import Composer_jieba
from basic.date import get_today, get_date_shift, check_is_UTC0, datetime_to_str, to_datetime, date_range, get_yesterday
from basic.decorator import timing
from gensim_compose.embedding import Composer
import pandas as pd
import numpy as np
import jieba
import jieba.analyse
from strsimpy.damerau import Damerau


def Extract_valid_keyword(criteria_score, criteria_cosine, date_save):
    result_dict, i = {}, 0
    for keyword_gtrend in keyword_list_gtrend:  ## keyword_gtrend: keyword to be matched
        ##### also save similarity_score and normalized damerau distance ######
        boolean_series_score, boolean_series_cosine = (df_score[keyword_gtrend] > criteria_score), (
                    df_score_cosine[keyword_gtrend] > criteria_cosine)
        boolean_series = boolean_series_merge(boolean_series_score, boolean_series_cosine)
        # boolean_array = (df_score[keyword_gtrend] > criteria) and (df_score_cosine[keyword_gtrend] > criteria_cosine)
        score_array = df_score[boolean_series][keyword_gtrend].values
        score_cosine_array = df_score_cosine[boolean_series][keyword_gtrend].values
        score_damerau_array = df_score_damerau[boolean_series][keyword_gtrend].values
        keyword_correlated_array = df_score[boolean_series].index.values
        ## save the keyword passed criteria
        for i, keyword_ecom in enumerate(keyword_correlated_array):
            ## only store keyword
            if keyword_ecom != keyword_gtrend:
                df_row = df_ecom_keyword.query(f"'{keyword_ecom}' in keyword")
                product_id, title, description, url = df_row['product_id'].values[0], df_row['title'].values[0], \
                                                      df_row['description'].values[0], df_row['url'].values[0]
                result_dict[i] = {'web_id': web_id, 'keyword_gtrend': keyword_gtrend, 'keyword_ecom': keyword_ecom,
                                  'title': title, 'description': description, 'url': url, 'product_id': product_id,
                                  'score': score_array[i], 'score_cosine': score_cosine_array[i],
                                  'score_damerau': score_damerau_array[i],
                                  'weight_cosine': weight_sim, 'weight_distance': weight_dis,
                                  'criteria_score': criteria_score, 'criteria_cosine': criteria_cosine, 'date':date_save}
                i += 1
    df_result = pd.DataFrame.from_dict(result_dict, "index")
    return df_result


def Calc_similarity(composer, keyword_row, keyword_col, weight_sim=1, weight_dis=1):
    # composer.load_model(path=path_model)
    similarity_matrix = np.array([[composer.similarity(k_gtrend, k_ecom) for k_gtrend
                                   in keyword_col] for k_ecom in keyword_row])
    print('finish building word embedding cosine similarity matrix')
    damerau = Damerau()
    damerau_matrix = np.array([[damerau.distance(k_gtrend, k_ecom) for k_gtrend
                                in keyword_col] for k_ecom in keyword_row])
    print('finish building damerau distance matrix')
    lmax_matrix = np.array([[max(len(k_gtrend), len(k_ecom)) for k_gtrend
                                in keyword_col] for k_ecom in keyword_row])
    df_score_cosine = pd.DataFrame(data=similarity_matrix, columns=keyword_col, index=keyword_row)
    df_score_damerau = pd.DataFrame(data=damerau_matrix/lmax_matrix, columns=keyword_col, index=keyword_row)

    score_matrix = weight_sim*similarity_matrix - weight_dis*damerau_matrix/lmax_matrix ## add similarity and damerau, bigger is more similar
    score_matrix_nor = (score_matrix + weight_dis/2)/(weight_sim + weight_dis/2)  ## normalize score to -1 ~ 1
    df_score = pd.DataFrame(data=score_matrix_nor, columns=keyword_col, index=keyword_row)
    return df_score, df_score_cosine, df_score_damerau


@timing
def Extract_ecom_df_keyword(df_ecom_content, composer, stopwords, stopwords_SEO, use_cut=False):
    data_dict, i = {}, 0
    for index, row in df_ecom_content.iterrows():
        product_id, title, description, url = row['product_id'], row['title'], row['description'], row['url']
        content = title + ' ' + description
        # content = title
        content_clean = composer.filter_symbol(content)
        content_clean = composer.filter_str(content_clean, pattern=product_id) ## remove its own product_id in content
        if use_cut:
            ## preserve chinese only
            content_clean = composer.preserve_str(content_clean, pattern="[\u4E00-\u9FFF]*")
            keyword_list = jieba.lcut(content_clean)
            keyword_list = [word for word in keyword_list if len(word) != 1]  # remove one word
        else:
            keyword_list = jieba.analyse.extract_tags(content_clean, topK=10)
            # keyword_list = jieba.analyse.textrank(content_clean, topK=4)
        keyword_list = composer.clean_keyword(keyword_list, stopwords)
        keyword_list = composer.clean_keyword(keyword_list, stopwords_SEO)
        for keyword in keyword_list:
            data_dict[i] = {'web_id': web_id, 'product_id': product_id, 'keyword': keyword, 'title': title,
                            'description': description, 'content': content, 'content_clean': content_clean, 'url': url}
            i += 1
    df_ecom_keyword = pd.DataFrame.from_dict(data_dict, "index")
    return df_ecom_keyword, composer

## preserve n_day in table
def Remove_expired_rows(n_day=91):
    date_expired = get_date_shift(get_yesterday(check_is_UTC0()), n_day-2) ## delete rows when date < today - 90
    query = f"DELETE FROM seo_sim_products WHERE date<'{date_expired}'"
    print(query)
    MySqlHelper('roas_report').ExecuteUpdate(query)

@timing
def fetch_ecom_content(web_id):
    # query = f"SELECT product_id, CONCAT(title, ' ', description) as content, url FROM report_data.item_list where web_id='{web_id}'"
    query = f"SELECT product_id, title, description, url FROM report_data.item_list where web_id='{web_id}'"
    print(query)
    data = MySqlHelper('db_webpush-api02').ExecuteSelect(query)
    # df_ecom_content = pd.DataFrame(data, columns=['product_id', 'content', 'url'])
    df_ecom_content = pd.DataFrame(data, columns=['product_id', 'title', 'description', 'url'])
    return df_ecom_content

## get date-1 ~ date keywords
@timing
def fetch_gtrend_keyword_list(date, n_limit=200):
    # is_UTC0 = check_is_UTC0()
    date_end = to_datetime(date)
    # date_end = get_today(is_UTC0=is_UTC0)
    date_start = get_date_shift(date_ref=date_end, days=1)
    query = f"""
            SELECT 
                keyword, SUM(traffic) AS traffic_total
            FROM
                google_trend_keyword
            WHERE
                date between '{date_start}' and '{date_end}'
            GROUP BY keyword
            ORDER BY traffic_total DESC
            LIMIT {n_limit}   
            """
    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    keyword_list = list(set([d[0] for d in data]))
    return keyword_list

## get date-1 ~ date keywords, use for initializing db
@timing
def fetch_gtrend_init_keyword_list(date, n_limit=200):
    date = to_datetime(date)
    query = f"""
            SELECT 
                keyword, SUM(traffic) AS traffic_total
            FROM
                google_trend_keyword
            WHERE
                date='{date}'
            GROUP BY keyword
            ORDER BY traffic_total DESC
            LIMIT {n_limit}   
            """
    print(query)
    data = MySqlHelper('dione').ExecuteSelect(query)
    keyword_list = list(set([d[0] for d in data]))
    return keyword_list


@timing
def save_seo_sim_products(df_save):
    query = MySqlHelper.generate_update_SQLquery(df_save, 'seo_sim_products')
    df_save_list_dict = df_save.to_dict('records')
    MySqlHelper('roas_report').ExecuteUpdate(query, df_save_list_dict)


def boolean_series_merge(boolean_series_1, boolean_series_2, type='and'):
    index = boolean_series_1.index
    output = []
    for key in index:
        if type=='and':
            output += [boolean_series_1[key] and boolean_series_2[key]]
        else:
            output = [boolean_series_1[key] or boolean_series_2[key]]
    return pd.Series(data=output, index=index)

def fetch_web_id_list():
    query = "SELECT web_id FROM seo_web_id_table WHERE gconsole_enable=1"
    print(query)
    data = MySqlHelper('roas_report').ExecuteSelect(query)
    web_id_list = [d[0] for d in data]
    return web_id_list

## get keyword of Ecom and get similarity with google trend keywords
if __name__ == '__main__':
    use_cut = False ## False: TF-IDT extraction, True: cut
    web_id_list = fetch_web_id_list()
    # web_id_list = ['i3fresh'] #'nanooneshop'
    composer = Composer()
    composer.set_config()  ## add all user dictionary (add_words, google_trend, all_hashtag)
    stopwords = composer.get_stopword_list()
    stopwords_SEO = composer.read_file('./jieba_based/stop_words_SEO.txt')
    ## pre-load model
    path_model = './gensim_compose/word2vec_zhonly_remove_one_v150m3w5.model'  ##'./gensim_compose/word2vec_zhonly_remove_one_v300m10w5.model'
    composer.load_model(path=path_model)

    yesterday = get_yesterday(check_is_UTC0())
    date_list = [yesterday]
    # n_days = 1
    # date_list = date_range(get_date_shift(yesterday, n_days), n_days)
    for web_id in web_id_list:
        for date in date_list:
            ## get ecom content
            df_ecom_content = fetch_ecom_content(web_id)
            # keyword_list_gtrend = fetch_gtrend_init_keyword_list(date, n_limit=100)  ## use for init table
            keyword_list_gtrend = fetch_gtrend_keyword_list(date, n_limit=200)  ## get latest two day keywords
            df_ecom_keyword, composer = Extract_ecom_df_keyword(df_ecom_content, composer, stopwords, stopwords_SEO, use_cut)
            ## get keyword list of Ecom
            keyword_list_ecom = list(set(df_ecom_keyword.keyword))

            ## get similarity matrix
            weight_sim, weight_dis = 1, 1
            df_score, df_score_cosine, df_score_damerau = Calc_similarity(composer, keyword_row=keyword_list_ecom,
                                                keyword_col=keyword_list_gtrend, weight_sim=weight_sim, weight_dis=weight_dis)

            ## build result Dataframe
            criteria_score, criteria_cosine = 0.1, 0.66 ## 0.1, 0.7
            df_result = Extract_valid_keyword(criteria_score, criteria_cosine, date)

            ## save to db
            save_seo_sim_products(df_result)

    ## delete data when date < today-90
    # Remove_expired_rows(n_day=90)



            ## get similarity matrix
            ############# quickly fine tune ################
            # composer.load_model(path='./gensim_compose/word2vec_remove_one.model')
            # composer.load_model(path='./gensim_compose/word2vec_zhonly_remove_one.model')
            # composer.load_model(path='./gensim_compose/word2vec_zhonly_remove_one_v200m10.model')
            # composer.load_model(path='./gensim_compose/word2vec_zhonly_remove_one_v300m20w10.model')
            # similarity_matrix = np.array([[composer.similarity(k_gtrend, k_ecom) for k_gtrend
            #                                in keyword_list_gtrend] for k_ecom in keyword_list_ecom])
            # print('finish building word embedding similarity matrix')
            # df_similarity = pd.DataFrame(data=similarity_matrix, columns=keyword_list_gtrend, index=keyword_list_ecom)
            # damerau = Damerau()
            # damerau_matrix = np.array([[damerau.distance(k_gtrend, k_ecom) for k_gtrend
            #                             in keyword_list_gtrend] for k_ecom in keyword_list_ecom])
            # print('finish building damerau distance matrix')
            # lmax_matrix = np.array([[max(len(k_gtrend), len(k_ecom)) for k_gtrend
            #                             in keyword_list_gtrend] for k_ecom in keyword_list_ecom])
            # df_score_cosine = pd.DataFrame(data=similarity_matrix, columns=keyword_list_gtrend, index=keyword_list_ecom)
            # df_score_damerau = pd.DataFrame(data=damerau_matrix/lmax_matrix, columns=keyword_list_gtrend, index=keyword_list_ecom)
            # weight_sim, weight_dis = 1, 0.5
            # score_matrix = weight_sim*similarity_matrix - weight_dis*damerau_matrix/lmax_matrix ## add similarity and damerau, bigger is more similar
            # score_matrix_nor = (score_matrix + weight_dis/2)/(weight_sim + weight_dis/2)  ## normalize score to -1 ~ 1
            # df_score = pd.DataFrame(data=score_matrix_nor, columns=keyword_list_gtrend, index=keyword_list_ecom)
            ############# quickly fine tune ################


