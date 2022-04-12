from db import DBhelper
from basic import timing, logging_channels
from jieba_based import Composer_jieba
import pandas as pd
import jieba.analyse
import numpy as np


## web_id: newtalk, nownews, moneyweekly, gvm

@timing
def fetch_no_keyword_articles(web_id, id_offset=0):
    query = f"""
            SELECT 
                id, web_id, CONCAT(title, ' ', content) AS article
            FROM
                article_list
            WHERE
                web_id = '{web_id}'
                AND keywords IN ('' , '_')
                AND id>{id_offset}
            ORDER BY id
            LIMIT 100
            """
    print(query)
    data = DBhelper('dione').ExecuteSelect(query)
    df_article = pd.DataFrame(data, columns=['id', 'web_id', 'article'])
    return df_article

def save2csv(web_id, df_article, df_keywords, df_addwords):
    df_article.to_csv(f"{web_id}_articles.csv")
    df_keywords.to_csv(f"{web_id}_keywords.csv")
    df_addwords.to_csv(f"{web_id}_addwords.csv")


@timing
def fetch_id_record():
    query = f"SELECT web_id, id_record FROM article_list_id_record WHERE enable=1"
    print(query)
    data = DBhelper('dione').ExecuteSelect(query)
    id_record_dict = {}
    for d in data:
        id_record_dict.update({d[0]:d[1]})
    return id_record_dict

@timing
def update_id_record(id_record_dict, update_SQL=False):
    df_id_record = pd.DataFrame.from_dict(id_record_dict, orient='index', columns=['id_record']).reset_index()
    df_id_record.rename(columns={"index": "web_id"}, inplace=True)
    if update_SQL:
        query = DBhelper.generate_insertDup_SQLquery(df_id_record, 'article_list_id_record', ['id_record'])
        DBhelper('dione').ExecuteUpdate(query, df_id_record.to_dict("records"))
    return df_id_record



if __name__ == '__main__':
    id_record_dict = fetch_id_record()
    ## set up config (add word, user_dict.txt ...)
    jieba_base = Composer_jieba()
    all_hashtag = jieba_base.set_config()
    stopwords = jieba_base.get_stopword_list()

    # for web_id in web_id_list:
    for web_id,id_record in id_record_dict.items():
        ## fetch articles without keywords
        df_article = fetch_no_keyword_articles(web_id, id_record)
        ## update id_record
        id_record_dict[web_id] = max(df_article['id'])
        keyword_list_all = []
        for article in df_article['article']:
            ## pattern for removing https
            article = jieba_base.filter_str(article, pattern="https:\/\/([0-9a-zA-Z.\/]*)")
            ## pattern for removing symbol, -,+~.
            article = jieba_base.filter_symbol(article)
            keyword_list = jieba.analyse.extract_tags(article, topK=80)
            ## remove 2 digit number, floating, 1-4 lowercase letter and 2 Upper
            keyword_list = Composer_jieba().filter_str_list(keyword_list,
                                                            pattern="[0-9.]*|[a-z]{1,4}|[A-Z]{2}")
            ## remove number+quantifier, ex: 5.1萬
            keyword_list = Composer_jieba().filter_quantifier(keyword_list)
            keyword_list = Composer_jieba().clean_keyword(keyword_list, stopwords)[:10]  ## remove stopwords
            keyword_list_all += keyword_list

        df_keywords = pd.DataFrame(keyword_list_all, columns=['keywords'])
        df_keywords['disable'] = np.zeros(df_keywords.shape).astype(int)
        df_addwords = pd.DataFrame()
        save2csv(web_id, df_article, df_keywords, df_addwords)
        ## update id_record
        df_id_record = update_id_record(id_record_dict, update_SQL=False)


