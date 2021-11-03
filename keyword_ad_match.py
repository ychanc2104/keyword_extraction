import jieba.analyse
import pandas as pd
import numpy as np
import socket
import re
from gensim_compose.embedding import Composer
from basic.decorator import timing
from db.DB import DB


class Keyword_ad:
    def __init__(self, web_id='ctnews', config_path='./db/sysconfig_local.ini'):
        self.db = DB(config_path=config_path)
        self.web_id = web_id
        self.composer_gensim = Composer()
        self.local_ip = socket.gethostbyname(socket.gethostname())

    @timing
    def main(self, is_insert=True):
        ## load config
        self._load_kw_config()
        self._load_cut_config()
        ## fetch ad and article from db
        ad_id_list, ad_description_list = self.get_ad_info()
        ad_id_list, ad_description_list = self.remove_rejected_ad(ad_id_list, ad_description_list)
        article_title_list, article_content_list = self.get_article_info()
        ## analyze keyword
        article_keyword_list = [self.analyze_keyword(f'{title},{content}') for title, content in zip(article_title_list, article_content_list)]
        ad_keyword_list = [self.analyze_keyword(ad_description) for ad_description in ad_description_list]  ##('v', 'PER', 'n', 'a', 'ad')
        ## save variable
        self.article_keyword_list = article_keyword_list
        self.ad_keyword_list = ad_keyword_list
        ## load word2vector model
        self.composer_gensim.load_model()
        ## pair article keyword and ad keyword
        keyword_ad = self.pair_keyword(article_keyword_list, ad_keyword_list)
        df = pd.DataFrame(data=np.array([[self.web_id]*len(article_title_list), article_title_list, article_content_list, keyword_ad]).T,
                          columns=['web_id', 'meta_title', 'content', 'keyword_ad'])
        ## insert into NewsTitleKeyword table
        if is_insert:
            self.insert_data(df)
        return df
    @timing
    def main_postmall(self, is_insert=True, n_article=-1, n=100, model_path='./gitignore/word2vec_ckip100.model'):
        ## load config
        self._load_kw_config()
        self._load_cut_config()
        ## fetch ad and article from db
        ad_id_list, item_title_list, item_url_list = self.get_postmall_info(n=n)
        article_title_list, article_content_list = self.get_article_info()
        if n_article >= 0:
            article_title_list = article_title_list[:n_article]
            article_content_list = article_content_list[:n_article]
        ## analyze keyword
        article_keyword_list = [self.analyze_keyword(f'{self.remove_en_num(title)},{self.remove_en_num(content)}') for title, content in zip(article_title_list, article_content_list)]
        item_keyword_list = [self.analyze_keyword(self.remove_en_num(item_title)) for item_title in item_title_list]  ##('v', 'PER', 'n', 'a', 'ad')
        ## save variable
        self.article_keyword_list = article_keyword_list
        self.item_keyword_list = item_keyword_list
        ## load word2vector model
        self.composer_gensim.load_model(path=model_path)
        ## pair article keyword and ad keyword
        keyword_ad = self.pair_keyword(article_keyword_list, item_keyword_list)
        item_title = [item_title_list[index] for index in self.ad_index]
        item_keyword = [item_keyword_list[index] for index in self.ad_index]

        url = [item_url_list[index] for index in self.ad_index]
        df = pd.DataFrame(data=np.array([[self.web_id]*len(article_title_list), article_title_list, article_content_list, keyword_ad, item_title, item_keyword, url]).T,
                          columns=['web_id', 'meta_title', 'content', 'keyword_ad', 'item_title', 'item_keyword', 'url'])
        ## insert into NewsTitleKeyword table
        if is_insert:
            self.insert_data(df)
        return df

    @timing
    def insert_data(self, df):
        if self.local_ip == '127.0.1.1':
            db_comet = self.db.connect_mysql('db_comet3', 'ad_record')
        else:
            db_comet = self.db.connect_mysql('db_rhea1_db0', 'crescent_hodo')
        meta_title_list = df['meta_title']
        content_list = df['content']
        web_id_list = df['web_id']
        keyword_ad_list = df['keyword_ad']
        with db_comet:
            with db_comet.cursor() as cursor:
                for meta_title,content,web_id,keyword_ad in zip(meta_title_list,content_list,web_id_list,keyword_ad_list):
                    # sql = 'INSERT IGNORE INTO NewsTitleKeyword_test (meta_title, content, web_id, keyword_ad, add_time) VALUES (%s, %s, %s, %s, NOW())'
                    # cursor.execute(sql, [meta_title,content,web_id,keyword_ad])
                    sql = 'INSERT IGNORE INTO NewsTitleKeyword_test (meta_title, web_id, keyword_ad, add_time) VALUES (%s, %s, %s, NOW())'
                    cursor.execute(sql, [meta_title,web_id,keyword_ad])
                    db_comet.commit()

    @timing
    def pair_keyword(self, article_keyword_list, ad_keyword_list):
        keyword, id_ad, keyword_ad, ad_index = [], [], [], []
        for articles in article_keyword_list:
            best_scores, ad_kw_index = [], []
            ## find best ad_keyword for each article
            for j, ads in enumerate(ad_keyword_list):
                similarity_ad_article = self.similarity_matrix(articles, ads)  ## (row, col)=(ads, articles)
                similarity_upper = self.get_upper_similarity(similarity_ad_article, axis=1)
                similarity = self.get_upper_similarity(similarity_upper)
                ad_kw_index += [int(np.argmax(similarity_upper))]  ## each best index of ad_keyword
                best_scores += [similarity]  ## each best score of ad_keyword
            best_score_index = int(np.argmax(best_scores))  ## best ad index for this article
            keyword += [ad_keyword_list[best_score_index][ad_kw_index[best_score_index]]]
            id_ad += [self.ad_id_list[best_score_index]]
            ad_index += [best_score_index]
            keyword_ad += [id_ad[-1] + '_' + keyword[-1]]
        self.ad_index = ad_index
        return keyword_ad

    def similarity_matrix(self, article_keyword, ad_keyword):
        row = len(ad_keyword)
        col = len(article_keyword)
        similarity_ad_article = np.array([[self.composer_gensim.similarity(ad, article) for article in article_keyword] for ad in ad_keyword])  ## (row, col)=(ads, articles)
        return similarity_ad_article.reshape((row,col))

    def analyze_keyword(self, text, topK=6, allowPOS=()): ## type: ad or other
        text_replaced = text.replace('，', ',')
        text_list = self._sep_short_text(text_replaced)
        fraction_short_word = self._get_fraction_below_length(text_list, 4)
        if fraction_short_word > 0.8: ## most of text is short word, use flexible criteria, too short or too less
            keyword_list = text_list ## use input text directly
        else:
            keyword_list = jieba.analyse.extract_tags(text, topK=topK, withWeight=False, allowPOS=allowPOS) ## allow all POS , allowPOS=('n', 'v', 'a', 'PER')
        if len(keyword_list) == 0:
            keyword_list = ['xxx找不到xxx']
        # elif len(keyword_list) == 1:
        #     keyword_list = keyword_list*2

        return keyword_list

    ## fetch rejected ad of media
    def remove_rejected_ad(self, ad_id_list, ad_description_list):
        web_id = self.web_id
        db_sun = self.db.connect_mysql('db_sun_new', 'crescent_hodo')
        with db_sun:
            with db_sun.cursor() as cursor:
                cursor.execute(f"SELECT list_id FROM media_rejected WHERE web_id = '{web_id}'")
                rejected_ad_list = set([i[0] for i in cursor.fetchall()])
        ad_id_list_remove = []
        ad_description_list_remove = []
        for i,ad_id in enumerate(ad_id_list):
            if ad_id not in rejected_ad_list:
                ad_id_list_remove += [ad_id]
                ad_description_list_remove += [ad_description_list[i]]
        return ad_id_list_remove, ad_description_list_remove


    @timing
    def get_ad_info(self):
        db_sun = self.db.connect_mysql('db_sun_new', 'crescent_hodo')
        with db_sun:
            with db_sun.cursor() as cursor:
                cursor.execute("SELECT id, ad_description FROM ad_list WHERE company = '富愷廣告主' and current_date between start_time and end_time and execute_status = 0 and status = 2")
                # cursor.execute("SELECT id, ad_description FROM ad_list WHERE company = '富愷廣告主'")
                ad_list = cursor.fetchall()
        ad_id_list = [ad[0] for ad in ad_list]
        ad_description_list = [self._filter_symbol(ad[1]) for ad in ad_list]
        self.ad_id_list = ad_id_list
        self.ad_description_list = ad_description_list
        return ad_id_list, ad_description_list

    @timing
    def get_article_info(self):
        web_id = self.web_id
        db_jupiter = self.db.connect_mysql('db_jupiter', 'web_push')
        with db_jupiter:
            with db_jupiter.cursor() as cursor:
                cursor.execute(f"SELECT title, content FROM news_table where web_id = '{web_id}'")
                article_list = cursor.fetchall()
        article_title_list = [article[0] for article in article_list]
        article_content_list = [article[1] for article in article_list]
        self.article_title_list = article_title_list
        self.article_content_list = article_content_list
        return article_title_list, article_content_list

    @timing
    def get_postmall_info(self, n=100):
        db_webpush_api_02 = self.db.connect_mysql('db_webpush_api_02', 'report_data')
        with db_webpush_api_02:
            with db_webpush_api_02.cursor() as cursor:
                cursor.execute(f"SELECT product_id, title, url FROM item_list where web_id = 'postmall' limit {n}")
                item_list = cursor.fetchall()
        ad_id_list = [item[0] for item in item_list]
        item_title_list = [item[1] for item in item_list]
        item_url_list = [item[2] for item in item_list]

        self.item_title_list = item_title_list
        self.ad_id_list = ad_id_list
        self.item_url_list = item_url_list
        return ad_id_list, item_title_list, item_url_list

    def remove_en_num(self, text):
        regex = re.compile(r'[^A-Za-z0-9]+')
        text_list = regex.findall(text)
        text_clean = ''.join(text_list)
        return text_clean

    def get_upper_similarity(self, similarity_matrix, axis=None):
        if axis == None:
            # return np.mean(similarity_matrix)
            return np.mean(similarity_matrix) + 1.0 * np.std(similarity_matrix) ## to increase prob. of picking #keyword>1
        else:
            # return np.mean(similarity_matrix, axis=axis)
            return np.mean(similarity_matrix, axis=axis) + 1.0 * np.std(similarity_matrix, axis=axis)


    def _filter_symbol(self, text):
            text = text.replace('[', '')
            text = text.replace(']', '')
            text = text.replace('"', '')
            return text

    def _sep_short_text(self, text):
        text_replaced = text.replace('，', ',')
        keyword_list = text_replaced.split(',')
        keyword_list = [word for word in keyword_list if word != '']
        return keyword_list

    def _get_fraction_below_length(self, str_list, l):
        count = len([1 for str in str_list if len(str) <= l])
        return count/len(str_list)
    @timing
    def _load_kw_config(self, in_sub_folder=False):
        if in_sub_folder:
            relative_path = '..' ## in main project folder
        else:
            relative_path = '.' ## in sub project folder

        jieba.analyse.set_stop_words(f'{relative_path}/jieba_based/stop_words.txt')
        jieba.analyse.set_idf_path(f'{relative_path}/jieba_based/jieba_idf.txt')

    @timing
    def _load_cut_config(self, in_sub_folder=False):
        if in_sub_folder:
            relative_path = '..' ## in main project folder
        else:
            relative_path = '.' ## in sub project folder
        jieba.set_dictionary(f'{relative_path}/jieba_based/dict.txt.big.txt')  ## tradictional chinese dictionary
        jieba.load_userdict(f'{relative_path}/jieba_based/user_dict.txt')  ## user-defined dictionary (word, weight, POS(PartOfSpeech)), can also affect jieba.analyse.extract_tags


if __name__ == '__main__':
    web_id = 'ctnews'
    KW_ad = Keyword_ad(web_id)
    # df = KW_ad.main(is_insert=False)
    # KW_ad.insert_data(df)
    # ad_id_list, item_title_list = KW_ad.get_postmall_info(n=1000)
    # item_keyword_list = [KW_ad.analyze_keyword(item_title) for item_title in item_title_list]  ##('v', 'PER', 'n', 'a', 'ad')

    df_postmall = KW_ad.main_postmall(is_insert=False, n_article=100, n=30000)
    df_postmall.to_excel('keyword_postmall_3w_v4.xlsx')

    # KW_ad.composer_gensim.most_similar('開藥方')
    # a = 'phoneNumRegex 後方加個句點，輸入search123456方加個句'
    # b = KW_ad.remove_en(a)
    # similarity_ad_article = KW_ad.similarity_matrix(KW_ad.article_keyword_list[4], KW_ad.item_keyword_list[7])  ## (row, col)=(ads, articles)
    # upper = KW_ad.get_upper_similarity(similarity_ad_article,1)
    ## visualization
    # article_keyword = KW_ad.article_keyword_list[12]
    # ad_keyword = KW_ad.ad_keyword_list[2]
    # text = '又爆黑天鵝 謝金河：港股慘不忍睹 下個市場也難逃 - 財經,台股從八月以來持續走低，尤其是從八月五日之後，台股連續下挫，Ｋ線出現連九黑的走勢，這是罕見的弱勢，指數從一七六四三．九七下殺到一六六五七．六三，下跌九八六．三四點，這個弱勢不但跌破七月二十八日的一六八'
    # article_keyword2 = KW_ad.analyze_keyword(text)
    # article_keyword3 = jieba.analyse.extract_tags(text, topK=8, withWeight=False, allowPOS=('n', 'v', 'a', 'PER'))
    # ad_keyword = KW_ad.ad_keyword_list[3]
    # similarity_matrix = KW_ad.similarity_matrix(article_keyword, ad_keyword)
    # df_similarty = pd.DataFrame(similarity_matrix, columns=article_keyword, index=ad_keyword)
    # upper_similarity = KW_ad.get_upper_similarity(similarity_matrix, axis=1)
    # mean_similarity = np.mean(similarity_matrix, axis=1)


