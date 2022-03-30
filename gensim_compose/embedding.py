import os.path
import re
from gensim.models import word2vec
from gensim.corpora import WikiCorpus
import jieba
# from ckiptagger import WS, POS, NER
from opencc import OpenCC
import numpy as np
from jieba_based.jieba_utils import Composer_jieba
from basic.date import get_today, datetime_to_str
from basic.decorator import timing
from db import DBhelper
from definitions import ROOT_DIR
import pandas as pd
import math
from sklearn.feature_extraction.text import CountVectorizer


class Composer(Composer_jieba):
    # def __init__(self):
    ## 日文\u0800-\u4e00
    ## \u4e00 -\u9fa5(中文)
    ## \x3130 -\x318F(韓文)
    ## \xAC00 -\xD7A3(韓文)

    def extract_wiki(self, path_read='/home/clare/Downloads/zhwiki-20210801-pages-articles-multistream.xml.bz2', path_write='wiki_text.txt'):
        ## extract wiki article
        wiki_corpus = WikiCorpus(path_read, dictionary={})
        text_num = 0
        today_str = datetime_to_str(get_today())
        with open(path_write, 'w', encoding='utf-8') as f:
            for text in wiki_corpus.get_texts():
                f.write(' '.join(text) + '\n')
                text_num += 1
                if text_num % 10000 == 0:
                    print('{} articles processed.'.format(text_num))
            print('{} articles processed.'.format(text_num))

    def file_ct2tw(self, path):
        path_write = path.split('/')
        path_write[2] = 'tw_'+path_write[2]
        path_write = '/'.join(path_write)
        with open(path_write, 'w', encoding='utf-8') as new_f:
            with open(path, 'r', encoding='utf-8') as f:
                for i, data in enumerate(f, 1):
                    print(f'data num: {i}, data: {data}')
                    data = self.zwcn2tw(data)
                    new_f.write(data)

    def clean_keyword_list(self, keyword_list, stopwords, stopwords_missoner):
        keyword_list = Composer_jieba().clean_keyword(keyword_list, stopwords)  ## remove stopwords
        keyword_list = Composer_jieba().clean_keyword(keyword_list, stopwords_missoner)  ## remove stopwords
        keyword_list = Composer_jieba().filter_quantifier(keyword_list)  ## remove number+quantifier, ex: 5.1萬
        keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9]{2}")  ## remove 2 digit number
        keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9.]*")  ## remove floating
        keyword_list = Composer_jieba().filter_str_list(keyword_list,
                                                        pattern="[a-z]{1,4}|[A-Z]{2}")  ## remove 1-4 lowercase letter and 2 Upper
        keyword_list = [keyword for keyword in keyword_list if keyword != '']  ## remove blank
        return keyword_list


    # def ckip_cut(self, path_read='../gitignore/wiki_text.txt', path_write='../gitignore/wiki_text_seg_ckip.txt'):
    #     path_data = r'../gitignore/data'
    #     ws = WS(path_data)
    #     # pos = POS(path_data)
    #     # ner = NER(path_data)
    #     with open(path_write, 'w', encoding='utf-8') as new_f:
    #         with open(path_read, 'r', encoding='utf-8') as f:
    #             for times, data in enumerate(f, 1):
    #                 data = self.remove_en(self.zwcn2tw(data))
    #                 print(f'data num: {times}, data: {data}')
    #                 # data_cut = ws([data], sentence_segmentation=True)[0]
    #                 data_cut = ws([data])[0]
    #                 self.data_cut_ckip = data_cut
    #                 data_clean = []
    #                 for word in data_cut:
    #                     if word != '' and word != ' ':
    #                         data_clean += [word]
    #                 # data = [word for word in data if word != ' ']
    #                 result = ' '.join(data_clean)
    #                 print(f'data_cut num: {times}, data_cut: {result}')
    #                 new_f.write(result)


    def cut(self, path_read='../gitignore/wiki_text.txt', path_write='wiki_text_seg.txt', enable_parallel=None, user_config=True, in_sub_folder=False):
        # Tokenize
        ## make word to be recognized, jieba.suggest_freq(word, tune=True)
        if enable_parallel != None:
            jieba.enable_parallel(enable_parallel)
        if user_config:
            self.set_config(in_sub_folder=in_sub_folder)
        stopword_list = self.get_stopword_list(in_sub_folder=in_sub_folder)
        with open(path_write, 'w', encoding='utf-8') as new_f:
            with open(path_read, 'r', encoding='utf-8') as f:
                for times, text in enumerate(f, 1):
                    text = self.zwcn2tw(text) ## to tw_zh
                    # preserve chinese only
                    # text = self.preserve_str(text)
                    ## preserve chinese and English
                    # text = self.preserve_str(text, pattern="[\u4E00-\u9FFF|a-zA-Z]*")
                    ## preserve chinese only
                    text = self.preserve_str(text, pattern="[\u4E00-\u9FFF]*")
                    # ## pattern for removing https
                    # text = self.filter_str(text, pattern="https:\/\/([0-9a-zA-Z.\/]*)")
                    # text = self.filter_str(text, pattern="[A-Za-z]*") ## remove English
                    # ## pattern for removing symbol, -,+~.
                    # text = self.filter_symbol(text)
                    print(f'data num: {times}, data: {text}')
                    text_cut = jieba.cut(text, HMM=False)
                    cut_list = [word for word in text_cut if word != ' ']
                    cut_list = [word for word in cut_list if len(word) != 1] # remove one word
                    cut_list = self.clean_keyword(cut_list, stopword_list)  ## remove stopwords
                    # cut_list = composer.filter_str_list(cut_list, pattern="[a-z]{2}")  ## remove 2 letter English
                    # cut_list = self.filter_str_list(cut_list, pattern="[0-9]{2}")  ## remove 2 digit number
                    # cut_list = self.filter_str_list(cut_list, pattern="[0-9.]*")  ## remove floating
                    # cut_list = self.filter_str_list(cut_list, pattern="[A-Za-z]*")  ## remove English
                    cut_list_join = ' '.join(cut_list)
                    print(f"cut results: {cut_list_join}")
                    new_f.write(cut_list_join)

    def fit(self, path_read='wiki_text_seg.txt', path_write='word2vec.model', seed=1, sg=0, window_size=10, vector_size=100, min_count=1, workers=8, epochs=5, batch_words=10000):
        train_data = word2vec.LineSentence(path_read)
        model = word2vec.Word2Vec(
            train_data,
            min_count=min_count,
            vector_size=vector_size,
            workers=workers,
            epochs=epochs,
            window=window_size,
            sg=sg,
            seed=seed,
            batch_words=batch_words
        )
        model.save(path_write)

    @timing
    def load_model(self, path='./gensim_compose/word2vec.model'):
        self.model = word2vec.Word2Vec.load(path)
        self.mean_vector = np.mean(self.model.syn1neg, axis=0)

    def mean_word2vector(self, keyword_list):
        n = len(keyword_list)
        sum_vector = np.zeros(len(self.mean_vector))
        for keyword in keyword_list:
            try:
                sum_vector += self.model.wv[keyword]
            except:
                n -= 1
                sum_vector += np.zeros(len(self.mean_vector))
        if n == 0:
            mean = np.zeros(len(self.mean_vector))
        else:
            mean = sum_vector/n
        return mean

    def most_similar(self, word='生物'):
        # self.model = word2vec.Word2Vec.load('word2vec.model')
        print(self.model.wv[word].shape)
        for item in self.model.wv.most_similar(word):
            print(item)
    ## similarity of two words
    def similarity(self, word_1='生物', word_2='微生物', default=-1):
        not_in_1 = False
        not_in_2 = False
        try:
            vector_1 = self.model.wv[word_1]
        except:
            vector_1 = self.mean_vector
            not_in_1 = True
        try:
            vector_2 = self.model.wv[word_2]
        except:
            vector_2 = self.mean_vector
            not_in_2 = True
        norm_1 = np.sqrt(sum(vector_1**2))
        norm_2 = np.sqrt(sum(vector_2**2))
        if not_in_1 or not_in_2: ## both words are not in vector model
            similarity = default # -1
        else:
            similarity = sum(vector_1*vector_2)/(norm_1*norm_2)
        return similarity

    @staticmethod
    def fetch_no_keyword_articles(LIMIT=100, OFFSET=0):
        # if LIMIT == None:
        #     query = f"SELECT concat(title, ',', content) as article FROM dione.article_list where keywords='' or  keywords='_'"
        # else:
        #     query = f"SELECT concat(title, ',', content) as article FROM dione.article_list where keywords='' or  keywords='_' LIMIT {LIMIT} OFFSET {OFFSET}"
        web_id_list = ['ctnews', 'upmedia', 'cmoney', 'mirrormedia', 'nownews',
                        'bnext', 'setn', 'managertoday', 'gvm', 'newtalk', 'btnet',
                        'moneyweekly', 'pansci', 'edh', 'sportz', 'cnews', 'healthbw',
                       'edge435', 'babyhome', 'pixnet', 'xuite'] ## 1100 w
        query = f"""SELECT concat(title, ',', content) as article FROM dione.article_list where web_id in ('{"','".join(web_id_list)}') and keywords='' or  keywords='_' order by id desc LIMIT {LIMIT} OFFSET {OFFSET}"""
        print(query)
        data = DBhelper('dione').ExecuteSelect(query)
        df_articles = pd.DataFrame(data, columns=['article'])
        return df_articles

    @staticmethod
    def fetch_keywords(n=None, is_join=False):
        web_id_list = ['ctnews', 'upmedia', 'cmoney', 'mirrormedia', 'nownews',
                        'bnext', 'setn', 'managertoday', 'gvm', 'newtalk', 'btnet',
                        'moneyweekly', 'pansci', 'edh', 'sportz', 'cnews', 'healthbw',
                       'edge435', 'babyhome', 'pixnet', 'xuite']
        if n==None:
            query = f"""SELECT keywords FROM dione.article_list where web_id in ('{"','".join(web_id_list)}') and keywords!='' and  keywords!='_'"""
        else:
            query = f"""SELECT keywords FROM dione.article_list where web_id in ('{"','".join(web_id_list)}') and keywords!='' and  keywords!='_' LIMIT {n}"""
        print(query)
        data = DBhelper('dione').ExecuteSelect(query)
        if is_join:
            keywords_list = [' '.join([k.strip() for k in kw[0].split(',')]) for kw in data]
        else:
            keywords_list = [[k.strip() for k in kw[0].split(',')] for kw in data]
        return keywords_list


    @staticmethod
    def train_idf_sklearn(keywords_list, chunk_size=1000,is_save=False, filefolder=None, filename='idf_train.txt'):
        vectorizer = CountVectorizer()
        vt = vectorizer.fit_transform(keywords_list).toarray()
        idf_array = np.log10(vt.shape[1] / np.sum(vt, axis=0)) ## actually is tf-idf but tf=1
        word_list = vectorizer.get_feature_names()  # 詞袋
        n_keywords = len(word_list)
        keywords_idf = {}
        for i,(word,idf) in enumerate(zip(word_list, idf_array)):
            keywords_idf.update({word: idf})
            if i%chunk_size==0:
                if is_save:
                    print(f"save {i}/{n_keywords} round")
                    Composer.save_idf_file(keywords_idf, filefolder=filefolder, filename=filename)
                    # keywords_idf = {}
            elif i==n_keywords-1:
                if is_save:
                    print('save last round')
                    Composer.save_idf_file(keywords_idf, filefolder=filefolder, filename=filename)
        return keywords_idf

    @staticmethod
    def train_idf(keywords_list, is_save=False, filefolder=None, filename='idf_train.txt'):
        keywords_flatten = list(set(Composer.flatten(keywords_list)))
        keywords_idf = {}
        n_articles = len(keywords_list)
        n_keywords = len(keywords_flatten)
        for i,keyword in enumerate(keywords_flatten):
            count = 0
            for article_keywords in keywords_list:
                if keyword in article_keywords:
                    count += 1
            idf = math.log(n_articles/count ,10)
            keywords_idf.update({keyword: idf})
            if i%1000==0:
                print(f"finish train idf {i}/{n_keywords}")
                if is_save:
                    Composer.save_idf_file(keywords_idf, filefolder=filefolder, filename=filename)
                    keywords_idf = {}
            elif i==n_keywords-1:
                if is_save:
                    print('save last round')
                    Composer.save_idf_file(keywords_idf, filefolder=filefolder, filename=filename)
        return keywords_idf

    @staticmethod
    def save_idf_file(keywords_idf, filefolder=None, filename='idf_train.txt'):
        if filefolder==None:
            filefolder = os.path.join(ROOT_DIR, 'jieba_based')
        path = os.path.join(filefolder, filename)
        if os.path.exists(path):
            mode = 'a'
        else:
            mode = 'w'
        with open(path, mode, encoding='utf-8') as f:
            for keyword, idf in keywords_idf.items():
                if len(keyword.split(' '))==1:
                    f.write(f"{keyword} {idf}\n")

    @staticmethod
    def flatten(keywords_list):
        keywords_flatten = [k for kw in keywords_list for k in kw]
        return keywords_flatten

    @staticmethod
    def articles_to_keywords(df_articles, preserve_pattern="[\u4E00-\u9FFF|a-zA-Z]*", is_join=False):
        jieba_base = Composer_jieba()
        jieba_base.set_config()
        jieba_base.get_stopword_list()
        keywords_cut = []
        n = df_articles.shape[0]
        for i, row in df_articles.iterrows():
            articles = row[0]
            ## pattern for removing https
            articles_clean = jieba_base.filter_str(articles, pattern="https:\/\/([0-9a-zA-Z.\/]*)")
            ## pattern for removing symbol, -,+~.
            # articles_clean = jieba_base.filter_symbol(articles_clean)
            articles_clean = jieba_base.preserve_str(articles_clean, pattern=preserve_pattern)
            keyword_list = jieba.lcut(articles_clean)
            # keyword_list = jieba.analyse.extract_tags(articles_clean, topK=80)[::-1]
            # keyword_list = Composer_jieba().clean_keyword(keyword_list, stopwords)  ## remove stopwords
            keyword_list = Composer_jieba().filter_quantifier(keyword_list)  ## remove number+quantifier, ex: 5.1萬
            # keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9]{2}")  ## remove 2 digit number
            # keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9.]*")  ## remove floating
            keyword_list = Composer_jieba().filter_str_list(keyword_list,
                                                            pattern="[a-z]{1,4}|[A-Z]{2}")  ## remove 1-4 lowercase letter and 2 Upper
            keyword_list = [keyword for keyword in keyword_list if keyword != '' and len(keyword)>1]  ## remove blank
            keyword_list = list(set(keyword_list)) ## tf = 1
            # keywords = ','.join(keyword_list)  ## add keywords
            if is_join:
                keywords_cut += [' '.join(keyword_list)] ## for sklearn
            else:
                keywords_cut += [keyword_list]
            if i%500==0:
                print(f"finish cut {i}/{n}")
        return keywords_cut

    @staticmethod
    def main_save_train_idf(keywords_list=None, n_articles=100, offset_articles=0, n_keywords=None, is_save=True, filename='idf_train.txt'):
        df_articles = Composer.fetch_no_keyword_articles(LIMIT=n_articles, OFFSET=offset_articles)
        keywords_cut = Composer.articles_to_keywords(df_articles)
        if keywords_list==None:
            keywords_list = Composer.fetch_keywords(n_keywords)
        keywords_collect = keywords_list + keywords_cut
        del df_articles, keywords_cut, keywords_list
        keyword_idf = Composer.train_idf(keywords_collect, is_save=is_save, filename=filename)
        return keyword_idf


if __name__ == '__main__':
    # n_articles = 100000
    # offset_list = np.arange(0, 10000000, n_articles)
    # keywords_list = Composer.fetch_keywords(None)
    # for offset in offset_list:
    #     keyword_idf = Composer.main_save_train_idf(keywords_list, n_articles=n_articles, offset_articles=offset, filename='idf_train_all.txt')



    # keywords_list = Composer.fetch_keywords(None)
    keyword_idf = Composer.main_save_train_idf(keywords_list=None, n_articles=200000, offset_articles=0, filename='idf_train_200000.txt')


    # from sklearn.feature_extraction.text import TfidfTransformer
    # from sklearn.feature_extraction.text import CountVectorizer
    #
    #
    # keywords_list = Composer.fetch_keywords(50)
    # df_articles = Composer.fetch_no_keyword_articles(LIMIT=50, OFFSET=0)
    # keywords_cut = Composer.articles_to_keywords(df_articles)
    # keywords_all = keywords_list + keywords_cut
    # vectorizer = CountVectorizer()  # 該類會將文字中的詞語轉換為詞頻矩陣，矩陣元素a[i][j] 表示j詞在i類文字下的詞頻
    # transformer = TfidfTransformer()  # 該類會統計每個詞語的tf-idf權值
    #
    # tfidf = transformer.fit_transform(
    #     vectorizer.fit_transform(keywords_all))  # 第一個fit_transform是計算tf-idf，第二個fit_transform是將文字轉為詞頻矩陣
    # word = vectorizer.get_feature_names()  # 獲取詞袋模型中的所有詞語
    #
    # vt = vectorizer.fit_transform(keywords_all).toarray()
    # idf = np.log10(vt.shape[1]/np.sum(vt, axis=0))
    # word = vectorizer.get_feature_names() #詞袋
    

    # articles = Composer.fetch_no_keyword_articles(100)
    # df_articles = pd.DataFrame(articles, columns=['article'])
    #
    # jieba_base = Composer_jieba()
    # all_hashtag = jieba_base.set_config()
    # stopwords = jieba_base.get_stopword_list()
    #
    # keywords_cut = Composer.articles_to_keywords(df_articles)
    #
    # keywords_cut = []
    # for i,row in df_articles.iterrows():
    #     articles = row[0]
    #     # print(row)
    #     ## pattern for removing https
    #     articles_clean = jieba_base.filter_str(articles, pattern="https:\/\/([0-9a-zA-Z.\/]*)")
    #     ## pattern for removing symbol, -,+~.
    #     articles_clean = jieba_base.filter_symbol(articles_clean)
    #     keyword_list = jieba.analyse.extract_tags(articles_clean, topK=80)[::-1]
    #     keyword_list = Composer_jieba().clean_keyword(keyword_list, stopwords)  ## remove stopwords
    #     keyword_list = Composer_jieba().filter_quantifier(keyword_list)  ## remove number+quantifier, ex: 5.1萬
    #     keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9]{2}")  ## remove 2 digit number
    #     keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[0-9.]*")  ## remove floating
    #     keyword_list = Composer_jieba().filter_str_list(keyword_list, pattern="[a-z]{1,4}|[A-Z]{2}")  ## remove 1-4 lowercase letter and 2 Upper
    #     keyword_list = [keyword for keyword in keyword_list if keyword != '']  ## remove blank
    #     keyword_list = list(set(keyword_list))
    #     # keywords = ','.join(keyword_list)  ## add keywords
    #     keywords_cut += [keyword_list]
    # keywords = Composer.fetch_keywords(100)
    # keywords_list = [[k.strip() for k in kw[0].split(',')] for kw in keywords]
    # keywords_flatten = [k for kw in keywords_list for k in kw]
    #
    # keywords_collect = keywords_list + keywords_cut
    # keyword_idf = Composer.train_idf(keywords_collect, is_save=True)



    # from jieba_based import foler_path


    # Composer.save_idf_file(keyword_idf)
    # df_keywords = pd.DataFrame(keywords, columns=['keywords'])

    # idf = f"{keyword} {idf}\n"

    # composer = Composer()
    ############## extract wiki from .xml.bz2 to .txt ###############
    # composer.extract_wiki(path_read='../gitignore/wiki/zhwiki-20211101-pages-articles-multistream.xml.bz2', path_write='20211101_wiki_text.txt')
    ############## extract wiki from .xml.bz2 to .txt ###############

    ################## tokenize wiki text ###################
    # composer.cut(in_sub_folder=True, path_read='../gitignore/wiki/20211101_wiki_text.txt', path_write='20211101_wiki_text_seg_zhonly_remove_one.txt')
    ################## tokenize wiki text ###################

    ############## train word embedding ###############
    # composer.fit(path_read='wiki_text_seg.txt', path_write='word2vec.model')
    # composer.fit(path_read='wiki_text_seg_zh_only.txt', path_write='word2vec_zh_only.model')
    # composer.fit(path_read='20211101_wiki_text_seg_remove_one.txt', path_write='word2vec_remove_one.model')
    # composer.fit(path_read='20211101_wiki_text_seg_zhonly_remove_one.txt', path_write='word2vec_zhonly_remove_one.model')
    # composer.fit(path_read='20211101_wiki_text_seg_zhonly_remove_one.txt',
    #              path_write='word2vec_zhonly_remove_one_v200m10.model', vector_size=200, min_count=10)
    # composer.fit(path_read='20211101_wiki_text_seg_zhonly_remove_one.txt',
    #              path_write='word2vec_zhonly_remove_one_v300m10w5.model', vector_size=300, min_count=10)
    # composer.fit(path_read='20211101_wiki_text_seg_zhonly_remove_one.txt',
    #              path_write='word2vec_zhonly_remove_one_v300m20w10.model', vector_size=300, min_count=20, window_size=10)
    # composer.fit(path_read='20211101_wiki_text_seg_zhonly_remove_one.txt',
    #              path_write='word2vec_zhonly_remove_one_v150m5w10.model', vector_size=150, min_count=5, window_size=10)
    # composer.fit(path_read='20211101_wiki_text_seg_zhonly_remove_one.txt',
    #              path_write='word2vec_zhonly_remove_one_v300m100w5.model', vector_size=300, min_count=100, window_size=5)
    ############## train word embedding ###############

    ############## load word embedding model ###############
    # composer.load_model(path='word2vec_zhonly_remove_one.model')
    # composer.load_model(path='word2vec_remove_one.model')
    # composer.load_model(path='word2vec_zhonly_remove_one_v200m10.model')
    ############## load word embedding model ###############

    ############## test similarity ###############
    # composer.most_similar('iphone')
    # similarity1 = composer.similarity('男性', '強腎方')
    ############## test similarity ###############

    # text = '船歌 barcarolle 源自意大利語 barca 意爲 起源於意大利威尼斯 有趣的是 把這種曲調發揚起來的 反而並不是意大利作曲家 船歌一般都是採用以 拍寫成 例如貝多芬的 號交響曲 樂章便是最好的引證'
    # text_cut = jieba.cut(text, HMM=False)
    # cut_list = [word for word in text_cut if word != ' ']
    # cut_list = [word for word in cut_list if len(word) != 1]  # remove one word
    # cut_list = composer.filter_str_list(cut_list, pattern="[a-z]{2}")  ## remove English

