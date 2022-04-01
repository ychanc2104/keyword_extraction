import jieba
import jieba.analyse
import numpy as np
import datetime
import re, os
from basic.decorator import timing
from opencc import OpenCC
from db.mysqlhelper import MySqlHelper
from db import DBhelper
from definitions import ROOT_DIR
## jieba.analyse.set_stop_words, 可使extract keyword阻擋這些字（cut, 切字無法）
## jieba.set_dictionary, jieba.load_userdict, 可同時影響cut and extract_tag

foler_path = os.path.dirname(os.path.abspath(__file__))

class Composer_jieba:
    def __init__(self): ##
        # self.web_id = web_id
        self.web_id_Vietnam = ['thanhnien2021', 'tuoitrexahoi']
        self.web_id_with_hash_tag = ['ctnews', 'mirrormedia', 'upmedia', 'btnet', 'bnext',
                                     'dailyview', 'moneyweekly', 'nongnong', 'newscts', 'newtalk',
                                     'setn', 'nownews']
        self.ROOT_DIR = ROOT_DIR

    def set_config(self, filename_stopwords='stop_words.txt', filename_idf='idf_train_1000000.txt',
                   filename_dictionary='idf_POS_collect.txt', filename_userdict='user_dict.txt'):
        # jieba_base = Composer_jieba()
        jieba.re_han_default = re.compile("([\u4E00-\u9FD5a-zA-Z0-9+#&\._% -]+)", re.U) ## English word can be recognized, ex: macbook pro
        self._load_cut_config(filename_dictionary, filename_userdict)
        self._load_kw_config(filename_stopwords, filename_idf)
        ## add file of add_word.txt
        self.add_words()
        all_hashtag = self.fetch_all_hashtags()
        gtrend_keywords = self.fetch_gtrend_keywords()
        ## add all hashtags and google trend keywords to the dictionary
        self.add_words(all_hashtag)
        self.add_words(gtrend_keywords)
        return all_hashtag

    @timing
    def _load_kw_config(self, filename_stopwords='stop_words.txt', filename_idf='idf_train_1000000.txt'):
        # jieba.analyse.set_stop_words(f'{self.ROOT_DIR}/jieba_based/stop_words.txt')
        # jieba.analyse.set_idf_path(f'{self.ROOT_DIR}/jieba_based/idf_collect.txt')
        jieba.analyse.set_stop_words(os.path.join(foler_path, filename_stopwords))
        jieba.analyse.set_idf_path(os.path.join(foler_path, filename_idf))

    @timing
    def _load_cut_config(self, filename_dictionary='idf_POS_collect.txt', filename_userdict='user_dict.txt'):
        # jieba.set_dictionary(f'{relative_path}/jieba_based/dict.txt.big.txt')  ## tradictional chinese dictionary
        # jieba.set_dictionary(f'{relative_path}/jieba_based/dict_zhTW.txt')  ## tradictional chinese dictionary
        # jieba.set_dictionary(f'{self.ROOT_DIR}/jieba_based/idf_POS_collect.txt')  ## merged tradictional chinese dictionary
        # jieba.load_userdict(f'{self.ROOT_DIR}/jieba_based/user_dict.txt')  ## user-defined dictionary (word, weight, POS(PartOfSpeech)), can also affect jieba.analyse.extract_tags
        jieba.set_dictionary(os.path.join(foler_path, filename_dictionary))  ## merged tradictional chinese dictionary
        jieba.load_userdict(os.path.join(foler_path, filename_userdict))  ## user-defined dictionary (word, weight, POS(PartOfSpeech)), can also affect jieba.analyse.extract_tags

    @timing
    def get_stopword_list(self):
        path = f'{ROOT_DIR}/jieba_based/stop_words.txt'
        # stopwords = [line.strip() for line in open(path, 'r', encoding='utf-8').readlines()] ## strip: remove space at the head or tail
        stopword_list = self.read_file(path)
        stopword_list_lower = [word.lower() for word in stopword_list]
        return stopword_list_lower

    @timing
    def fetch_hashtag(self):
        query = f"SELECT keywords FROM news_table WHERE keywords!='_' AND web_id!='thanhnien2021' AND web_id!='tuoitrexahoi'"
        data = MySqlHelper('jupiter_new').ExecuteSelect(query)
        hashtag_list = [d[0].split(',') for d in data]
        hashtag_list_flat = [item for sublist in hashtag_list for item in sublist]

        self.query_news_table = query
        self.sql_news_table = data
        return hashtag_list_flat

    def fetch_gtrend_keywords(self):
        query = "SELECT keyword, relatedQueries from google_trend_keyword"
        data = MySqlHelper('dione').ExecuteSelect(query)
        keyword_list = []
        for d in data:
            if d[1] != '':
                keyword_list += [d[0]] + d[1].split(',')
        return keyword_list

    @timing
    def fetch_all_hashtags(self):
        query = f"""SELECT keywords FROM article_list 
        WHERE keywords!='_' AND keywords!='' AND web_id in ('{"','".join(self.web_id_with_hash_tag)}')"""
        print(query)
        data = DBhelper('dione').ExecuteSelect(query)
        hashtag_list = list(set([word.strip() for d in data for word in d[0].split(',')]))
        return hashtag_list


    @timing
    def add_words(self, words=None):
        path = f'{self.ROOT_DIR}/jieba_based/add_words.txt'
        if words == None: ## use words in add_words.txt
            # words = [line.rstrip('\n') for line in open(path, 'r', encoding='utf-8').readlines()]
            words = self.read_file(path)
        elif type(words) == 'str':
            words = [words]
        # if type(words) == 'str':
        #     words = [words]
        for word in words:
            jieba.add_word(word)
            jieba.suggest_freq(word, tune=True)
        self.words_added = words
        return words

    ## no matter English upper or lower case
    def clean_keyword(self, keyword_list, stopwords):
        # keyword_list_lower = [keyword.lower() for keyword in keyword_list]
        stopwords_lower = [stopword.lower() for stopword in stopwords]
        # data_remove_stopword = []
        # for keyword in keyword_list:
        #     if keyword.lower() not in stopwords_lower:
        #         data_remove_stopword += [[keyword]]
        data_clean = [word for word in keyword_list if word != ' ']
        data_remove_stopword = [word for word in data_clean if word.lower() not in stopwords_lower] ## compare with same lowercase
        return data_remove_stopword

    @timing
    def file_remove_repeat_words(self, path):
        words = self.read_file(path)
        words_clean = list(set(words))
        words_sort = list(np.sort(words_clean))
        with open(f'../jieba_based/stop_words_clean.txt', 'w', encoding='utf-8') as f:
            for text in words_sort:
                f.write(text + '\n')
        return words, words_clean

    def filter_str_list(self, keyword_list, pattern="[0-9]*[.][0-9]+"):  # [0-9]*[.][0-9]+, [+-]?([0-9]*[.])?[0-9]+
        keyword_list_filter = []
        for keyword in keyword_list:
            match_list = re.findall(pattern, keyword) # [] if no matched
            if keyword not in match_list: # not a pure floating number
                keyword_list_filter += [keyword]
        return keyword_list_filter

    ## can be optimized
    def filter_quantifier(self, keyword_list):
        pattern = "[0-9.]*"
        quantifier_list = self.read_file(f"{ROOT_DIR}/jieba_based/filter_quantifier.txt")
        keyword_list_filter = []
        for keyword in keyword_list:
            number = re.findall(pattern, keyword)[0]
            number_quantifier_list = [str(number)+quantifier for quantifier in quantifier_list]
            if keyword not in number_quantifier_list:
                keyword_list_filter += [keyword]
                # print(f'obtain: {keyword}')
            # else:
                # print('in quantifier, should be filtered')
        # print(f'filter out quantifier: {number_quantifier_list}')
        return keyword_list_filter

    ## filter out pattern
    def filter_str(self, text, pattern = "[https]{4,5}:\/\/[0-9a-zA-Z.\/]*"):
        text_clean = ''.join(re.split(pattern, text))
        return text_clean

    ## only preserve the letter matched pattern, default is chinese
    def preserve_str(self, text, pattern="[\u4E00-\u9FFF]*"):
        text_match_list = re.findall(pattern=pattern, string=text)
        text_match_list = [text for text in text_match_list if text != '']
        text_clean = ' '.join(text_match_list)
        return text_clean


    def filter_symbol(self, text, pattern = "-|!|\.|~|&|\+|_|～|:|，|\/"):
        # pattern = "-|!|\.|~|&|\+|_|～|:|，|\/"
        text_clean = ''.join(re.split(pattern, text))
        return text_clean

    def read_file(self, path):
        words = [line.rstrip('\n') for line in open(path, 'r', encoding='utf-8').readlines()]
        return words

    def save_list(self, data_list, path='save_text.txt'):
        today_str = self._get_today_str()
        path = today_str + '_' + path
        with open(path, 'w', encoding='utf-8') as f:
            for text in data_list:
                f.write(text + '\n')

    def zwcn2tw(self, data_cn):
        # Initial
        cc = OpenCC('s2t')
        if type(data_cn) == str:
            data_tw = cc.convert(data_cn)
        else:
            data_tw = [cc.convert(data) for data in data_cn]
        return data_tw

    def _get_today_str(self):
        today = datetime.datetime.today()
        today_zero = today - datetime.timedelta(days=0,hours=today.hour,minutes=today.minute,seconds=today.second,microseconds=today.microsecond)
        today_str = datetime.datetime.strftime(today_zero, '%Y-%m-%d')
        return today_str

    ## word_list[i]: (word, idf, POS)
    def remove_POS(self, word_list):
        idf = self.zwcn2tw(word_list)
        idf_clean = list(set(idf))
        idf_sort = list(np.sort(idf_clean))
        idf_only = [' '.join(word.split(' ')[:-1]) for word in idf_sort]
        return idf_only

if __name__ == '__main__':
    # web_id_with_hash_tag = ['ctnews', 'mirrormedia', 'upmedia', 'btnet', 'bnext',
    #                         'dailyview', 'moneyweekly', 'nongnong', 'newscts', 'newtalk',
    #                         'setn', 'nownews']
    # query = f"""SELECT keywords FROM article_list
    # WHERE keywords!='_' AND keywords!='' AND web_id in ('{"','".join(web_id_with_hash_tag)}')"""
    # print(query)
    # data = DBhelper('dione').ExecuteSelect(query)
    # hashtag_list2 = list(set([word.strip() for d in data for word in d[0].split(',')]))
    #
    # hashtag_list_flat = []
    #
    # for d in data:
    #     # remove pure number
    #     hashtag_list = [word.strip() for word in d[0].split(',') if word not in re.findall('[0-9]+', word)]
    #     hashtag_list_flat += hashtag_list
    # a = list(set(hashtag_list_flat))
    # keyword_list = Composer_jieba().fetch_gtrend_keywords()
    # jieba.analyse.set_stop_words(f'../jieba_based/stop_words.txt')
    # # jieba.analyse.set_idf_path(f'{relative_path}/jieba_based/jieba_idf.txt')
    # jieba.analyse.set_idf_path(f'../jieba_based/dict_zhTW.txt')
    # jieba.analyse.set_idf_path(f'../jieba_based/idf_collect2.txt')

    news = '--aaaaa急--請教第四台線路高手～～贈15點-- 標題: 2010暑假墾丁遊3的12次方-7的6次方經分解後的質因數中......!!-- HI~~ 我想找便宜的逢甲民宿，大家有台中逢甲團體住宿推薦嗎-1到710的正整數中與42互質的有幾個-2004'

    news = '卵巢癌完善治療 多部科協同是關鍵 | 健康 | NOWnews今日新聞,【健康醫療網／記者曾正豪報導】50歲王小姐因腹痛6個月，而且一直有解便不順的困擾，經超音波檢查後發現是卵巢長了4公分的巧克力囊腫，於是接受了腹腔鏡患側卵巢輸卵管...'

    news_clean = Composer_jieba().preserve_str(news, pattern="[\u4E00-\u9FFF|a-zA-Z]*")
    jieba_base = Composer_jieba()
    jieba_base.set_config(filename_idf='idf_train_150000.txt') #idf_train_120000
    # jieba_base.set_config()

    jieba_base.get_stopword_list()
    a = jieba.analyse.extract_tags(news_clean)
    b = jieba.lcut(news_clean)
    # print('/'.join(a))
    # print('/'.join(b))

    # preserve_str
    #
    # a = re.findall(pattern="[\u4E00-\u9FFF]*", string=news)
    # news_clean = ' '.join(a)
    # a=re.split("-|!|\.|~|&|\+|_|～|:", news)
    # text_clean = ''.join(a)

    # stopword_list = Composer_jieba().get_stopword_list( in_sub_folder=True)
    # stopword_list_lower = [word.lower() for word in stopword_list]


    ################################################ test section #####################################################
    # relative_path = '..'
    # jieba.set_dictionary(f'{relative_path}/jieba_based/dict_zhTW.txt')  ## tradictional chinese dictionary
    # jieba.load_userdict(f'{relative_path}/jieba_based/user_dict.txt')  ## user-defined dictionary (word, weight, POS(PartOfSpeech)), can also affect jieba.analyse.extract_tags
    # #
    # jieba.analyse.set_stop_words(f'{relative_path}/jieba_based/stop_words.txt')
    # jieba.analyse.set_idf_path(f'{relative_path}/jieba_based/jieba_idf.txt')
    # news = '我沒有心 我沒有真實的自我 我只有消瘦的臉孔 所謂軟弱 所謂的順從一向是我 的座右銘'
    # words = jieba.cut(news)
    # keywords = jieba.analyse.extract_tags(news)
    # print('/'.join(words))
    # print('/'.join(keywords))
    ################################################ test section #####################################################


    ################################################ collect idf #####################################################
    # idf_1 = Composer_jieba().read_file('dict_zhTW.txt')
    # idf_1_only = Composer_jieba().remove_POS(idf_1)
    #
    # idf_big = Composer_jieba().read_file('dict.txt.big.txt')
    # idf_big_only = Composer_jieba().remove_POS(idf_big)
    #
    # idf_2 = Composer_jieba().read_file('jieba_idf.txt')
    # idf_2_only = Composer_jieba().remove_POS(idf_2)
    # idf_2_only = Composer_jieba().zwcn2tw(idf_2_only)
    #
    # idf_all = idf_1_only + idf_big_only + idf_2_only
    # idf_all = [idf.strip() for idf in idf_all]
    # idf_clean = list(set(idf_all))
    # idf_sort = list(np.sort(idf_clean))
    # Composer_jieba().save_list(idf_sort, 'idf_collect.txt')
    ################################################ collect idf #####################################################

    ################################################ collect idf_POS #####################################################
    # idf_1 = Composer_jieba().read_file('dict_zhTW.txt')
    # idf_2 = Composer_jieba().read_file('jieba_idf.txt')
    # idf_big = Composer_jieba().read_file('dict.txt.big.txt')
    #
    # idf_all = idf_1 + idf_2 + idf_big
    # idf_all = list(set(idf_all))
    # idf_all = [idf.strip() for idf in idf_all]
    # idf_all = list(np.sort(idf_all))
    # idf_all_sep = [idf.split(' ') for idf in idf_all]
    # idf_all_sep = [idf for idf in idf_all_sep if len(idf)==3]
    # idf_all_join = [' '.join(idf) for idf in idf_all_sep]
    # Composer_jieba().save_list(idf_all_join, 'idf_POS_collect.txt')
    ################################################ collect idf_POS #####################################################



    # idf = Composer_jieba().read_file('idf_collect.txt')
    # idf_sep = [word.split(' ') for word in idf]
    # idf_del = [' '.join(idf) for idf in idf_sep if len(idf)==2]
    # Composer_jieba().save_list(idf_del, 'idf_collect2.txt')

