import re
from gensim.models import word2vec
from gensim.corpora import WikiCorpus
import jieba
# from ckiptagger import WS, POS, NER
from opencc import OpenCC
import numpy as np
from jieba_based.utility import Composer_jieba
from basic.date import get_today, datetime_to_str
from basic.decorator import timing

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
                    text = self.preserve_str(text, pattern="[\u4E00-\u9FFF|a-zA-Z]*")

                    # ## pattern for removing https
                    # text = self.filter_str(text, pattern="https:\/\/([0-9a-zA-Z.\/]*)")
                    # text = self.filter_str(text, pattern="[A-Za-z]*") ## remove English
                    # ## pattern for removing symbol, -,+~.
                    # text = self.filter_symbol(text)
                    print(f'data num: {times}, data: {text}')
                    text_cut = jieba.cut(text, HMM=False)
                    cut_list = [word for word in text_cut if word != ' ']
                    cut_list = self.clean_keyword(cut_list, stopword_list)  ## remove stopwords
                    # cut_list = self.filter_str_list(cut_list, pattern="[0-9]{2}")  ## remove 2 digit number
                    # cut_list = self.filter_str_list(cut_list, pattern="[0-9.]*")  ## remove floating
                    # cut_list = self.filter_str_list(cut_list, pattern="[A-Za-z]*")  ## remove English

                    cut_list_join = ' '.join(cut_list)
                    new_f.write(cut_list_join)

    def fit(self, path_read='wiki_text_seg.txt', path_write='word2vec.model', seed=666, sg=0, window_size=10, vector_size=100, min_count=1, workers=8, epochs=5, batch_words=10000):
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
    def similarity(self, word_1='生物', word_2='微生物'):
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
            similarity = -1
        else:
            similarity = sum(vector_1*vector_2)/(norm_1*norm_2)
        return similarity


if __name__ == '__main__':

    composer = Composer()
    ############## extract wiki text ###############
    composer.extract_wiki(path_read='../gitignore/wiki/zhwiki-20211101-pages-articles-multistream.xml.bz2', path_write='20211101_wiki_text.txt')
    ############## extract wiki text ###############

    ############## tokenize wiki text ###############
    # composer.cut(in_sub_folder=True, path_write='wiki_text_seg_zh_only.txt')
    ############## tokenize wiki text ###############

    ############## train word embedding ###############
    # composer.fit(path_read='wiki_text_seg.txt', path_write='word2vec.model')
    # composer.fit(path_read='wiki_text_seg_zh_only.txt', path_write='word2vec_zh_only.model')
    ############## train word embedding ###############

    # composer.load_model(path='word2vec_zh_only.model')
    # composer.most_similar('疫苗')

    #
    # mean_vector = composer.mean_word2vector(['海鹽', '巧克力'])

    # keyword_list = ['海鹽', '巧克力']
    #
    #
    # n = len(keyword_list)
    # sum_vector = np.zeros(len(composer.mean_vector))
    # for keyword in keyword_list:
    #     try:
    #         sum_vector += composer.model.wv[keyword]
    #     except:
    #         sum_vector += np.zeros(len(composer.mean_vector))
    # mean_vector = sum_vector / n
    # composer.file_ct2tw(path='../jieba_based/stop_words.txt')
    # composer.cut(path_write='../gitignore/wiki_text_seg_cn2.txt', user_config=True)
    # composer.ckip_cut(path_write='../gitignore/wiki_text_seg_ckip_2.txt')


    # text='法學 jurisprudence legal theory 又称 法律学 法律科学 是社會科學中一門特殊的學科 theory theory 所有的秩序都可以說是一種 法律 例如自然規律 倫常 邏輯法則或美學 而法學就是研究法律的法則 雖然許多法學家 例如 認定法學沒有學術性質 法律史學以及法律科學等三大部門'
    # text = '算經十書 數學競賽 数学题 註記 参考书目 benson donald the moment of proof mathematical epiphanies oxford university press usa new ed edition december isbn boyer carl history of mathematics wiley edition march isbn concise history of mathematics from the concept of number to contemporary mathematics courant and robbins what is mathematics an elementary approach to ideas and methods oxford university press usa edition july isbn davis philip and hersh reuben the mathematical experience mariner books reprint edition january isbn gentle introduction to the world of mathematics eves howard an introduction to the history of mathematics sixth edition saunders isbn gullberg jan mathematics from the birth of numbers norton company st edition october isbn an encyclopedic overview of mathematics presented in clear simple language hazewinkel michiel ed 數學百科全書 kluwer academic publishers translated and expanded version of soviet mathematics encyclopedia in ten expensive volumes the most complete and authoritative work available also in paperback and on cd rom and online jourdain philip the nature of mathematics in the world of mathematics james newman editor dover isbn kline morris mathematical thought from ancient to modern times oxford university press usa paperback edition march isbn 牛津英語詞典 second edition ed john simpson and edmund weiner clarendon press isbn the oxford dictionary of english etymology reprint isbn pappas theoni the joy of mathematics wide world publishing revised edition june isbn peterson ivars mathematical tourist new and updated snapshots of modern mathematics owl books isbn 参考网址 rusin dave the mathematical atlas 英文版 现代数学漫游 weisstein eric world of mathematics 一个在线的数学百科全书 数学 另一个在线的数学百科全书 mathforge 一个包含数学 物理 epistemath 数学知识 香港科技大学 数学网'
    # regex = re.compile(r'[^A-Za-z]+')
    # text_list = regex.findall(text)
    # text_clean = ''
    # for text in text_list:
    #     if text != ' ':
    #         text_clean += text
    # text_clean2 = text_clean.replace('  ',' ')


    # text_list = [text.replace(' ', '') for text in text_list if text != ' ']
    # text_clean = ' '.join(text_list)
    #
    # path_data = r'../gitignore/data'
    # ws = WS(path_data)
    # word_cut2 = ws([text], sentence_segmentation=True)[0]
    # data = [word.replace(' ','') for word in word_cut2 if word != ' ']
    # data = [word for word in data if word != '']
    # data2 = ' '.join(data)
    # composer.fit(path_read='../gitignore/wiki_text_seg_ckip.txt', path_write='../gitignore/word2vec_ckip100.model', vector_size=100)
    # composer.load_model(path='../gensim_compose/word2vec.model')
    # similarity1 = composer.similarity('男性', '強腎方')
    # similarity2 = composer.similarity('比特幣', '投資')
    # print('/'.join(jieba.cut('「台中」台中正確應該不會被切開', HMM=False)))
