import os.path
from fastapi import FastAPI
from gensim_compose.embedding import Composer
from definitions import ROOT_DIR
import jieba
import jieba.analyse
import uvicorn

description = """
# 切字API
用於切字, 抽取關鍵字, 計算兩個詞之間相似度, 自定義辭彙, 自新增停止詞與還原初始詞庫設定
---
* /similarity 會用到 word embedding model, 若沒有事先載入指定的model會使用default model, 使用完後請釋放model減少RAM佔用 (put /model/release)
"""
tags_metadata = [
    {
        "name": "Tokenize sentence",
        "description": "Split a phrase, sentence or entire text document into smaller units.",
    },
    {
        "name": "Extract keywords",
        "description": "Extract keywords based on TF-IDF.",
    },
    {
        "name": "Add words",
        "description": "Add a word or a list of words separated by sep(default is ',')."
                       "These words can be recognized by /cut and /keyword.",
    },
    {
        "name": "Stop words",
        "description": "Add a stop word or a list of stop words separated by sep(default is ',')."
                       "These words can be stopped by /cut and /keyword.",
    },
    {
        "name": "Show stopwords",
        "description": "Show current stored stopwords.",
    },
    {
        "name": "Initialize config",
        "description": "Initialize to default status including stop words, add words and model. Spent about 10 seconds.",
    },
    {
        "name": "Initialize stopwords",
        "description": "Load default stop words.",
    },
    {
        "name": "Load model",
        "description": "Load word embedding model produced by gensim.",
        "externalDocs": {
            "description": "gensim docs",
            "url": "https://radimrehurek.com/gensim/models/word2vec.html",
        },
    },
    {
        "name": "Release model",
        "description": "Release model to empty RAM.",
    },
    {
        "name": "Get similarity",
        "description": "Calculate cosine similarity of two words based on model.",
    },
]
app = FastAPI(title="CutApp", description=description, openapi_tags=tags_metadata)


## jieba config
composer = Composer()
composer.set_config(filename_stopwords='stop_words.txt', filename_idf='idf_train_200000.txt',
                   filename_dictionary='idf_POS_collect.txt', filename_userdict='user_dict.txt')  ## add all user dictionary (add_words, google_trend, all_hashtag)
stopwords = composer.get_stopword_list()
ROOT_DIR = ROOT_DIR

@app.get("/cut", tags=["Tokenize sentence"])
def cut(s: str=''):
    global composer
    if s=='':
        return {"message": "no sentence input", "data": ""}
    else:
        s_clean = composer.preserve_str(s, pattern="[\u4E00-\u9FFFa-zA-Z]*")
        data = jieba.lcut(s_clean)
        data = composer.clean_keyword(data, stopwords)
        # data = [word for word in data if len(word) != 1]
        return {"message": "sentence tokenization", "data": data}

@app.get("/keyword", tags=["Extract keywords"])
def keyword_extraction(s: str='', topK: int=10):
    global composer, stopwords
    if s=='':
        return {"message": "no sentence input", "data": ""}
    else:
        s_clean = composer.preserve_str(s, pattern="[\u4E00-\u9FFF]*")
        data = jieba.analyse.extract_tags(s_clean, topK=topK)
        data = [word for word in data if word != ' ']
        data = composer.clean_keyword(data, stopwords)
        return {"message": "keyword extraction", "data": data}

@app.post("/word/add", tags=["Add words"])
def add_word(s: str='', s_join: str=None, sep: str=','):
    if s != '':
        jieba.add_word(s)
        jieba.suggest_freq(s, tune=True)
        return {"message": "successfully add a word", "data": s}
    elif s_join != None:
        s_list = s_join.split(sep=sep)
        for s in s_list:
            jieba.add_word(s)
            jieba.suggest_freq(s, tune=True)
        return {"message": "successfully add list of words", "data": s_list}

@app.post("/word/stop", tags=["Stop words"])
def stop_word(s: str='', s_join: str=None, sep: str=','):
    global stopwords
    if s != '':
        stopwords.append(s)
        return {"message": "successfully add a stop word"}
    elif s_join != None:
        s_list = s_join.split(sep=sep)
        for s in s_list:
            stopwords.append(s)
        return {"message": "successfully add list of words"}

@app.get("/word/stop", tags=["Show stopwords"])
def show_stopword():
    global stopwords
    return {"message": "show current stop words", "data": stopwords}

@app.put("/word/init", tags=["Initialize config"])
def init_config(filename_stopwords: str='stop_words.txt', filename_idf: str='idf_train_200000.txt',
                filename_dictionary: str='idf_POS_collect.txt', filename_userdict: str='user_dict.txt'):
    global stopwords, composer
    ## init dictionary
    composer.set_config(filename_stopwords=filename_stopwords, filename_idf=filename_idf,
                   filename_dictionary=filename_dictionary, filename_userdict=filename_userdict)  ## add all user dictionary (add_words, google_trend, all_hashtag)
    stopwords = composer.get_stopword_list()
    return {"message": "finish resetting config"}

@app.put("/word/idf/init", tags=["Reset idf file for keyword_extraction"])
def reset_keyword_idf(filename: str='idf_train_120000.txt'):
    global composer
    composer._load_kw_config(filename_idf=filename)
    return {"message": "finish resetting keyword config"}

@app.put("/word/stop/init", tags=["Initialize stopwords"])
def init_stopword(s_join: str=None, sep: str=','):
    global stopwords, composer
    stopwords.clear()
    if s_join:
        stopwords = s_join.split(sep=sep)
    else:
        stopwords = composer.get_stopword_list()
    return {"message": "finish resetting stop words"}


@app.put("/model/load", tags=["Load model"])
def load_model(model_name: str='word2vec_zhonly_remove_one_v150m3w5.model'):
    global composer, ROOT_DIR
    path_model = os.path.join(ROOT_DIR, 'gensim_compose', model_name) ##'./gensim_compose/word2vec_zhonly_remove_one_v300m10w5.model'
    composer.load_model(path=path_model)
    return {"message": "finish loading word embedding model"}

@app.put("/model/release", tags=["Release model"])
def release_model():
    global composer
    del composer.model
    return {"message": "finish releasing model"}

@app.get("/similarity", tags=["Get similarity"])
def cosine_similarity(s1: str=None, s2: str=None):
    global composer
    ## load model if not loaded
    if not hasattr(composer, 'model'):
        model_name = 'word2vec_zhonly_remove_one_v150m3w5.model'
        path_model = os.path.join(ROOT_DIR, 'gensim_compose', model_name)
        composer.load_model(path=path_model)
    if s1 and s2:
        sim = composer.similarity(s1, s2, default=0)
        return {"message": f"cosine similarity of {s1} and {s2}", "data": sim}
    else:
        return {"message": "Please enter both s1 and s2", "data": 0}


if __name__ == "__main__":
    uvicorn.run("api.keyword_api:app", host="0.0.0.0", port=8000, log_level="info")