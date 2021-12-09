import os.path

from fastapi import FastAPI
from gensim_compose.embedding import Composer
from definitions import ROOT_DIR
import jieba
import jieba.analyse
import uvicorn
app = FastAPI()
# print(f"start api server")
## jieba config
composer = Composer()
composer.set_config()  ## add all user dictionary (add_words, google_trend, all_hashtag)
stopwords = composer.get_stopword_list()
ROOT_DIR = ROOT_DIR

@app.get("/cut")
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

@app.get("/keyword")
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

@app.put("/word/add")
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

@app.put("/word/stop")
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

@app.get("/word/stop")
def show_stopword():
    global stopwords
    return {"message": "show current stop words", "data": stopwords}

@app.put("/word/init")
def init_config():
    global stopwords, composer
    ## init dictionary
    composer.set_config()  ## add all user dictionary (add_words, google_trend, all_hashtag)
    stopwords = composer.get_stopword_list()
    return {"message": "finish resetting config"}

@app.put("/word/stop/init")
def init_stopword(s_join: str=None, sep: str=','):
    global stopwords, composer
    stopwords.clear()
    if s_join:
        stopwords = s_join.split(sep=sep)
    else:
        stopwords = composer.get_stopword_list()
    return {"message": "finish resetting stop words"}


@app.put("/model/load")
def load_model(model_name: str='word2vec_zhonly_remove_one_v150m3w5.model'):
    global composer, ROOT_DIR
    path_model = os.path.join(ROOT_DIR, 'gensim_compose', model_name) ##'./gensim_compose/word2vec_zhonly_remove_one_v300m10w5.model'
    composer.load_model(path=path_model)
    return {"message": "finish loading word embedding model"}

@app.put("/model/release")
def release_model():
    global composer
    del composer.model
    return {"message": "finish releasing model"}

@app.get("/similarity")
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