from fastapi import FastAPI
from gensim_compose.embedding import Composer
import jieba
import jieba.analyse
import uvicorn

app = FastAPI()
print(f"start api server")
## jieba config
composer = Composer()
composer.set_config()  ## add all user dictionary (add_words, google_trend, all_hashtag)
stopwords = composer.get_stopword_list()


@app.get("/cut")
def cut(s: str=''):
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
    return {"message": "show current stop words", "data": stopwords}

@app.put("/word/init")
def init_config():
    global stopwords
    ## init dictionary
    composer.set_config()  ## add all user dictionary (add_words, google_trend, all_hashtag)
    stopwords = composer.get_stopword_list()
    return {"message": "finish resetting config"}

@app.put("/word/stop/init")
def init_stopword(s_join: str=None, sep: str=','):
    global stopwords
    stopwords.clear()
    if s_join:
        stopwords = s_join.split(sep=sep)
    else:
        stopwords = composer.get_stopword_list()
    return {"message": "finish resetting stop words"}


if __name__ == "__main__":
    uvicorn.run("api.keyword_api:app", host="0.0.0.0", port=8000, log_level="info")