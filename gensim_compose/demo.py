from gensim_compose.models import word2vec
from gensim_compose.corpora import WikiCorpus




## extract wiki article
wiki_corpus = WikiCorpus('/home/clare/Downloads/zhwiki-20210801-pages-articles-multistream.xml.bz2', dictionary={})
text_num = 0

with open('wiki_text.txt', 'w', encoding='utf-8') as f:
    for text in wiki_corpus.get_texts():
        f.write(' '.join(text)+'\n')
        text_num += 1
        if text_num % 10000 == 0:
            print('{} articles processed.'.format(text_num))

    print('{} articles processed.'.format(text_num))



# Settings
seed = 666
sg = 0
window_size = 10
vector_size = 100
min_count = 1
workers = 8
epochs = 5
batch_words = 10000

train_data = word2vec.LineSentence('wiki_text_seg.txt')
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

model.save('word2vec.model')



model = word2vec.Word2Vec.load('word2vec.model')
print(model.wv['生物'].shape)

for item in model.wv.most_similar('生物'):
    print(item)