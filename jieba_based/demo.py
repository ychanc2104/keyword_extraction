import jieba.posseg as pseg
import jieba
from jieba_based.jieba_utils import Composer_jieba
from opencc import OpenCC

def cut_and_print(text):
    words =pseg.cut(text)
    print(", ".join(["%s[%s]" % (word.word, word.flag) for word in words]))
    print(" ".join(jieba.cut(text)))
    text_tw = Composer_jieba().zwcn2tw(text)
    words_tw =pseg.cut(text_tw)
    print(", ".join(["%s[%s]" % (word.word, word.flag) for word in words_tw]))
    print(" ".join(jieba.cut(text_tw)))

cut_and_print("例如英语：'Knowledge is power“，可自然分割为 Knowledge/ is/ power 三个词。")
cut_and_print("西三旗硅谷先锋小区半地下室出租,便宜可合租硅谷")
cut_and_print("工信处女干事每月经过下属科室都要亲口交代24口交换机等技术性器件的安装工作")
cut_and_print("某,处女,干部赏月经过花店，一次,性交,付100元。")

text = "例如英语：'Knowledge is power“，可自然分割为 Knowledge/ is/ power 三个词。"
words = pseg.cut(text)
