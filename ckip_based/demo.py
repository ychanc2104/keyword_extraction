from ckiptagger import WS, POS, NER
from opencc import OpenCC

path_data = r'../gitignore/data'
ws = WS(path_data)
pos = POS(path_data)
ner = NER(path_data)

sentence_list = ["傅達仁今將執行安樂死，卻突然爆出自己20年前遭緯來體育台封殺，他不懂自己哪裡得罪到電視台。",
                  "美國參議院針對今天總統布什所提名的勞工部長趙小蘭展開認可聽證會，預料她將會很順利通過參議院支持，成為該國有史以來第一 位的華裔女性內閣成員。",
                  "",
                  "土地公有政策?？還是土地婆有政策。.",
                  "… 你確定嗎… 不要再騙了……",
                  "最多容納59,000個人,或5.9萬人,再多就不行了.這是環評的結論.",
                  "科長說:1,坪數對人數為1:3。2,可以再增加。",
                  '歐幾里得 西元前三世紀的古希臘數學家，現在被認為是幾何之父，此畫為拉斐爾的作品，雅典學院，数学，是研究數量，结构，变化以及空间等概念的一門学科，从某种角度看屬於形式科學的一種，數學透過抽象化和邏輯推理的使用。'
                 ]

text = '歐幾里得 西元前三世紀的古希臘數學家 現在被認為是幾何之父 此畫為拉斐爾的作品 雅典學院 数学 是研究數量 结构 变化以及空间等概念的一門学科 从某种角度看屬於形式科學的一種 數學透過抽象化和邏輯推理的使用'
# text = '傅達仁今將執行安樂死 卻突然爆出自己20年前遭緯來體育台封殺 他不懂自己哪裡得罪到電視台'

# cc = OpenCC('s2t')
# text_tw = cc.convert(text)
word_cut2 = ws([text], sentence_segmentation=True)
# word_cut2 = ws([text], sentence_segmentation=True, segment_delimiter_set={'?', '？', '!', '！', '。', ',',
#                                    '，', ';', ':', '、'})