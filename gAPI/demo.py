from pytrends.request import TrendReq
from pprint import pprint
from urllib.parse import unquote, quote
from basic.decorator import timing

@timing
def test1(n):
     result = []
     a=[i for i in range(100)]
     for i in range(n):
          result += [a]
     return result

@timing
def test2(n):
     result = []
     a=[i for i in range(100)]
     for i in range(n):
          result.append(a)
     return result


n=10000000
a=test1(n)
b=test2(n)


## https://trends.google.com.tw/trends/api/widgetdata/relatedsearches?hl=zh-TW&tz=-480&req=%7B%22restriction%22:%7B%22geo%22:%7B%22country%22:%22TW%22%7D,%22time%22:%222020-10-25+2021-10-25%22,%22originalTimeRangeForExploreUrl%22:%22today+12-m%22%7D,%22keywordType%22:%22QUERY%22,%22metric%22:%5B%22TOP%22,%22RISING%22%5D,%22trendinessSettings%22:%7B%22compareTime%22:%222019-10-25+2020-10-24%22%7D,%22requestOptions%22:%7B%22property%22:%22youtube%22,%22backend%22:%22IZG%22,%22category%22:0%7D,%22language%22:%22zh%22,%22userCountryCode%22:%22TW%22%7D&token=APP6_UEAAAAAYXebgZTvvK_QMi6okoRBrl3ZSQNk306i



# pytrend = TrendReq(hl='en-US', tz=360)
# # keywords = ['Python', 'Java', 'C++', '博恩夜夜秀', '博 恩 夜 夜 秀']
# # keywords = ['褲子']
# keywords = ['褲子', '襪子']
#
# pytrend.build_payload(
#      kw_list=keywords,
#      # cat=0,
#      timeframe='today 1-m',
#      # timeframe='2021-10-11 2021-10-12',
#      geo='TW',
#      # geo='US',
#      gprop='youtube')
#
# interest = pytrend.interest_over_time() # index of hotness
# related = pytrend.related_queries()
# topics = pytrend.related_topics()
# hot = pytrend.trending_searches(pn='taiwan')
# suggestions = pytrend.suggestions(quote('褲子'))
# pprint(interest)
# pprint(related)
