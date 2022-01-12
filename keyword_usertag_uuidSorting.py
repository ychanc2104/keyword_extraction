from collections import Counter
from db.mysqlhelper import MySqlHelper
from basic.decorator import timing
from random import sample

class keyword_usertag_sorting():
    @timing
    def __init__(self, web_id, uuidData, keywordLimit:int=100, challengeNum:int=10):
        """
        ## Purpose:
        # 1. Sorting the uuidData by frequency and google values.

        :param  web_id              (str)       web_id
        :param  uuidData            (dict)      Descirbe the uuid's keywords data (key: uuid)
                viewArticles        (int)       The amount of article the user view.
                keywordList         (list)      List storage the uuid's keywords (unsorted).
        :param  keyowrdLimit        (int)       The number of keywords want the reserve.
        :param  challengeNum        (int)       The number of abandon keywords want to challenge
        """
        self.web_id = web_id
        self.keywordLimit = keywordLimit
        self.challengeNum = challengeNum
        self.uuidData = uuidData
        
        self.dealExistedUUID()
        self.sortingData(sampleNum=int(1/5*keywordLimit), challengeNum=self.challengeNum)

    def fetch_uuidData(self):
        return self.uuidData
    
    def sortingData(self, sampleNum:int=20, challengeNum:int=10):
        """
        :param  sampleNum       (int)       The number of sample candidated keywords after split top N frequency keywords.
        :param  challengeNum    (int)       The number of sample incandidated keywords.
        """
        keywordValue = self.fetch_keywords_value()
        for uuid in self.uuidData:
            abandonData = {}
            self.uuidData[uuid]['keywordList'], abandonData['keywordList'] = self.uuidData[uuid]['keywordList'][:self.keywordLimit], self.uuidData[uuid]['keywordList'][self.keywordLimit:self.keywordLimit+challengeNum]
            self.uuidData[uuid]['keywordFreq'], abandonData['keywordFreq'] = self.uuidData[uuid]['keywordFreq'][:self.keywordLimit], self.uuidData[uuid]['keywordFreq'][self.keywordLimit:self.keywordLimit+challengeNum]
            if len(abandonData['keywordList'])>0:
                selectSample = sample(self.uuidData[uuid]['keywordList'], sampleNum)
                sampleScore = [keywordValue[keyword] if keyword in keywordValue else 0 for keyword in selectSample]
                abandonScore = [keywordValue[keyword] if keyword in keywordValue else 0 for keyword in abandonData['keywordList']]
                swapTarget = self.compareScore((selectSample, sampleScore), (abandonData['keywordList'], abandonScore))
                for keyword in swapTarget:
                    swapKeyword, swapLoc = swapTarget[keyword]
                    swapFreq = abandonData['keywordFreq'][swapLoc]
                    keywordLoc = self.uuidData[uuid]['keywordList'].index(keyword)
                    self.uuidData[uuid]['keywordList'][keywordLoc] = swapKeyword
                    self.uuidData[uuid]['keywordFreq'][keywordLoc] = swapFreq
    
    def compareScore(self, samplePair, challangePair):
        sampleKeywordList, sampleScoreList = samplePair
        abandonKeywordList, abandonScoreList = challangePair
        swapTarget = {}
        for loc, score in enumerate(abandonScoreList):
            pointer = 0
            intercept = 0
            for sampleLoc, sampleScore in enumerate(sampleScoreList):
                diff = score - sampleScore
                if diff>intercept:
                    pointer = sampleLoc
                    intercept = diff
            if pointer > 0:
                sampleKeyword = sampleKeywordList[sampleLoc]
                del sampleKeywordList[sampleLoc], sampleScoreList[sampleLoc]
                abandonKeyword = abandonKeywordList[loc]
                swapTarget[sampleKeyword] = (abandonKeyword, loc)
        return swapTarget
    
    def dealExistedUUID(self):
        """
        ## Purpose: 
        # 1. Sorting duplicated to keywords and freqs format and order by frequency.
        # 2. Merge with existed uuid keywords and frequencies.

        :param uuidData         (dict)      Descirbe the uuid's keywords data (key: uuid)
               keywordList      (list)      List storage the uuid's keywords.
               keywordFreq      (list)      List storage frequency corresponding uuid's keywords. (len(keywordList)==len(keywordFreq))
        """
        uuidList = list(self.uuidData.keys())
        for uuid in self.uuidData:
            keywords, freq = zip(*(Counter(self.uuidData[uuid]['keywordList']).most_common()))
            self.uuidData[uuid]['keywordList'] = list(keywords)
            self.uuidData[uuid]['keywordFreq'] = list(freq)
        existed_uuidData = self.fetch_existed_uuid_record(self.web_id, uuidList)
        for uuid in existed_uuidData:
            self.uuidData[uuid]['viewArticles']+=existed_uuidData[uuid]['viewArticles']
            for loc, keyword in enumerate(existed_uuidData[uuid]['keywordList']):
                freq = existed_uuidData[uuid]['keywordFreq'][loc]
                if keyword in self.uuidData[uuid]['keywordList']:
                    self.uuidData[uuid]['keywordFreq'][self.uuidData[uuid]['keywordList'].index(keyword)] += freq
                else:
                    self.uuidData[uuid]['keywordList'].append(keyword)
                    self.uuidData[uuid]['keywordFreq'].append(freq)
            keywordFreqTuple = [item for item in zip(*[self.uuidData[uuid]['keywordList'], self.uuidData[uuid]['keywordFreq']])]
            keywordFreqTuple = sorted(keywordFreqTuple, key=lambda x:x[1])
            self.uuidData[uuid]['keywordList'], self.uuidData[uuid]['keywordFreq'] = zip(*keywordFreqTuple)
            self.uuidData[uuid]['keywordList'] = list(self.uuidData[uuid]['keywordList'])
            self.uuidData[uuid]['keywordFreq'] = list(self.uuidData[uuid]['keywordFreq'])
            

    def fetch_keywords_value(self):
        query = \
            f"""
            SELECT
                keyword,
                high_price
            FROM keyword_value
            """
        keywordValue = {item[0]:int(item[1]) for item in MySqlHelper('rhea_web_push', is_ssh=True).ExecuteSelect(query)}
        return keywordValue


    def fetch_existed_uuid_record(self, web_id, uuidList):
        query = \
            f"""
            SELECT
                uuid, 
                keywordList, 
                keywordFreq,
                viewArticles
            FROM
                usertag_uuid_sorted
            WHERE
                web_id= '{web_id}'
                    AND uuid IN ('{"','".join(uuidList)}')
            """
        uuidData = MySqlHelper('missioner', is_ssh=True).ExecuteSelect(query)
        uuidData = {item[0]:{
            "keywordList":eval(item[1]),
            "keywordFreq":eval(item[2]),
            "viewArticles":item[3]} for item in uuidData}
        return uuidData