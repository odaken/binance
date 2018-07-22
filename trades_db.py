from pymongo import MongoClient
import pymongo
from time import sleep
from  datetime import datetime, timedelta, timezone
import pytz
import time
from IPython.display import clear_output
import binance.binance as bi
import bitmex
# bmex = bitmex.bitmex(test=False, api_key='##############', api_secret='###########################')

class TradesDB:
    
    def __init__(self, pair):
        self.pair = pair
        self.client = MongoClient()
        self.db = self.client['binance']
        self.col = self.db[pair]
        
    def str2ts(self,str_datetime):
        # 2017/12/28 10:0:0
        return int(time.mktime(datetime.strptime(str_datetime, "%Y/%m/%d %H:%M:%S").timetuple()) *1000)

    def trades(self, fromTS, endTS):
        return list(self.col.find({'timestamp': {'$gte': fromTS,'$lt':endTS}},{'_id': False}).sort('timestamp'))

    def addIndex(self):
        self.col.create_index("aggregateID",unique = True)
        self.col.create_index([("timestamp",pymongo.ASCENDING)])
        
    def lastID(self):
        try:
            val = list(self.col.find({}, {'aggregateID': 1, '_id':0}).sort([('aggregateID',-1)]).limit(1))[0]['aggregateID']
        except:
            print("DB not found.")
            val = -1
        return val

    def recentID(self):
        recentTrades = bi.aggregateTrades(self.pair)
        #print(recentTrades[-1])
        try:
            recentID = recentTrades[-1]['aggregateID']
        except:
            print("error on api call {}".format(recentTrades))
            return -1
        return recentID


    def update(self):
        self.addIndex()
        nextID = self.lastID() + 1
        recentID = self.recentID()
        print("FromID: " , nextID, " ToID:", recentID)
        calls = 0
        while nextID < recentID:
            trades = bi.aggregateTrades(self.pair, fromId = nextID, limit = 500)
            calls = calls + 1
            nextID = trades[-1]['aggregateID']+1
            print("{} Next ID:{} / {} progress: {}%".format(self.pair, nextID, recentID, nextID / recentID * 100.0))
            results = self.col.insert_many(trades)
            print("Inserted results:", results)
            if calls % 10 == 0:
                sleep(2.5)
                clear_output()
        print("Update finished on {}".format(self.pair))



class TradesDBBitmex:
    def __init__(self, colName, pair):
        self.pair = pair
        self.client = MongoClient()
        self.db = self.client['bitmex']
        self.col = self.db[colName]
        
    def str2ts(self,str_datetime):
        # 2017/12/28 10:0:0
        return int(time.mktime(datetime.strptime(str_datetime, "%Y/%m/%d %H:%M:%S").timetuple()) *1000)

    def quotes(self, fromTS, endTS):
        return list(self.col.find({'timestamp': {'$gte': fromTS,'$lt':endTS}},{'_id': False}).sort('timestamp'))

    def addIndex(self):
        self.col.create_index([("timestamp",pymongo.ASCENDING)])

    def lastTS(self):
        try:
            val = list(self.col.find({}, {'timestamp': 1, '_id':0}).sort([('timestamp',-1)]).limit(1))[0]['timestamp']
            val = pytz.utc.localize(val)
        except:
            print("DB not found.")
            val = datetime(1970, 1, 1, 0, 0,tzinfo=timezone.utc)
        return val

    def recentTS(self):
        trades = bmex.Trade.Trade_get(symbol=self.pair, count=1,reverse=True).result()
        try:
            recentTS = trades[0][0]['timestamp']
        except:
            print("error on api call {}".format(trades))
            return -1
        return recentTS

    def update(self):
        self.addIndex()
        nextTS = self.lastTS() + timedelta(milliseconds=1)
        recentTS = self.recentTS()
        print("From: " , nextTS, " To:", recentTS)
        calls = 0
        while nextTS < recentTS:
            trades = bmex.Trade.Trade_get(symbol=self.pair, count=500, startTime=nextTS).result()
            calls = calls + 1
            nextTS = trades[0][-1]['timestamp'] +  timedelta(milliseconds=1)
            print("{} Next:{} / {} progress: {}%".format(self.pair, nextTS, recentTS, nextTS.timestamp() / recentTS.timestamp() * 100.0))
            # print("trades:", trades)
            # print("trades[0]:", trades[0])
            results = self.col.insert_many(trades[0])
            print("Insert result:", str(results))
            # for t in trades[0]:
            #   print("Trying to insert:", t)
            #   results = self.col.insert_one(t)
            sleep(1.0)
            if calls % 10 == 0:
                clear_output()
        print("Update finished on {}".format(self.pair))



class QuotesDBBitmex:
    def __init__(self, colName, pair):
        self.pair = pair
        self.client = MongoClient()
        self.db = self.client['bitmex']
        self.col = self.db[colName]
        
    def str2ts(self,str_datetime):
        # 2017/12/28 10:0:0
        return int(time.mktime(datetime.strptime(str_datetime, "%Y/%m/%d %H:%M:%S").timetuple()) *1000)

    def quotes(self, fromTS, endTS):
        return list(self.col.find({'timestamp': {'$gte': fromTS,'$lt':endTS}},{'_id': False}).sort('timestamp'))

    def addIndex(self):
        self.col.create_index([("timestamp",pymongo.ASCENDING)], unique = True)

        
    def lastTS(self):
        try:
            val = list(self.col.find({}, {'timestamp': 1, '_id':0}).sort([('timestamp',-1)]).limit(1))[0]['timestamp']
            val = pytz.utc.localize(val)
        except:
            print("Collection {} not found.".format(self.col))
            val = datetime(1970, 1, 1, 0, 0,tzinfo=timezone.utc)
        return val

    def recentTS(self):
        quotes = bmex.Quote.Quote_getBucketed(symbol=self.pair, binSize='1m', count=1, reverse=True).result()
        try:
            recentTS = quotes[0][0]['timestamp']
        except:
            print("error on api call {}".format(quotes))
            return -1
        return recentTS

    def update(self):
        self.addIndex()
        nextTS = self.lastTS() + timedelta(minutes=1)
        recentTS = self.recentTS()
        print("From: " , nextTS, " To:", recentTS)
        calls = 0
        while nextTS < recentTS:
            quotes = bmex.Quote.Quote_getBucketed(symbol=self.pair, binSize='1m', count=500, startTime=nextTS).result()
            calls = calls + 1
            nextTS = quotes[0][-1]['timestamp'] +  timedelta(minutes=1)
            print("{} Next:{} / {} progress: {}%".format(self.pair, nextTS, recentTS, nextTS.timestamp() / recentTS.timestamp() * 100.0))
            results = self.col.insert_many(quotes[0])
            print("Inserted results:", results)
            sleep(1.0)
            if calls % 10 == 0:
                clear_output()
        print("Update finished on {}".format(self.pair))



