
import MeCab as mc
from collections import defaultdict
from requests_oauthlib import OAuth1Session
from requests.exceptions import ConnectionError, ReadTimeout, SSLError
import json, datetime, time, pytz, re, sys,traceback, pymongo
#from pymongo import Connection     # Connection classは廃止されたのでMongoClientに変更 
from pymongo import MongoClient
import numpy as np
import bson
import unicodedata

KEYS = { # 自分のアカウントで入手したキーを下記に記載
        'consumer_key':'DQNS5hauEQcZzZeOyt2XW0NPo',
        'consumer_secret':'fgSZYzufckHt0YSJpaLk47iEYv9Wr7kje0K6ETm4xo2NidbAld',
        'access_token':'2913114504-NKxUONdYEIsLnCj5AG07DD6lMtcLUUL8pF8LhsS',
        'access_secret':'ErSYXHlr2TqW9SsTwLA0EJkxDPPUT2mfEgKduhozZ1wwa',
       }

twitter = None
connect = None
db      = None
tweetdata = None
meta    = None

def initialize(): # twitter接続情報や、mongoDBへの接続処理等initial処理実行
    global twitter, twitter, connect, db, tweetdata, meta
    twitter = OAuth1Session(KEYS['consumer_key'],KEYS['consumer_secret'],
                            KEYS['access_token'],KEYS['access_secret'])
#   connect = Connection('localhost', 27017)     # Connection classは廃止されたのでMongoClientに変更 
    connect = MongoClient('localhost', 27017)
    db = connect.sotsuron
    tweetdata = db.tweetdata
    meta = db.metadata

initialize()


regx = bson.regex.Regex("譲","求")

pipeline = [
    {"$match":{"text":{"$not":regx}}},
    #tweetdata中のtextからregxに該当するものを避ける
    {"$group":{"_id":"$text"}},
    #重複するtextが表示されなくなる
    {"$sample":{"size":10}}
    #ランダムでｎ件返却
]


sample_data = tweetdata.aggregate(pipeline) #配列


def mecab_analysis(sentence):
    t = mc.Tagger(r"C:\Program Files\mecab-ipadic-neologd")
    text = sentence.replace('\n','')
    t.parse('')
    node = t.parseToNode(text) 
    result_dict = defaultdict(list)
    while(node):
        if node.surface != "": #ヘッダとフッタを除外
            word_type = node.feature.split(",")[0]
            #parseToNode()は先頭のノード（形態素情報）を返し、
            # surfaceで表層形、featureで形態素情報を取得できます。
            # 両方とも文字列です。
            #featureは , で区切られているのでsplit()などで分割して必要な情報を抽出します。
            if word_type in ["名詞","形容詞","動詞"]:
                plain_word = node.feature.split(",")[6]
                #if plain_word != "*":
                    #result_dict[word_type.decode('utf-8')].append(plain_word.decode('utf-8'))
        node = node.next
    return result_dict

        

for d in sample_data.find({},{'_id':1, 'id':1, 'text':1,'noun':1,'verb':1,'adjective':1,'adverb':1}):
    ###tweetdataのすべてを対象に、オブジェクトid、ID、本文、名詞、動詞、形容詞、副詞を返却⇒ｄ
    res = mecab_analysis(unicodedata.normalize('NFKC', d['text'])) # 半角カナを全角カナに

    # 品詞毎にフィールド分けして入れ込んでいく
    for k in res.keys(): ###keys(): 各要素のキーkeyに対してforループ処理
        if k == u'形容詞': # adjective  
            adjective_list = []    ###形容詞リストをつくる（初期化？）
            for w in res[k]:                adjective_list.append(w)   
                ###resはResponseオブジェクトで、通信結果を保持しています。append():末尾に要素を追加
            sample_data.update_one({'_id' : d['_id']},{'$push': {'adjective':{'$each':adjective_list}}})
            ###$push:配列に指定された値を追加します。$each:配列フィールドに複数の値を追加します。
            ###update()メソッドの引数に別の辞書オブジェクトを指定すると、その辞書オブジェクトの要素がすべて追加される。
            ###⇒d["_id"] を ドキュメント(mongoDBの一つのデータのこと)の _id(識別子) としてpush(追加)する
        elif k == u'動詞': # verb
            verb_list = []
            for w in res[k]:
                verb_list.append(w)
            sample_data.update_one({'_id' : d['_id']},{'$push': {'verb':{'$each':verb_list}}})
        elif k == u'名詞': # noun
            noun_list = []
            for w in res[k]:                noun_list.append(w)
            sample_data.update_one({'_id' : d['_id']},{'$push': {'noun':{'$each':noun_list}}})
        elif k == u'副詞': # adverb
            adverb_list = []
            for w in res[k]:
                adverb_list.append(w)
            sample_data.update_one({'_id' : d['_id']},{'$push': {'adverb':{'$each':adverb_list}}})
    # 形態素解析済みのツイートにMecabedフラグの追加
    sample_data.update_one({'_id' : d['_id']},{'$set': {'mecabed':True}})


for data in sample_data:
    print(data["_id"])

    #for {中で使うための変数名} in {コレクション（配列など）}:
    # #毎回行いたい式