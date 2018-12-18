#!/usr/bin/env python3
#-*- coding: utf-8 -*-
"""
@author: Dongyu Zhang
"""
from bilibili import *
import pandas as pd
import csv
from multiprocessing import Pool

class GetMissid(object):

    def __init__(self, got):
        self.got = got

    def get_missid(self, idlists):
        e, idlist = idlists
        print("process %d start" % e)
        newll = [[i] for i in idlist if i not in self.got]
        print("process %d finish" % e)
        return newll

    def run(self, idlists):
        pool = Pool(50)
        results = pool.map(self.get_missid, enumerate(idlists))
        output = list()
        for i in results:
            output.extend(i)
        thepath = "missids.csv"
        with open(thepath, 'w', encoding='utf-8-sig', newline='') as out:
            csv_out = csv.writer(out)
            csv_out.writerows(output)
        print("Finish")

if __name__ == '__main__':
    columns = ["aid", "ifexist", "videos", "tname",
               "pubdate", "view", "danmaku", "reply",
               "favorite", "coin", "share", "now_rank",
               "his_rank", "like", "dislike", "duration"]
    random.seed(2018)
    idlists = get_sample(start=1, stop=32300001, step=100000, sub_sample_size=1000)
    data = pd.read_csv('TotalResult.csv', sep=",", encoding='utf-8-sig', header=None,  names=columns)
    got = data['aid'].tolist()
    get_miss = GetMissid(got=got)
    get_miss.run(idlists=idlists)