#!/usr/bin/env python3
#-*- coding: utf-8 -*-
"""
@author: Dongyu Zhang
"""
import pandas as pd
import os

class DataCombiner(object):

    def __init__(self, columns):
        self.columns = columns

    def run(self, inputpath, outputpath):
        datalist = list()
        for i in os.listdir(inputpath):
            print(i)
            try:
                newdata = pd.read_csv(inputpath + i, sep=",", header=None, names=columns)
                datalist.append(newdata)
            except pd.errors.EmptyDataError:
                continue
        data = pd.concat(datalist, axis=0)
        data[['aid']] = data[['aid']].astype(int)
        data = data.sort_values(by=['aid'])
        data.to_csv(outputpath, sep=",", encoding='utf-8-sig', index=False, header=False)
        print("Finished")


if __name__ == '__main__':
    columns = ["aid", "ifexist", "videos", "tname",
                                 "pubdate", "view", "danmaku", "reply",
                                 "favorite", "coin", "share", "now_rank",
                                 "his_rank", "like", "dislike", "duration"]

    inputpath = "./Results/"
    outputpath = "TotalResult.csv"
    datacombiner = DataCombiner(columns)
    datacombiner.run(inputpath=inputpath, outputpath=outputpath)
