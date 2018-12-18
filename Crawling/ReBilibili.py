# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Dongyu Zhang
"""
from bilibili import *
import pandas as pd


if __name__ == '__main__':
    missdata = pd.read_csv("missids.csv", sep=',', encoding='utf-8-sig', header=None,  names=['aid'])
    misslist = missdata['aid'].tolist()
    idlists = list()
    for i in range(0, len(misslist), 500):
        idlists.extend([misslist[i:500+i]])
    print(type(idlists[0][0]))
    url = 'https://api.bilibili.com/x/web-interface/view?aid='
    path = './MissResults/'
    job = AccessBilibiliAPI(maxpnum=1000)
    job.main_process(url=url, path=path, idlists=idlists)
    job.re_run_missid_nums()
    job.get_missed_list()


