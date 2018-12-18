#!/usr/bin/env python3
#-*- coding: utf-8 -*-
"""
@author: Dongyu Zhang
"""

import pandas as pd

if __name__ == '__main__':

    columns = ["aid", "ifexist", "videos", "tname",
                   "pubdate", "view", "danmaku", "reply",
                   "favorite", "coin", "share", "now_rank",
                   "his_rank", "like", "dislike", "duration"]
    total_result = pd.read_csv('TotalResult.csv', sep=",", encoding='utf-8-sig', header=None,  names=columns)
    miss_result = pd.read_csv('MissResult.csv', sep=",", encoding='utf-8-sig', header=None,  names=columns)
    final_result = pd.concat([total_result, miss_result],  axis=0)
    final_result[['aid']] = final_result[['aid']].astype(int)
    final_result = final_result.sort_values(by=['aid'])
    final_result.to_csv('FinalResult.csv', sep=",", encoding='utf-8-sig', index=False, header=False)
    print("Finished")