# -*- coding: utf-8 -*-
"""
Created on Sun Sep 09 18:16:25 2018


"""

import requests
# import json
import csv
import time
from multiprocessing import Pool
import os
import random
# if you cannot get the package resource, you do not need to import it
# import resource


class AccessBilibiliAPI(object):
    def __init__(self, maxpnum=20, timeout=3):
        self.maxpnum = maxpnum
        self.timeout = timeout

    def get_result(self, e_idlist):
        """
        extract the data from api then store in csv file
        :param e_idlist: enumerate(idlist)
        :return missaids: aids with no response, not no videos
        """
        existid_num = 0
        nullid_num = 0
        missid_num = 0
        e, idlist = e_idlist
        print("Process %d Starts!" % e)
        try:
            assert type(idlist) == list
        except AssertionError:
            print("idlist should be list!!!")
        else:
            if len(idlist) == 0:
                return [], 0, 0, 0
            results = []
            missaids = []
            for i in idlist:
                theurl = self.url + str(i)
                if i % 10 == 0:
                    time.sleep(3)
                try:
                    web = requests.get(theurl, timeout=self.timeout).json()['data']
                except:
                    print("Something wrong with page aid: %d" % i)
                    missid_num += 1
                    missaids.append(i)
                    continue
                else:
                    if web is None:
                        data = (
                            i,      # aid
                            0,      # ifexist
                            0,      # videos
                            "",     # tname
                            "",     # pubdate
                            "",     # view
                            "",     # danmaku
                            "",     # reply
                            "",     # favorites
                            "",     # coin
                            "",     # share
                            "",     # now_rank
                            "",     # his_rank
                            "",     # like
                            "",     # dislike
                            "",     # duration
                        )
                        nullid_num += 1
                    else:
                        data = (
                            i,                          # aid
                            1,                          # ifexist
                            web['videos'],              # videos
                            web['tname'],               # tname
                            web['pubdate'],             # pubdate
                            web['stat']['view'],        # view
                            web['stat']['danmaku'],     # danmaku
                            web['stat']['reply'],       # reply
                            web['stat']['favorite'],    # favorite
                            web['stat']['coin'],        # coin
                            web['stat']['share'],       # share
                            web['stat']['now_rank'],    # now_rank
                            web['stat']['his_rank'],    # his_rank
                            web['stat']['like'],        # like
                            web['stat']['dislike'],     # dislike
                            web['duration'],            # duration
                        )
                        existid_num += 1
                    results.append(data)

            the_path = self.path + "result_%d_%d.csv" % (idlist[0], idlist[-1])
            time.sleep(random.random())
            with open(the_path, 'w', encoding='utf-8-sig', newline='') as out:
                csv_out = csv.writer(out)
                csv_out.writerows(results)
            print("Process %d Finished!" % e)
            return missaids, existid_num, nullid_num, missid_num

    def main_process(self, url, path, idlists):
        """
        the main process of this object
        :param url: API url without aid
        :param path: Output file directory
        :param idlists: list of idlist
        :return:
        """
        start_time = time.time()
        try:
            assert type(idlists) == list
        except AssertionError:
            print("idlists should be list!!!")
        else:
            self.url = url
            self.path = path
            if os.path.exists(self.path):
                pass
            else:
                os.mkdir(self.path)
            totaljob = len(idlists)
            pnum = min(self.maxpnum, totaljob)
            pool = Pool(pnum)
            results = pool.map(self.get_result, enumerate(idlists))
            self.missaids_list = [i[0] for i in results]
            self.existid_nums = [i[1] for i in results]
            self.nullid_nums = [i[2] for i in results]
            self.missid_nums = [i[3] for i in results]
            end_time = time.time()
            print("All sub processes finished, within %.2fs" % (end_time - start_time))
            print("%d ids exist, %d ids null, %d ids lost" % (
                sum(self.existid_nums), sum(self.nullid_nums), sum(self.missid_nums)))
            time.sleep(5)


    def re_run_missid_nums(self, timeout=10, times=0, stop=5):
        miss_list = list()
        for ll in self.missaids_list:
            miss_list.extend(ll)
        try:
            assert len(miss_list) > 0
        except AssertionError:
            print('No need to re run')
        else:
            start_time = time.time()
            self.timeout = timeout
            idlists = [il for il in self.missaids_list if len(il) > 0]
            totaljob = len(idlists)
            pnum = min(self.maxpnum, totaljob)
            pool = Pool(pnum)
            results = pool.map(self.get_result, enumerate(idlists))
            self.missaids_list = [i[0] for i in results]
            self.existid_nums = [i[1] for i in results]
            self.nullid_nums = [i[2] for i in results]
            self.missid_nums = [i[3] for i in results]
            end_time = time.time()
            print("All sub processes finished, within %.2fs" % (end_time - start_time))
            print("%d ids exist, %d ids null, %d ids lost" % (
                sum(self.existid_nums), sum(self.nullid_nums), sum(self.missid_nums)))
            times += 1
            if times == stop:
                print("too many times rerun!!!")
            else:
                time.sleep(5)
                return self.re_run_missid_nums(timeout=timeout, times=times, stop=stop)





    def get_missed_list(self):
        miss_list = [ll for ll in self.missaids_list if len(ll) > 0]
        try:
            assert len(miss_list) > 0
        except AssertionError:
            print('No need to get missed list')
        else:
            the_path = self.path + "missed.csv"
            with open(the_path, 'w', encoding='utf-8-sig', newline='') as out:
                csv_out = csv.writer(out)
                csv_out.writerows(miss_list)
            print("missed list output")



def get_sample(start, stop, step, sub_sample_size):
    idlists = list()
    for i in range(start, stop, step):
        ilist = random.sample(list(range(i, step + i)), sub_sample_size)
        ilist.sort()
        idlists.append(ilist)
    return idlists


if __name__ == '__main__':
    # 32300000 目前估计的总数
    # If you import the resource, you could run the next two lines to maximize the number of files you could open
    # otherwise, you do not need to run them
    # soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    # resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    random.seed(2018)
    idlists = get_sample(start=1, stop=32300001, step=100000, sub_sample_size=1000)
    url = 'https://api.bilibili.com/x/web-interface/view?aid='
    path = './Results/'
    job = AccessBilibiliAPI(maxpnum=1000)
    job.main_process(url=url, path=path, idlists=idlists)
    job.re_run_missid_nums()
    job.get_missed_list()
