#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2018/8/7 11:47
@Author  : Liu Yusheng
@File    : xmd_events_clear_gz.py
@Description: 定时清理压缩包——只保留最近十次
"""

import pandas as pd
import os
import shutil
from apscheduler.schedulers.background import BlockingScheduler
import time

gz_path = './gz/'

sched = BlockingScheduler()


@sched.scheduled_job('cron', hour='2', minute='22', second='22')
def clear_gz():
    print("==============BEGIN CLEAR OK FILE: %s============\n"%time.strftime('%Y-%m-%d %H:%M:%S'))
    ok_file = gz_path +'OK.txt'
    if not os.path.exists(ok_file):
        return
    ok_lines = open(ok_file, 'r', encoding='utf-8').readlines()
    # print(time_dirs)
    if len(ok_lines) <= 10:
        return
    time_dirs = [line.strip() for line in ok_lines]
    # print(time_dirs)
    # retain_dirs = time_dirs[-10:]
    remove_dirs = time_dirs[:-10]
    retain_lines = ok_lines[-10:]
    os.remove(ok_file)
    with open(ok_file, 'w', encoding='utf-8') as fp:
        fp.writelines(retain_lines)
    for remove_dir in remove_dirs:
        remove_path = gz_path + remove_dir
        if os.path.exists(remove_path):
            shutil.rmtree(remove_path)
            print("Have removed %s already!"%remove_dir)
    print("\n==============END CLEAR OK FILE: %s============" % time.strftime('%Y-%m-%d %H:%M:%S'))
    return


if __name__ == "__main__":
    # 定时跑程序
    if 1:
        # scheduler = BlockingScheduler()
        #
        # scheduler.add_job(clear_gz, 'date', hour='2')
        sched.start()

