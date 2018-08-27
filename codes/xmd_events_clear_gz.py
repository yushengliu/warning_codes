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
import logging
import sys

gz_path = '../gz/'
event_data_path = '../events_data/'
node_codes = ['ENV_POTENTIAL', 'STABLE_WARNING']

logger = logging.getLogger('mylogger')
formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
file_handler = logging.FileHandler(event_data_path + 'clear.log')
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.formatter = formatter
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

sched = BlockingScheduler()

# 清理多余的压缩包
@sched.scheduled_job('cron', hour='2', minute='22', second='22')
def clear_gz_and_data_version():
    if 1:
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

    if 1:
        print("==============START CLEAR EVENTS VERSION FILE: %s==========="% time.strftime('%Y-%m-%d %H:%M:%S'))
        for node_code in node_codes:
            events_node_folder = event_data_path + node_code + '/'
            version_file = events_node_folder + 'version.txt'
            path = os.listdir(events_node_folder)
            folder_names = []
            for p in path:
                if os.path.isdir(events_node_folder+p):
                    folder_names.append(p)
            folder_names.sort()
            os.remove(version_file)
            with open(version_file, 'a', encoding='utf-8') as fp:
                for folder_name in folder_names:
                    fp.write(folder_name+'\n')
                    # logger.info('Have update record[%s/version/%s] successfully'%(node_code, folder_name))
        print("==============START CLEAR EVENTS VERSION FILE: %s===========" % time.strftime('%Y-%m-%d %H:%M:%S'))
    return


# 清理多余的事件信息文件夹 —— 不要操作version文件，会冲突；相当于每六个小时清一次文件夹，保证只有近十次追踪的数据，但version文件不执行删除【可考虑凌晨定时删除version文件】 2018/8/26更改，每天清一次
@sched.scheduled_job('cron', hour='2', minute='22', second='22')
def clear_events_data_folder():
    print("==============BEGIN CLEAR EVENTS DATA FOLDER: %s============\n" % time.strftime('%Y-%m-%d %H:%M:%S'))
    for node_code in node_codes:
        events_node_folder = event_data_path + node_code + '/'

        path = os.listdir(events_node_folder)
        if len(path) <= 15:
            continue
        folder_names = []
        for p in path:
            if os.path.isdir(events_node_folder+p):
                folder_names.append(p)
        folder_names.sort()
        for folder_name in folder_names[0:-10]:
            shutil.rmtree(events_node_folder+folder_name)
            logger.info('remove folder[%s] successfully'%(events_node_folder+folder_name))

    print("\n==============END CLEAR EVENTS DATA FILE: %s============" % time.strftime('%Y-%m-%d %H:%M:%S'))
    return


if __name__ == "__main__":
    # 定时跑程序
    if 1:
        # scheduler = BlockingScheduler()
        #
        # scheduler.add_job(clear_gz, 'date', hour='2')
        sched.start()

    # test
    if 0:
        clear_events_data_folder()

    if 0:
        clear_gz_and_data_version()

