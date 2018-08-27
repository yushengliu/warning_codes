#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2018/8/4 11:25
@Author  : Liu Yusheng
@File    : xmd_events_warning_data_generation.py
@Description: 产生事件预警数据文件
"""

# from MyModule import DataBasePython, TextDispose
import pandas as pd
from copy import deepcopy
import re
import os
import sys
import logging
# from OpnModule import match_warning_keywords_frontend
import time
from datetime import datetime
import psycopg2
import json
from multiprocessing import Pool
from apscheduler.schedulers.background import BlockingScheduler

data_path = './data/'
event_data_path = '../events_data/'
gaode_geo_path = data_path + 'df_2861_gaode_geo.csv'

# client_path = './apps/'

df_2861_geo_county = pd.read_csv(gaode_geo_path, index_col='gov_code', encoding='utf-8')
df_2861_county = df_2861_geo_county[df_2861_geo_county['gov_type'] > 2]

logger = logging.getLogger('mylogger')
formatter = logging.Formatter('%(asctime)s %(levelname)-8s:%(message)s')
file_handler = logging.FileHandler(event_data_path + 'fetch_data_record.log')
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.formatter = formatter
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


# 对Python自带的数据库的包
class DataBasePython:
    def __init__(self, host="192.168.0.117", user="readonly", pwd="123456", port="5555"):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.port = port

    def select_data_from_db_one_by_one(self, db, sql):
        rows = []
        j = 10
        while j >= 0:
            try:
                conn = psycopg2.connect(dbname=db, user=self.user, password=self.pwd, host=self.host, port=self.port, client_encoding='utf-8', keepalives=1, keepalives_idle=20, keepalives_interval=20, keepalives_count=3, connect_timeout=10)
                cur = conn.cursor()
                cur.execute(sql)
                break
            except Exception as e:
                print(e)
                j -= 1

        rowcount = cur.rowcount
        # row = 0
        for i in range(0, rowcount):
            try:
                row = cur.fetchone()
                # print(row)
                rows.append(row)
            except Exception as e:
                # print(row)
                # sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
                print(db+': '+str(e))
                # row = 0
                continue
        conn.close()
        return rows

    def execute_any_sql(self, db, sql):
        try:
            conn = psycopg2.connect(dbname=db, user=self.user, host=self.host, password=self.pwd, port=self.port, client_encoding='utf-8', keepalives=1, keepalives_idle=20, keepalives_interval=20, keepalives_count=3, connect_timeout=10)
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            print(db+':'+e)
        conn.close()
        return


# 微博文本预处理
class TextDispose:
    def __init__(self, text):
        self.text = text

    # 去除浏览器、编码
    def get_weibo_valid_info(self, go_on=False):
        content = repr(self.text)[1:-1]
        pattern = re.compile(r'\<[\s\S]*?\>')  # 去掉中间的无效浏览器内容
        result = pattern.sub('', content)
        pattern2 = re.compile(r'&quot|\u200b|\u3000|\\u200b|\\u3000')
        result2 = pattern2.sub('', result)
        if go_on:
            self.text = result2
        # print(result)
        return result2

    # 去除[网页链接] / @ / #话题# 等
    def text_preparation(self, min_len, max_len, go_on=False):
        # if not len(text):
        #     return None
        text = self.text.replace('网页链接', '').strip().replace(r'\u200b', '').replace('[带着微博去旅行]', ' ')
        text = re.sub('#.*?#', '', text)
        if len(re.findall("@", text)) >= 3:
            text = re.sub('@.*? |@.*?\r\n|@.*?(：|:)', '', text)
        text = text.strip()
        # print(len(text))
        if min_len <= len(text) <= max_len and len(re.findall("/", text)) <= 4:
            # text = jieba_split_sentence(text)
            # words, flags = self.jieba_cut_sentences(text)
            # text = ' '.join(words)
            if go_on:
                self.text = text
            return text
        else:
            return None


# 关键词系列

#区县官职名
gov_guanzhi_keyword_new = \
    u"县委副书记|区委副书记|市委副书记|党委副书记|纪委副书记|政法委副书记|政协副主席|人大副主任|副县长|副区长|副市长|副旗长|" \
    u"县委书记|区委书记|市委书记|党委书记|纪委书记|政法委书记|政协主席|人大主任|县长|区长|市长|旗长|" \
    u"副书记|副局长|副处长|副科长|副部长|副主任|大队长|中队长|副镇长|副乡长|副村长|副秘书长|" \
    u"书记|局长|处长|科长|部长|主任|队长|镇长|乡长|村长|秘书长|领导|干部|当官的|官员"

#区县部门名
gov_department_keyword_new = \
    u"县委|区委|市委|旗委|党委|纪委|政法委|县政协|区政协|市政协|旗政协|县人大|区人大|市人大|旗人大|县政府|区政府|市政府|旗政府" \
    u"人武部|法院|检察院|管理处|信息办|保密局|防邪办|组织部|老干局|党建办|编办|宣传部|国土资源局|" \
    u"文明办|社科联|新闻宣传中心|文联|政研室|统战部|民宗局|机关工委|工信局|群工局|国资办|法制办|应急指挥中心|" \
    u"应急办|公共资源交易中心|安监局|人社局|审计局|公安局|公安分局|派出所|司法局|民政局|档案局|督查办|督查室|扶贫开发局|" \
    u"扶贫办|扶贫局|发改局|统计局|交通运输局|交通局|商旅局|工商局|旅游局|商业局|商务局|招商局|供销联社|财政局|" \
    u"住房公积金中心|住房公积金管理处|农办|农业局|林业局|水务局|管理局|管理处|住建局|国土局|国土分局|环保局|防洪办|" \
    u"教体局|教育局|体育局|卫计局|卫生局|防震减灾局|文广新局|文化局|新闻局|文广局|投促局|投资促进局|国保大队|邮政局|" \
    u"气象局|食药监局|质监局|烟草专卖局|烟草局|供电局|国税局|地税局|税务局|惠民帮扶中心|帮扶中心|金融办|城管局|规划局|" \
    u"规划分局|消防大队|消防局|消防队|党校|总工会|妇联|台办|工商联|残联|红十字会|机关事务局|机关事务管理局|网信办|土地局|" \
    u"信访办|信访局|建设局|工商税务|园林局|管理委员会|接待办|人防|人民防空|农牧局|渔业局|保障局|药监局|交警队|刑警队|" \
    u"特警队|防暴警察|维稳办|安全局|档案局|物价局|空管局|执法局|管委会|测绘局|勘测局|勘探局|" \
    u"有关部门|相关部门|政府部门|医院|学校|城管|督察组|中学|小学|幼儿园"


# 获取敏感词库信息
def get_sensitive_word_list():
    t_file_in = './data/sensitive_word_userdict.txt'
    with open(t_file_in,'rt', encoding='utf-8') as f_in:
        pattern1 = re.compile(r' ')
        sensitive_word_list = []
        while True:
            s = f_in.readline()
            t_strlen = len(s)
            if t_strlen > 0:
                match1 = pattern1.search(s)
                if match1:
                    t_str_list = pattern1.split(s)
                    if t_str_list[0] != '':
                        sensitive_word_list.append(t_str_list[0])
            else:
                break
    return sensitive_word_list


# 预警前端
def match_warning_keywords_frontend(content, type="sensitive"):
    match_results = []
    if type == "sensitive":
        sensitive_words = get_sensitive_word_list()
        key_words_str = '|'.join(sensitive_words)
    elif type == "department":
        key_words_str = gov_department_keyword_new
    elif type == "guanzhi":
        key_words_str = gov_guanzhi_keyword_new
    else:
        print("没有当前类别对应的关键词，请重新输入类别:sensitive/department/guanzhi")
        return False
    # print(key_words_str)
    words_res = re.findall(key_words_str, content)
    if len(words_res) > 0:
        words_remove = set(words_res)
        total = len(words_res)
        for word in words_remove:
            match_dict = {}
            match_dict["word"] = word
            count = words_res.count(word)
            match_dict["count"] = count
            match_dict["freq"] = round(count/total, 4)
            match_dict["type"] = type
            match_results.append(match_dict)
    return match_results


trace_db_info = {
    "host": "39.108.127.36",
    "port": "5432",
    "user": "postgres",
    "pwd": "jiatao",
    "db_name": "public_sentiment",
}
base_events_sync_table = "base_events_sync_table"
trace_seed_table = "public_sentiment_trace_seed_table"
trace_detail_table = "public_sentiment_trace_detail_table"
trace_info_table = "public_sentiment_trace_info_table"

trace_db_obj = DataBasePython(host=trace_db_info["host"], user=trace_db_info["user"],
                              pwd=trace_db_info["pwd"], port=trace_db_info["port"])

# warning_path = './data/warning_frontend/'
TRACE_LIMIT = 10
COMMENTS_LIMIT = 100
SHOW_COMMENTS_LIMIT = 6
# max_key_words_num = 10
history_events_limit = 10

# 微博数、评论数、阅读数、转发数、基础评论长度的系数，用于舆情评估模型
g_all_events = {
    "1000":{
        "events_model": "稳定",
        "events_type": 1000,
        "parameters": {
            "K_weibo": 6,  # 计算影响力value值的参数
            "K_read": 1,
            "K_comment": 3,
            "K_share": 2,
            "Size_comment": 5,
            "Events_value_Thd": 1000,  # 影响力高于此值的事件才需要跟踪，产生trace seed
            "Events_trace_end_cnt_Thd": 5,  # 停止跟踪的判断次数（连续5次速度小于门限）
            "Events_trace_end_v_Thd": 10,  # 停止跟踪的判断速度值，单位（影响力val/小时）
        }
    },

    "2000":{
        "events_model": "环境",
        "events_type": 2000,
        "parameters": {
            "K_weibo": 6,  # 计算影响力value值的参数
            "K_read": 1,
            "K_comment": 3,
            "K_share": 2,
            "Size_comment": 5,
            "Events_value_Thd": 100,  # 影响力高于此值的事件才需要跟踪，产生trace seed
            "A_WARNING_Thd": 300,  # 影响力高于此值的事件属于A级预警（区域事件）
            "B_WARNING_Thd": 800,  # 影响力高于此值的事件属于B级预警（省域事件）
            "C_WARNING_Thd": 5000,  # 影响力高于此值的事件属于C级预警（全国事件）
            "Events_trace_v_Thd": 720,  # 当跟踪的平均速度>=720(val/小时)时，采样频率最高，T最短为T_base(默认20分钟)，
            # 这样的话，当速度<=10(val/小时)时，trace_T就增加到24小时。
            # 平均速度<=30(val/小时)时，trace_T就增加到8小时，可以跨过夜间无人更新微博的时间段。
            # t=T_base/(1+(v-Events_trace_v_Thd)/Events_trace_v_Thd)，
            "Events_trace_inactive_Thd": 3600 * 24,  # 非实时跟踪事件的判断条件（跟踪周期为1天一次的为非实时跟踪）
            "Events_trace_end_cnt_Thd": 5,  # 停止跟踪的判断次数（连续5次速度小于门限）
            "Events_trace_end_v_Thd": 10,  # 停止跟踪的判断速度值，单位（影响力val/小时）
        }
    }
}

warning_dict = {
    "STABLE_WARNING": {
        "events_model": "稳定",
        "events_type": 1000,
        "parameters": {
            "K_weibo": 6, # 计算影响力value值的参数
            "K_read": 1,
            "K_comment": 3,
            "K_share": 2,
            "Size_comment": 5,
            "Events_value_Thd": 500, # 影响力高于此值的事件才需要跟踪，产生trace seed
            "Events_weibo_num_Thd": 3,  # 同一主题的微博数量大于门限的事件才需要跟踪，产生trace seed
            "A_WARNING_Thd": 20000,  # 影响力高于此值的事件属于A级预警（区域事件）
            "B_WARNING_Thd": 50000,  # 影响力高于此值的事件属于B级预警（省域事件）
            "C_WARNING_Thd": 100000,  # 影响力高于此值的事件属于C级预警（全国事件）
            "Events_trace_v_Thd": 720, # 当跟踪的平均速度>=720(val/小时)时，采样频率最高，T最短为T_base(默认20分钟)，
                                    # 这样的话，当速度<=10(val/小时)时，trace_T就增加到24小时。
                                    # 平均速度<=30(val/小时)时，trace_T就增加到8小时，可以跨过夜间无人更新微博的时间段。
                                    # t=T_base/(1+(v-Events_trace_v_Thd)/Events_trace_v_Thd)，
            "Events_trace_inactive_Thd": 3600 * 24,# 非实时跟踪事件的判断条件（跟踪周期为1天一次的为非实时跟踪）
            "Events_trace_end_cnt_Thd": 5, # 停止跟踪的判断次数（连续5次速度小于门限）
            "Events_trace_end_v_Thd": 10, # 停止跟踪的判断速度值，单位（影响力val/小时）
        }
    },
    "ENV_POTENTIAL": {
        "events_model": "环境",
        "events_type": 2000,
        "parameters": {
            "K_weibo": 6,  # 计算影响力value值的参数
            "K_read": 1,
            "K_comment": 3,
            "K_share": 2,
            "Size_comment": 5,
            "Events_value_Thd": 100,  # 影响力高于此值的事件才需要跟踪，产生trace seed
            "Events_weibo_num_Thd": 2,  # 同一主题的微博数量大于门限的事件才需要跟踪，产生trace seed
            "A_WARNING_Thd": 300,  # 影响力高于此值的事件属于A级预警（区域事件）
            "B_WARNING_Thd": 800,  # 影响力高于此值的事件属于B级预警（省域事件）
            "C_WARNING_Thd": 5000,  # 影响力高于此值的事件属于C级预警（全国事件）
            "Events_trace_v_Thd": 720,  # 当跟踪的平均速度>=720(val/小时)时，采样频率最高，T最短为T_base(默认20分钟)，
            # 这样的话，当速度<=10(val/小时)时，trace_T就增加到24小时。
            # 平均速度<=30(val/小时)时，trace_T就增加到8小时，可以跨过夜间无人更新微博的时间段。
            # t=T_base/(1+(v-Events_trace_v_Thd)/Events_trace_v_Thd)，
            "Events_trace_inactive_Thd": 3600 * 24,  # 非实时跟踪事件的判断条件（跟踪周期为1天一次的为非实时跟踪）
            "Events_trace_end_cnt_Thd": 5,  # 停止跟踪的判断次数（连续5次速度小于门限）
            "Events_trace_end_v_Thd": 10,  # 停止跟踪的判断速度值，单位（影响力val/小时）
        }
    }
}

# NODE_CODES = ['STABLE_WARNING', 'ENV_POTENTIAL']


# 找出当前正在跟踪的事件
def get_running_trace_seed_list(events_type, record_now, limit=0):
    # 2018/8/23 注掉， 应该没问题
    # sql = "select a.events_head_id, a.gov_id, a.gov_name, a.key_word_str, a.start_date, a.sync_time from %s a, " \
    #       "(select events_head_id, max(sync_time) as sync_time from %s where trace_state=%d and events_type=%d" \
    #       "group by events_head_id) b where a.sync_time = b.sync_time and a.events_head_id = b.events_head_id order by start_date desc;"%\
    #       (trace_seed_table, trace_seed_table, trace_state, events_type)
    # 2018/8/23 沁哥改了代码，当前追踪中的事件从seed里拿出search_cnt，用这个实时更新的search_cnt去detail表取数据【历史数据不用取search_cnt——为null】
    if record_now:
        trace_state = 2
        sql = "select events_head_id, gov_id, gov_name, key_word_str, start_date, sync_time, search_cnt from %s where trace_state=%d and events_type=%d order by start_date desc"%(trace_seed_table, trace_state, events_type)

    else:
        trace_state = 4
        sql = "select events_head_id, gov_id, gov_name, key_word_str, start_date, sync_time from %s where trace_state=%d and events_type=%d order by start_date desc"%(trace_seed_table, trace_state, events_type)

    if limit > 0:
        sql = sql + " limit %d"%limit
    rows = trace_db_obj.select_data_from_db_one_by_one(trace_db_info["db_name"], sql)
    return rows


# 找出追踪相关信息 —— 要判断一下data_num / 判断有没有找到weibo_detail —— 2018/8/7后不用再判断
def get_events_trace_info(events_head_id, limit=0, gov_id=None):
    in_time = time.time()
    if gov_id is None:
        if limit > 0:
            sql = "select events_head_id, gov_id, search_key, do_time, data_num, trace_v, trace_a, trace_t, count_read, count_comment, count_share, warning_a, " \
                  "warning_b, warning_c from %s where events_head_id='%s' order by do_time ASC limit %d"%\
                  (trace_info_table, events_head_id, limit)
        else:
            sql = "select events_head_id, gov_id, search_key, do_time, data_num, trace_v, trace_a, trace_t, count_read, count_comment, count_share, warning_a, " \
                  "warning_b, warning_c from %s where events_head_id='%s' order by do_time ASC"%(trace_info_table, events_head_id)

    # 2018/8/7 更新SQL，加入gov_id判断，多取search_cnt
    else:
        if limit > 0:
            sql = "select events_head_id, gov_id, search_key, do_time, data_num, trace_v, trace_a, trace_t, count_read, count_comment, count_share, warning_a, warning_b, warning_c, search_cnt from %s where events_head_id='%s' and gov_id = %d order by do_time ASC limit %d" % (trace_info_table, events_head_id, gov_id, limit)
        else:
            sql = "select events_head_id, gov_id, search_key, do_time, data_num, trace_v, trace_a, trace_t, count_read, count_comment, count_share, warning_a, warning_b, warning_c, search_cnt from %s where events_head_id='%s' and gov_id = %d order by do_time ASC"%(trace_info_table, events_head_id, gov_id)

    rows = trace_db_obj.select_data_from_db_one_by_one(trace_db_info["db_name"], sql)
    get_trace_time = time.time()
    print("Single Process-%s get trace time: %s" % (os.getpid(), (get_trace_time - in_time)), flush=True)
    return rows


# 找出weibo_detail —— 可能有seed， 但没有跟踪到微博，如果这样的话，前端就不展示; 筛选之后保证每条微博输出最新一次追踪的信息，并拼接所有的评论
def get_events_detail_weibo(events_head_id, with_comments=True, search_cnt=None, gov_id=None):
    in_time = time.time()
    # 拼接全量comments
    if with_comments:
        sql = "SELECT a.events_head_id, a.gov_id, a.pub_time, a.do_time, a.content, b.comments, a.count_read, a.count_comment," \
              "a.count_share, a.post_name, a.followers_count, a.follow_count, a.last_comment_time FROM public_sentiment_trace_detail_table a, (SELECT data_id, max(events_head_id) " \
              "as events_head_id,  MAX(do_time) AS do_time, string_agg(substring(comments, 2, length(comments)-2),',') " \
              "AS comments FROM public_sentiment_trace_detail_table WHERE events_head_id = '%s'" \
              " GROUP BY data_id) b WHERE a.do_time = b.do_time AND a.events_head_id = b.events_head_id AND " \
              "a.data_id= b.data_id;"%events_head_id
    else:
        if search_cnt is None:
            sql = "SELECT a.events_head_id, a.gov_id, a.data_id, a.pub_time, a.do_time, a.content, a.count_read, a.count_comment," \
                  "a.count_share, a.post_name, a.followers_count, a.follow_count, a.last_comment_time FROM public_sentiment_trace_detail_table a, (SELECT max(events_head_id) " \
                  "as events_head_id,  MAX(search_cnt) AS search_cnt FROM public_sentiment_trace_info_table WHERE events_head_id = '%s') b WHERE a.search_cnt = b.search_cnt AND a.events_head_id = b.events_head_id " % events_head_id

        # 2018/8/7 优化SQL
        else:
            if gov_id is None:
                sql = "select events_head_id, gov_id, data_id, pub_time, do_time, content, count_read, count_comment, count_share, post_name, followers_count, follow_count, last_comment_time from %s where events_head_id = '%s' and search_cnt = %d order by count_comment DESC"%(trace_detail_table, events_head_id, search_cnt)
            else:
                sql = "select events_head_id, gov_id, data_id, pub_time, do_time, content, count_read, count_comment, count_share, post_name, followers_count, follow_count, last_comment_time from %s where events_head_id = '%s' and search_cnt = %d and gov_id = %d order by count_comment DESC" % (
                trace_detail_table, events_head_id, search_cnt, gov_id)

    rows = trace_db_obj.select_data_from_db_one_by_one(trace_db_info["db_name"], sql)
    get_detail_time = time.time()
    print("Single Process-%s get detail time: %s" % (os.getpid(), (get_detail_time - in_time)), flush=True)
    return rows


# 处理comments里的内容
def deal_with_comments(comments, comments_typical=True):
    # comments_list = comments.split('-->')

    # pattern = re.compile(r'\{".*?"\}')
    # comments_list = pattern.findall(str(comments))
    # real_comments_list = [eval(cmt) for cmt in comments]

    # comments字段存的就是list，所以不用eval了
    real_comments_list = comments
    # print(real_comments_list)

    # for cmt in real_comments_list:
    #     # cmt = eval(cmt)
    #     cmt['content'] = TextDispose(cmt['content']).get_weibo_valid_info()

    # 多进程
    p = Pool(10)
    for i in range(len(real_comments_list)):

        # 典型评论，已经把原始评论的content字段都抽出来了
        if comments_typical:
            real_comments_list[i] = p.map(TextDispose(real_comments_list[i]).get_weibo_valid_info, (real_comments_list[i],))[0]

        # 针对评论，是dict的list
        else:
            real_comments_list[i]['content'] = p.map(TextDispose(real_comments_list[i]['content']).get_weibo_valid_info,(real_comments_list[i]['content'],))[0]
    p.close()
    p.join()
    # print(real_comments_list)
    return real_comments_list


# 生成数据表
def get_events_data(version_date, events_type, record_now=True, events_limit=None):
    print("==========================\nversion_date=%s    events_type=%s    record_now=%s"%(version_date, events_type, record_now), flush=True)
    start_time = time.time()
    # 影响力权重值
    parameters = g_all_events[str(events_type)]["parameters"]
    K_weibo = parameters['K_weibo']
    K_read = parameters['K_read']
    K_comment = parameters['K_comment']
    K_share = parameters['K_share']
    
    # 取事件
    # if record_now:
    if events_limit is None:
        running_seed_list = get_running_trace_seed_list(events_type=events_type, record_now=record_now)
    else:
        running_seed_list = get_running_trace_seed_list(events_type=events_type, record_now=record_now, limit=events_limit)
    # 取历史数据 —— events_head_id有重复
    # else:


    seed_time = time.time()
    print("Get running seeds done: %s" % (seed_time - start_time), flush=True)

    # 2018/8/23 seed表追踪事件和历史事件取法不一
    if record_now:
        df_basic_events = pd.DataFrame(running_seed_list, columns=['events_head_id', 'gov_id', 'gov_name', 'key_word_str', 'start_time', 'sync_time', 'search_cnt'])
        # 清洗search_cnt为nan的情况
        # if record_now:
        df_basic_events.drop(df_basic_events.index[df_basic_events['search_cnt'] != df_basic_events['search_cnt']],inplace=True)
    else:
        df_basic_events = pd.DataFrame(running_seed_list, columns=['events_head_id', 'gov_id', 'gov_name', 'key_word_str', 'start_time', 'sync_time'])

    # 对历史事件的events_head_id去重 —— 历史事件的events_head_id有重复啊怒！！！-2018/8/17  --当前事件也要去重。。。尼玛。。2018/8/20
    # if not record_now:
    events_head_ids_origin = list(df_basic_events['events_head_id'])
    events_head_ids_exact = set(events_head_ids_origin)
    for events_head_id in events_head_ids_exact:
        if events_head_ids_origin.count(events_head_id) <= 1:
            continue
        df_basic_events.drop(df_basic_events.index[(df_basic_events['events_head_id'] == events_head_id) & (
                df_basic_events.sync_time != df_basic_events[df_basic_events['events_head_id'] == events_head_id][
            'sync_time'].max())], inplace=True)

    df_basic_events.reset_index(drop=True, inplace=True)

    # 删掉高新区的数据 —— 暂时没法上前端
    events_gov_ids = df_basic_events['gov_id'].tolist()
    events_gov_ids_exact = [i if i in df_2861_county['gov_id'].tolist() else 0 for i in events_gov_ids]

    # if gov_id not in df_2861_county['gov_id'].tolist():
    #     continue

    df_basic_events.drop(df_basic_events.index[df_basic_events['gov_id'] != events_gov_ids_exact], inplace=True)


    for col in list(df_basic_events):
        globals()[col+'s'] = list(df_basic_events[col])

    # events_head_ids_new = []
    # gov_ids_new = []
    # gov_names_new = []
    # key_word_strs_new = []
    # start_times_new = []
    weibo_details_all = []
    weibo_details_columns = ['events_head_id', 'gov_id', 'data_id', 'pub_time', 'do_time', 'content', 'count_read',
                             'count_comment', 'count_share', 'post_name', 'followers_count', 'follow_count',
                             'last_comment_time']
    values_for_pic = []
    values_columns = ['events_head_id', 'gov_id', 'search_key', 'do_time', 'data_num', 'trace_v', 'trace_a', 'trace_t','count_read', 'count_comment', 'count_share', 'warning_a', 'warning_b', 'warning_c', 'search_cnt']

    seed_clean = time.time()
    print("Clean seed table done: %s" % (seed_clean - seed_time), flush=True)

    # 多进程
    # p = Pool(10)
    # for i in range(len(events_head_ids)):
    #     weibo_details = get_events_detail_weibo(events_head_ids[i], with_comments=False)
    #     if len(weibo_details) != 0:
    #         # print(weibo_details)
    #         trace_info = get_events_trace_info(events_head_ids[i])
    #         # 判断trace_t 是否等于一天， 是的话就不show
    #         if trace_info[-1][7] >= 86400:
    #             continue
    #         events_head_ids_new.append(events_head_ids[i])
    #         gov_ids_new.append(gov_ids[i])
    #         gov_names_new.append(gov_names[i])
    #         key_word_strs_new.append(key_word_strs[i])
    #         start_times_new.append(start_times[i])
    #
    #         weibo_details_all.extend(weibo_details)
    #         values_for_pic.extend(trace_info)

    # 2018/08/07 优化代码，SQL语句等
    max_comment_dataids = []
    events_head_ids_new = []
    gov_ids_new = []
    gov_names_new = []
    key_word_strs_new = []
    start_times_new = []
    for events_head_id in events_head_ids:
        event_index = events_head_ids.index(events_head_id)
        gov_id = gov_ids[event_index]
        trace_info = get_events_trace_info(events_head_id, gov_id=gov_id)

        # # 需要判断一个事件的追踪周期是否有重复，有的话这个事件就暂时不要
        # one_event_trace_scnts = [i[-1] for i in trace_info]
        if record_now:
            search_cnt = search_cnts[events_head_ids.index(events_head_id)]
        else:
            search_cnt = max([i[-1] for i in trace_info])
        weibo_details = get_events_detail_weibo(events_head_id, with_comments=False, search_cnt=search_cnt, gov_id=gov_id)
        if len(weibo_details) == 0:
            continue
        #     max_comment_dataid = "0"
        # else:
        events_head_ids_new.append(events_head_id)
        gov_ids_new.append(gov_id)
        gov_names_new.append(gov_names[event_index])
        key_word_strs_new.append(key_word_strs[event_index])
        start_times_new.append(start_times[event_index])
        # sync_times_new.append(sync_times[event_index])

        max_comment_dataid = weibo_details[0][2]
        max_comment_dataids.append(max_comment_dataid)
        weibo_details_all.extend(weibo_details)
        values_for_pic.extend(trace_info)
            
    trace_detail_time = time.time()
    print("Get details and trace info done: %s" % (trace_detail_time - seed_clean), flush=True)

    # 微博详情
    if len(weibo_details_all) != 0:
        df_warning_details = pd.DataFrame(weibo_details_all, columns=weibo_details_columns)
        for i in ['pub_time', 'do_time', 'last_comment_time']:
            df_warning_details[i] = df_warning_details[i].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_warning_details = df_warning_details.astype('object')
        # df_warning_details = df_warning_details.astype('object')
        # df_warning_details['comments'] = df_warning_details['comments'].apply(lambda x: deal_with_comments(x))



        # 每个事件取评论数最多的那条微博 （及其) 的评论
        # data_ids = []
        # weibo_contents = []
        comments = []
        for events_head_id in events_head_ids_new:
            print("\r%d/%d:%s"%(events_head_ids_new.index(events_head_id), len(events_head_ids_new), events_head_id), end='', flush=True)

            data_id_max = max_comment_dataids[events_head_ids_new.index(events_head_id)]
            data_ids_chosen = df_warning_details[df_warning_details['events_head_id']==events_head_id][['count_comment','data_id']].sort_values(by=['count_comment'], ascending=False)["data_id"].tolist()
            merge_comments = []
            j = 0
            while True:
                # 是取到就行 还是取到100条才行？
                if len(merge_comments) >= COMMENTS_LIMIT:
                    break
                if j == len(data_ids_chosen):
                    break
                else:
                    if j == 0:
                        data_id = data_id_max
                    else:
                        data_id = data_ids_chosen[j]
                # 最大评论数为0， 直接break 不再循环
                # a = df_warning_details.loc[(df_warning_details['data_id'] == data_id)&(df_warning_details['events_head_id']==events_head_id), 'count_comment']
                # b = int(a)
                if int(df_warning_details.loc[(df_warning_details['data_id'] == data_id)&(df_warning_details['events_head_id']==events_head_id), 'count_comment'].max()) == 0:
                    merge_comments.extend([{"content":"无"}])
                    break
                for i in range(10):
                    if len(merge_comments) >= COMMENTS_LIMIT:
                        break
                    sql_comments = "SELECT comments from public_sentiment_trace_detail_table where events_head_id = '%s' and data_id = '%s' and comments != '[]' order by search_cnt desc limit %d offset %d"%(events_head_id, data_id, TRACE_LIMIT, i*TRACE_LIMIT)
                    rows = trace_db_obj.select_data_from_db_one_by_one(trace_db_info["db_name"], sql_comments)
                    if len(rows) == 0:
                        break
                    for row in rows:
                        row_comments = eval(row[0])
                        if len(merge_comments) >= COMMENTS_LIMIT:
                            break
                        merge_comments.extend(row_comments)
                j += 1

            # comments.append(str(merge_comments))
            comments.append(merge_comments)
            # weibo_contents.append(weibo_content)
        get_comments_time = time.time()
        print("Get 100 comments : %s" % (get_comments_time - trace_detail_time), flush=True)

        df_warning_weibo_comments = pd.DataFrame({"events_head_id":events_head_ids_new, "data_id":max_comment_dataids, "comments":comments})   # "content":weibo_contents, —— 去掉content

        df_warning_weibo_comments = df_warning_weibo_comments.astype('object')

        # 先不清洗comments，先对comments都过关键词，提取典型评论
        df_warning_weibo_comments['comments_only'] = df_warning_weibo_comments['comments'].apply(
            lambda x: [a["content"] for a in x])
        df_warning_weibo_comments['comments_typical'] = df_warning_weibo_comments['comments_only'].apply(
            lambda x: [a for a in x if sum([len(match_warning_keywords_frontend(a, b)) for b in ['sensitive', 'guanzhi', 'department']]) != 0])

        typical_comments_time = time.time()
        print("Get typical comments: %s" % (typical_comments_time - get_comments_time), flush=True)

        # 再提取前端需要的典型评论条数 进行清洗
            
        # 先提量， 再清洗
        if 1:
            df_warning_weibo_comments['comments_shown'] = df_warning_weibo_comments['comments_typical'].apply(
                lambda x: x[0:SHOW_COMMENTS_LIMIT])
            df_warning_weibo_comments['comments_shown'] = df_warning_weibo_comments['comments_shown'].apply(lambda x: deal_with_comments(x))
            # df_warning_weibo_comments.to_csv('comments_test2.csv', encoding='utf-8-sig')
            
        # 全量清洗
        if 0:
            df_warning_weibo_comments['comments_shown'] = df_warning_weibo_comments['comments_shown'].apply(
                lambda x: deal_with_comments(x))
            # df_warning_weibo_comments.to_csv('comments_test3.csv', encoding='utf-8-sig')


        deal_comments_time = time.time()
        print("Deal with comments signs: %s" % (deal_comments_time - typical_comments_time), flush=True)

        # df_warning_details.to_csv('details.csv', encoding='utf-8-sig')
        
        
        # 事件关键词
        key_words_all_events = []
        for event_head_id in events_head_ids_new:
            position = events_head_ids_new.index(event_head_id)
            series_event = df_warning_details[df_warning_details["events_head_id"] == event_head_id]["content"]
            series_comments = df_warning_weibo_comments[df_warning_weibo_comments["events_head_id"] == event_head_id]["comments_typical"].apply(lambda x:''.join(x))
            content_merge = ''.join(series_event)
            comments_merge = ''.join(series_comments)
            content_and_comments = content_merge+comments_merge
            for type in ['sensitive', 'guanzhi', 'department']:
                key_words_list = match_warning_keywords_frontend(content_and_comments, type)
                if len(key_words_list) == 0:
                    continue
                for key_word in key_words_list:
                    key_word["events_head_id"] = event_head_id
                    key_word["gov_id"] = gov_ids_new[position]
                    key_word["gov_name"] = gov_names_new[position]
                    key_word["key_word_str"] = key_word_strs_new[position]
                    key_word["start_time"] = start_times_new[position]
                key_words_all_events.extend(key_words_list)
        get_keywords_time = time.time()
        print("Get keywords: %s" % (get_keywords_time - deal_comments_time), flush=True)

        if len(key_words_all_events) != 0:
            df_warning_key_words = pd.DataFrame(key_words_all_events)
            df_warning_key_words['start_time'] = df_warning_key_words['start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            # df_warning_key_words.to_csv('keywords.csv', encoding='utf-8-sig')
        else:
            df_warning_key_words = pd.DataFrame()

        comments_words_time = time.time()
        print("Get typical comments and keywords done : %s" % (comments_words_time - get_keywords_time), flush=True)

    else:
        df_warning_details = pd.DataFrame()
        df_warning_weibo_comments = pd.DataFrame()
        df_warning_key_words = pd.DataFrame()

    # 数据信息
    if len(values_for_pic) != 0:
        df_warning_trace_info = pd.DataFrame(values_for_pic, columns=values_columns)
        df_warning_trace_info['do_time'] = df_warning_trace_info['do_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_warning_trace_info['weibo_value'] = K_weibo*df_warning_trace_info['data_num'] + \
                                               K_comment*df_warning_trace_info['count_comment'] + \
                                               K_read*df_warning_trace_info['count_read'] + \
                                               K_share*df_warning_trace_info['count_share']
        # df_warning_trace_info.to_csv('trace_info.csv', encoding='utf-8-sig')

        end_time = time.time()
        print("Other time: %s" % (end_time - comments_words_time), flush=True)
    else:
        df_warning_trace_info = pd.DataFrame()



    return df_warning_trace_info, df_warning_key_words, df_warning_details, df_warning_weibo_comments, len(events_head_ids_new)


# 定时生成数据文件
sched = BlockingScheduler()


# @sched.scheduled_job('cron', hour='5-23', minute='*/15')
@sched.scheduled_job('cron', hour='6-23', minute='50')
def fetch_events_data_regularly():
    for node_code in warning_dict.keys():
        record_now = True
        # 先不管稳定
        if 0:
            if node_code == "STABLE_WARNING":
                continue
        monitor_datetime = datetime.now()
        monitor_time = datetime.strftime(monitor_datetime, '%Y-%m-%d-%H-%M-%S')
        record_time = datetime.strftime(monitor_datetime, '%Y-%m-%d %H:%M:%S')
        # time_dir = '-'.join('-'.join(monitor_time.split(' ')).split())
        events_type = warning_dict[node_code]["events_type"]
        df_warning_trace_info, df_warning_key_words, df_warning_details, df_warning_weibo_comments, events_num = get_events_data(monitor_time, events_type)
        if events_num <= 0:
            record_now = False
            df_warning_trace_info, df_warning_key_words, df_warning_details, df_warning_weibo_comments, events_num = get_events_data(monitor_time, events_type, record_now=record_now, events_limit=history_events_limit)

        # 事件信息储存
        events_node_dir = event_data_path + node_code + '/'
        events_data_dir = events_node_dir + monitor_time + '/'
        if not os.path.exists(events_data_dir):
            os.makedirs(events_data_dir)
        df_warning_trace_info.to_csv(events_data_dir+'trace_info.csv', encoding='utf-8-sig')
        df_warning_key_words.to_csv(events_data_dir+'keywords.csv', encoding='utf-8-sig')
        df_warning_details.to_csv(events_data_dir+'details.csv', encoding='utf-8-sig')
        df_warning_weibo_comments.to_csv(events_data_dir+'comments.csv', encoding='utf-8-sig')

        # 转为dict / json 存取
        trace_dict = df_warning_trace_info.to_dict()
        keywords_dict = df_warning_key_words.to_dict()
        details_dict = df_warning_details.to_dict()
        comments_dict = df_warning_weibo_comments.to_dict()

        # 写入json文件
        json.dump(trace_dict, open(events_data_dir+'trace_info.json', 'w'))
        json.dump(keywords_dict, open(events_data_dir+'keywords.json','w'))
        json.dump(details_dict, open(events_data_dir+'details.json', 'w'))
        json.dump(comments_dict, open(events_data_dir+'comments.json', 'w'))

        with open(events_node_dir+'version.txt', 'a', encoding='utf-8-sig') as fp:
            fp.write(monitor_time+'\n')
        fp.close()

        # 记录拿数据的信息和时间
        logger.info('-->events:%s-->monitor_time:%s-->record_now:%s-->events_num:%d-->time_taken:%.3f minutes'%(node_code, monitor_time, record_now, events_num, (datetime.now()-monitor_datetime).seconds/60))

    return

if __name__ == "__main__":
    version_date = time.strftime('%Y-%m-%d %H:%M:%S')
    # 环保
    if 0:
        df_warning_trace_info, df_warning_key_words, df_warning_details, df_warning_weibo_comments, events_num = get_events_data(version_date, 2000)
    # 稳定
    if 0:
        df_warning_trace_info, df_warning_key_words, df_warning_details, df_warning_weibo_comments, events_num = get_events_data(version_date, 1000)


    # 调试
    if 0:
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        events_type = 1000
        get_events_data(current_time, events_type)

    # 测试定时跑
    if 1:
        sched.start()

    # 测试
    if 0:
        fetch_events_data_regularly()

    if 0:
        # version_date =
        events_type = 2000
        get_events_data(version_date, events_type, record_now=False, events_limit=None)


    pass




















