#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2018/8/4 11:55
@Author  : Liu Yusheng
@File    : xmd_events_warning_frontend_display.py
@Description: 預警前端文件生成 —— 用服務器上定時程序啟動
"""

# 系统模块
import os
import json
import shutil
import paramiko
import re

# 第三方模块
import pandas as pd
import numpy as np
from copy import deepcopy
from apscheduler.schedulers.background import BlockingScheduler
from multiprocessing import Pool
import time
from datetime import datetime, timedelta
import threading
import collections

# 自定义模块
from db_interface import database
from xmd_events_warning_data_generation_crontab import get_events_data, trace_db_obj, trace_info_table, trace_db_info, trace_seed_table

from utilities.trace_event_line_fit import lineFitting
from utilities import web_charts


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

            # "A_WARNING_Thd": 3800,  # 影响力高于此值的事件属于A级预警（区域事件） —— 2000000*1.9‰
            # "B_WARNING_Thd": 57000,  # 影响力高于此值的事件属于B级预警（省域事件）—— 30000000*1.9‰
            # "C_WARNING_Thd": 190000,  # 影响力高于此值的事件属于C级预警（全国事件）—— 100000000*1.9‰
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

            # "A_WARNING_Thd": 300,  # 影响力高于此值的事件属于A级预警（区域事件）  3800 / 12.67
            # "B_WARNING_Thd": 4500,  # 影响力高于此值的事件属于B级预警（省域事件）
            # "C_WARNING_Thd": 15000,  # 影响力高于此值的事件属于C级预警（全国事件）
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

data_path = './data/'
sesi_words_path = data_path + 'sensitive_word_userdict.txt'
gaode_geo_path = data_path + 'df_2861_gaode_geo_new.csv'

client_path = '../apps/'
event_data_path = '../events_data/'

df_2861_geo_county = pd.read_csv(gaode_geo_path, index_col='gov_code', encoding='utf-8')
df_2861_county = df_2861_geo_county[df_2861_geo_county['gov_type'] > 2]

# 常用颜色
COL_LIGHT_YELLOW = "rgba(255,228,47,.8)"  # 柱子常用
COL_NOBEL_BLUE = "rgba(56,220,255,.8)"

# 常用颜色
FT_SOBER_BLUE = "rgba(84, 180, 415, 1)"  # 字体常用
FT_DEEP_BLUE = "#5696d8"
FT_ORANGE = "orange"
FT_BLUE_WITH_LG = "#000937"
FT_LIGHT_RED = "#EE4000"
FT_PURE_WHITE = "#FFFFFF"
FT_YELLOW_WITH_DB = "#ffde79"
FT_BLACK = "#111"
FT_LIGHT_GRAY = "LightSlateGray"

# 按钮专用
BT_TIFFANY_BLUE = "#00ada9"
BT_DEEP_GREEN = "rgba(15, 163, 102, 1)"

# 背景常用
BG_YELLOW = "#ffb10a"
BG_GREEN = "#00c574"
BG_BLUE = "#224276"
BG_RED = "#ff1c47"

# 划线常用
LN_GOLDEN = "rgba(251,189,4,1)"
LN_YELLOW = "rgba(253,253,2,1)"
LN_RED = "rgba(227,8,10,1)"
ZZ_BLUE = "#9ff2e7"
LN_WARNING_PINK = "rgba(250,192,230,1)"

# 统一之后
UN_TITLE_YELLOW = "#ffcc00"
UN_STRESS_RED = "#ff1133"
UN_LIGHT_GREEN = "#6cf2cb"

BAR_PINK = "rgb(237,85,101)"

WCOLORS = {'A':LN_GOLDEN, "B":LN_YELLOW, "C":LN_RED, '追':ZZ_BLUE}

scopes_dict = {"A":"区域级", "B":"省域级", "C":"全国级", "追":"追踪中"}


WSTATUSES = {'warning_a': {'name': 'A级', 'area': '区域', 'desc':'影响较小，易被忽略。但隐患正在积累，蓄势待发。', 'advise':"<span style='color: orange'>立即回应，进行处理</span>。此时处理，提前消除隐患，成本最低，体现对互联网的掌控能力。"},
             'warning_b': {'name': 'B级', 'area': '省域', 'desc':'影响中等，引发政府公信力危机，分管领导有可能被问责。', 'advise':"<span style='color: orange'>积极回应，及时处理</span>。若及时处理，可转危为安，体现应急响应能力。"},
             'warning_c': {'name': 'C级', 'area': '全国', 'desc':'影响恶劣，响应迟钝或者处理不当，地方一把手的执政能力会受到质疑。', 'advise':"<span style='color: orange'>积极参与处理或回应</span>。若积极回应，可与媒体积极互动，提高政府公信力。"}}

# 'warning_c': {'name': 'C级', 'area': '全国', 'desc':'影响恶劣，响应迟钝或者处理不当，地方一把手的执政能力会受到质疑。', 'advise':"<span style='color: orange'>立即处理</span>。若处理得当，可亡羊补牢，降低掉帽子的风险。"

max_key_words_num = 15
max_medias_num = 20
history_events_limit = 10
interhour = 1
SHOW_COMMENTS_LIMIT = 50
SHOW_WEIBO_WORDS_LIMIT = 80
common_folder_code = '110101'
HIDDEN_RATIO = 0.377  # 抽样检出

show_x_nums = 20
page_before_lines = 32
fixed_comments_num = 5  # 固定不滚动的评论数

fitting_min_cnts = 5
events_desc_width = "25%"

# 调整扫描时间
scan_total = 18
unit1_scan = 8
unit2_scan = 6
unit3_scan = 4


df_warning_trace_info = pd.DataFrame()
df_warning_keywords = pd.DataFrame()
df_warning_details = pd.DataFrame()
df_warning_weibo_comments = pd.DataFrame()

# 右侧描述   ——  每个线程（不同gov_code）都在改变这个变量，所以不能把右侧描述设为全局变量，会出错
# list_desc = {}

# 事件相关
EVENTS_HEAD_IDS = []
EVENTS_SHORT_DICT = {}
GOV_IDS = []
GOV_CODES = []
GOV_NAMES = []
NEWLY_WEIBO_VALUES = []
EVENTS_LINKS = []
THD_GRADES = []
SCOPE_GRADES = []
DF_EVENTS_BASIC = pd.DataFrame()

# 展示在全国图上的实时追踪区县
df_trace_group = pd.DataFrame()

# 'we0_value_index_trend'
# 'we0_sens_word_trend'
# 'we0_publisher_trend'

# 服务器信息
source_dict = {"ip":"120.78.222.247", "port":22, "uname":"root", "password":"zhikuspider"}

# 云端数据库
product_server = database.get_database_server_by_nick(database.SERVER_PRODUCT)
product_db = 'product'
stats_table = 'xmd_weibo_stats'
stable_past_table = 'xmd_stable_past_events_renew'
es_stats_table = 'es_event_stats'


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)


# 生成json文件
def write_client_datafile_json(target_dir_path, file_name, postfix, ret_content):
    if not os.path.exists(target_dir_path):
        os.makedirs(target_dir_path)
    with open(target_dir_path + file_name + postfix, 'w') as outfile:
        json.dump(ret_content, outfile, cls=MyEncoder)
    return


# 隐患接口：画有柱子的地图 —— 改一下， title和subtitle默认不给
def get_column_map_dict(area_mode, gov_code, df_id, title=None, subtitle=None, tips=None, column_names=None):
    map_dict = {}
    if title is not None:
        map_dict["title"] = title
    if subtitle is not None:
        map_dict["subtitle"] = subtitle
    map_dict["type"] = "columnMap"
    # 全国
    if area_mode == 1:
        map_dict["geo_data"] = "2861_county_echars_geo"
    # 试一下省图
    elif area_mode == 2:
        map_dict["geo_data"] = "province_" + str(gov_code)[0:2] + "_county_echars_geo"
    # 测试一下市图
    elif area_mode == 3:
        map_dict["geo_data"] = "city_"+ str(gov_code)[0:4] +"_county_echars_geo"

    # 其他乱输的情况 —— 默认给全国图
    else:
        map_dict["geo_data"] = "2861_county_echars_geo"

    details = {}
    datas = []
    columns = list(df_id)
    for index, row in df_id.iterrows():
        data_dict = {}
        for col in columns:
            # print(col)
            data_dict[col] = row[col]

            # 判断一下元素有dict的情况 —— 正则
            if re.search("{", str(row[col])):
                data_dict[col] = eval(row[col])

        datas.append(data_dict)
    details["datas"] = datas
    if tips is None:
        details["tips"] = {"value":"指数", "rank":"排名"}
    else:
        details["tips"] = tips
    column = {}
    # column["column_toggle"] = [True]
    if column_names:
        column["column_names"] = column_names
    else:
        column["column_names"] = ["当前隐患突出的区县"]

    default_colors = list(WCOLORS.values())
    # 全国在最前面
    default_colors[0], default_colors[2] = default_colors[2], default_colors[0]
    default_types = list(WCOLORS.keys())
    default_types[0], default_types[2] = default_types[2], default_types[0]

    # default_types = [i.lower() if i != "追" else "z" for i in default_types]

    column_colors = list(set(df_id["column_color"]))
    column_types = list(set(df_id["column_type"]))

    column_colors.sort(key=default_colors.index)
    column_types.sort(key=default_types.index)

    # column["column_types"] = list(set(df_id["column_type"]))
    # column['column_colors'] = list(set(df_id["column_color"]))

    column["column_colors"] = column_colors
    column["column_types"] = column_types
    column["column_toggle"] = [True] * len(column_types)

    column_names = ["%d个区县："%list(df_id["column_type"]).count(x)+scopes_dict[x]+"事件追踪中" if x != "追" else "%d个区县：隐患追踪中"%list(df_id["column_type"]).count(x) for x in column_types]   # x.upper()

    # column_types = [i if i != "z" else "Z" for i in column_types]
    # column["column_types"] = [i.lower() for i in column_types]
    column["column_names"] = column_names

    details["column"] = column
    map_dict["detail"] = details
    return map_dict


# 通用接口：生成左侧的mixed折线柱状混合图的dict; df_id里至少包括三栏【value, rank可以二选一】——x_name, value(柱子),
#  color(没有的话默认为lightblue), rank(折线); line_type=False: 平滑的曲线, line_type=True: 折线; point_width: 控制柱子大小
def get_mixed_line_dict(title, subtitle, df_id, line_type=False, point_width=20, tips=None, extra_pw=None):
    line_mixed_dict = {}
    line_mixed_dict["type"] = "mixed1"
    line_mixed_dict["title"] = title
    line_mixed_dict["subtitle"] = subtitle
    detail_dict = {}
    if not tips:
        detail_dict["tips"] = {"left":"指数", "right":"排名"}
    else:
        detail_dict["tips"] = tips
    extra_dict = {}
    if line_type:
        extra_dict["line_type"] = "line"
    else:
        extra_dict["line_type"] = ""
    if extra_pw:
        extra_dict["point_width"] = point_width
    detail_dict["extra"] = extra_dict
    columns = list(df_id)
    detail_dict["x_name"] = list(df_id["x_name"])
    if 'rank' in columns:
        detail_dict["right"] = list(df_id["rank"])
    if 'value' in columns:
        left_info = []
        if 'color' not in columns:
            default_color = 'lightblue'
            for index in df_id.index:
                left_dict = {}
                left_dict["y"] = df_id.loc[index, 'value']
                left_dict["color"] = default_color
                left_info.append(left_dict)
        else:
            for index in df_id.index:
                left_dict = {}
                left_dict["y"] = df_id.loc[index, 'value']
                left_dict["color"] = df_id.loc[index, 'color']
                left_info.append(left_dict)
        detail_dict["left"] = left_info
    line_mixed_dict["detail"] = detail_dict
    return line_mixed_dict


# 通用接口：生成多line类型的图——df_data：包括所有lines的数据【column用标志位替代，如a,b,c……】，和x轴的刻度【column名为x_name】
# df_info: 以lines数据的标志位为index, 含两列，lines的name和对应每根线的color/style/dashStyle; xfont: 横轴的样式-{"xfont":18,"fontWeight":"bold","color":"#333"},//fontWeight:bold||normal；linewith：加粗的那根线（type为line）的宽度设置；ZeroMin=True:保证最小值大于0时，取纵轴最小为0--保证绝对位置
def get_lines_graph_dict(title, subtitle, df_data, df_info, y_name, fontshow=False, nodeshow=False, linewith=None, xfont=None, ZeroMin=True, signlist=None, signname=None, max_thd=None, tickx=None):
    lines_graph_dict = {}
    lines_graph_dict["type"] = "line"
    lines_graph_dict["title"] = title
    lines_graph_dict["subtitle"] = subtitle
    data_detail = {}
    # 针对事件预警特殊处理，为了之后将需要标点的事件指数线的数据放在line_list第一位 —— 保证事件指数线的sign为'event'
    data_signs = [i for i in list(df_data) if i != "x_name" and i != 'event']
    if 'event' in list(df_data):
        data_signs.insert(0, 'event')

    if linewith:
        data_detail['linewith'] = linewith
    if tickx:
        data_detail['tickx'] = tickx
    if xfont is not None:
        data_detail['xfont'] = xfont
    data_detail["fontshow"] = fontshow
    data_detail["nodeshow"] = nodeshow
    data_detail["xAxis_name"] = list(df_data["x_name"])
    line_list = []
    all_data_list = []
    for sign in data_signs:
        line_dict = {}
        data_list = []
        # line_dict["name"] = df_info.loc[sign, "name"]
        # line_dict["color"] = df_info.loc[sign, "color"]
        # if 'dashStyle' in list(df_info):
        #     line_dict["dashStyle"] = df_info.loc[sign, "dashStyle"]
        for col_type in list(df_info):
            line_dict[col_type] = df_info.loc[sign, col_type]
        # 事件预警的定制化需求
        # 可能会有传dict而非值的情况，这里做一下判断
        # all_data_list.extend(list(df_data[sign]))
        for i in df_data[sign]:
            # if isinstance(i, int) or isinstance(i, float) or isinstance(i, int64):
            #     all_data_list.append(i)
            #     data_list.append(i)
            if isinstance(i, str) and (i != "null"):
                all_data_list.append(eval(i)['y'])
                data_list.append(eval(i))
            else:
                data_list.append(i)
                if i != "null":
                    all_data_list.append(i)

        line_dict["data"] = data_list
        line_list.append(line_dict)

    data_detail["line_list"] = line_list
    if ZeroMin:
        min_thd = 0
    else:
        min_thd = min(all_data_list)
    if max_thd is None:
        max_thd = max(all_data_list)+10
    data_detail["yAxis"] = {"name": y_name, "min": min_thd, "max":max_thd}
    # signlist控制背景着色的区域 —— 分级预警
    if signlist is not None:
        signlist.append(max_thd)
        data_detail['signlist'] = signlist
    # signname对应不同背景着色区域的级别
    if signname is not None:
        data_detail['signname'] = signname
    lines_graph_dict["detail"] = data_detail
    return lines_graph_dict


# 预警柱子分布图
def get_warning_map_data(gov_code, node_code, parameters, version_date, record_now):
    # global list_desc
    # list_desc["page_list"] = {}

    gov_name = df_2861_county.loc[gov_code, "full_name"]
    gov_type = df_2861_county.loc[gov_code, "gov_type"]

    warning_map_data_list = []
    warning_map_data_name_list = []
    a_thd = parameters['A_WARNING_Thd']
    b_thd = parameters['B_WARNING_Thd']
    c_thd = parameters['C_WARNING_Thd']
    event_type = warning_dict[node_code]["events_model"]
    thd_dict = {"A级": a_thd, "B级": b_thd, "C级": c_thd}
    color_dict = {"A级": LN_GOLDEN, "B级": LN_YELLOW, "C级": LN_RED}
    grades = ["A级", "B级", "C级"]
    events_grades = ["区域内", "省域级", "全国性"]
    tips = {"events_num":"追踪事件数", "lala":"点击圆点"}   # "value":"事件影响力指数", "rank":"影响力排名",

    # # 尝试画省图 —— 清洗只有本省的数据 2018/11/21
    # df_column_in_trace = pd.DataFrame(
    #     {"value": NEWLY_WEIBO_VALUES, "name": GOV_NAMES, "link": EVENTS_LINKS, "column_type": "a",
    #      "column_color": FT_SOBER_BLUE, "lala": ["查看本县事件"] * len(NEWLY_WEIBO_VALUES), "gov_code":GOV_CODES})  # , index=GOV_CODES
    #
    # # 同一区县合并事件，加matte_pannel
    # df_trace_group = df_column_in_trace.groupby(["name"]).agg({"value":"max", "column_type":"max", "lala":"max", "gov_code":'max'}).reset_index()
    # df_trace_group.set_index(["gov_code"], inplace=True)
    # df_column_in_trace.set_index(["gov_code"],inplace=True)
    #
    # for index, row in df_trace_group.iterrows():
    #
    #     df_gov = DF_EVENTS_BASIC[DF_EVENTS_BASIC["gov_name"] == row["name"]]
    #     df_gov = df_gov.sort_values(by=["newly_weibo_value"], ascending=False).reset_index(drop=True)
    #
    #     # 判断本县最严重事件的等级，确定柱状气泡颜色
    #     gov_bubble_color = WCOLORS[df_gov["thd_grade"].values[0][0]]
    #     df_trace_group.loc[index, "column_color"] = gov_bubble_color
    #     df_trace_group.loc[index, "column_type"] = df_gov["thd_grade"].values[0][0]
    #
    #     # 加一个区县追踪事件数
    #     df_trace_group.loc[index, "events_num"] = df_gov.shape[0]
    #
    #     # 点击气泡后，弹出框的内容
    #     content_list = [
    #         "<p><span style='color:%s;font-size:16px;font-weight:bold;'>%s - <span style='color:%s'>%s</span>事件：%d件。</span></p><br/>" % (FT_PURE_WHITE, row["name"], UN_TITLE_YELLOW, event_type, df_gov.shape[0])]
    #
    #     # 框里的事件详情
    #     for gov_index, gov_row in df_gov.iterrows():
    #         event_str = "<p><%d>&nbsp<span style='color:%s'>■&nbsp&nbsp%s</span> - %s：%s<br/><a style='text-align:right;width:40%%;display:block;float:right;min-width:120px' class='link-widget' state='green_glass' href='#dataWarningCover:%s'>查看详情</a></p>"%(gov_index+1, WCOLORS[gov_row["thd_grade"][0]], (gov_row["scope_grade"]+"级影响" if len(gov_row["scope_grade"]) < 3 else gov_row["scope_grade"]), gov_row["events_occur_time"], (str(gov_row["events_content"])[0:SHOW_WEIBO_WORDS_LIMIT]+"……" if not str(gov_row["events_content"])[0:SHOW_WEIBO_WORDS_LIMIT].endswith("。") else str(gov_row["events_keytitle"]) if "暂无详情" in str(gov_row["events_content"]) else str(gov_row["events_content"])[0:SHOW_WEIBO_WORDS_LIMIT]), gov_row["events_link"])
    #         content_list.append(event_str)
    #
    #     df_trace_group.loc[index, "matte_pannel"] = str({"cont":content_list[0]+"<br/><br/>".join(content_list[1:])})

    df_column_in_trace_city = df_trace_group.filter(regex='\A' + str(gov_code)[0:4], axis=0)
    df_column_in_trace_prov = df_trace_group.filter(regex='\A' + str(gov_code)[0:2], axis=0)

    # df_column_in_trace["rank"] = df_column_in_trace["value"].rank(ascending=False)
    df_trace_group["rank"] = df_trace_group["value"].rank(ascending=False)
    df_column_in_trace_city["rank"] = df_column_in_trace_city["value"].rank(ascending=False)
    df_column_in_trace_prov["rank"] = df_column_in_trace_prov["value"].rank(ascending=False)

    if record_now:
        title = "2861区县%s隐患-追踪中" % event_type
        subtitle = "更新时间：%s" % str(version_date).split('.')[0]
        column_names = ['正在追踪的事件：%d件'%len(NEWLY_WEIBO_VALUES)]
    else:
        title = "2861区县%s隐患-历史追踪事件" % event_type
        subtitle = "最后一次追踪时间：%s" % str(version_date).split('.')[0]
        column_names = ['历史追踪的事件：%d件'%len(NEWLY_WEIBO_VALUES)]

    # 直辖市的区县 / 省直辖县 / 直辖市的高新区 / 国家级新区 —— 没有市图
    if gov_type not in [4, 5, 7, 10]:
        # 本市有事件发生才加
        if df_column_in_trace_city.shape[0] > 0:

            title_city = "<span style='color:%s;font-size:24px;font-weight:bold;'> %s </span>：正在追踪的<span style='color:%s;font-weight:bold;'>%s</span>事件"%(UN_TITLE_YELLOW, gov_name.split('|')[1], UN_TITLE_YELLOW, event_type)

            column_map_trace_dict = get_column_map_dict(3, gov_code, df_column_in_trace_city, title_city, subtitle, tips=tips)  # column_names=column_names,

            warning_map_data_list.append(column_map_trace_dict)
            warning_map_data_name_list.append('warning_column_trace_map')

    if df_column_in_trace_prov.shape[0] > 0:
        title_prov = "<span style='color:%s;font-size:24px;font-weight:bold;'> %s </span>：正在追踪的<span style='color:%s;font-weight:bold;'>%s</span>事件"%(UN_TITLE_YELLOW, gov_name.split('|')[0], UN_TITLE_YELLOW, event_type)
        column_map_prov_dict = get_column_map_dict(2, gov_code, df_column_in_trace_prov, title_prov, subtitle, tips=tips)   # column_names=column_names,

        warning_map_data_list.append(column_map_prov_dict)
        warning_map_data_name_list.append('warning_column_prov_map')

    if df_trace_group.shape[0] > 0:
        title_country = "<span style='color:%s;font-size:24px;font-weight:bold;'> 全国 </span>：当前所有追踪的<span style='color:%s;font-weight:bold;'>%s</span>事件" % (
        UN_TITLE_YELLOW, UN_TITLE_YELLOW, event_type)
        column_map_country_dict = get_column_map_dict(1, gov_code, df_trace_group, title_country, subtitle, tips=tips)   # column_names=column_names,

        warning_map_data_list.append(column_map_country_dict)
        warning_map_data_name_list.append("warning_column_country_map")

    # grades_re = grades[::-1]
    grades_re = deepcopy(grades)
    df_column_in_trace = deepcopy(df_trace_group)
    grades_data_list = []
    grades_name_list = []
    for i in range(len(grades_re)):
        if i == len(grades_re)-1:
            df_column = deepcopy(df_column_in_trace[df_column_in_trace["value"] >= thd_dict[grades_re[i]]])
        else:
            df_column = deepcopy(df_column_in_trace[(df_column_in_trace["value"] >= thd_dict[grades_re[i]])&(df_column_in_trace["value"]<thd_dict[grades_re[i+1]])])

        # 各个预警等级有事件才往下走
        if df_column.shape[0] > 0:

            df_column["column_color"] = color_dict[grades_re[i]]
            df_column["rank"] = df_column["value"].rank(ascending=False)
            if record_now:
                # title = "2861区县%s隐患-%s预警中" % (event_type, grades_re[i])
                title = "<span style='color:%s;font-size:24px;font-weight:bold;'> 全国 </span>：%s事件<span style='color:%s;font-weight:bold;'>%s</span>预警 - %s级事件"%(UN_TITLE_YELLOW, event_type, UN_TITLE_YELLOW, grades_re[i], events_grades[i][:-1])
                subtitle = "更新时间：%s" % str(version_date).split('.')[0]
                column_names = [events_grades[i]+"影响力事件：%d件"%df_column.shape[0]]
            else:
                title = "2861区县%s隐患-历史%s事件" % (event_type, grades_re[i])
                subtitle = "最后一次追踪时间：%s" % str(version_date).split('.')[0]
                column_names = ["历史"+events_grades[i]+"事件：%d件"%df_column.shape[0]]

            column_map_dict = get_column_map_dict(1, gov_code, df_column, title, subtitle, column_names=column_names, tips=tips)
            grades_data_list.append(column_map_dict)
            grades_name_list.append('warning_column_%s_map' % grades_re[i][0])

    warning_map_data_list.extend(grades_data_list[::-1])
    warning_map_data_name_list.extend(grades_name_list[::-1])

    # 本县有无事件发生 —— 对本县情况先做判断，更改市图/省图/国图，anyway第一张地图的标题
    # if gov_name in df_trace_group["name"].tolist():
    title_local = "<br/>当前区县：<span style='color:%s;font-size:24px;font-weight:bold;'> %s </span> - 追踪事件：%d件" % (
    UN_TITLE_YELLOW, gov_name.split('|')[-1], DF_EVENTS_BASIC[DF_EVENTS_BASIC["gov_name"] == gov_name].shape[0])

    # 判断本县预警情况
    if gov_name in df_trace_group["name"].tolist():
        warning_type = df_trace_group[df_trace_group["name"] == gov_name]["column_type"].values[0]
        title_local += "；%s事件预警中。" % scopes_dict[warning_type] if warning_type != "追" else "；隐患尚在追踪，未达预警门限。"
    else:
        title_local += "；暂未监测到%s事件。" % event_type

    # 还会有全国一件事儿都不发生的情况吗？！NAIVE ┓( ´∀` )┏ —— 为了严谨！！！
    if len(warning_map_data_list) > 0:
        warning_map_data_list[0]["title"] += title_local

    return warning_map_data_list, warning_map_data_name_list


# 各项指数走势
def get_warning_indexes_lines_data(df_warning_trace_info, parameters, version_date, record_now):
    a_thd = parameters['A_WARNING_Thd']
    b_thd = parameters['B_WARNING_Thd']
    c_thd = parameters['C_WARNING_Thd']
    thds = [a_thd, b_thd, c_thd]
    thd_names = ['A级预警', 'B级预警', 'C级预警']
    warning_line_data_list = []
    warning_line_data_name_list = []
    # index_cols = ['weibo_value', 'trace_v', 'trace_a', 'data_num', 'count_read', 'count_comment', 'count_share']
    # index_names = ['事件影响力指数', '事件传播速度', '事件传播加速度', '事件相关微博总量', '事件相关微博阅读总量', '事件相关微博评论总量', '事件相关微博转发总量']
    # index_cols = ['weibo_value', 'trace_v', 'trace_a']
    # index_names = ['事件影响力指数', '事件传播速度', '事件传播加速度']
    index_cols = ['weibo_value']
    index_names = ['实际事态发展']
    for events_head_id in EVENTS_HEAD_IDS:

        # debug
        if 0:
            if events_head_id != 'ca79491202e53bfd6ac4347d945623a9':
                continue

        df_event = df_warning_trace_info.loc[df_warning_trace_info['events_head_id'] == events_head_id, :].reset_index(drop=True)
        df_event.loc[:, 'do_time'] = df_event.loc[:, 'do_time'].apply(lambda x: str(x).split('.')[0])
        df_event = df_event.sort_values(by=['do_time']).reset_index(drop=True)  # , inplace=True
        # 越改越只针对指数了
        for index_col in index_cols:
            if index_col != 'weibo_value':
                continue
            if record_now:
                # title = "%s: %s变化趋势" % (
                # GOV_NAMES[EVENTS_HEAD_IDS.index(events_head_id)], index_names[index_cols.index(index_col)])

                title = "%s: 事态发展趋势" % (
                    GOV_NAMES[EVENTS_HEAD_IDS.index(events_head_id)])

                subtitle = "更新时间：%s" % str(version_date).split('.')[0]
            else:
                title = "%s: %s历史走势" % (
                    GOV_NAMES[EVENTS_HEAD_IDS.index(events_head_id)], index_names[index_cols.index(index_col)])
                # last_trace_time = df_event['do_time'].max()
                subtitle = "最后一次追踪时间：%s" % df_event['do_time'].max()
            # tips = {"right": "%s" % index_names[index_cols.index(index_col)]}

            x_name_list = []
            event_index_list = []

            x_prelist = df_event['do_time'].tolist()
            index_prelist = df_event[index_col].tolist()

            # 采样周期大于5才补
            if len(x_prelist) >= fitting_min_cnts:
                index_prelist[0] = 0

                event_trace_data = [x_prelist, index_prelist]
                warning_threshold_data = [a_thd, b_thd, c_thd]

                warning_predict = []

                # 判断有没有预警 —— 取最近那级预警
                if df_event.iloc[-1,:]["warning_c"] > 0:
                    warning_predict = [df_event.iloc[-1,:]["warning_c"], c_thd]
                if df_event.iloc[-1,:]["warning_b"] > 0:
                    warning_predict = [df_event.iloc[-1,:]["warning_b"], b_thd]
                if df_event.iloc[-1,:]["warning_a"] > 0:
                    warning_predict = [df_event.iloc[-1,:]["warning_a"], a_thd]

                if not len(warning_predict):
                    warning_predict = None

                fit_results = lineFitting.get_trace_event_all_line(event_trace_data, warning_threshold_data, warning_predict)

                thds_fit = [fit_results["classic_A_line"][1][0], fit_results["classic_B_line"][1][0], fit_results["classic_C_line"][1][0]]
                x_name_list = deepcopy(fit_results["classic_A_line"][0])

                event_xes_fit = deepcopy(fit_results["event_line"][0])
                event_indexes_fit = deepcopy(fit_results["event_line"][1])

                event_xes_with_points = []
                event_indexes_with_points = []

                # for i in range(len(event_indexes_fit)):
                #     value = event_indexes_fit[i]
                #     x_time = event_xes_fit[i]
                #
                #     if i == 0:
                #         point_dict = str({"y":value, "name":"事件爆发"})
                #         event_indexes_with_points.append(point_dict)
                #         event_xes_with_points.append(x_time)
                #
                #     # 判断每个点在不在界限上
                #     if value in thds_fit:
                #         point_dict = str({"y":value, "name":thd_names[thds_fit.index(value)]})

                event_index_list = fit_results["event_line"][1] + ["null"]*len(list(set(x_name_list)-set(fit_results["event_line"][0])))

                # 事件影响力指数曲线标点
                df_id = pd.DataFrame({'x_name': x_name_list, 'event': event_index_list})

                if fit_results["warning_line"][1] is not None:
                    if len(fit_results["warning_line"][0]) >= len(fit_results["cooldown_line"][0]):
                        warning_list = ["null"] * len(list(set(x_name_list)-set(fit_results["warning_line"][0]))) + list(fit_results["warning_line"][1])

                        noncool_x = list(set(x_name_list)-set(fit_results["cooldown_line"][0]))
                        noncool_x.sort()
                        noncool_x = np.array(noncool_x)

                        cooldown_list = ["null"] * len(noncool_x[noncool_x <= fit_results["cooldown_line"][0][0]]) + list(fit_results["cooldown_line"][1]) + ["null"] * len(noncool_x[noncool_x >= fit_results["cooldown_line"][0][-1]])

                    else:
                        cooldown_list = ["null"] * len(list(set(x_name_list)-set(fit_results["cooldown_line"][0]))) + list(fit_results["cooldown_line"][1])

                        nonwarn_x = list(set(x_name_list)-set(fit_results["warning_line"][0]))
                        nonwarn_x.sort()
                        nonwarn_x = np.array(nonwarn_x)

                        warning_list = ["null"] * len(
                            nonwarn_x[nonwarn_x <= fit_results["warning_line"][0][0]]) + list(
                            fit_results["warning_line"][1]) + ["null"] * len(
                            nonwarn_x[nonwarn_x >= fit_results["warning_line"][0][-1]])

                    # print(events_head_id)
                    df_id["cooldown"] = cooldown_list
                    df_id["warning"] = warning_list

                else:
                    cooldown_list = ["null"] * len(
                        list(set(x_name_list) - set(fit_results["cooldown_line"][0]))) + list(
                        fit_results["cooldown_line"][1])
                    df_id["cooldown"] = cooldown_list
                # a, b, c 由拟合后返回
                df_id['A'] = fit_results["classic_A_line"][1]
                df_id['B'] = fit_results["classic_B_line"][1]
                df_id['C'] = fit_results["classic_C_line"][1]

                signlist = [event_index_list[0], fit_results["classic_A_line"][1][0], fit_results["classic_B_line"][1][0], fit_results["classic_C_line"][1][0]]

                value_max = max(fit_results["event_line"][1])
                max_thd = max([value_max + 10, fit_results["classic_C_line"][1][0] + fit_results["classic_A_line"][1][0]])

                tickx = int(df_id.shape[0] / show_x_nums)

            else:
                # 影响力指数标出交点
                for i in df_event.index:
                    value = df_event.loc[i, index_col]
                    x_time = df_event.loc[i, 'do_time']

                    # 首先判断第一个点 —— 事件爆发点
                    if i == df_event.index.min():
                        point_dict = {"y":value, "name":"事件爆发"}
                        event_index_list.append(str(point_dict))
                        x_name_list.append(x_time)
                        continue

                    # 先判断每个点自己在不在界限上
                    if value in thds:
                        point_dict = {"y":value, "name":thd_names[thds.index(value)]}
                        event_index_list.append(str(point_dict))
                    else:
                        event_index_list.append(value)
                    x_name_list.append(x_time)

                    # 再判断两点之间有没有点在界线上
                    if i == df_event.index.max():
                        break
                    next_value = df_event.loc[i+1, index_col]
                    next_time = df_event.loc[i+1, 'do_time']
                    if value >= next_value:
                        continue
                    this_datetime = datetime.strptime(x_time, '%Y-%m-%d %H:%M:%S')
                    next_datetime = datetime.strptime(next_time, '%Y-%m-%d %H:%M:%S')
                    interseconds = (next_datetime - this_datetime).days*24*3600 + (next_datetime - this_datetime).seconds
                    for t in thds:
                        if value < t < next_value:
                            point_dict = {"y":t, "name":thd_names[thds.index(t)]}
                            event_index_list.append(str(point_dict))
                            # 计算一下时间，把x_name补上 —— 不要指望前端。。。2018/8/22
                            cross_seconds = (t-value)/(next_value-value)*interseconds
                            cross_x_time = this_datetime + timedelta(seconds=cross_seconds)
                            x_name_list.append(str(cross_x_time).split('.')[0])
                # df_id = pd.DataFrame(index=df_event.index, columns=['x_name', 'event'])
                # df_id['x_name'] = x_name_list
                # df_id['event'] = event_index_list
                df_id = pd.DataFrame({'x_name':x_name_list, 'event': event_index_list})

                df_id['A'] = [a_thd] * len(x_name_list)
                df_id['B'] = [b_thd] * len(x_name_list)
                df_id['C'] = [c_thd] * len(x_name_list)

                signlist = [df_event[index_col][0], a_thd, b_thd, c_thd]

                value_max = df_event[index_col].max()
                max_thd = max([value_max + 10, c_thd + a_thd])
                tickx = None

            # if index_col == 'weibo_value':
                # df_id['A'] = [a_thd]*len(list(df_event['do_time']))
                # df_id['B'] = [b_thd]*len(list(df_event['do_time']))
                # df_id['C'] = [c_thd]*len(list(df_event['do_time']))

            df_info = pd.DataFrame(index=[i for i in list(df_id) if i != 'x_name'], columns=["name", "color"])
            df_info.loc["event", "name"] = "<span style='font-size:16px;color:%s;'>"%FT_PURE_WHITE + index_names[index_cols.index(index_col)] + "</span>"
            df_info.loc["event", "color"] = BT_TIFFANY_BLUE
            if "cooldown" in list(df_id):
                df_info.loc["cooldown", "name"] = "<span style='font-size:16px;color:%s;'>事态自然冷却</span>"%FT_PURE_WHITE      # "事件热度冷却曲线"
                if "warning" in list(df_id):
                    df_info.loc["cooldown", "name"] = "<span style='font-size:16px;color:%s;'>预测妥善处理后发展</span>"%FT_PURE_WHITE
                df_info.loc["cooldown", "color"] = BT_TIFFANY_BLUE                          # BT_TIFFANY_BLUE
                df_info.loc["cooldown", "type"] = "spline"
                df_info.loc["cooldown", "dashStyle"] = "dash"
            if "warning" in list(df_id):
                df_info.loc["warning", "name"] = "<span style='font-size:16px;color:%s;'>预警事态发展</span>"%FT_PURE_WHITE               # "事件预警曲线"
                df_info.loc["warning", "color"] = LN_WARNING_PINK
                df_info.loc["warning", "type"] = "spline"
                df_info.loc["warning", "dashStyle"] = "dash"
            if index_col == "weibo_value":
                df_info.loc["event", "color"] = BT_TIFFANY_BLUE
                df_info.loc["event", "type"] = "line"
                df_info.loc["event", "dashStyle"] = "solid"
                for index in df_info.index.values:
                    if index in ["A", "B", "C"]:
                        df_info.loc[index, "name"] = "<span style='font-size:16px;color:%s;'>"%FT_PURE_WHITE + index+"级预警" +"</span>"
                        df_info.loc[index, "color"] = WCOLORS[index]
                        df_info.loc[index, "type"] = "spline"
                        df_info.loc[index, "dashStyle"] = "dash"
            y_name = "<span style='font-size:16px;color:%s;'>事件扩散影响指数</span>"%FT_PURE_WHITE
            linewidth = 3
            xfont = {"xfont":12, "fontWeight":"normal", "color":FT_PURE_WHITE}
            signname = ['A级', 'B级', 'C级']
            # signlist = [df_event[index_col][0], a_thd, b_thd, c_thd]
            # value_max = df_event[index_col].max()
            # max_thd = max([value_max+10, c_thd+a_thd])
            index_line_dict = get_lines_graph_dict(title, subtitle, df_id, df_info, y_name,linewith=linewidth, xfont=xfont, signname=signname, signlist=signlist, max_thd=max_thd, tickx=tickx)
            # index_line_dict = get_mixed_line_dict(title, subtitle, df_id, tips=tips)
            warning_line_data_list.append(index_line_dict)
            warning_line_data_name_list.append('%s_%s_index_trend' % (EVENTS_SHORT_DICT[events_head_id], index_col.split('_')[-1]))
    return warning_line_data_list, warning_line_data_name_list


# 各事件的微博和评论textbox
def get_content_comments_textbox():
    warning_content_textbox_list = []
    warning_content_textbox_name_list = []

    for events_head_id in EVENTS_HEAD_IDS:
        # 微博内容
        # df_event = df_warning_details[df_warning_details.events_head_id == events_head_id]

        # 直接从基本信息里取
        df_event = DF_EVENTS_BASIC[DF_EVENTS_BASIC.events_head_id == events_head_id]
        DETAILDESC = ""  # 语音播报
        # weibo_max_len = max([len(x) for x in df_event["content"].tolist()])
        # if weibo_max_len == 0:
        #     weibo_content = "暂无详情。"
        # else:
        #     weibo_content = df_event[df_event["content"].apply(lambda x:len(x)==weibo_max_len)]["content"].values[0]

        # 事件
        # title = "%s：事件及评论详情"%GOV_NAMES[EVENTS_HEAD_IDS.index(events_head_id)]
        title = ""
        subtitle = ""
        # search_key = df_warning_trace_info[df_warning_trace_info["events_head_id"]==events_head_id]["search_key"].values[0]
        # search_key = "<span style='color:%s'>%s</span>"%(UN_TITLE_YELLOW, ' '.join(search_key.split(' ')[1:]))
        # earliest_pub_time = str(df_event["pub_time"].min()).split('.')[0]
        event_toptitle = "事件详情"
        event_toplist = ["关键字：<span style='color:%s'>%s</span>"%(UN_TITLE_YELLOW, df_event["events_keytitle"].values[0]), "首发时间：%s&nbsp&nbsp&nbsp&nbsp首发博主：%s"%(df_event["events_occur_time"].values[0],df_event["events_first_post"].values[0]), "事件详情：%s"%df_event["events_content"].values[0]]

        df_fieldset = pd.DataFrame({"text-align":["left"], "scroll":[0], "height":["45%"], "top_title":[event_toptitle], "content_list":[str(event_toplist)]})

        # first_textbox = web_charts.get_textbox_dict(title, subtitle, event_toptitle, event_toplist)

        # 评论
        df_comments = df_warning_weibo_comments[df_warning_weibo_comments.events_head_id==events_head_id]
        comments_list = []
        for comment_info in df_comments["comments_shown"]:
            if len(comment_info) == 0:
                continue
            for comment in comment_info:
                comments_list.append(comment)

        if len(comments_list) == 0:
            comments_list = ["暂无。"]
            comment_toplist = comments_list
        else:
            comments_list = list(set(comments_list))
            if len(comments_list) > SHOW_COMMENTS_LIMIT:
                comments_list = comments_list[0:SHOW_COMMENTS_LIMIT]
            comment_toplist = ["<%d> %s" % (i + 1, comments_list[i]) for i in range(len(comments_list))]

        comment_toptitle = "典型评论"

        if len(comment_toplist) < fixed_comments_num:
            df_fieldset = df_fieldset.append(pd.DataFrame(
                {"text-align": ["left"], "scroll": [0], "height": ["45%"], "top_title": [comment_toptitle],
                 "content_list": [str(comment_toplist)]}))
        else:
            df_fieldset = df_fieldset.append(pd.DataFrame({"text-align":["left"], "scroll":[1], "height":["45%"], "top_title":[comment_toptitle], "content_list":[str(comment_toplist)]}))

        content_textbox = web_charts.get_textbox2_dict(title, subtitle, df_fieldset)

        # next_textbox = web_charts.get_textbox_dict(title, subtitle, comment_toptitle, comment_toplist)

        ## 揉成一页
        # first_textbox["detail"]["top"] += next_textbox["detail"]["top"]

        warning_content_textbox_list.append(content_textbox)
        warning_content_textbox_name_list.append("%s_content_textbox"%EVENTS_SHORT_DICT[events_head_id])

    return warning_content_textbox_list, warning_content_textbox_name_list


# 媒体占比分布饼状图
def get_top_media_stackbar(version_date):
    warning_media_stackbar_list = []
    warning_media_stackbar_name_list = []

    for events_head_id in EVENTS_HEAD_IDS:
        gov_name = GOV_NAMES[EVENTS_HEAD_IDS.index(events_head_id)]
        event_search_key = ' '.join(df_warning_trace_info[df_warning_trace_info.events_head_id == events_head_id]["search_key"].values[0].split(' ')[1:8])

        df_event = df_warning_details[df_warning_details.events_head_id == events_head_id]

        # 聚合同一postname
        df_event = df_event.groupby(["post_name"]).agg({"event_participation":"sum", "events_head_id":"max", "followers_count":"max"}).reset_index()

        df_event = df_event.sort_values(by=["event_participation"], ascending=False).reset_index(drop=True)

        if df_event.shape[0] > max_medias_num:
            df_event = df_event[0:max_medias_num]

        # 万人次
        df_event["event_participation"] /= 19

        df_event["event_participation"] = df_event["event_participation"].apply(lambda x:float("%.2f"%x))

        # title = "%s：[%s] 事件意见领袖TOP%d"%(gov_name.split('|')[-1], event_search_key, int(df_event.shape[0]))

        title = "<span style='color:%s;font-size:30px;font-weight:bold'>本事件主力传播媒体</span>"%UN_TITLE_YELLOW
        # subtitle = "更新时间：%s"%str(version_date).split('.')[0]
        subtitle = ""
        xAxis_name = df_event["post_name"].tolist()
        yAxis_dict = {"name":"", "color":FT_PURE_WHITE, "fontsize":16}
        df_data = pd.DataFrame({"data": [str(df_event["event_participation"].tolist())], "name":["意见传播覆盖人次（万）"], "color":[BAR_PINK]})

        media_stackbar_dict = web_charts.get_stackbar_dict(title, subtitle, xAxis_name, yAxis_dict, df_data, percentage=False)

        warning_media_stackbar_list.append(media_stackbar_dict)
        warning_media_stackbar_name_list.append("%s_media_stackbar"%EVENTS_SHORT_DICT[events_head_id])

    return warning_media_stackbar_list, warning_media_stackbar_name_list


# 民众评价关键词textbox图
def get_public_keywords_textbox(version_date):
    warning_keywords_textbox_list = []
    warning_keywords_textbox_name_list = []

    word_types = ["sensitive", "department", "guanzhi"]
    word_names = ["相关敏感词", "涉及部门", "涉及官员"]

    for events_head_id in EVENTS_HEAD_IDS:
        event_search_key = ' '.join(
            df_warning_trace_info[df_warning_trace_info.events_head_id == events_head_id]["search_key"].values[0].split(
                ' ')[1:8])
        words_dict = {}

        gov_name = GOV_NAMES[EVENTS_HEAD_IDS.index(events_head_id)]
        for word_type in word_types:
            df_event = df_warning_keywords[(df_warning_keywords.events_head_id == events_head_id) & (df_warning_keywords.type == word_type)]
            df_event = df_event.sort_values(by=['freq'], ascending=False).reset_index(drop=True)

            if df_event.shape[0] > max_key_words_num:
                df_event = df_event[0:max_key_words_num]
                freq_total = sum(list(df_event['freq']))
                df_event['freq'] = df_event['freq'].apply(lambda x:round(x/freq_total, 4))

            words_list = []
            for index, row in df_event.iterrows():
                word_str = "%s（%.2f%%）"%(row["word"], row["freq"] * 100)
                words_list.append(word_str)

            words_dict[word_type] = words_list

        # title = "%s：[%s] 事件评价关键词"%(gov_name.split('|')[-1], event_search_key)

        title = ""

        subtitle = ""

        toptitle = "民意高频词"

        toplist = []

        # df_event_trace = df_warning_trace_info[df_warning_trace_info.events_head_id == events_head_id].reset_index(drop=True)
        # latest_weibo_value = df_event_trace[df_event_trace.do_time == df_event_trace["do_time"].max()]["weibo_value"].values[0]

        # first_desc = "当前事件，传播覆盖人次达：%s"%(get_proper_unit_data(latest_weibo_value/0.0019))

        # toplist.append(first_desc)

        toplist.append("")

        toplist.append("".join(["<span style='width:33%%;display:block;float:left;font-weight:bold;color:%s'>"%UN_TITLE_YELLOW +x+"</span>" for x in word_names]))

        max_len = max([len(words_dict[i]) for i in words_dict.keys()])

        for j in range(0, max_len):
            row_str = "".join(["<span style='width:33%;display:block;float:left'>"+words_dict[i][j] + "</span>" if len(words_dict[i])>j else "<span style='width:33%;display:block;float:left'>" + "&nbsp" + "</span>" for i in words_dict.keys()])
            toplist.append(row_str)

        df_id = pd.DataFrame({"text-align":["left"], "height":["58%"],"scroll":[0], "top_title":[toptitle], "content_list":[str(toplist)]})

        # keywords_textbox = web_charts.get_textbox_dict(title, subtitle, toptitle, toplist, div=True)
        keywords_textbox = web_charts.get_textbox2_dict(title, subtitle, df_id)

        warning_keywords_textbox_list.append(keywords_textbox)
        warning_keywords_textbox_name_list.append("%s_public_textbox"%EVENTS_SHORT_DICT[events_head_id])

    return warning_keywords_textbox_list, warning_keywords_textbox_name_list


# 关键词占比
def get_warning_words_lines_data(df_warning_keywords, version_date):
    warning_line_data_list = []
    warning_line_data_name_list = []
    word_types = ['sensitive', 'department', 'guanzhi']
    word_names = ['相关敏感词', '涉及部门', '涉及官员']
    for events_head_id in EVENTS_HEAD_IDS:
        gov_name = GOV_NAMES[EVENTS_HEAD_IDS.index(events_head_id)]
        for word_type in word_types:
            df_event = df_warning_keywords.loc[
                       (df_warning_keywords.events_head_id == events_head_id) & (df_warning_keywords.type == word_type),:]
            df_event = df_event.sort_values(by=['freq'], ascending=False).reset_index(drop=True)
            if df_event.iloc[:, 0].size > 20:
                df_event = df_event[0:20]
                freq_total = sum(list(df_event['freq']))
                df_event.loc[:, 'freq'] = df_event.loc[:, 'freq'].apply(lambda x: round(x / freq_total, 4))
            title = "%s: 事件%s" % (gov_name, word_names[word_types.index(word_type)])
            subtitle = "更新时间：%s" % version_date.split('.')[0]
            tips = {"left": "词频", "right": "占比"}
            df_id = pd.DataFrame(index=df_event.index, columns=['x_name', 'value', 'rank'])
            df_id['x_name'] = df_event['word']
            df_id['value'] = df_event['count']
            df_id['rank'] = df_event['freq']
            df_id['color'] = 'orange'
            line_word_dict = get_mixed_line_dict(title, subtitle, df_id, tips=tips)
            warning_line_data_list.append(line_word_dict)
            warning_line_data_name_list.append('%s_%s_word_trend' % (EVENTS_SHORT_DICT[events_head_id], word_type[0:4]))
    return warning_line_data_list, warning_line_data_name_list


# 博主影响力&事件参与度
def get_warning_bloggers_lines_data(df_warning_details, df_warning_trace_info, para_dict, version_date, record_now):

    warning_line_data_list = []
    warning_line_data_name_list = []
    w0 = para_dict["K_weibo"]
    wr = para_dict["K_read"]
    wc = para_dict["K_comment"]
    ws = para_dict["K_share"]
    # 事件参与度
    df_details_temp = deepcopy(df_warning_details)
    df_details_temp["event_participation"] = w0 * 1 + wr * df_details_temp["count_read"] + wc * \
                                                df_details_temp["count_comment"] + ws * df_details_temp["count_share"]
    # 博主影响力
    df_details_temp["publisher_impact"] = df_details_temp["followers_count"]
    for events_head_id in EVENTS_HEAD_IDS:
        gov_name = GOV_NAMES[EVENTS_HEAD_IDS.index(events_head_id)]
        event_search_key = ' '.join(
            df_warning_trace_info.loc[df_warning_trace_info.events_head_id == events_head_id, "search_key"].values[
                0].split(' ')[1:])
        df_event = df_details_temp.loc[(df_details_temp.events_head_id == events_head_id), :]
        df_event = df_event.sort_values(by=["event_participation"], ascending=False).reset_index(drop=True)
        if df_event.iloc[:, 0].size > 20:
            df_event = df_event[0:20]
        title = "事件：%s 【%s】 各发布者的影响力及参与度" % (gov_name, event_search_key)
        if record_now:
            sub_title = "更新时间：%s" % str(version_date).split('.')[0]
        else:
            sub_title = '最后一次追踪时间：%s'% df_warning_trace_info.loc[df_warning_trace_info.events_head_id == events_head_id, "do_time"].max()
        tips = {"left": "事件参与度", "right": "博主影响力"}
        df_id = pd.DataFrame(index=df_event.index, columns=['x_name', 'value', 'rank'])
        df_id["x_name"] = df_event["post_name"]
        df_id["value"] = df_event["event_participation"]
        df_id["rank"] = df_event["publisher_impact"]
        line_word_dict = get_mixed_line_dict(title, sub_title, df_id, tips=tips)
        warning_line_data_list.append(line_word_dict)
        warning_line_data_name_list.append('%s_publisher_trend' % EVENTS_SHORT_DICT[events_head_id])
    # print(warning_line_data_name_list)
    return warning_line_data_list, warning_line_data_name_list


# 预警相关的所有图
def get_warning_map_line_basic_data(gov_code, node_code, df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, monitor_time, record_now=True):
    # print(df_warning_trace_info)
    parameters = warning_dict[node_code]["parameters"]
    # 柱子地图（多张）
    warning_map_data_list = []
    warning_map_data_name_list = []

    # 趋势地图（多张）
    warning_line_data_list = []
    warning_line_data_name_list = []

    # 按事件提取
    # events_head_ids_origin = list(df_warning_trace_info['events_head_id'])
    # events_head_ids = list(set(events_head_ids_origin))
    # events_head_ids.sort(key=events_head_ids_origin.index)
    # events_short_dict = {}
    # for events_head_id in events_head_ids:
    #     events_short_dict[events_head_id] = 'we' + str(events_head_ids.index(events_head_id))
    # # 万一一个区县发生了两件事儿呢？
    # # gov_ids = set(df_warning_trace_info['gov_id'])
    # gov_ids = []
    # gov_names = []
    # newly_weibo_values = []
    # events_links = []
    # for events_head_id in events_head_ids:
    #     gov_id = df_warning_trace_info[df_warning_trace_info['events_head_id'] == events_head_id]['gov_id'].values[0]
    #     gov_name = df_2861_county[df_2861_county['gov_id'] == gov_id]['full_name'].values[0]
    #     weibo_value = max(
    #         list(df_warning_trace_info[df_warning_trace_info['events_head_id'] == events_head_id]['weibo_value']))
    #     gov_ids.append(gov_id)
    #     gov_names.append(gov_name)
    #     newly_weibo_values.append(weibo_value)
    #
    #     # 给柱状图的牌子加上link
    #     # gov_code = df_2861_county[df_2861_county.gov_id == gov_id].index.values[0]
    #     gov_code_str = str(gov_code)[0:6]
    #     # events_links.append("%s/%s/setting_%s_value_indexes"%(node_code, gov_code_str, events_short_dict[events_head_id]))
    #     events_links.append("%s/%s/setting_%s_value_indexes" % (node_code, common_folder_code, events_short_dict[events_head_id]))

    nearest_trace_time = max(df_warning_trace_info['do_time'].tolist())
    # 预警柱子分布图
    warning_map_data, warning_map_data_names = get_warning_map_data(gov_code, node_code, parameters, monitor_time, record_now)
    warning_map_data_list.extend(warning_map_data)
    warning_map_data_name_list.extend(warning_map_data_names)

    if str(gov_code)[0:6] == common_folder_code:
        # 事件走势图 —— 影响力指数、(速度、加速度（、总微博数、评论数、转发数）) —— 2018/8/2 只画影响力指数的图，速度/加速度不要了
        warning_indexes_data, warning_indexes_names_list = get_warning_indexes_lines_data(df_warning_trace_info,
                                                                                          parameters,
                                                                                          monitor_time, record_now)
        warning_line_data_list.extend(warning_indexes_data)
        warning_line_data_name_list.extend(warning_indexes_names_list)

        # 事件详情textbox图
        warning_content_data, warning_content_names = get_content_comments_textbox()
        warning_line_data_list.extend(warning_content_data)
        warning_line_data_name_list.extend(warning_content_names)

        # 媒体事件参与度分布stackbar图
        warning_media_data, warning_media_names = get_top_media_stackbar(monitor_time)
        warning_line_data_list.extend(warning_media_data)
        warning_line_data_name_list.extend(warning_media_names)

        # 事件关键词textbox图
        warning_keywords_data, warning_keywords_names = get_public_keywords_textbox(monitor_time)
        warning_line_data_list.extend(warning_keywords_data)
        warning_line_data_name_list.extend(warning_keywords_names)

    return warning_map_data_list, warning_map_data_name_list, warning_line_data_list, warning_line_data_name_list


# 过往事件list描述 —— 2018/9/27改版为【事件记录】，所以要加上当前追踪的事件及按钮
def get_past_events_desc_per_gov(gov_code, df_past):
    gov_code_str = str(gov_code)[0:6]
    gov_name = df_2861_county.loc[gov_code, 'full_name']
    df_gov_past = deepcopy(df_past[df_past['gov_code'] == int(gov_code_str)])
    df_gov_past = df_gov_past.reset_index(drop=True)
    current_event_info = []
    past_event_info = []

    # 当前追踪
    # 小标题
    current_desc = "<p><span style='color: %s;font-weight: bold'>当前追踪事件</span></p>"%FT_SOBER_BLUE
    current_event_info.append({"cols":[{"text":current_desc}]})

    df_current_events = deepcopy(DF_EVENTS_BASIC[DF_EVENTS_BASIC["gov_code"] == gov_code])
    df_current_events = df_current_events.sort_values(by=['thd_grade'], ascending=True).reset_index(drop=True)
    if df_current_events.shape[0] <= 0:
        current_desc = "<p><span style='color: #ff1133;font-weight: bold'>本地暂无追踪事件。</span></p>"
        current_event_info.append({"cols": [{"text": current_desc}]})
    else:
        j = 0
        for index, row in df_current_events.iterrows():
            j += 1
            if row["thd_grade"] == "追踪中":
                extent_word = row["scope_grade"]
            else:
                extent_word = row["scope_grade"] + "级"
            current_desc = "<p><span style='color: #ffcc00;'>%d，[%s]%s</span><br/>时间：%s   传播覆盖人次：%s</p>"%(j, extent_word+"事件", row["events_content"], row["events_occur_time"], get_proper_unit_data(row["newly_weibo_value"]*526.32))
            current_col = {"cols":[{"text": current_desc}]}
            button_col = {"cols":[{"text":""}, {"text":""}, {"text":"查看追踪详情","link":"#data:%s"%row["events_link"], "islink":False}]}
            current_event_info.extend([current_col, button_col])
            # extent_word, row["events_content"], get_proper_unit_data(row["newly_weibo_value"] * 526.32)),
            #                "link": "%s" % (row["events_link"])}
            # unit4_cols.append(current_col)


    # 过往事件
    # 小标题
    desc = "<p><span style='color: %s;font-weight: bold'>过往典型事件</span></p>"%FT_SOBER_BLUE
    past_event_info.append({"cols":[{"text":desc}]})
    if df_gov_past.iloc[:,0].size == 0:
        desc = "<p><span style='color: #ff1133;font-weight: bold'>暂未监测到跟本地有关的热门话题！</span></p>"
        past_event_info.append({"cols":[{"text":desc}]})
    else:
        # desc = ""
        for i in df_gov_past.index.values:
            # desc += "<p><span style='color: #ffcc00;'>%d，[主题]%s</span>" \
            #         "<br/>时间：%s" \
            #         "<br/><span style='color: #ffcc00;'>涉及职务：</span>%s" \
            #         "<br/><span style='color: #ffcc00;'>涉及部门：</span>%s" \
            #         "<br/><span style='color: #ffcc00;'>涉及关键词：</span>%s</p>"\
            #         %(i+1,df_gov_past.loc[i, 'event_title'],df_gov_past.loc[i, 'event_time_start'],df_gov_past.loc[i, 'gov_post'],df_gov_past.loc[i, 'department'],df_gov_past.loc[i, 'sensitive_word'])
            # 2018/9/10 改为指出能有微博详情的格式
            desc = "<p><span style='color: #ffcc00;'>%d，[主题]%s</span>" \
                    "<br/>时间：%s   传播覆盖人次：%s" \
                    "<br/><span style='color: #ffcc00;'>涉及职务：</span>%s" \
                    "<br/><span style='color: #ffcc00;'>涉及部门：</span>%s" \
                    "<br/><span style='color: #ffcc00;'>涉及关键词：</span>%s</p>"\
                    %(i+1,df_gov_past.loc[i, 'event_title'],df_gov_past.loc[i, 'event_time_start'], get_proper_unit_data(df_gov_past.loc[i, "event_value"]*526.32), df_gov_past.loc[i, 'gov_post'],df_gov_past.loc[i, 'department'],df_gov_past.loc[i, 'sensitive_word'])
            event_col = {"cols":[{"details":df_gov_past.loc[i, "first_content"], "text":desc, "ismore":"查看微博全文"}]}
            # 2018/9/10 暂时不上【查看详情】，有bug
            # event_col = {"cols":[{"text":desc}]}
            past_event_info.append(event_col)

    # datas = ""
    # title = "<h1>%s</h1>" % (gov_name)
    # datas += title
    # datas += desc

    data_info = []
    title_info = {"cols":[{"text":"<h1>%s</h1>" % (gov_name)}]}
    data_info.append(title_info)
    # 追踪中的事件
    data_info.extend(current_event_info)

    # 过往事件记录
    data_info.extend(past_event_info)

    # return datas
    return data_info


# 去掉所有的HTML标签，将p标签用换行符隔开
def tidy_rich_text(content):
    new_content = content.replace('</p>','\n')
    dr = re.compile(r'<[^>]+>', re.S)
    new_content = dr.sub('', new_content)

    return new_content


# 调整数字单位， 默认最小单位是1
def get_proper_unit_data(num):

    if num >= 100000000:
        return "%.2f亿"%(num/100000000)
    if num >= 10000000:
        return "%.2f千万"%(num/10000000)
    if num >= 10000:
        # print(type(num))
        # print(num)
        return "%.2f万"%(num/10000)
    return "%d"%num


# 生成新版360刷新首页 —— 2018/9/25
def get_warning_cover_data(gov_code, node_code, monitor_time, info_dict, record_now=True):
    gov_code_str = str(gov_code)[0:6]
    gov_name = df_2861_county.loc[gov_code, 'full_name']
    event_type = warning_dict[node_code]["events_model"]
    warning_cover_dict = {}
    warning_cover_dict["title"] = ""
    warning_cover_dict["subtitle"] = ""
    warning_cover_dict["type"] = "warningCover"

    warning_cover = {}
    # 历史事件 —— 环境暂时没有
    if node_code == "STABLE_WARNING":
        warning_cover["up_button"] = [{"text":"事件记录", "link":"#list:%s/%s/past"%(node_code, gov_code_str)}]
    else:
        warning_cover["up_button"] = []
    events_grades = ["全国级", "省域级", "区域级"]
    events_engs = ['C级', 'B级', 'A级']

    # 去掉down_button
    # warning_cover["down_button"] = []
    # for i in range(len(events_grades)):
    #     grade_button = {"text":events_grades[i], "link":"%s/%s/setting_%s"%(node_code, gov_code_str, events_engs[i][0])}
    #     warning_cover["down_button"].append(grade_button)

    # 追踪中
    trace_button = {"text":"追踪中", "link":"%s/%s/setting"%(node_code, gov_code_str)}     # #dataWarningCover:
    # warning_cover["down_button"].append(trace_button)

    # 传多少个扫描单元
    warning_cover["unit_num"] = 4
    warning_cover["scan_total"] = scan_total
    unit_details = []

    # 第一个扫描单元
    unit1 = {}
    unit1["up_title"] = "正在扫描系统爬虫库：获取海量互联网信息"
    unit1["up_subtitle"] = "提取2861个区县的草根数据，7*24小时不间断运行"
    unit1["down_title"] = "基础信息："
    unit1["scan_time"] = unit1_scan
    # unit1_cols = []

    monitor_datetime = datetime.strptime(monitor_time, '%Y-%m-%d %H:%M:%S')
    # 过去一周系统新增
    info_col1 = {"text":"过去一周，系统新增互联网信息：", "res":"%s条"%info_dict["past_week"]["sys_info"]}
    county_info_aug = get_past_week_sys_info_num(monitor_datetime, gov_code)
    county_info_all = get_total_sys_info_num(monitor_datetime, gov_code)
    info_col2 = {"text":"%s-新增："%gov_name, "res":"%s条"%county_info_aug}
    info_col3 = {"text":"迄今为止，系统累计互联网信息：", "res":"%s条"%info_dict["total"]["sys_info"]}
    info_col4 = {"text":"%s-累计："%gov_name, "res":"%s条"%county_info_all}

    unit1["cols"] = [info_col1, info_col2, info_col3, info_col4]

    unit_details.append(unit1)

    # 第二个扫描单元
    unit2 = {}
    unit2["up_title"] = "正在扫描隐患事件库：不放过任何一件小事"
    unit2["up_subtitle"] = "检测潜在隐患，警惕小事变大事"
    unit2["down_title"] = "隐患小事："
    unit2["scan_time"] = unit2_scan

    _, county_trifle_aug = get_past_week_es_events_num(monitor_datetime, gov_code)
    _, county_trifle_all = get_total_es_events_num(monitor_datetime, gov_code)
    trifle_col1 = {"text":"过去一周，全国新增隐患小事：", "res":"%s件"%info_dict["past_week"]["trifles_num"]}
    trifle_col2 = {"text":"%s-新增："%gov_name, "res":"%s件"%county_trifle_aug}
    trifle_col3 = {"text":"迄今为止，全国累计小事：", "res":"%s件"%info_dict["total"]["trifles_num"]}
    trifle_col4 = {"text":"%s-累计："%gov_name, "res":"%s件"%county_trifle_all}

    unit2["cols"] = [trifle_col1, trifle_col2, trifle_col3, trifle_col4]

    unit_details.append(unit2)

    # 第三个扫描单元
    unit3 = {}
    unit3["up_title"] = "正在扫描热点追踪库：持续追踪中不溜秋、易被忽略的范围热点"
    unit3["up_subtitle"] = "系统实时追踪，侦测事态发展"
    unit3["down_title"] = "热点事件："
    unit3["scan_time"] = unit3_scan

    county_event_aug = get_past_week_trace_events_num(monitor_datetime, gov_code)
    county_event_all = get_total_trace_events_num(monitor_datetime, gov_code)
    event_col1 = {"text":"过去一周，系统追踪热点事件：", "res":"%s件"%info_dict["past_week"]["events_num"]}
    event_col2 = {"text":"%s-新增追踪："%gov_name, "res":"%s件"%county_event_aug}
    event_col3 = {"text":"迄今为止，累计追踪热点事件：", "res":"%s件"%info_dict["total"]["events_num"]}
    event_col4 = {"text":"%s-累计追踪："%gov_name, "res":"%s件"%county_event_all}

    unit3["cols"] = [event_col1, event_col2, event_col3, event_col4]

    unit_details.append(unit3)

    # 结果单元  —— 2018/9/30 有更改， 加上统计信息
    unit4 = {}
    county_num = GOV_CODES.count(gov_code)
    prov_num = len([i for i in GOV_CODES if str(i).startswith(gov_code_str[0:2])])
    country_num = len(GOV_CODES)
    unit4["up_title"] = "查询完成，%s当前追踪【%s】相关事件%s件，本省%s件，全国共%s件"%(gov_name.split('|')[-1], event_type, county_num, prov_num, country_num)
    unit4["up_subtitle"] = "2861系统持续关注，每10~20分钟采样一次"
    unit4["down_title"] = "%s-监测结果(%s)："%(gov_name, monitor_time)
    unit4["scan_time"] = 0
    unit4_cols = []

    # current_col1 = {"text":"监测时间：%s"%monitor_time}
    gov_id = df_2861_county.loc[gov_code, 'gov_id']
    # current_col2 = {"text":"%s-当前追踪[%s]相关事件：%d件"%(gov_name, event_type, county_num)}
    current_col2 = {"text": "本县当前追踪[%s]相关事件：%d件" % (event_type, county_num)}
    # unit4_cols.extend([current_col1, current_col2])
    unit4_cols.extend([current_col2])
    # DF_EVENTS_BASIC
    if county_num > 0 :
        # thd_scopes = ["全国", "省域", "区域", "追踪中"]
        df_events_county = deepcopy(DF_EVENTS_BASIC[DF_EVENTS_BASIC["gov_code"]==gov_code])
        df_events_county = df_events_county.sort_values(by=['thd_grade'], ascending=True).reset_index(drop=True)
        for index, row in df_events_county.iterrows():
            if row["thd_grade"] == "追踪中":
                extent_word = row["scope_grade"]
            else:
                extent_word = row["scope_grade"]+"级"
            current_col = {"text":"%s事件:<br/>%s<br/>约%s人次在互联网上参与讨论。(事件预测准确率：75%%)"%(extent_word, (str(row["events_content"])[0:SHOW_WEIBO_WORDS_LIMIT]+"……" if not str(row["events_content"])[0:SHOW_WEIBO_WORDS_LIMIT].endswith("。") else str(row["events_content"])[0:SHOW_WEIBO_WORDS_LIMIT]), get_proper_unit_data(row["newly_weibo_value"]*526.32)), "link":"%s"%(row["events_link"])}   # #dataWarningCover:
            unit4_cols.append(current_col)

    # current_col3 = {"text":"全国范围内当前追踪[%s]相关事件：共%d件"%(event_type,country_num)}
    # current_col4 = {"text":"其中，全国级影响力事件：%d件"%THD_GRADES.count("A级")}
    # current_col5 = {"text": "省域级影响力事件：%d件" % THD_GRADES.count("B级")}
    # current_col6 = {"text": "区域级影响力事件：%d件" % THD_GRADES.count("C级")}

    current_col3 = {"text":"<span style='color:%s'>全国范围内当前追踪[%s]相关事件：共%d件。<br/>其中，全国级影响力事件：%d件；省域级影响力事件：%d件；区域级影响力事件：%d件。</span>"%("#48D1CC", event_type,country_num, THD_GRADES.count("C级"), THD_GRADES.count("B级"), THD_GRADES.count("A级"))}

    # unit4_cols.extend([current_col3, current_col4, current_col5, current_col6])
    unit4_cols.append(current_col3)

    # 2018/9/30 —— 加上统计信息
    current_col7 = {"text":"基础统计：过去一周，系统新增互联网信息%s条；本县新增%s条"%(info_dict["past_week"]["sys_info"], county_info_aug)}
    current_col8 = {"text":"隐患信息：全国新增隐患小事%s件；本县新增%s件"%(info_dict["past_week"]["trifles_num"], county_trifle_aug)}
    current_col9 = {"text":"热点事件：系统新增追踪%s件；本县追踪%s件"%(info_dict["past_week"]["events_num"], county_event_aug)}

    current_col10 = {"text":"<span style='color:%s'>追踪引擎：过去一周，系统新增追踪%s件 [累计%s件]；%s - 追踪%s件 [累计%s件]<br/>隐患引擎：全国新增隐患小事%s件 [累计%s件]；%s - 新增%s件 [累计%s件]<br/>采集引擎：系统新增互联网信息%s条 [累计：%s条]；%s - 新增%s条 [累计%s条]</span>"%(FT_PURE_WHITE, info_dict["past_week"]["events_num"], info_dict["total"]["events_num"], gov_name.split('|')[-1], county_event_aug, county_event_all, info_dict["past_week"]["trifles_num"], info_dict["total"]["trifles_num"], gov_name.split('|')[-1], county_trifle_aug, county_trifle_all, info_dict["past_week"]["sys_info"],info_dict["total"]["sys_info"], gov_name.split('|')[-1], county_info_aug, county_info_all)}

    # unit4_cols.extend([current_col7, current_col8, current_col9])
    unit4_cols.append(current_col10)
    unit4["cols"] = unit4_cols
    unit4["color"] = "orange"

    unit_details.append(unit4)

    warning_cover["unit_details"] = unit_details
    warning_cover["final_desc"] = [{"cols":[{"text":"2861%s监测系统-%s<br/>"%(event_type, gov_name)}]},{"cols":[{"text":"%s - 本县追踪事件%s件，本省%s件，全国共%s件"%(monitor_time, county_num, prov_num, country_num)}]}]

    warning_cover_dict["detail"] = warning_cover

    return warning_cover_dict


# 预警前端
def get_warning_setting_desc_data(gov_code, node_code, warning_map_data_name_list, df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, monitor_time, info_dict, record_now=True):
    """

    :type node_code: object
    """
    gov_code_str = str(gov_code)[0:6]
    warning_type = warning_dict[node_code]["events_model"]
    para_dict = warning_dict[node_code]["parameters"]

    # 多setting
    setting_list = []
    setting_name_list = []

    # 右侧描述
    # global list_desc
    list_desc = {}

    # 按事件提取
    # events_head_ids_origin = list(df_warning_trace_info['events_head_id'])
    # events_head_ids = list(set(events_head_ids_origin))
    # events_head_ids.sort(key=events_head_ids_origin.index)
    # events_short_dict = {}
    # for events_head_id in events_head_ids:
    #     events_short_dict[events_head_id] = 'we'+str(events_head_ids.index(events_head_id))
    #
    # # 万一一个区县发生了两件事儿呢？
    # # gov_ids = set(df_warning_trace_info['gov_id'])
    a_thd = para_dict['A_WARNING_Thd']
    b_thd = para_dict['B_WARNING_Thd']
    c_thd = para_dict['C_WARNING_Thd']
    thd_dict = {"A级": a_thd, "B级": b_thd, "C级": c_thd}
    thds = [c_thd, b_thd, a_thd]
    # thd_desc = ["C级", "B级", "A级", " "]      # 空格表示追踪中，没到三级预警门限
    # thd_scopes = ["全国", "省域", "区域", " "]  # 空格表示追踪中，没到三级预警门限
    #
    # gov_ids = []
    # gov_names = []
    # newly_weibo_values = []
    # thd_grades = []
    # scope_grades = []
    # for events_head_id in events_head_ids:
    #     gov_id = df_warning_trace_info[df_warning_trace_info['events_head_id']==events_head_id]['gov_id'].values[0]
    #     gov_name = df_2861_county[df_2861_county['gov_id']==gov_id]['full_name'].values[0]
    #     weibo_value = max(list(df_warning_trace_info[df_warning_trace_info['events_head_id']==events_head_id]['weibo_value']))
    #     for thd in thds:
    #         if weibo_value >= thd:
    #             thd_grades.append(thd_desc[thds.index(thd)])
    #             scope_grades.append(thd_desc[thds.index(thd)])
    #             break
    #         if thds.index(thd) == 2:
    #             thd_grades.append(thd_desc[3])
    #             scope_grades.append(thd_scopes[3])
    #     gov_ids.append(gov_id)
    #     gov_names.append(gov_name)
    #     newly_weibo_values.append(weibo_value)

    # ------------------------------------------zhong yu ba tu hua wan le-----------------------------------------
    gov_name_current = df_2861_county.loc[gov_code, 'full_name']
    nearest_trace_time = str(max(df_warning_trace_info['do_time'].tolist())).split('.')[0]
    color_dict = {"A级": LN_GOLDEN, "B级": LN_YELLOW, "C级": LN_RED}

    # 首页是warning_cover —— 2018/11/26 改
    setting_cover = {"title":"本区县预警监测", "datas":[{"id":"cover", "node_code":node_code, "data":gov_code_str+'/'+"warning_cover", "name":"本区县预警"}]}
    setting_list.append(setting_cover)
    setting_name_list.append("setting")

    # current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    monitor_time_short = datetime.strptime(monitor_time, '%Y-%m-%d %H:%M:%S').strftime('%m-%d %H:%M')

    # 追踪事件 —— 每个区县必须有一个setting，一个list
    # 地图排列的setting改动  —— 2018/11/30
    if 1:
        for map_name in warning_map_data_name_list:
            set_id = re.findall(r'warning_column_(.*?)_map', map_name)[0]
            setting_list.append({"title":"2861预警系统", "datas":[{"id":set_id, "node_code":node_code, "data":gov_code_str+'/'+map_name, "name":"2861预警系统"}]})

            setting_name_list.append("setting_%s"%set_id)

    page_settings = deepcopy(setting_name_list)
    page_setting_names = deepcopy(setting_list)

    # 'we0_value_index_trend'
    # 'we0_sens_word_trend'  —— 2018/8/2 不再生成
    # 'we0_publisher_trend'
    # index_cols = ['weibo_value', 'trace_v', 'trace_a', 'data_num', 'count_read', 'count_comment', 'count_share']
    # index_names = ['事件影响力指数', '事件传播速度', '事件传播加速度', '事件相关微博总量', '事件相关微博阅读总量', '事件相关微博评论总量', '事件相关微博转发总量']
    # index_cols = ['weibo_value', 'trace_v', 'trace_a']
    # index_names = ['事件影响力指数', '事件传播速度', '事件传播加速度']
    index_cols = ['weibo_value']
    index_names = ['事件影响力指数']
    # 以下部分setting可以公用【全国所有事件，2861展示时都一样】 —— 2018/8/15 —— 前端又要改，这个先暂缓 —— 2018/8/22，前端已改，测一下
    # 追踪指数的setting
    if gov_code_str == common_folder_code:
        for events_head_id in EVENTS_HEAD_IDS:
            # data_dict_list = []
            for i in index_cols:
                setting2 = {}
                setting2["title"] = '%s: "%s"' %(index_names[index_cols.index(i)], df_warning_trace_info.loc[
                    df_warning_trace_info.events_head_id == events_head_id, "search_key"].values[0])
                data_dict = {}
                data_dict["id"] = EVENTS_SHORT_DICT[events_head_id]+i.split('_')[-1]
                data_dict["node_code"] = node_code
                data_dict["name"] = index_names[index_cols.index(i)]
                data_dict["data"] = common_folder_code+'/'+EVENTS_SHORT_DICT[events_head_id]+'_'+i.split('_')[-1]+'_index_trend'
                # data_dict_list.append(data_dict)
                setting2["datas"] = [data_dict]
                setting_list.append(setting2)
                setting_name_list.append('setting_%s_%s_indexes'%(EVENTS_SHORT_DICT[events_head_id], i.split('_')[-1]))

            # content的setting
            setting_list.append({"title":"事件详情", "datas":[{"id":EVENTS_SHORT_DICT[events_head_id]+"cont", "node_code":node_code, "name":"事件详情", "data":common_folder_code+'/'+EVENTS_SHORT_DICT[events_head_id]+'_content_textbox'}]})
            setting_name_list.append('setting_%s_content'%EVENTS_SHORT_DICT[events_head_id])

            # 媒体的setting
            setting_list.append({"title": "事件发布媒体", "datas": [
                {"id": EVENTS_SHORT_DICT[events_head_id] + "media", "node_code": node_code, "name": "事件发布媒体",
                 "data": common_folder_code + '/' + EVENTS_SHORT_DICT[events_head_id] + '_media_stackbar'}]})
            setting_name_list.append('setting_%s_media' % EVENTS_SHORT_DICT[events_head_id])

            # 民众评价的关键词的setting
            setting_list.append({"title": "民众评价关键词", "datas": [
                {"id": EVENTS_SHORT_DICT[events_head_id] + "public", "node_code": node_code, "name": "民众评价关键词",
                 "data": common_folder_code + '/' + EVENTS_SHORT_DICT[events_head_id] + '_public_textbox'}]})
            setting_name_list.append('setting_%s_public' % EVENTS_SHORT_DICT[events_head_id])

# -----------------------------------------------setting zhong yu jie shu le--------------------------------------------
    # list_desc
    # 'setting_we0_value_indexes'
    # 'setting_we0_sens_words'  —— 2018/8/2 不需要关键词的setting了
    # 'setting_we0_publishers'

    # # 加上翻页试试
    list_desc["page_list"] = {}
    list_desc["page_list"]["setting_list"] = [node_code + '/' + gov_code_str + '/' + i for i in page_settings]
    list_desc["page_list"]["global_mode"] = True

    # if "past_events" in info_dict.keys():
    if node_code == "STABLE_WARNING":
        past_dict = info_dict["past_events"]
        df_past = pd.DataFrame(past_dict)
        # desc_past = get_past_events_desc_per_gov(gov_code, df_past)
        past_info = get_past_events_desc_per_gov(gov_code, df_past)
        # back_info = {"cols": [{"text": "返回", "link": "#list:%s/%s/trace"%(node_code, gov_code_str)}], "bg_color": BT_TIFFANY_BLUE,"color": FT_PURE_WHITE, "strong": True}
        # 2018-09-27改版，不是返回，是收起
        #  "bg_color": BT_TIFFANY_BLUE,
        back_info = {"cols": [{"text": "收起", "link": "#closeList:"}],
                    "color": FT_PURE_WHITE, "strong": True}

        # past_info = {"cols": [{"text": "%s"%desc_past}]}
        datas = [back_info]
        datas.extend(past_info)
        list_desc["past"] = {"title":"", "sub_title":"", "width":"35%", "datas":datas}

    if gov_code_str == common_folder_code:
        for events_head_id in EVENTS_HEAD_IDS:
            # 每个事件的翻页按钮
            pages_button_list = [{"text": "1", "link": "#data:%s/%s/setting_%s_value_indexes" % (node_code, gov_code_str, EVENTS_SHORT_DICT[events_head_id])}, {"text": "2", "link": "#data:%s/%s/setting_%s_content" % (node_code, gov_code_str,EVENTS_SHORT_DICT[events_head_id])},{"text": "3", "link": "#data:%s/%s/setting_%s_media" % (node_code, gov_code_str, EVENTS_SHORT_DICT[events_head_id])}, {"text": "4", "link": "#data:%s/%s/setting_%s_public" % (node_code, gov_code_str,EVENTS_SHORT_DICT[events_head_id])}]
            for index_col in index_cols:
                index_id = EVENTS_SHORT_DICT[events_head_id]+index_col.split('_')[-1]
                list_desc[index_id] = {"title":"", "sub_title": "", "width": events_desc_width}
                list_desc_data2 = []

                # 2018/12/4 —— 改版
                # 第一行：标题
                # gov_title = "<h3><section style='text-align:center'>%s</section></h3>" % GOV_NAMES[EVENTS_HEAD_IDS.index(events_head_id)]
                # gov_info = {"cols": [{"text": gov_title}]}
                # list_desc_data2.append(gov_info)
                #
                # # 第二/三行：预警详情
                # df_event = df_warning_trace_info.loc[df_warning_trace_info['events_head_id'] == events_head_id, :].reset_index(
                #     drop=True)
                # df_event.loc[:, 'do_time'] = df_event.loc[:, 'do_time'].apply(lambda x: str(x).split('.')[0])
                # df_event = df_event.sort_values(by=['do_time']).reset_index(drop=True)
                #
                # latest_trace = df_event.loc[:, 'do_time'].max()
                # warn_event_data = ""
                # earliest_pub_time = str(df_warning_details.loc[df_warning_details['events_head_id'] == events_head_id, 'pub_time'].min()).split('.')[0]
                # # earliest_trace_time = df_event.loc[: 'do_time'].min()
                # county_name = GOV_NAMES[EVENTS_HEAD_IDS.index(events_head_id)].split('|')[-1]
                # search_key = ' '.join(df_event["search_key"].values[0].split(' ')[1:])
                #
                # # warn_title = "<h3><section style='color: orange; text-align:center'>预警详情</section></h3>"
                # warn_title_info = {"cols": [{"text": "<section style='text-align:center'>预警详情</section>"}, {"text": "部分信息来源", "link": "#data:%s/%s/setting_%s_publishers" % (node_code, gov_code_str, EVENTS_SHORT_DICT[events_head_id])}, {"text": "返回首页", "link": "#back:"}], "bg_color":BT_TIFFANY_BLUE, "color":FT_PURE_WHITE, "strong":True}
                #
                # # "text": "返回首页", "link": "#data:setting"
                #
                # latest_weibo_value = df_event.loc[df_event['do_time'] == latest_trace, 'weibo_value'].values[0]
                #
                # warn_event_data = "本系统于%s，在互联网上监测到%s发生了一件%s隐患事件，该事件关键字为“<span style='color: orange'>%s</span>”，当前事件网上传播覆盖人次：<span style='color: orange'>%s</span>。此后系统持续追踪，本事件在网上的影响力扩散见左图。<br/>事件当前状态及离各级预警的距离如下：" % (earliest_pub_time, county_name, warning_type, search_key, get_proper_unit_data(latest_weibo_value*526.32))
                # EVENTDESC = warn_event_data.split('<br/>')[0]
                # EVENTDESC = tidy_rich_text(EVENTDESC)
                #
                # warn_info = {"cols": [{"text": warn_event_data}]}
                # list_desc_data2.extend([warn_title_info, warn_info, null_line])
                #
                # latest_trace_v = df_event.loc[df_event['do_time'] == latest_trace, 'trace_v'].values[0]
                # latest_trace_a = df_event.loc[df_event['do_time'] == latest_trace, 'trace_a'].values[0]
                #
                #
                # latest_ws = []
                # for status in WSTATUSES.keys():
                #     locals()[status] = df_event.loc[df_event['do_time'] == latest_trace, status].values[0]
                #     latest_ws.append(locals()[status])
                # # 判断几级事件
                #
                # # if latest_wc == 0:
                # if -2 not in latest_ws:
                #     current_events_judge = {}
                #     cant_reach_events_judge = {}
                #     for i in range(0, len(WSTATUSES)+1):
                #         if i == 0:
                #             current_events_judge[i] = ""
                #             cant_reach_events_judge[i] = ""
                #         else:
                #             current_events_judge[i] = list(WSTATUSES.keys())[i-1]
                #             cant_reach_events_judge[i] = list(WSTATUSES.keys())[len(WSTATUSES)-i]
                #     need_time_to_reach = {}
                #     for status in WSTATUSES.keys():
                #         if locals()[status] > 0:
                #             need_time_to_reach[status] = locals()[status]
                #
                #     # 当前的事件等级
                #     event_status = current_events_judge[latest_ws.count(0)]
                #     # 达不到的等级
                #     cant_reach = cant_reach_events_judge[latest_ws.count(-1)]
                #     if event_status == '':
                #         event_info = "事件追踪中。"
                #         advise_info = "<span style='color: orange'>持续关注，及时应对。</span>"
                #         already_pass_dict = {}
                #     else:
                #         status_info = WSTATUSES[event_status]
                #         first_warn_time = datetime.strptime(df_event.loc[df_event[event_status] == 0, 'do_time'].min(),
                #                                             '%Y-%m-%d %H:%M:%S')
                #         latest_trace_time = datetime.strptime(latest_trace, '%Y-%m-%d %H:%M:%S')
                #         interdays = (latest_trace_time - first_warn_time).days
                #         interseconds = (latest_trace_time - first_warn_time).seconds
                #         warn_interhours = interdays*24 + interseconds/3600
                #         event_info = "<span style='color: orange'>%s</span>事件，触发%s预警已<span style='color: orange'>%.2f</span>小时，已成为<span style='color: orange'>%s</span>范围内的热点话题。%s"%(status_info['name'], status_info['name'], warn_interhours, status_info['area'], status_info['desc'])
                #         advise_info = status_info['advise']
                #         # 已超过的其他门限
                #         already_pass_dict = {}
                #         for warning_key in WSTATUSES.keys():
                #             if warning_key < event_status:
                #                 first_warning_time = datetime.strptime(df_event.loc[df_event[warning_key]==0, 'do_time'].min(), '%Y-%m-%d %H:%M:%S')
                #                 other_interdays = (latest_trace_time-first_warning_time).days
                #                 other_interseconds = (latest_trace_time-first_warning_time).seconds
                #                 other_warn_interhours = other_interdays*24 + other_interseconds/3600
                #                 other_event_info = "触发<span style='color: orange'>%s</span>预警已<span style='color: orange'>%.2f</span>小时。影响范围早已超过<span style='color: orange'>%s</span>，现事件影响程度已高于%s事件。<br/>一般而言，%s事件的影响程度为：%s"%(WSTATUSES[warning_key]['name'],other_warn_interhours, WSTATUSES[warning_key]['area'], WSTATUSES[warning_key]['name'], WSTATUSES[warning_key]['name'], WSTATUSES[warning_key]['desc'])
                #                 already_pass_dict[WSTATUSES[warning_key]['name']] = other_event_info
                #
                #     if cant_reach != '':
                #         cant_reach_dict = {}
                #         cant_reach_info = WSTATUSES[cant_reach]
                #         events_grades = ["A级", "B级", "C级"]
                #         events_scopes = ["区域", "省域", "全国"]
                #         warning_signs = ['warning_a', 'warning_b', 'warning_c']
                #         cant_reach_grades = events_grades[events_grades.index(cant_reach_info['name']):]
                #         for i in cant_reach_grades:
                #             cant_reach_event_info = "评估本事件当前的传播速度、加速度等，发展为<span style='color: orange'>%s</span>性(%s)事件的概率仅为：<span style='color: orange'>10%%~20%%</span>。<br/>一般而言，%s事件的影响程度为：%s"%(events_scopes[events_grades.index(i)], i, i,WSTATUSES[warning_signs[events_grades.index(i)]]['desc'])
                #             cant_reach_dict[i] = cant_reach_event_info
                #
                #     if len(need_time_to_reach) != 0:
                #         need_time_list = []
                #         need_time_dict = {}
                #         for key in need_time_to_reach.keys():
                #             need_time_info = WSTATUSES[key]
                #             need_time_str = "本事件有<span style='color: orange'>80%%~90%%</span>的概率将于<span style='color: orange'>%.2f小时</span>后发展为%s（%s）事件。<br/>一般而言，%s事件的影响程度为：%s"%(need_time_to_reach[key], need_time_info['area'], need_time_info['name'], need_time_info['name'], need_time_info['desc'])
                #             need_time_list.append(need_time_str)
                #             need_time_dict[need_time_info['name']] = need_time_str
                #         need_time_desc = "若放任不管，经评估当前状态，"+''.join(need_time_list)
                #         # advise_info += need_time_desc
                #
                # # 如果是刚开始的几个周期，就根据值来判断当前等级，并给其他两个等级为 —— “刚开始追踪，距离/超过时间尚在计算”
                # else:
                #     events_grades = ["A级", "B级", "C级"]
                #     events_scopes = ["区域", "省域", "全国"]
                #     warning_signs = ['warning_a', 'warning_b', 'warning_c']
                #     # 当前事件所处等级
                #     event_grade = THD_GRADES[EVENTS_HEAD_IDS.index(events_head_id)]
                #     need_time_to_reach = {}
                #     already_pass_dict = {}
                #     cant_reach = "newly_start"
                #     cant_reach_dict = {}
                #     # 追踪中
                #     if event_grade == "追踪中":
                #         event_status = ''
                #         event_info = "事件追踪中。"
                #         advise_info = "<span style='color: orange'>持续关注，及时应对。</span>"
                #         for i in events_grades:
                #             cant_reach_dict[i] = "当前处于追踪的前五个采样周期，事件距离<span style='color: orange'>%s</span>性(%s)事件的时间和概率尚在计算中。<br/>一般而言，%s事件的影响程度为：%s"%(events_scopes[events_grades.index(i)], i, i,WSTATUSES[warning_signs[events_grades.index(i)]]['desc'])
                #     # 已达到其他等级的事件
                #     else:
                #         event_status = warning_signs[events_grades.index(event_grade)]
                #         status_info = WSTATUSES[event_status]
                #         # first_warn_time = datetime.strptime(df_event.loc[df_event[event_status] == 0, 'do_time'].min(),
                #         #                                     '%Y-%m-%d %H:%M:%S')
                #         # latest_trace_time = datetime.strptime(latest_trace, '%Y-%m-%d %H:%M:%S')
                #         # warn_interval = (latest_trace_time - first_warn_time).seconds
                #         event_info = "<span style='color: orange'>%s</span>事件，已成为<span style='color: orange'>%s</span>范围内的热点话题。%s" % (status_info['name'], status_info['area'], status_info['desc'])
                #         advise_info = status_info['advise']
                #         other_grades = [i for i in events_grades if i != event_grade]
                #         for i in other_grades:
                #             cant_reach_dict[i] = "当前处于追踪的前五个采样周期，事件距离<span style='color: orange'>%s</span>性(%s)事件的时间和概率尚在计算中。<br/>一般而言，%s事件的影响程度为：%s" % (events_scopes[events_grades.index(i)], i, i,WSTATUSES[warning_signs[events_grades.index(i)]]['desc'])
                #
                # C_warning_button = {
                #     "cols": [{"text": "全国性（C级）事件", "link": "#list:%s/%s/%sC" % (node_code, gov_code_str, EVENTS_SHORT_DICT[events_head_id])}, {"text": ""},
                #              {"text": ""}]}
                # B_warning_button = {
                #     "cols": [{"text": "省域性（B级）事件", "link": "#list:%s/%s/%sB" % (node_code, gov_code_str, EVENTS_SHORT_DICT[events_head_id])},
                #              {"text": ""}]}
                # A_warning_button = {
                #     "cols": [{"text": "区域性（A级）事件", "link": "#list:%s/%s/%sA" % (node_code, gov_code_str, EVENTS_SHORT_DICT[events_head_id])}]}
                # TRACE_warning_button = {
                #     "cols": [{"text": "正在追踪的事件", "link": "#list:%s/%s/%strace" % (node_code, gov_code_str, EVENTS_SHORT_DICT[events_head_id])}]}
                # tri_buttons_list = [C_warning_button, B_warning_button, A_warning_button, TRACE_warning_button]
                #
                # thd_desc = ["C级", "B级", "A级", " "]  # 空格表示追踪中，没到三级预警门限
                # thd_scopes = ["全国", "省域", "区域", " "]  # 空格表示追踪中，没到三级预警门限
                # thd_dict = {"A级": a_thd, "B级": b_thd, "C级": c_thd}
                # color_dict = {"A级": LN_GOLDEN, "B级": LN_YELLOW, "C级": LN_RED, " ": FT_SOBER_BLUE}
                #
                # if event_status != '':
                #     status_grade = WSTATUSES[event_status]['name']
                #     del tri_buttons_list[-1]
                #     # grade_type = status_grade[0]
                # else:
                #     status_grade = " "
                #     # grade_type = "trace"
                # status_index = thd_desc.index(status_grade)
                # # del tri_buttons_list[status_index]["cols"][0]["link"]
                # # tri_buttons_list[status_index]["cols"][0]["text"] = "<section style='text-align:center'>"+"本县当前："+tri_buttons_list[status_index]["cols"][0]["text"]+"</section>"
                # tri_buttons_list[status_index]["strong"] = True
                # # tri_buttons_list[status_index]["color"] = color_dict[status_grade]
                # tri_buttons_list[status_index]["cols"][0]["text"] = "<span style='color: %s'>"%(color_dict[status_grade]) + tri_buttons_list[status_index]["cols"][0]["text"] + "</span>"
                # tri_buttons_list[status_index]["cols"][0]["link"] = "#list:%s/%s/%svalue"%(node_code, gov_code_str, EVENTS_SHORT_DICT[events_head_id])
                # # "#data:setting_%s_%s_indexes"%(events_short_dict[events_head_id], index_col.split('_')[-1])
                # tri_buttons_list[status_index]["cols"][0]["color"] = color_dict[status_grade]
                # status_desc = "<span style='color: orange'>事件当前状态</span>：%s<br/><span style='color: orange'>系统建议</span>：%s"%(event_info, advise_info)
                # status_desc_line = {"cols":[{"text":"%s"%status_desc}]}  # , "color":color_dict[status_grade]
                # tri_buttons_list.insert(status_index+1, status_desc_line)
                # # 加了状态位之后，对应事件级数索引后加0
                # thd_desc.insert(status_index+1, 0)
                # list_desc_data2.extend(tri_buttons_list)
                #
                # list_desc_data2.append(null_line)
                # word_types = ['sensitive', 'department', 'guanzhi']
                # word_names = ['事件关键词', '涉及部门', '涉及官员']
                # title_cols = []
                # for word_name in word_names:
                #     text = {"text": word_name}
                #     title_cols.append(text)
                # word_title = {"cols": title_cols, "strong": True, "size": 14,
                #               "color": FT_ORANGE}
                #
                # # 第四~N行：关键词
                # words_dict = {}
                # words_lines = []
                # for word_type in word_types:
                #     df_event = df_warning_keywords.loc[
                #                (df_warning_keywords.events_head_id == events_head_id) & (df_warning_keywords.type == word_type),
                #                :]
                #     df_event = df_event.sort_values(by=['freq'], ascending=False).reset_index(drop=True)
                #     if df_event.iloc[:, 0].size > max_key_words_num:
                #         df_event = df_event[0:max_key_words_num]
                #         freq_total = sum(list(df_event['freq']))
                #         df_event.loc[:, 'freq'] = df_event.loc[:, 'freq'].apply(lambda x: round(x / freq_total, 4))
                #     words_list = []
                #     for index, row in df_event.iterrows():
                #         word_str = "%s ( %.2f%% )" % (row['word'], row['freq'] * 100)
                #         words_list.append(word_str)
                #     words_dict[word_type] = words_list
                #
                # max_len = max([len(words_dict[i]) for i in words_dict.keys()])
                # for j in range(0, max_len):
                #     word_cols = [{"text":""}, {"text":""}, {"text":""}]
                #     for i in range(0,3):
                #         word_type = list(words_dict.keys())[i]
                #         if len(words_dict[word_type]) > j:
                #             word_cols[i]["text"] = words_dict[word_type][j]
                #     word_line = {"cols": word_cols, "color": FT_SOBER_BLUE}
                #     words_lines.append(word_line)
                #
                # list_desc_data2.append(word_title)
                # list_desc_data2.extend(words_lines)

                # # 新的版式，右侧list留白，只在底端加一排页码按钮 —— 2018/11/27
                # page_buttons = {"cols":[{"text":"返回地图", "link":"#backWarningCover"}, {"text":"<section style='text-align:center'>1</section>"}, {"text":"2", "link":"#data:%s/%s/setting_%s_content"%(node_code, gov_code_str, EVENTS_SHORT_DICT[events_head_id])}, {"text":"3", "link":"#data:%s/%s/setting_%s_media"%(node_code, gov_code_str, EVENTS_SHORT_DICT[events_head_id])},{"text":"4", "link":"#data:%s/%s/setting_%s_public"%(node_code, gov_code_str, EVENTS_SHORT_DICT[events_head_id])}], "bg_color":BT_TIFFANY_BLUE, "color":FT_PURE_WHITE, "strong":True}

                # 用天博专门定制的按钮样式，省了空行的形式 —— 2018/12/4
                pbs_str = "".join(["<a class='link-widget' state='page_flip_act'>%s</a>"%button["text"] if button["text"] == "1" else "<a class='link-widget' state='page_flip' href='%s'>%s</a>"%(button["link"], button["text"]) for button in pages_button_list])

                page_buttons = [{"cols":[{"text":"<div style='padding-top:550px;text-align:center;'>%s</div>"%pbs_str}]}, {"cols":[{"text":"返回地图页", "link":"#backWarningCover"}]}]   # padding-bottom:40px;

                # list_desc_data2 = [null_line]*page_before_lines + [page_buttons]

                list_desc_data2 = page_buttons

                list_desc[index_id]["datas"] = list_desc_data2
                # 解说
                # list_desc[index_id]["desc"] = EVENTDESC

                # # 剩下的其它状态
                # last_statuses = [i for i in thd_desc if i not in [status_grade, " ", 0]]
                # for status in last_statuses:
                #     index_id_other = EVENTS_SHORT_DICT[events_head_id] + status[0]
                #     list_desc[index_id_other] = {"title": "", "sub_title": "", "width": "35%", "desc":EVENTDESC}
                #     list_desc_data3 = [gov_info, warn_title_info, warn_info, null_line]
                #     other_status_index = thd_desc.index(status)
                #     tri_buttons_list_other = deepcopy(tri_buttons_list)
                #     tri_buttons_list_other[other_status_index]["cols"][0]["link"] = "#list:%s/%s/%svalue"%(node_code, gov_code_str, EVENTS_SHORT_DICT[events_head_id])
                #     tri_buttons_list_other[other_status_index]["strong"] = True
                #     # tri_buttons_list_other[other_status_index]["color"] = color_dict[status]
                #     tri_buttons_list_other[other_status_index]["cols"][0]["text"] = "<span style='color: %s'>" % (
                #     color_dict[status]) + tri_buttons_list_other[other_status_index]["cols"][0]["text"] + "</span>"
                #     # tri_buttons_list_other[other_status_index]["cols"][0]["color"] = color_dict[status]
                #     status_desc = ""
                #     if cant_reach != '':
                #         if status in cant_reach_dict.keys():
                #             status_desc += cant_reach_dict[status]
                #     if len(need_time_to_reach) != 0:
                #         if status in need_time_dict.keys():
                #             status_desc += need_time_dict[status]
                #     if len(already_pass_dict) != 0:
                #         if status in already_pass_dict.keys():
                #             status_desc += already_pass_dict[status]
                #     status_desc_line = {"cols": [{"text": "%s" % status_desc}]}  # , "color":color_dict[status_grade]
                #     tri_buttons_list_other.insert(other_status_index + 1, status_desc_line)
                #     list_desc_data3.extend(tri_buttons_list_other)
                #     # 关键词
                #     list_desc_data3.extend([null_line, word_title])
                #     list_desc_data3.extend(words_lines)
                #     list_desc[index_id_other]["datas"] = list_desc_data3

            # content页的描述 —— 2018/12/4改
            pbs_str_content = "".join(["<a class='link-widget' state='page_flip_act'>%s</a>"%button["text"] if button["text"] == "2" else "<a class='link-widget' state='page_flip' href='%s'>%s</a>"%(button["link"], button["text"]) for button in pages_button_list])
            page_buttons_content = [{"cols":[{"text":"%s"%(re.sub(re.match(r"<div.*?>(.*?)</div>", page_buttons[0]["cols"][0]["text"])[1], pbs_str_content, page_buttons[0]["cols"][0]["text"]))}]}, page_buttons[1]]
            list_desc[EVENTS_SHORT_DICT[events_head_id] + "cont"] = {"title":"", "sub_title":"", "datas":page_buttons_content, "width": events_desc_width}

            # 媒体页的描述

            pbs_str_media = "".join(["<a class='link-widget' state='page_flip_act'>%s</a>" % button["text"] if button["text"] == "3" else "<a class='link-widget' state='page_flip' href='%s'>%s</a>" % (
            button["link"], button["text"]) for button in pages_button_list])
            page_buttons_media = [{"cols": [{"text": "%s" % (
                re.sub(re.match(r"<div.*?>(.*?)</div>", page_buttons[0]["cols"][0]["text"])[1], pbs_str_media,
                       page_buttons[0]["cols"][0]["text"]))}]}, page_buttons[1]]
            list_desc[EVENTS_SHORT_DICT[events_head_id]+"media"] = {"title":"", "sub_title":"", "datas":page_buttons_media, "width": events_desc_width}

            # 评价关键词的描述

            pbs_str_keywords = "".join(["<a class='link-widget' state='page_flip_act'>%s</a>" % button["text"] if button["text"] == "4" else "<a class='link-widget' state='page_flip' href='%s'>%s</a>" % (
                button["link"], button["text"]) for button in pages_button_list])
            page_buttons_keywords = [{"cols": [{"text": "%s" % (
                re.sub(re.match(r"<div.*?>(.*?)</div>", page_buttons[0]["cols"][0]["text"])[1], pbs_str_keywords,
                       page_buttons[0]["cols"][0]["text"]))}]}, page_buttons[1]]
            list_desc[EVENTS_SHORT_DICT[events_head_id] + "public"] = {"title": "", "sub_title": "",
                                                                      "datas": page_buttons_keywords, "width": events_desc_width}
    return setting_list, setting_name_list, list_desc


# 产生前端细节文件
def generate_html_content(gov_code, node_code, df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, monitor_time, info_dict, record_now=True):
    # 有问题？？？
    gov_code_str = str(gov_code)[0:6]
    target_dir_path = client_path + node_code + '/' + str(gov_code)[0:6] + '/'
    if not os.path.exists(target_dir_path):
        os.makedirs(target_dir_path)

    # 预警首页 —— 每个区县都有
    warning_cover = get_warning_cover_data(gov_code, node_code, monitor_time, info_dict)
    write_client_datafile_json(target_dir_path, 'warning_cover', '.json', warning_cover)

    # 地域分布
    if gov_code_str == common_folder_code:
        # 得到基础数据
        warning_map_data_list, warning_map_data_name_list, warning_line_data_list, warning_line_data_name_list = get_warning_map_line_basic_data(gov_code, node_code, df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, monitor_time, record_now)
        if len(warning_line_data_list) > 0 and len(warning_map_data_list)>0:
            # print('map & columns')
            for set in range(len(warning_line_data_list)):
                write_client_datafile_json(target_dir_path, warning_line_data_name_list[set], '.json', warning_line_data_list[set])
            for position in range(len(warning_map_data_list)):
                write_client_datafile_json(target_dir_path, warning_map_data_name_list[position], '.json',warning_map_data_list[position])

        # 得到setting和list文件
        setting_list, setting_name_list, list_desc = get_warning_setting_desc_data(gov_code, node_code, warning_map_data_name_list, df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, monitor_time, info_dict, record_now)
        if len(setting_list) > 0:
            for set in range(len(setting_list)):
                write_client_datafile_json(target_dir_path, setting_name_list[set], '.json',setting_list[set])
            write_client_datafile_json(target_dir_path, 'list', '.json', list_desc)

    # 其他区县
    else:
        # 得到基础数据
        warning_map_data_list, warning_map_data_name_list, warning_line_data_list, warning_line_data_name_list = get_warning_map_line_basic_data(
            gov_code, node_code, df_warning_trace_info, df_warning_keywords, df_warning_details,
            df_warning_weibo_comments, monitor_time, record_now)
        if len(warning_map_data_list) > 0:
            for position in range(len(warning_map_data_list)):
                write_client_datafile_json(target_dir_path, warning_map_data_name_list[position], '.json',
                                           warning_map_data_list[position])

        # 得到setting和list文件
        setting_list, setting_name_list, list_desc = get_warning_setting_desc_data(gov_code, node_code, warning_map_data_name_list, df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, monitor_time, info_dict, record_now)
        if len(setting_list) > 0:
            # for position in range(len(warning_map_data_list)):
            #     write_client_datafile_json(target_dir_path, warning_map_data_name_list[position], '.json',warning_map_data_list[position])
            for set in range(len(setting_list)):
                write_client_datafile_json(target_dir_path, setting_name_list[set], '.json',
                                                     setting_list[set])
            # 测试不要setting可不可以 —— 不行， 页面先找的setting
            write_client_datafile_json(target_dir_path, 'list', '.json', list_desc)

    return


# 阻塞远程调用命令，直到该命令在服务器上执行完成，并记录状态
def run_command(sshclient, the_cmd):
    print('-> ' + the_cmd)
    ssh_transp = sshclient.get_transport()
    chan = ssh_transp.open_session()
    chan.setblocking(0)
    chan.exec_command(the_cmd)
    outdata, errordata = '', ''
    count = 0
    while True:
        while chan.recv_ready():
            outdata += chan.recv(1000).decode()
        while chan.recv_stderr_ready():
            errordata += chan.recv_stderr(1000).decode()
        if chan.exit_status_ready():
            break
        print('still running %s...' % count)
        count += 1
        time.sleep(1)
    retcode = chan.recv_exit_status()
    chan.close()
    return retcode, outdata, errordata


# 拿最新的事件
def get_newliest_events_data(node_code):
    events_node_dir = event_data_path + node_code + '/'
    version_file = events_node_dir + 'version.txt'
    newest_date = open(version_file, 'r', encoding='utf-8-sig').readlines()[-1].strip()
    events_data_path = events_node_dir + newest_date + '/'
    df_warning_trace_info = pd.DataFrame(json.load(open(events_data_path+'trace_info.json', 'r')))
    df_warning_keywords = pd.DataFrame(json.load(open(events_data_path+'keywords.json', 'r')))
    df_warning_details = pd.DataFrame(json.load(open(events_data_path+'details.json', 'r')))
    df_warning_weibo_comments = pd.DataFrame(json.load(open(events_data_path+'comments.json', 'r')))

    record_now = True
    events_num = 0

    log_infos = open(event_data_path+'fetch_data_record.log', 'r', encoding='utf-8').readlines()

    for record_log in log_infos:
        if (node_code in record_log) and (newest_date in record_log):
            infos_list = record_log.strip().split('-->')
            for i in infos_list:
                if 'record_now' in i:
                    record_now_str = i.split('record_now:')[-1].strip()
                    record_now = True if record_now_str == 'True' else False
                if 'events_num' in i:
                    events_num = int(i.split('events_num:')[-1].strip())
            break

    return df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, events_num, record_now


# 装饰器包裹函数 —— 处理返回数字自动转换
def properUnit(func):
    def wrapper(*args, **kwargs):
        res = func(*args, **kwargs)
        if isinstance(res, tuple):
            res = tuple(get_proper_unit_data(i) for i in res)
        else:
            res = get_proper_unit_data(res)
        return res
    return wrapper


# 计算两个更新周期之间的事件信息新增条数
@properUnit
def events_update_info_num_between(monitor_datetime):
    last_time = monitor_datetime - timedelta(hours=interhour)
    sqlstr = "SELECT SUM(update_info) FROM (SELECT events_head_id, MAX(data_num+count_read+count_comment+count_share)-MIN(data_num+count_read+count_comment+count_share) AS update_info, MAX(do_time) AS do_time FROM %s WHERE do_time BETWEEN '%s' AND '%s' GROUP BY events_head_id) b;"%(trace_info_table, last_time, monitor_datetime)

    rows = trace_db_obj.select_data_from_db_one_by_one(trace_db_info["db_name"], sqlstr)
    # print("rows:", rows)
    if len(rows) == 0 or rows[0][0] is None:
        res = 0
    else:
        res = rows[0][0]
    return res


# 计算近一周追踪事件数
@properUnit
def get_past_week_trace_events_num(monitor_datetime, gov_code=None):
    last_time = monitor_datetime - timedelta(days=7)
    sqlstr = "SELECT count(events_head_id) from %s where sync_time between '%s' and '%s'"%(trace_seed_table, last_time, monitor_datetime)
    if gov_code is not None:
        gov_id = df_2861_county.loc[gov_code, 'gov_id']
        sqlstr += " and gov_id=%d"%gov_id

    rows = trace_db_obj.select_data_from_db_one_by_one(trace_db_info["db_name"], sqlstr)
    # print(rows[0][0])
    if len(rows) == 0 or rows[0][0] is None:
        if gov_code is None:
            res = 800
        else:
            res = 0
    else:
        res = rows[0][0]
    return res


# 计算累计追踪事件数
@properUnit
def get_total_trace_events_num(monitor_datetime, gov_code=None):
    sqlstr = "SELECT count(events_head_id) from %s"%trace_seed_table
    if gov_code is not None:
        gov_id = df_2861_county.loc[gov_code, 'gov_id']
        sqlstr += " where gov_id=%d"%gov_id
    rows = trace_db_obj.select_data_from_db_one_by_one(trace_db_info["db_name"], sqlstr)
    if len(rows) == 0 or rows[0][0] is None:
        if gov_code is None:
            res = 3769
        else:
            res = 0
    else:
        res = rows[0][0]
    return res


# 计算近一周追踪事件信息条数
@properUnit
def get_past_week_trace_info_num(monitor_datetime):
    last_time = monitor_datetime - timedelta(days=7)
    # 2018/9/9 由于累计的评论数目没有统计，所以暂改为全部只用微博条数 # +count_comment
    sqlstr = "SELECT SUM(update_info) FROM (SELECT events_head_id, MAX(data_num)-MIN(data_num) AS update_info, MAX(do_time) AS do_time FROM %s WHERE do_time BETWEEN '%s' AND '%s' GROUP BY events_head_id) b;" % (trace_info_table, last_time, monitor_datetime)

    rows = trace_db_obj.select_data_from_db_one_by_one(trace_db_info["db_name"], sqlstr)

    if len(rows) == 0 or rows[0][0] is None:
        res = 0
    else:
        res = rows[0][0]
    # conn.disconnect()
    # print(rows[0][0])
    return res


# 计算近一周系统入库信息条数
@properUnit
def get_past_week_sys_info_num(monitor_datetime, gov_code=None):
    latest_time = (monitor_datetime - timedelta(days=2)).date()
    last_time = (monitor_datetime - timedelta(days=9)).date()
    # 2018/9/9 由于累计的评论数目没有统计，所以暂改为全部只用微博条数 # +daily_comment
    sqlstr = "SELECT SUM(daily_weibo) as sum from %s where pub_date >= '%s' and  pub_date <= '%s'"%(stats_table, last_time, latest_time)
    if gov_code is not None:
        gov_id = df_2861_county.loc[gov_code, 'gov_id']
        sqlstr += " and gov_id = %d" % gov_id
    conn = database.ConnDB(product_server, product_db)
    conn.switch_to_arithmetic_write_mode()
    ret = conn.read(sqlstr)
    # print(ret.code)
    rows = ret.data
    # print(ret.result)
    # print(rows)
    # print(rows[0]['sum'])
    conn.disconnect()

    # 如果没取到数， 就默认1.01千万 —— 全国
    if len(rows) == 0 or rows[0]['sum'] is None:
        if gov_code is None:
            res = 10100000
        else:
            res = 0
    else:
        res = rows[0]['sum']
    return res


# 取系统累计互联网信息条数
@properUnit
def get_total_sys_info_num(monitor_datetime, gov_code=None):
    latest_time = (monitor_datetime - timedelta(days=2)).date()
    # 2018/9/9 由于累计的评论数目没有统计，所以暂改为全部只用微博条数 # +daily_comment
    sqlstr = "SELECT SUM(total_weibo) as sum from %s where pub_date = '%s'" % (
    stats_table, latest_time)
    if gov_code is not None:
        gov_id = df_2861_county.loc[gov_code, 'gov_id']
        sqlstr += " and gov_id=%d"%gov_id
    conn = database.ConnDB(product_server, product_db)
    conn.switch_to_arithmetic_write_mode()
    ret = conn.read(sqlstr)
    # print(ret.code)
    if not ret.code:
        print("Failed to get total sys info num ! ", ret.result)
    rows = ret.data
    # print(ret.result)
    # print("rows:", rows)
    # print(rows[0]['sum'])
    num = rows[0]['sum']
    # 没取到/没更新，就给个默认值——2.8亿
    if rows[0]['sum'] is None:
        if gov_code is None:
            num = 280000000
        else:
            num = 0
    return num


# # 计算近一周本县入库信息条数
# def


# 统计小事件信息条数/件数
@properUnit
def get_past_week_es_events_num(monitor_datetime, gov_code=None):
    latest_time = (monitor_datetime - timedelta(days=5)).date()
    last_time = (monitor_datetime - timedelta(days=12)).date()
    sqlstr = "SELECT SUM(events_weibo_num) as info, SUM(events_num_origin) as num from %s where events_start_date >= '%s' and events_start_date <= '%s'"%(es_stats_table, last_time, latest_time)
    if gov_code is not None:
        gov_id = df_2861_county.loc[gov_code, 'gov_id']
        sqlstr += " and gov_id = %d"%gov_id
    conn = database.ConnDB(product_server, product_db)
    conn.switch_to_arithmetic_write_mode()
    ret = conn.read(sqlstr)
    rows = ret.data
    if not ret.code:
        print("Failed to get past week es events num ! ", ret.result)
    # print(rows)
    # if len(rows) == 0:

    info = rows[0]['info']
    # try:
    num = rows[0]['num']
    if info is None:
        if gov_code is None:
            info = 50000
        else:
            info = 0
    if num is None:
        if gov_code is None:
            num = 5000
        else:
            num = 0
    num = num * HIDDEN_RATIO
    # except Exception as e:
    #     print(gov_code, e)
    if 0 < num < 1:
        num = 1
    conn.disconnect()
    # print(info, num)
    return info, num


# 统计过往累计小事件信息条数/件数
@properUnit
def get_total_es_events_num(monitor_datetime, gov_code=None):
    sqlstr = "SELECT SUM(events_weibo_num) as info, SUM(events_num_origin) as num from %s where events_start_date <= '%s'"%(es_stats_table, monitor_datetime)
    if gov_code is not None:
        gov_id = df_2861_county.loc[gov_code, 'gov_id']
        sqlstr += " and gov_id = %d"%gov_id
    conn = database.ConnDB(product_server, product_db)
    conn.switch_to_arithmetic_write_mode()
    ret = conn.read(sqlstr)
    rows = ret.data
    if not ret.code:
        print("Failed to get total es events num ! ", ret.result)
    info = rows[0]['info']
    num = rows[0]['num']
    if info is None:
        if gov_code is None:
            info = 4000000
        else:
            info = 0
    if num is None:
        if gov_code is None:
            num = 390000
        else:
            num = 0
    num = num * HIDDEN_RATIO
    if 0 < num < 1:
        num = 1
    conn.disconnect()
    return info, num


# 从云端数据库取历史事件信息
def get_past_events_info(time_start, time_end):
    sqlstr = "select * from %s where event_time_start between '%s' and '%s'"%(stable_past_table, time_start, time_end)
    conn = database.ConnDB(product_server, product_db)
    conn.switch_to_arithmetic_write_mode()
    ret = conn.read(sqlstr)
    if not ret.code:
        print("取历史事件错误原因：", ret.result)
    rows = ret.data
    # print(rows)
    return rows


# 生成事件简称dict、事件id及对应的gov_id， gov_name， 事件影响力值、事件链接、事件等级(A/B/C)、事件影响范围级别（全国/省级/区域）
def assign_events_related_global_variables(node_code):
    global EVENTS_HEAD_IDS
    global EVENTS_SHORT_DICT
    global GOV_IDS
    global GOV_CODES
    global GOV_NAMES
    global NEWLY_WEIBO_VALUES
    global EVENTS_LINKS
    global THD_GRADES
    global SCOPE_GRADES
    global DF_EVENTS_BASIC

    # 染全国追踪区县的数据
    global df_trace_group

    EVENTS_HEAD_IDS = []
    EVENTS_SHORT_DICT = {}
    EVENTS_SHORT = []
    GOV_IDS = []
    GOV_CODES = []
    GOV_NAMES = []
    NEWLY_WEIBO_VALUES = []
    EVENTS_LINKS = []
    THD_GRADES = []
    SCOPE_GRADES = []

    # 事件相关微博首发时间
    EVENTS_OCCUR_TIME = []
    # 首发博主
    EVENTS_FIRST_POST = []

    EVENTS_CONTENTS = []
    # 事件主题关键词 —— 2018/11/29 加
    EVENTS_KEYTITLE = []

    DF_EVENTS_BASIC = pd.DataFrame()

    df_trace_group = pd.DataFrame()

    para_dict = warning_dict[node_code]["parameters"]

    # 按事件提取
    events_head_ids_origin = list(df_warning_trace_info['events_head_id'])
    EVENTS_HEAD_IDS = list(set(events_head_ids_origin))
    EVENTS_HEAD_IDS.sort(key=events_head_ids_origin.index)
    # events_short_dict = {}
    for events_head_id in EVENTS_HEAD_IDS:
        EVENTS_SHORT_DICT[events_head_id] = 'we' + str(EVENTS_HEAD_IDS.index(events_head_id))
        EVENTS_SHORT.append('we' + str(EVENTS_HEAD_IDS.index(events_head_id)))


    # 万一一个区县发生了两件事儿呢？
    # gov_ids = set(df_warning_trace_info['gov_id'])
    a_thd = para_dict['A_WARNING_Thd']
    b_thd = para_dict['B_WARNING_Thd']
    c_thd = para_dict['C_WARNING_Thd']
    # thd_dict = {"A级": a_thd, "B级": b_thd, "C级": c_thd}
    thds = [c_thd, b_thd, a_thd]
    thd_desc = ["C级", "B级", "A级", "追踪中"]  # 空格表示追踪中，没到三级预警门限
    thd_scopes = ["全国", "省域", "区域", "追踪中"]  # 空格表示追踪中，没到三级预警门限

    # gov_ids = []
    # gov_names = []
    # newly_weibo_values = []
    # thd_grades = []
    # scope_grades = []
    for events_head_id in EVENTS_HEAD_IDS:
        gov_id = df_warning_trace_info[df_warning_trace_info['events_head_id'] == events_head_id]['gov_id'].values[0]
        gov_code = df_2861_county[df_2861_county['gov_id'] == gov_id].index.values[0]
        gov_name = df_2861_county[df_2861_county['gov_id'] == gov_id]['full_name'].values[0]
        # earliest_pub_time = str(df_warning_details.loc[df_warning_details['events_head_id'] == events_head_id, 'pub_time'].min()).split('.')[0]
        earliest_pub_time = df_warning_details.loc[df_warning_details['events_head_id'] == events_head_id, 'pub_time'].min()

        # 首发博主 2018/11/29
        first_post = df_warning_details[(df_warning_details.events_head_id == events_head_id)&(df_warning_details.pub_time == earliest_pub_time)]["post_name"].values[0]

        earliest_pub_time = str(earliest_pub_time).split('.')[0]

        weibo_value = max(
            list(df_warning_trace_info[df_warning_trace_info['events_head_id'] == events_head_id]['weibo_value']))

        event_title = " ".join(df_warning_trace_info[df_warning_trace_info.events_head_id == events_head_id]["search_key"].values[0].split(' ')[1:])

        for thd in thds:
            if weibo_value >= thd:
                THD_GRADES.append(thd_desc[thds.index(thd)])
                SCOPE_GRADES.append(thd_scopes[thds.index(thd)])
                break
            if thds.index(thd) == 2:
                THD_GRADES.append(thd_desc[3])
                SCOPE_GRADES.append(thd_scopes[3])
        GOV_IDS.append(gov_id)
        GOV_CODES.append(gov_code)
        GOV_NAMES.append(gov_name)
        NEWLY_WEIBO_VALUES.append(weibo_value)
        EVENTS_OCCUR_TIME.append(earliest_pub_time)

        # 首发博主
        EVENTS_FIRST_POST.append(first_post)

        EVENTS_LINKS.append("%s/%s/setting_%s_value_indexes"%(node_code, common_folder_code, EVENTS_SHORT_DICT[events_head_id]))
        EVENTS_KEYTITLE.append(event_title)

        # 事件内容
        df_event = df_warning_details[df_warning_details.events_head_id == events_head_id]
        # df_event = df_event.sort_values(by=["content"], ascending=False).reset_index(drop=True)

        # 根据content的长度排序，而不是直接根据content排序 —— 2018/11/29改
        df_event["content_len"] = df_event["content"].str.len()
        df_event = df_event.sort_values(by=["content_len"],ascending=False).reset_index(drop=True)

        event_weibo_num = df_event.shape[0]
        k = 0
        for index, row in df_event.iterrows():
            k += 1
            content = row["content"].split("//@")[-1]
            if len(content) >= 10:
                # if len(content) <= 150:
                #     k = len(content)
                # else:
                #     k = 150

                # 改为给全量，展示的时候再取 —— 2018/11/29
                # EVENTS_CONTENTS.append(content[0:k]+"...")
                EVENTS_CONTENTS.append(content)
                break
            else:
                if k == event_weibo_num:
                    if len(df_event.loc[0, "content"].split("//@")[-1]) == 0:
                        EVENTS_CONTENTS.append("暂无详情。")
                    else:
                        EVENTS_CONTENTS.append(df_event.loc[0, "content"].split("//@")[-1])
                else:
                    continue

    df_data_dict = {"events_head_id":EVENTS_HEAD_IDS, "events_short":EVENTS_SHORT, "gov_id":GOV_IDS, "gov_code":GOV_CODES, "gov_name":GOV_NAMES, "newly_weibo_value":NEWLY_WEIBO_VALUES, "events_link":EVENTS_LINKS, "thd_grade":THD_GRADES, "scope_grade":SCOPE_GRADES, "events_occur_time":EVENTS_OCCUR_TIME, "events_content":EVENTS_CONTENTS, "events_keytitle":EVENTS_KEYTITLE, "events_first_post":EVENTS_FIRST_POST}
    DF_EVENTS_BASIC = pd.DataFrame(df_data_dict)

    # 全国追踪区县的数据
    df_column_in_trace = pd.DataFrame({"value":NEWLY_WEIBO_VALUES, "name":GOV_NAMES, "link":EVENTS_LINKS, "column_type":"a", "column_color":FT_SOBER_BLUE, "lala":"查看本县事件", "gov_code":GOV_CODES})

    # 统一区县合并事件，加matte_pannel
    df_trace_group = df_column_in_trace.groupby(["name"]).agg({"value":"max", "column_type":"max", "lala":"max", "gov_code":"max"}).reset_index()
    df_trace_group.set_index(["gov_code"], inplace=True)
    df_column_in_trace.set_index(["gov_code"], inplace=True)

    for index, row in df_trace_group.iterrows():

        df_gov = DF_EVENTS_BASIC[DF_EVENTS_BASIC["gov_name"] == row["name"]]
        df_gov = df_gov.sort_values(by=["newly_weibo_value"], ascending=False)

        # 判断本县最严重事件的等级，确定柱状气泡地图
        gov_bubble_color = WCOLORS[df_gov["thd_grade"].values[0][0]]
        df_trace_group.loc[index, "column_color"] = gov_bubble_color
        df_trace_group.loc[index, "column_type"] = df_gov["thd_grade"].values[0][0]

        # 加一个区县追踪事件数
        df_trace_group.loc[index, "events_num"] = df_gov.shape[0]

        # 点击气泡后，弹出框的内容
        event_type = warning_dict[node_code]["events_model"]
        content_list = ["<p><span style='color:%s;font-size:16px;font-weight:bold;'>%s - <span style='color:%s'>%s</span>事件：%d件。</span></p><br/>" % (FT_PURE_WHITE, row["name"], UN_TITLE_YELLOW, event_type, df_gov.shape[0])]

        # 框里的事件详情
        for gov_index, gov_row in df_gov.iterrows():
            event_str = "<p><%d>&nbsp<span style='color:%s'>■&nbsp&nbsp%s</span> - %s：%s<br/><a style='text-align:right;width:40%%;display:block;float:right;min-width:120px' class='link-widget' state='green_glass' href='#dataWarningCover:%s'>查看详情</a></p>"%(gov_index+1, WCOLORS[gov_row["thd_grade"][0]], (gov_row["scope_grade"]+"级影响" if len(gov_row["scope_grade"]) < 3 else gov_row["scope_grade"]), gov_row["events_occur_time"], (str(gov_row["events_content"])[0:SHOW_WEIBO_WORDS_LIMIT]+"……" if not str(gov_row["events_content"])[0:SHOW_WEIBO_WORDS_LIMIT].endswith("。") else str(gov_row["events_keytitle"]) if "暂无详情" in str(gov_row["events_content"]) else str(gov_row["events_content"])[0:SHOW_WEIBO_WORDS_LIMIT]), gov_row["events_link"])
            content_list.append(event_str)

        df_trace_group.loc[index, "matte_pannel"] = str({"cont":content_list[0]+"<br/><br/>".join(content_list[1:])})

    return


# 单独生成最后一级的详细解读
def web_leaves_datafile(provinces, monitor_time, same_provs):
    '''
    :param provinces:
    :param file_date:
    :return:
    '''
    # 遍历指定省

    time_loop_start = time.time()

    # ok_file = './OK.txt'
    # if os.path.exists(ok_file):
    #     os.remove(ok_file)

    time_dir = '-'.join('-'.join(monitor_time.split(' ')).split(':'))

    info_dict = {}
    info_dict["past_week"] = {}
    info_dict["total"] = {}
    this_time = datetime.strptime(monitor_time, '%Y-%m-%d %H:%M:%S')
    # next_time = this_time + timedelta(hours=interhour)
    info_dict["monitor_aug"] = events_update_info_num_between(this_time)
    info_dict["past_week"]["events_num"] = get_past_week_trace_events_num(this_time)
    info_dict["past_week"]["events_info"] = get_past_week_trace_info_num(this_time)
    info_dict["past_week"]["sys_info"] = get_past_week_sys_info_num(this_time)
    info, num = get_past_week_es_events_num(this_time)
    info_dict["past_week"]["trifles_info"] = info
    info_dict["past_week"]["trifles_num"] = num
    info_dict["total"]["sys_info"] = get_total_sys_info_num(this_time)
    total_info, total_trifles = get_total_es_events_num(this_time)
    info_dict["total"]["trifles_num"] = total_trifles
    info_dict["total"]["events_num"] = get_total_trace_events_num(this_time)

    for node_code in warning_dict.keys():
        node_begin_datetime = datetime.now()
        count = 0
        # 取历史数据
        if node_code == "STABLE_WARNING":
            time_start = datetime(2018,1,1)
            time_end = this_time
            rows = get_past_events_info(time_start, time_end)
            df_temp = pd.DataFrame(rows)
            info_dict["past_events"] = df_temp.to_dict()

        if 0:
            # 先不管环境
            if node_code == "ENV_POTENTIAL":
                continue
        if 0:
            # 先不管稳定
            if node_code == "STABLE_WARNING":
                continue

        global df_warning_trace_info
        global df_warning_keywords
        global df_warning_details
        global df_warning_weibo_comments

        events_type = warning_dict[node_code]["events_type"]
        parameters = warning_dict[node_code]["parameters"]
        if 0:
            df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, events_num = get_events_data(monitor_time, events_type)

        # 调试环境数据
        if 0:
            df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, events_num = get_events_data(monitor_time, events_type, record_now=False, events_limit=history_events_limit)

        # 先用读文件来调试
        if 0:
            df_warning_keywords = pd.read_csv("trace_events_keywords_%s.csv" % warning_dict[node_code]["events_type"], encoding='utf-8')
            df_warning_trace_info = pd.read_csv("trace_info_%s.csv" % warning_dict[node_code]["events_type"], encoding='utf-8')
            df_warning_details = pd.read_csv("trace_weibo_details_%s.csv" % warning_dict[node_code]["events_type"], encoding='utf-8')

            events_num = len(set(list(df_warning_trace_info['events_head_id'])))

            # print("keywords:", df_warning_keywords.info())
            # print("trace_info:", df_warning_trace_info.info())
            # print("details:", df_warning_details.info())

            # 对象转为数字
            df_warning_details[["count_comment", "count_share"]] = df_warning_details[["count_comment", "count_share"]].apply(pd.to_numeric)

        # 新版文件调试
        if 0:
            df_warning_trace_info = pd.read_csv('../trace_info.csv', encoding='utf-8')
            df_warning_keywords = pd.read_csv('../keywords.csv', encoding='utf-8')
            df_warning_details = pd.read_csv('../details.csv', encoding='utf-8')
            df_warning_weibo_comments = pd.read_csv('../comments_test2.csv', encoding='utf-8')

            events_num = len(set(list(df_warning_trace_info['events_head_id'])))

        # 2018/8/24 —— 更改了拿数据的方式，load json 或者 读csv
        if 1:
            df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, events_num, record_now = get_newliest_events_data(node_code)

            # 事件参与度
            df_warning_details["event_participation"] = parameters["K_weibo"]*1 + parameters["K_read"]*df_warning_details["count_read"] + parameters["K_comment"]*df_warning_details["count_comment"] + parameters["K_share"]*df_warning_details["count_share"]

            # 博主影响力
            df_warning_details["publiser_impact"] = df_warning_details["followers_count"]

        print("GET EVENTS DATA DONE~")

        gz_path = '../gz/%s' % time_dir
        if not os.path.exists(gz_path):
            os.makedirs(gz_path)

        # 当前有事件的情况  —— 已经把这个逻辑加到取数据那边了， 这里可以保证可取到数据，不必再判断。2018/8/26
        # if len(df_warning_trace_info) + len(df_warning_keywords) + len(df_warning_details) > 0:
            # 有新数据的时候，删除老数据
        node_code_path = client_path + node_code
        # a = os.path.exists(node_code_path)
        if os.path.exists(node_code_path):
            shutil.rmtree(node_code_path)

        # 赋值事件相关的全局变量 —— 2018/9/3
        assign_events_related_global_variables(node_code)

        for prov in provinces:
            reg = '\A' + prov
            df_county_in_province = df_2861_county.filter(regex=reg, axis=0)
            gov_codes = df_county_in_province.index
            # 遍历一个省下的所有县
            # 多进程执行 —— 2018/9/3 改多线程
            # p = Pool(10)
            threads = []
            for i in range(len(gov_codes)):
                count += 1
                if gov_codes[i] not in df_2861_county.index.values:
                    continue

                t = threading.Thread(target=generate_html_content, args=(gov_codes[i], node_code, df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, monitor_time, info_dict, record_now))
                threads.append(t)
                t.start()
                # generate_html_content(gov_codes[i], node_code, df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, monitor_time, info_dict, record_now)
                time_loop1 = time.time()
                print('\r当前进度：%.2f%%, 耗时：%.2f秒, 还剩：%.2f秒'%((count*100/2852), (time_loop1-time_loop_start), (time_loop1-time_loop_start)*(2852-count)/count), end="")

            for i in range(len(threads)):
                threads[i].join()
            # p.close()
            # p.join()


        # 压缩数据 —— 保证每个node_code有数据的话，都在外面执行操作
        tar_file_name = node_code + '_apps.tar.gz'
        server_tar_file_path = gz_path + '/' + tar_file_name
        # server_src_zip = '../'
        # gz_cmd = "cd %s; tar -zcvf %s %s" % (server_src_zip, server_tar_file_path, 'apps/' + node_code)
        gz_cmd = "tar -zcvf %s %s" % (server_tar_file_path, '../apps/' + node_code)
        os.system(gz_cmd)

        node_end_datetime = datetime.now()
        with open('./trace_info_record.txt', 'a', encoding='utf-8') as fp_out:
            fp_out.write('monitor_time: %s;   done_time：%s    events_type: %s;    events_num: %d;    time_taken: %.3fminutes\n' % (node_begin_datetime, node_end_datetime, events_type, events_num, (node_end_datetime-node_begin_datetime).seconds/60))
            print("Have recorded this time-%s-%s info already" % (monitor_time, events_type), flush=True)

    # 压缩完成后生成OK.txt
    ok_file = '../gz/OK.txt'
    with open(ok_file, 'a', encoding='utf-8') as f:
        f.write('%s\n'%time_dir)
    f.close()
    return


def copy_column_maps(node_code_path, provinces):
    # 将公用文件夹（目前为东城区110101）下的首页四张事件全国分布图copy至其他文件夹下
    column_maps = ['A', 'B', 'C', 'trace']
    src_route = node_code_path + '/' + common_folder_code + '/'
    print("\nStart Copy Column Maps~")
    copy_begin = time.time()
    for prov in provinces:
        reg = '\A' + prov
        df_county_in_province = df_2861_county.filter(regex=reg, axis=0)
        gov_codes = df_county_in_province.index
        for i in range(len(gov_codes)):
            # count += 1
            gov_code_str = str(gov_codes[i])[0:6]
            if gov_code_str == common_folder_code:
                continue
            dst_route = node_code_path + '/' + gov_code_str
            for m in column_maps:
                src_file = 'warning_column_%s_map.json' % m
                if os.path.exists(src_route + src_file):
                    shutil.copy(src_route + src_file, dst_route)
    copy_end = time.time()
    print("END COPY COLUMN MAPS, TIME TAKEN: %.2f seconds" % (copy_end - copy_begin))
    return


provinces_all = [
        '11', '12', '13', '14', '15',
        '21', '22', '23',
        '31', '32', '33', '34', '35', '36', '37',
        '41', '42', '43', '44', '45', '46',
        '50', '51', '52', '53', '54',
        '61', '62', '63', '64', '65'
    ]


# 产生前端主界面文件
def generate_datafile(provinces=provinces_all, same_provs=True):
    # '110101',
    # provinces = ['110101','140221', '511324']
    # provinces = ['11', '12', '13']
    current_day = time.strftime('%Y-%m-%d %H:%M:%S')
    web_leaves_datafile(provinces, current_day, same_provs)
    # app_env_date = time.strftime('%Y-%m-%d %H:%M:%S')
    # return app_env_date
    return


if __name__ == "__main__":
    if 1:
        generate_datafile()

    if 0:
        monitor_datetime = datetime.now()
        # for gov_code in df_2861_county.index:
        gov_code = 130128000000
        get_past_week_es_events_num(monitor_datetime, gov_code=gov_code)

    if 0:
        monitor_datetime = datetime.now()
        gov_code = 110101000000
        print(get_total_es_events_num(monitor_datetime, gov_code=gov_code))
