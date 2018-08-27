#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2018/8/4 11:55
@Author  : Liu Yusheng
@File    : xmd_events_warning_frontend_display.py
@Description:
"""

# 系统模块
import os
import json
import shutil
import paramiko

# 第三方模块
import pandas as pd
import numpy as np
from copy import deepcopy
from apscheduler.schedulers.background import BlockingScheduler
from multiprocessing import Pool
import time
from datetime import datetime, timedelta

# 自定义模块
from db_interface import database
from xmd_events_warning_data_generation import get_events_data, trace_db_obj, trace_info_table, trace_db_info, trace_seed_table

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

data_path = './data/'
sesi_words_path = data_path + 'sensitive_word_userdict.txt'
gaode_geo_path = data_path + 'df_2861_gaode_geo.csv'

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

WCOLORS = {'A':LN_GOLDEN, "B":LN_YELLOW, "C":LN_RED}

WSTATUSES = {'warning_a': {'name': 'A级', 'area': '区域', 'desc':'影响较小，易被忽略。但隐患正在积累，蓄势待发。', 'advise':"<span style='color: orange'>立即回应，进行处理</span>。此时处理，提前消除隐患，成本最低，体现对互联网的掌控能力。"},
             'warning_b': {'name': 'B级', 'area': '省域', 'desc':'影响中等，引发政府公信力危机，分管领导有可能被问责。', 'advise':"<span style='color: orange'>积极回应，及时处理</span>。若及时处理，可转危为安，体现应急响应能力。"},
             'warning_c': {'name': 'C级', 'area': '全国', 'desc':'影响恶劣，响应迟钝或者处理不当，地方一把手的执政能力会受到质疑。', 'advise':"<span style='color: orange'>立即处理</span>。若处理得当，可亡羊补牢，降低掉帽子的风险。"}}

max_key_words_num = 10
history_events_limit = 10
interhour = 1
common_folder_code = '110101'

df_warning_trace_info = pd.DataFrame()
df_warning_keywords = pd.DataFrame()
df_warning_details = pd.DataFrame()
df_warning_weibo_comments = pd.DataFrame()

# 'we0_value_index_trend'
# 'we0_sens_word_trend'
# 'we0_publisher_trend'

# 服务器信息
source_dict = {"ip":"120.78.222.247", "port":22, "uname":"root", "password":"zhikuspider"}

# 云端数据库
product_server = database.get_database_server_by_nick(database.SERVER_PRODUCT)
product_db = 'product'
stats_table = 'xmd_weibo_stats'
stable_past_table = 'xmd_stable_past_events'



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
def get_column_map_dict( df_id, title=None, subtitle=None, tips=None, column_names=None):
    map_dict = {}
    if title is not None:
        map_dict["title"] = title
    if subtitle is not None:
        map_dict["subtitle"] = subtitle
    map_dict["type"] = "columnMap"
    map_dict["geo_data"] = "2861_county_echars_geo"
    details = {}
    datas = []
    columns = list(df_id)
    for index, row in df_id.iterrows():
        data_dict = {}
        for col in columns:
            # print(col)
            data_dict[col] = row[col]
        datas.append(data_dict)
    details["datas"] = datas
    if tips is None:
        details["tips"] = {"value":"指数", "rank":"排名"}
    else:
        details["tips"] = tips
    column = {}
    column["column_toggle"] = [True]
    if column_names:
        column["column_names"] = column_names
    else:
        column["column_names"] = ["当前隐患突出的区县"]
    column["column_types"] = list(set(df_id["column_type"]))
    column['column_colors'] = list(set(df_id["column_color"]))
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
def get_lines_graph_dict(title, subtitle, df_data, df_info, y_name, fontshow=False, nodeshow=False, linewith=None, xfont=None, ZeroMin=True, signlist=None, signname=None, max_thd=None):
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
            if isinstance(i, str):
                all_data_list.append(eval(i)['y'])
                data_list.append(eval(i))
            else:
                all_data_list.append(i)
                data_list.append(i)
        line_dict["data"] = data_list
        line_list.append(line_dict)

    data_detail["line_list"] = line_list
    if ZeroMin:
        if min(all_data_list) > 0:
            min_thd = 0
        else:
            min_thd = min(all_data_list)-10
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
def get_warning_map_data(gov_code, node_code, newly_weibo_values, gov_names, events_links, parameters, version_date, record_now):
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
    gov_code_str = str(gov_code)[0:6]
    df_column_in_trace = pd.DataFrame({"value": newly_weibo_values, "name": gov_names, "link": events_links, "column_type": "a", "column_color":FT_SOBER_BLUE})
    df_column_in_trace["rank"] = df_column_in_trace["value"].rank(ascending=False)
    if record_now:
        title = "2861区县%s隐患-追踪中" % event_type
        subtitle = "更新时间：%s" % str(version_date).split('.')[0]
        column_names = ['正在追踪的事件']
    else:
        title = "2861区县%s隐患-历史追踪事件" % event_type
        subtitle = "最后一次追踪时间：%s" % str(version_date).split('.')[0]
        column_names = ['历史追踪的事件']
    column_map_trace_dict = get_column_map_dict(df_column_in_trace, title, subtitle, column_names=column_names)
    # column_map_trace_dict = get_column_map_dict(df_column_in_trace)
    warning_map_data_list.append(column_map_trace_dict)
    warning_map_data_name_list.append('warning_column_trace_map')

    # # 分级画事件地图 —— 各级不能有交叉 2018/8/20
    # for grade in thd_dict.keys():
    #     df_column = deepcopy(df_column_in_trace[df_column_in_trace["value"] >= thd_dict[grade]])
    #     df_column["column_color"] = color_dict[grade]
    #     df_column["rank"] = df_column["value"].rank(ascending=False)
    #     if record_now:
    #         title = "2861区县%s隐患-%s预警中"%(event_type, grade)
    #         subtitle = "更新时间：%s" % str(version_date).split('.')[0]
    #     else:
    #         title = "2861区县%s隐患-历史%s事件" % (event_type, grade)
    #         subtitle = "最后一次追踪时间：%s" % str(version_date).split('.')[0]
    #     column_map_dict = get_column_map_dict(title, subtitle, df_column)
    #     warning_map_data_list.append(column_map_dict)
    #     warning_map_data_name_list.append('warning_column_%s_map'%grade[0])

    # 分级画事件地图 —— 各级不能有交叉 2018/8/20
    # if gov_code_str == common_folder_code: —— 试一下每个区县单独生成map
    for i in range(len(grades)):
        if i == len(grades)-1:
            df_column = deepcopy(df_column_in_trace[df_column_in_trace["value"] >= thd_dict[grades[i]]])
        else:
            df_column = deepcopy(df_column_in_trace[(df_column_in_trace["value"] >= thd_dict[grades[i]])&(df_column_in_trace["value"]<thd_dict[grades[i+1]])])
        df_column["column_color"] = color_dict[grades[i]]
        df_column["rank"] = df_column["value"].rank(ascending=False)
        if record_now:
            title = "2861区县%s隐患-%s预警中" % (event_type, grades[i])
            subtitle = "更新时间：%s" % str(version_date).split('.')[0]
            column_names = [events_grades[i]+"事件"]
        else:
            title = "2861区县%s隐患-历史%s事件" % (event_type, grades[i])
            subtitle = "最后一次追踪时间：%s" % str(version_date).split('.')[0]
            column_names = ["历史"+events_grades[i]+"事件"]
        column_map_dict = get_column_map_dict(df_column, title, subtitle, column_names=column_names)
        # column_map_dict = get_column_map_dict(df_column)
        warning_map_data_list.append(column_map_dict)
        warning_map_data_name_list.append('warning_column_%s_map' % grades[i][0])

    return warning_map_data_list, warning_map_data_name_list


# 各项指数走势
def get_warning_indexes_lines_data(df_warning_trace_info, events_head_ids, events_short_dict, gov_names, parameters, version_date, record_now):
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
    index_names = ['事件影响力指数']
    for events_head_id in events_head_ids:
        df_event = df_warning_trace_info.loc[df_warning_trace_info['events_head_id'] == events_head_id, :].reset_index(drop=True)
        df_event.loc[:, 'do_time'] = df_event.loc[:, 'do_time'].apply(lambda x: str(x).split('.')[0])
        df_event = df_event.sort_values(by=['do_time']).reset_index(drop=True)  # , inplace=True
        # 越改越只针对指数了
        for index_col in index_cols:
            if index_col != 'weibo_value':
                continue
            if record_now:
                title = "%s: %s变化趋势" % (
                gov_names[events_head_ids.index(events_head_id)], index_names[index_cols.index(index_col)])
                subtitle = "更新时间：%s" % str(version_date).split('.')[0]
            else:
                title = "%s: %s历史走势" % (
                    gov_names[events_head_ids.index(events_head_id)], index_names[index_cols.index(index_col)])
                # last_trace_time = df_event['do_time'].max()
                subtitle = "最后一次追踪时间：%s" % df_event['do_time'].max()
            # tips = {"right": "%s" % index_names[index_cols.index(index_col)]}

            x_name_list = []
            event_index_list = []
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

            if index_col == 'weibo_value':
                # df_id['A'] = [a_thd]*len(list(df_event['do_time']))
                # df_id['B'] = [b_thd]*len(list(df_event['do_time']))
                # df_id['C'] = [c_thd]*len(list(df_event['do_time']))
                df_id['A'] = [a_thd] * len(x_name_list)
                df_id['B'] = [b_thd] * len(x_name_list)
                df_id['C'] = [c_thd] * len(x_name_list)
            df_info = pd.DataFrame(index=[i for i in list(df_id) if i != 'x_name'], columns=["name", "color"])
            df_info.loc["event", "name"] = index_names[index_cols.index(index_col)]
            df_info.loc["event", "color"] = BT_TIFFANY_BLUE
            if index_col == "weibo_value":
                df_info.loc["event", "color"] = BT_TIFFANY_BLUE
                df_info.loc["event", "type"] = "line"
                df_info.loc["event", "dashStyle"] = "solid"
                for index in df_info.index.values:
                    if index != "event":
                        df_info.loc[index, "name"] = index+"级预警"
                        df_info.loc[index, "color"] = WCOLORS[index]
                        df_info.loc[index, "type"] = "spline"
                        df_info.loc[index, "dashStyle"] = "dash"
            y_name = index_names[index_cols.index(index_col)]
            linewidth = 3
            xfont = {"xfont":10, "fontWeight":"normal", "color":FT_PURE_WHITE}
            signname = ['A级', 'B级', 'C级']
            signlist = [df_event[index_col][0], a_thd, b_thd, c_thd]
            value_max = df_event[index_col].max()
            max_thd = max([value_max+10, c_thd+a_thd])
            index_line_dict = get_lines_graph_dict(title, subtitle, df_id, df_info, y_name,linewith=linewidth, xfont=xfont, signname=signname, signlist=signlist, max_thd=max_thd)
            # index_line_dict = get_mixed_line_dict(title, subtitle, df_id, tips=tips)
            warning_line_data_list.append(index_line_dict)
            warning_line_data_name_list.append('%s_%s_index_trend' % (events_short_dict[events_head_id], index_col.split('_')[-1]))
    return warning_line_data_list, warning_line_data_name_list


# 关键词占比
def get_warning_words_lines_data(df_warning_keywords, events_head_ids, events_short_dict, gov_names, version_date):
    warning_line_data_list = []
    warning_line_data_name_list = []
    word_types = ['sensitive', 'department', 'guanzhi']
    word_names = ['相关敏感词', '涉及部门', '涉及官员']
    for events_head_id in events_head_ids:
        gov_name = gov_names[events_head_ids.index(events_head_id)]
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
            warning_line_data_name_list.append('%s_%s_word_trend' % (events_short_dict[events_head_id], word_type[0:4]))
    return warning_line_data_list, warning_line_data_name_list


# 博主影响力&事件参与度
def get_warning_bloggers_lines_data(df_warning_details, df_warning_trace_info, events_head_ids, events_short_dict,gov_names, para_dict, version_date, record_now):

    warning_line_data_list = []
    warning_line_data_name_list = []
    w0 = para_dict["K_weibo"]
    wr = para_dict["K_read"]
    wc = para_dict["K_comment"]
    ws = para_dict["K_share"]
    # 事件参与度
    df_details_temp = deepcopy(df_warning_details)

    # print('yes', flush=True)
    # print("count_comment: ", type(df_details_temp["count_comment"]), flush=True)
    # print("count_read: ", type(df_details_temp["count_read"]), flush=True)
    # print("count_share: ", type(df_details_temp["count_share"]), flush=True)
    # print(df_details_temp["count_comment"])
    # print(df_details_temp["count_read"])
    # print(df_details_temp["count_share"])
    # print(df_warning_details.info())
    # print(df_details_temp.info())
    #
    # df_details_temp = df_details_temp.convert_objects(convert_numeric=True)
    # print(df_details_temp.info())

    df_details_temp["event_participation"] = w0 * 1 + wr * df_details_temp["count_read"] + wc * \
                                                df_details_temp["count_comment"] + ws * df_details_temp["count_share"]

    # print('yes1', flush=True)


    # 博主影响力
    df_details_temp["publisher_impact"] = df_details_temp["followers_count"]
    for events_head_id in events_head_ids:
        gov_name = gov_names[events_head_ids.index(events_head_id)]
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
        warning_line_data_name_list.append('%s_publisher_trend' % events_short_dict[events_head_id])
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
    events_head_ids_origin = list(df_warning_trace_info['events_head_id'])
    events_head_ids = list(set(events_head_ids_origin))
    events_head_ids.sort(key=events_head_ids_origin.index)
    events_short_dict = {}
    for events_head_id in events_head_ids:
        events_short_dict[events_head_id] = 'we' + str(events_head_ids.index(events_head_id))
    # 万一一个区县发生了两件事儿呢？
    # gov_ids = set(df_warning_trace_info['gov_id'])
    gov_ids = []
    gov_names = []
    newly_weibo_values = []
    events_links = []
    for events_head_id in events_head_ids:
        gov_id = df_warning_trace_info[df_warning_trace_info['events_head_id'] == events_head_id]['gov_id'].values[0]
        gov_name = df_2861_county[df_2861_county['gov_id'] == gov_id]['full_name'].values[0]
        weibo_value = max(
            list(df_warning_trace_info[df_warning_trace_info['events_head_id'] == events_head_id]['weibo_value']))
        gov_ids.append(gov_id)
        gov_names.append(gov_name)
        newly_weibo_values.append(weibo_value)

        # 给柱状图的牌子加上link
        # gov_code = df_2861_county[df_2861_county.gov_id == gov_id].index.values[0]
        gov_code_str = str(gov_code)[0:6]
        # events_links.append("%s/%s/setting_%s_value_indexes"%(node_code, gov_code_str, events_short_dict[events_head_id]))
        events_links.append("%s/%s/setting_%s_value_indexes" % (node_code, common_folder_code, events_short_dict[events_head_id]))

    nearest_trace_time = max(df_warning_trace_info['do_time'].tolist())
    # 预警柱子分布图
    warning_map_data, warning_map_data_names = get_warning_map_data(gov_code, node_code, newly_weibo_values, gov_names, events_links, parameters, nearest_trace_time, record_now)
    warning_map_data_list.extend(warning_map_data)
    warning_map_data_name_list.extend(warning_map_data_names)

    if str(gov_code)[0:6] == common_folder_code:
        # 事件走势图 —— 影响力指数、(速度、加速度（、总微博数、评论数、转发数）) —— 2018/8/2 只画影响力指数的图，速度/加速度不要了
        warning_indexes_data, warning_indexes_names_list = get_warning_indexes_lines_data(df_warning_trace_info,
                                                                                          events_head_ids, events_short_dict, gov_names,
                                                                                          parameters,
                                                                                          monitor_time, record_now)
        warning_line_data_list.extend(warning_indexes_data)
        warning_line_data_name_list.extend(warning_indexes_names_list)

        # # 事件关键词 —— 官职、敏感词、部门，最多20个  —— 2018/8/2 不需要关键词的图了，转换成list_desc的文本形式
        # warning_words_data, warning_words_names_list = get_warning_words_lines_data(df_warning_keywords, events_head_ids,events_short_dict,
        #                                                                             gov_names, nearest_trace_time)
        # warning_line_data_list.extend(warning_words_data)
        # warning_line_data_name_list.extend(warning_words_names_list)

        # 博主影响力&事件参与度 —— 参与度最高的前二十个博主
        para_dict = warning_dict[node_code]["parameters"]
        warning_bloggers_data, warning_bloggers_names_list = get_warning_bloggers_lines_data(df_warning_details,
                                                                                             df_warning_trace_info,
                                                                                             events_head_ids,events_short_dict, gov_names,
                                                                                             para_dict, monitor_time, record_now)
        warning_line_data_list.extend(warning_bloggers_data)
        warning_line_data_name_list.extend(warning_bloggers_names_list)
    # print(warning_map_data_name_list)
    # print(warning_map_data_name_list)
    # print(warning_line_data_name_list)

    return warning_map_data_list, warning_map_data_name_list, warning_line_data_list, warning_line_data_name_list


# 过往事件list描述
def get_past_events_desc_per_gov(gov_code, df_past):
    gov_code_str = str(gov_code)[0:6]
    gov_name = df_2861_county.loc[gov_code, 'full_name']
    df_gov_past = deepcopy(df_past[df_past['gov_code'] == int(gov_code_str)])
    df_gov_past = df_gov_past.reset_index(drop=True)
    if df_gov_past.iloc[:,0].size == 0:
        desc = "<p><span style='color: #ff1133;font-weight: bold'>暂未监测到跟本地有关的热门话题！</span></p>"
    else:
        desc = ""
        for i in df_gov_past.index.values:
            desc += "<p><span style='color: #ffcc00;'>%d，[主题]%s</span>" \
                    "<br/>时间：%s" \
                    "<br/><span style='color: #ffcc00;'>涉及职务：</span>%s" \
                    "<br/><span style='color: #ffcc00;'>涉及部门：</span>%s" \
                    "<br/><span style='color: #ffcc00;'>涉及关键词：</span>%s</p>"\
                    %(i+1,df_gov_past.loc[i, 'event_title'],df_gov_past.loc[i, 'event_time_start'],df_gov_past.loc[i, 'gov_post'],df_gov_past.loc[i, 'department'],df_gov_past.loc[i, 'sensitive_word'])


    datas = ""
    title = "<h1>%s</h1>" % (gov_name)
    datas += title
    datas += desc
    return datas

# 预警前端
def get_warning_setting_desc_data(gov_code, node_code, df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, monitor_time, info_dict, record_now=True):
    """

    :type node_code: object
    """
    gov_code_str = str(gov_code)[0:6]
    warning_type = warning_dict[node_code]["events_model"]
    para_dict = warning_dict[node_code]["parameters"]

    # 柱子地图
    warning_map_data_list = []
    warning_map_data_name_list = []

    # 多setting
    setting_list = []
    setting_name_list = []

    # 右侧描述
    list_desc = {}

    # 按事件提取
    events_head_ids_origin = list(df_warning_trace_info['events_head_id'])
    events_head_ids = list(set(events_head_ids_origin))
    events_head_ids.sort(key=events_head_ids_origin.index)
    events_short_dict = {}
    for events_head_id in events_head_ids:
        events_short_dict[events_head_id] = 'we'+str(events_head_ids.index(events_head_id))

    # 万一一个区县发生了两件事儿呢？
    # gov_ids = set(df_warning_trace_info['gov_id'])
    a_thd = para_dict['A_WARNING_Thd']
    b_thd = para_dict['B_WARNING_Thd']
    c_thd = para_dict['C_WARNING_Thd']
    # thd_dict = {"A级": a_thd, "B级": b_thd, "C级": c_thd}
    thds = [c_thd, b_thd, a_thd]
    thd_desc = ["C级", "B级", "A级", " "]      # 空格表示追踪中，没到三级预警门限
    thd_scopes = ["全国", "省域", "区域", " "]  # 空格表示追踪中，没到三级预警门限

    gov_ids = []
    gov_names = []
    newly_weibo_values = []
    thd_grades = []
    scope_grades = []
    for events_head_id in events_head_ids:
        gov_id = df_warning_trace_info[df_warning_trace_info['events_head_id']==events_head_id]['gov_id'].values[0]
        gov_name = df_2861_county[df_2861_county['gov_id']==gov_id]['full_name'].values[0]
        weibo_value = max(list(df_warning_trace_info[df_warning_trace_info['events_head_id']==events_head_id]['weibo_value']))
        for thd in thds:
            if weibo_value >= thd:
                thd_grades.append(thd_desc[thds.index(thd)])
                scope_grades.append(thd_desc[thds.index(thd)])
                break
            if thds.index(thd) == 2:
                thd_grades.append(thd_desc[3])
                scope_grades.append(thd_scopes[3])
        gov_ids.append(gov_id)
        gov_names.append(gov_name)
        newly_weibo_values.append(weibo_value)

    # ------------------------------------------zhong yu ba tu hua wan le-----------------------------------------
    gov_name_current = df_2861_county.loc[gov_code, 'full_name']
    nearest_trace_time = str(max(df_warning_trace_info['do_time'].tolist())).split('.')[0]
    color_dict = {"A级": LN_GOLDEN, "B级": LN_YELLOW, "C级": LN_RED}

    # 生成一张本县的预警情况图  —— 2018/8/20 本地的图不用了

    # current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    monitor_time_short = datetime.strptime(monitor_time, '%Y-%m-%d %H:%M:%S').strftime('%m-%d %H:%M')
    if 0:
        title = "本区县%s预警状况"%warning_type
        subtitle = "更新时间：%s"%monitor_time
        if (gov_name_current in gov_names) and record_now:
            # ??? 有问题，一个区县多件事儿怎么办？
            gov_index = gov_names.index(gov_name_current)
            gov_value = newly_weibo_values[gov_index]
            thd_grade = thd_grades[gov_index]
            scope_grade = scope_grades[gov_index]
            if thd_grade == " ":
                thd_grade = 0
                scope_grade = 0
            elif thd_grade == "C级":
                thd_grade = scope_grade = 3
            elif thd_grade == "B级":
                thd_grade = scope_grade = 2
            elif thd_grade == "A级":
                thd_grade = scope_grade = 1
            df_id= pd.DataFrame({"value":[thd_grade], "name":gov_name_current, "rank":scope_grade})
            if thd_grades[gov_index] == " ":
                df_id['column_color'] = FT_SOBER_BLUE
            else:
                df_id['column_color'] = color_dict[thd_grades[gov_index]]

        else:
            df_id = pd.DataFrame({"value":[0], "name":gov_name_current,"rank":0})
            df_id['column_color'] = BG_GREEN

        df_id['column_type'] = "a"
        tips = {"value":"预警等级", "rank":"事件影响范围"}
        column_map_local_dict = get_column_map_dict(title, subtitle, df_id, tips=tips, column_names=[gov_name_current])  # , column_names=False
        warning_map_data_list.append(column_map_local_dict)
        warning_map_data_name_list.append('warning_column_local_map')

        # settings
        # column_map的setting， 2018/8/2 改为多个setting —— C/B/A/trace/base
        # 进入时给本县当前的预警情况地图
        setting = {}
        setting["title"] = gov_name_current
        data_dict = {}
        data_dict["id"] = "local"
        data_dict["node_code"] = node_code
        data_dict["name"] = "本区县%s预警状况" % warning_type
        data_dict["data"] = gov_code_str + '/' + 'warning_column_local_map'
        setting["datas"] = [data_dict]
        setting_list.append(setting)
        setting_name_list.append('setting')

    # 追踪事件 —— 每个区县必须有一个setting，一个list
    # if gov_code_str == "110101":
    setting0 = {}
    data_dict = {}
    data_dict["id"] = "trace"
    data_dict["node_code"] = node_code
    data_dict["data"] = gov_code_str+'/'+'warning_column_trace_map'
    # 这个数据也可以只生成一份 —— 不可以！！！前端的设计逻辑会根据数据来拼list路径，所以list不同的时候要单独生成数据文件
    # data_dict["data"] = common_folder_code+'/'+'warning_column_trace_map'
    setting0["datas"] = [data_dict]
    if record_now:
        data_dict["name"] = "2861区县%s隐患-追踪中" % warning_type
        setting0["title"] = "事件追踪中"
    else:
        data_dict["name"] = "2861区县%s隐患-历史追踪事件" % warning_type
        setting0["title"] = "历史追踪事件"
    setting_list.append(setting0)
    # setting_name_list.append('setting_trace')
    # 所有追踪的事件为首页
    setting_name_list.append('setting')

    # 神奇的事情发生了。。。首页可以只切setting但list取原有的 —— 2018/8/22 —— 是bug，天博已改 2018/8/23，所以只要有定制化，文件必须从本地取
    # if gov_code_str == "110101":
    # C/B/A级预警分布
    for key in WSTATUSES.keys():
        grade = WSTATUSES[key]['name']
        setting1 = {}
        data_dict = {}
        data_dict["id"] = grade[0]
        data_dict["node_code"] = node_code
        data_dict["data"] = gov_code_str+'/'+'warning_column_%s_map'%grade[0]
        # data_dict["data"] = '110101' + '/' + 'warning_column_%s_map'%grade[0]
        if record_now:
            data_dict["name"] = "2861区县%s隐患-%s预警中" % (warning_type, grade)
            setting1["title"] = grade + "预警"
        else:
            data_dict["name"] = "2861区县%s隐患-历史%s事件" % (warning_type, grade)
            setting1["title"] = grade + "历史事件"
        setting1["datas"] = [data_dict]

        setting_list.append(setting1)
        setting_name_list.append('setting_%s'%grade[0])

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
        for events_head_id in events_head_ids:
            # data_dict_list = []
            for i in index_cols:
                setting2 = {}
                setting2["title"] = '%s: "%s"' %(index_names[index_cols.index(i)], df_warning_trace_info.loc[
                    df_warning_trace_info.events_head_id == events_head_id, "search_key"].values[0])
                data_dict = {}
                data_dict["id"] = events_short_dict[events_head_id]+i.split('_')[-1]
                data_dict["node_code"] = node_code
                data_dict["name"] = index_names[index_cols.index(i)]
                data_dict["data"] = common_folder_code+'/'+events_short_dict[events_head_id]+'_'+i.split('_')[-1]+'_index_trend'
                # data_dict_list.append(data_dict)
                setting2["datas"] = [data_dict]
                setting_list.append(setting2)
                setting_name_list.append('setting_%s_%s_indexes'%(events_short_dict[events_head_id], i.split('_')[-1]))

        # 事件参与度&博主影响力的setting
        for events_head_id in events_head_ids:
            event_desc = df_warning_trace_info.loc[df_warning_trace_info.events_head_id == events_head_id, "search_key"].values[0]
            setting4 = {}
            setting4["title"] = '事件："%s"-各发布者的影响力及参与度'%event_desc
            data_dict = {}
            data_dict["id"] = events_short_dict[events_head_id]+'publisher'
            data_dict["node_code"] = node_code
            data_dict["name"] = '各发布者的影响力及参与度'
            data_dict["data"] = common_folder_code + '/' + events_short_dict[events_head_id]+'_publisher_trend'
            setting4["datas"] = [data_dict]
            setting_list.append(setting4)
            setting_name_list.append('setting_%s_publishers'%events_short_dict[events_head_id])

# -----------------------------------------------setting zhong yu jie shu le--------------------------------------------
    # list_desc
    # 'setting_we0_value_indexes'
    # 'setting_we0_sens_words'  —— 2018/8/2 不需要关键词的setting了
    # 'setting_we0_publishers'

    # 共用的一块儿预警情况 —— columnMap上方
    common_data = {}
    common_data["title"] = "2861%s隐患监测系统"%warning_type
    common_data["load_max"] = '2861'
    common_data["cur_area"] = gov_name_current
    common_data["cur_date_time"] = monitor_time

    # 目前为两小时更新一次
    this_time = datetime.strptime(monitor_time, '%Y-%m-%d %H:%M:%S')
    next_time = this_time + timedelta(hours=interhour)
    # common_data["remaining_time"] = '120'  # 先写死，两个小时 —— 后期动态和李科讨论 2018/08/20  ——改为传下次更新时间的时间戳给天博——2018/8/21
    # 下一次更新时间的时间戳
    common_data["remaining_time"] = time.mktime(next_time.timetuple())

    # 计算两个更新周期之间的新增事件信息条数
    # common_data["info_num"] = str(events_update_info_num_between(this_time))
    common_data["info_num"] = "%d"%info_dict["monitor_aug"]
    common_data["check_state"] = warning_type
    # if gov_name_current in gov_names:
    common_data["event"] = str(gov_names.count(gov_name_current))
    common_data["event_total"] = str(len(events_head_ids))
    # if "past_events" in info_dict.keys():
    if node_code == "STABLE_WARNING":
        common_data["history_link"] = node_code + '/' + gov_code_str + '/past'
    list_desc["column_title"] = common_data

    # if "past_events" in info_dict.keys():
    if node_code == "STABLE_WARNING":
        past_dict = info_dict["past_events"]
        df_past = pd.DataFrame(past_dict)
        desc_past = get_past_events_desc_per_gov(gov_code, df_past)
        back_info = {"cols": [{"text": "返回", "link": "#list:%s/%s/trace"%(node_code, gov_code_str)}], "bg_color": BT_TIFFANY_BLUE,"color": FT_PURE_WHITE, "strong": True}
        past_info = {"cols": [{"text": "%s"%desc_past}]}
        list_desc["past"] = {"title":"", "sub_title":"", "width":"35%", "datas":[back_info,past_info]}

    # columnMap对应的list_desc —— 去掉local对应的desc字段
    # list_desc["local"] = {"title": "", "sub_title": "", "width":"35%"}
    list_desc_data1 = []
    # 第一行：标题
    gov_title = "<h3><section style='text-align:center'>%s</section></h3>" % gov_name_current
    gov_info = {"cols": [{"text": gov_title}]}
    list_desc_data1.append(gov_info)

    # 第二行：监测详情 —— 本县
    if (gov_name_current not in gov_names) or (not record_now):
        detail = "<section style='text-align:center'>%s: 暂未监测到%s隐患</section>"%(monitor_time_short, warning_type)
        detail_text = {"text":"%s"%detail}
        monitor_line = {"cols": [{"text": "<span style='color: orange'>本区县监测详情：</span>"}, detail_text], "strong": True}
    else:
        # detail = "可能存在%s隐患，需引起注意！（详情见下列表）"%warning_type
        # ??? 一个区县多件事儿
        if thd_grades[gov_names.index(gov_name_current)] == " ":
            warn_grade = {
                "text": "<span style='color: %s'>" % (FT_SOBER_BLUE) + "事件追踪中" + "</span>"}
        else:
            warn_grade = {"text":"<span style='color: %s'>"%(color_dict[thd_grades[gov_names.index(gov_name_current)]])+thd_grades[gov_names.index(gov_name_current)]+"预警"+"</span>"}
        # detail = "存在%s隐患，查看详情"%warning_type
        detail = "查看详情"
        # list.index(i) —— 返回i元素在list中的第一个索引
        detail_text = {"text":"%s"%detail, "link":"#data:%s/%s/setting_%s_value_indexes"%(node_code, common_folder_code, events_short_dict[events_head_ids[gov_names.index(gov_name_current)]])}
        monitor_line = {"cols": [{"text": "<span style='color: orange'>本区县监测详情：</span>"}, warn_grade, detail_text], "strong":True}
    list_desc_data1.append(monitor_line)

    # 隐患区县 —— 设计成 C/B/A/tracing/base 阶梯状按钮形式
    if record_now:
        detail = "<section style='text-align:center'>当前%s隐患如下：</section>"%(warning_type)
    else:
        detail = "<section style='text-align:center'>当前未监测到%s隐患；历史事件如下：</section>" % ( warning_type)
    warning_title = {"cols": [{"text": "全国%s隐患监测详情："%warning_type}, {"text":"%s"%detail}],
                     "color": "orange", "strong":True}
    list_desc_data1.append(warning_title)
    # for key in WSTATUSES.keys():
    #     grade = WSTATUSES[key]['name']
    C_warning_button = {"cols":[{"text":"全国性事件：%d件"%thd_grades.count("C级"), "link":"#data:%s/%s/setting_C"%(node_code, gov_code_str)}, {"text":""}, {"text":""}]}
    B_warning_button = {"cols":[{"text":"省域级事件：%d件"%thd_grades.count("B级"), "link":"#data:%s/%s/setting_B"%(node_code, gov_code_str)}, {"text":""}]}
    A_warning_button = {"cols":[{"text":"区域内事件：%d件"%thd_grades.count("A级"), "link":"#data:%s/%s/setting_A"%(node_code, gov_code_str)}]}
    if record_now:
        TRACE_warning_button = {"cols":[{"text":"正在追踪的事件：%d件"%len(events_head_ids), "link":"#data:%s/%s/setting"%(node_code, gov_code_str)}]}
    else:
        TRACE_warning_button = {"cols": [{"text": "历史追踪的事件：%d件"%len(events_head_ids), "link": "#data:%s/%s/setting"%(node_code, gov_code_str)}]}

    # 打开运行中的基础预警数据， 不设按钮 —— 2018/8/22
    # BASE_warning_button = {"cols": [{"text":"运行中的基础预警数据", "link":"#list:%s/%s/base"%(node_code, gov_code_str)}]}
    BASE_warning_data = {"cols": [{"text":"<section style='text-align:center'>运行中的基础预警数据</section>"}], "strong":True, "color":FT_PURE_WHITE}

    null_line = {"cols": [{"text": ""}]}
    country_line2 = {"cols": [{"text": "正在被跟踪的区县：<span style='color:orange'>2861</span>个"}]}
    country_line3 = {"cols": [{"text": "正在跟踪的事件：<span style='color:orange'>%d</span>件" % len(events_head_ids)}]}
    country_line3_5 = {"cols": [{"text": "追踪采样频率：约每<span style='color:orange'>10~20分钟</span>采一次"}]}
    country_line4 = {"cols": [{"text": "跟踪爬虫：<span style='color:orange'>7*24</span>小时不间断运行"}]}

    # 过去一周的新增信息 —— 统计在本地库，所以需要每周/月在本地统计，通过文件定期更新至服务器的方式，让前端每次读文件更新。 2018/8/22
    country_line5 = {"cols": [{"text": "过去一周："}]}
    country_line6 = {"cols": [{"text": "追踪事件数：<span style='color:orange'>%d</span>件" % info_dict["past_week"]["events_num"]}]}
    country_line7 = {"cols": [{"text": "新增事件相关信息：<span style='color:orange'>%d</span>万条" % info_dict["past_week"]["events_info"]}]}
    country_line8 = {"cols": [{"text": "系统新增互联网信息：<span style='color:orange'>%d</span>万条" % info_dict["past_week"]["sys_info"]}]}
    # country_line9 = {"cols": [{"text": "(包括新增微博等信息<span style='color:orange'>%d</span>条，新增评论、转发<span style='color:orange'>%d</span>条等）" % (100, 200)}]}
    list_desc_data1.extend([C_warning_button, B_warning_button, A_warning_button, TRACE_warning_button, BASE_warning_data])
    list_desc_data1.extend([null_line, country_line2, country_line3, country_line3_5, country_line4, country_line5, country_line6, country_line7, country_line8])


    # list_desc["local"]["datas"] = list_desc_data1

    # C/B/A/trace/base
    # base的desc —— base 一直出现，而不是按钮
    # list_desc["base"] = {"title": "", "sub_title": "", "width": "35%"}
    # list_desc_data_base = deepcopy(list_desc_data1)
    # list_desc_data_base.pop(-1)
    #
    # # base_warning_desc = {"cols": [{"text":"<section style='text-align:center'>运行中的基础预警数据</section>"}], "strong":True, "color":FT_PURE_WHITE}
    # # 全国概况
    # # country_line1 = {"cols": [{"text": "全国概况"}], "color": "orange", "strong": True}
    #
    # list_desc_data_base.extend([base_warning_desc, null_line, country_line2, country_line3, country_line4])
    # list_desc_data_base.extend([country_line5, country_line6, country_line7, country_line8, country_line9])
    #
    # list_desc["base"]["datas"] = list_desc_data_base

    # trace的desc
    list_desc["trace"] = {"title": "", "sub_title": "", "width": "35%"}
    list_desc_data_trace = deepcopy(list_desc_data1)
    if record_now:
        list_desc_data_trace[-2-9] = {"cols": [{"text": "<section style='text-align:center'>正在追踪的事件：%d件</section>"%len(events_head_ids)}], "strong": True, "color": FT_SOBER_BLUE}
    else:
        list_desc_data_trace[-2-9] = {"cols": [{"text": "<section style='text-align:center'>历史追踪事件：%d件</section>"%len(events_head_ids)}],"strong": True, "color": FT_SOBER_BLUE}

    # 右边的一排按钮先去掉 —— 2018/8/22
    # df_events = pd.DataFrame({"value": newly_weibo_values, "name": gov_names, "events_id":events_head_ids,  "column_type": "a"})
    # df_events["rank"] = df_events["value"].rank(ascending=False)
    # df_events = df_events.sort_values(by="rank", ascending=True).reset_index(drop=True)
    # list_desc_data_trace.append(null_line)
    # for index, row in df_events.iterrows():
    #     event_info = {"cols":[{"text": "第%d名"%row["rank"]}, {"text": row["name"], "link": "#data:setting_%s_value_indexes"%events_short_dict[row["events_id"]]}], "color":FT_SOBER_BLUE}
    #     list_desc_data_trace.append(event_info)

    list_desc["trace"]["datas"] = list_desc_data_trace

    thd_dict = {"A级": a_thd, "B级": b_thd, "C级": c_thd}
    color_dict = {"A级": LN_GOLDEN, "B级": LN_YELLOW, "C级": LN_RED}
    grades = ["A级", "B级", "C级"]


    # C/B/A级的desc, 不重叠 —— 2018/8/22
    # for i in range(0,3):
    #     grade = list(color_dict.keys())[i]
    #     desc_id = grade[0]
    #     list_desc[desc_id] = {"title": "", "sub_title": "", "width": "35%"}
    #     list_desc_data = deepcopy(list_desc_data1)
    #     del list_desc_data[-3-i]["cols"][0]["link"]
    #     list_desc_data[-3 - i]["cols"][0]["text"] = "<section style='text-align:center'>"+list_desc_data[-3-i]["cols"][0]["text"]+"</section>"
    #     list_desc_data[-3-i]["strong"] = True
    #     list_desc_data[-3 - i]["color"] = color_dict[grade]
    #     df_grade = deepcopy(df_events[df_events['value']>=thd_dict[grade]])
    #     df_grade["rank"] = df_grade["value"].rank(ascending=False)
    #     df_grade = df_grade.sort_values(by="rank", ascending=True).reset_index(drop=True)
    #
    #     list_desc_data.append(null_line)
    #
    #     for index, row in df_grade.iterrows():
    #         event_info = {"cols": [{"text": "第%d名" % row["rank"]}, {"text": row["name"],
    #                                                                 "link": "#data:setting_%s_value_indexes" %
    #                                                                         events_short_dict[row["events_id"]]}], "color":color_dict[grade]}
    #         list_desc_data.append(event_info)
    #     list_desc[desc_id]["datas"] = list_desc_data

    # 保证A/B/C每个级别的事件不重叠 —— 2018/8/22
    for i in range(len(grades)):
        desc_id = grades[i][0]
        list_desc[desc_id] = {"title": "", "sub_title": "", "width": "35%"}
        list_desc_data = deepcopy(list_desc_data1)
        del list_desc_data[-3-9 - i]["cols"][0]["link"]
        list_desc_data[-3-9 - i]["cols"][0]["text"] = "<section style='text-align:center'>" + \
                                                    list_desc_data[-3-9 - i]["cols"][0]["text"] + "</section>"
        list_desc_data[-3-9-i]["strong"] = True
        list_desc_data[-3-9 - i]["color"] = color_dict[grades[i]]
        # if i == len(grades)-1:
        #     df_grade = deepcopy(df_events[df_events.value>=thd_dict[grades[i]]])
        # else:
        #     df_grade = deepcopy(df_events[(df_events.value>=thd_dict[grades[i]])&(df_events.value<thd_dict[grades[i+1]])])
        # df_grade["rank"] = df_grade["value"].rank(ascending=False)
        # df_grade = df_grade.sort_values(by="rank", ascending=True).reset_index(drop=True)
        #
        # list_desc_data.append(null_line)
        #
        # for index, row in df_grade.iterrows():
        #     event_info = {"cols": [{"text": "第%d名" % row["rank"]}, {"text": row["name"],
        #                                                             "link": "#data:setting_%s_value_indexes" %
        #                                                                     events_short_dict[row["events_id"]]}], "color":color_dict[grades[i]]}
        #     list_desc_data.append(event_info)
        list_desc[desc_id]["datas"] = list_desc_data

    # 追踪指数信息对应的list_desc  —— 以下list_desc可以公用 —— 2018/8/15 —— 前端又要改，这个先暂缓
    # nearest_trace_time = max(df_warning_trace_info['do_time'].tolist())
    # 空行
    # if gov_code_str == "110101":
    null_line = {"cols": [{"text": ""}]}
    if gov_code_str == common_folder_code:
        for events_head_id in events_head_ids:
            for index_col in index_cols:
                index_id = events_short_dict[events_head_id]+index_col.split('_')[-1]
                list_desc[index_id] = {"title":"", "sub_title": "", "width": "35%"}
                list_desc_data2 = []
                # 第一行：标题
                gov_title = "<h3><section style='text-align:center'>%s</section></h3>" % gov_names[events_head_ids.index(events_head_id)]
                gov_info = {"cols": [{"text": gov_title}]}
                list_desc_data2.append(gov_info)

                # 第二/三行：预警详情
                df_event = df_warning_trace_info.loc[df_warning_trace_info['events_head_id'] == events_head_id, :].reset_index(
                    drop=True)
                df_event.loc[:, 'do_time'] = df_event.loc[:, 'do_time'].apply(lambda x: str(x).split('.')[0])
                df_event = df_event.sort_values(by=['do_time']).reset_index(drop=True)

                latest_trace = df_event.loc[:, 'do_time'].max()
                warn_event_data = ""
                earliest_pub_time = str(df_warning_details.loc[df_warning_details['events_head_id'] == events_head_id, 'pub_time'].min()).split('.')[0]
                # earliest_trace_time = df_event.loc[: 'do_time'].min()
                county_name = gov_names[events_head_ids.index(events_head_id)].split('|')[-1]
                search_key = ' '.join(df_event["search_key"].values[0].split(' ')[1:])

                # warn_title = "<h3><section style='color: orange; text-align:center'>预警详情</section></h3>"
                warn_title_info = {"cols": [{"text": "<section style='text-align:center'>预警详情</section>"}, {"text": "部分信息来源", "link": "#data:setting_%s_publishers" % events_short_dict[events_head_id]}, {"text": "返回首页", "link": "#back:"}], "bg_color":BT_TIFFANY_BLUE, "color":FT_PURE_WHITE, "strong":True}

                # "text": "返回首页", "link": "#data:setting"

                warn_event_data = "本系统于%s，在互联网上监测到%s发生了一件%s隐患事件，该事件关键字为“<span style='color: orange'>%s</span>”。此后系统持续追踪，本事件在网上的影响力扩散见左图。<br/>事件当前状态及离各级预警的距离如下：" % (earliest_pub_time, county_name, warning_type, search_key)

                warn_info = {"cols": [{"text": warn_event_data}]}
                list_desc_data2.extend([warn_title_info, warn_info, null_line])


                latest_weibo_value = df_event.loc[df_event['do_time'] == latest_trace, 'weibo_value'].values[0]
                latest_trace_v = df_event.loc[df_event['do_time'] == latest_trace, 'trace_v'].values[0]
                latest_trace_a = df_event.loc[df_event['do_time'] == latest_trace, 'trace_a'].values[0]


                latest_ws = []
                for status in WSTATUSES.keys():
                    locals()[status] = df_event.loc[df_event['do_time'] == latest_trace, status].values[0]
                    latest_ws.append(locals()[status])
                # 判断几级事件

                # if latest_wc == 0:
                if -2 not in latest_ws:
                    current_events_judge = {}
                    cant_reach_events_judge = {}
                    for i in range(0, len(WSTATUSES)+1):
                        if i == 0:
                            current_events_judge[i] = ""
                            cant_reach_events_judge[i] = ""
                        else:
                            current_events_judge[i] = list(WSTATUSES.keys())[i-1]
                            cant_reach_events_judge[i] = list(WSTATUSES.keys())[len(WSTATUSES)-i]
                    need_time_to_reach = {}
                    for status in WSTATUSES.keys():
                        if locals()[status] > 0:
                            need_time_to_reach[status] = locals()[status]

                    # 当前的事件等级
                    event_status = current_events_judge[latest_ws.count(0)]
                    # 达不到的等级
                    cant_reach = cant_reach_events_judge[latest_ws.count(-1)]
                    if event_status == '':
                        event_info = "事件追踪中。"
                        advise_info = "<span style='color: orange'>持续关注，及时应对。</span>"
                        already_pass_dict = {}
                    else:
                        status_info = WSTATUSES[event_status]
                        first_warn_time = datetime.strptime(df_event.loc[df_event[event_status] == 0, 'do_time'].min(),
                                                            '%Y-%m-%d %H:%M:%S')
                        latest_trace_time = datetime.strptime(latest_trace, '%Y-%m-%d %H:%M:%S')
                        interdays = (latest_trace_time - first_warn_time).days
                        interseconds = (latest_trace_time - first_warn_time).seconds
                        warn_interhours = interdays*24 + interseconds/3600
                        event_info = "<span style='color: orange'>%s</span>事件，触发%s预警已<span style='color: orange'>%.2f</span>小时，已成为<span style='color: orange'>%s</span>范围内的热点话题。%s"%(status_info['name'], status_info['name'], warn_interhours, status_info['area'], status_info['desc'])
                        advise_info = status_info['advise']
                        # 已超过的其他门限
                        already_pass_dict = {}
                        for warning_key in WSTATUSES.keys():
                            if warning_key < event_status:
                                first_warning_time = datetime.strptime(df_event.loc[df_event[warning_key]==0, 'do_time'].min(), '%Y-%m-%d %H:%M:%S')
                                other_interdays = (latest_trace_time-first_warning_time).days
                                other_interseconds = (latest_trace_time-first_warning_time).seconds
                                other_warn_interhours = other_interdays*24 + other_interseconds/3600
                                other_event_info = "触发<span style='color: orange'>%s</span>预警已<span style='color: orange'>%.2f</span>小时。影响范围早已超过<span style='color: orange'>%s</span>，现事件影响程度已高于%s事件。<br/>一般而言，%s事件的影响程度为：%s"%(WSTATUSES[warning_key]['name'],other_warn_interhours, WSTATUSES[warning_key]['area'], WSTATUSES[warning_key]['name'], WSTATUSES[warning_key]['name'], WSTATUSES[warning_key]['desc'])
                                already_pass_dict[WSTATUSES[warning_key]['name']] = other_event_info

                    if cant_reach != '':
                        cant_reach_dict = {}
                        cant_reach_info = WSTATUSES[cant_reach]
                        events_grades = ["A级", "B级", "C级"]
                        events_scopes = ["区域", "省域", "全国"]
                        warning_signs = ['warning_a', 'warning_b', 'warning_c']
                        cant_reach_grades = events_grades[events_grades.index(cant_reach_info['name']):]
                        for i in cant_reach_grades:
                            cant_reach_event_info = "评估本事件当前的传播速度、加速度等，发展为<span style='color: orange'>%s</span>性(%s)事件的概率仅为：<span style='color: orange'>10%%~20%%</span>。<br/>一般而言，%s事件的影响程度为：%s"%(events_scopes[events_grades.index(i)], i, i,WSTATUSES[warning_signs[events_grades.index(i)]]['desc'])
                            cant_reach_dict[i] = cant_reach_event_info

                    if len(need_time_to_reach) != 0:
                        need_time_list = []
                        need_time_dict = {}
                        for key in need_time_to_reach.keys():
                            need_time_info = WSTATUSES[key]
                            need_time_str = "本事件有<span style='color: orange'>80%%~90%%</span>的概率将于<span style='color: orange'>%.2f小时</span>后发展为%s（%s）事件。<br/>一般而言，%s事件的影响程度为：%s"%(need_time_to_reach[key], need_time_info['area'], need_time_info['name'], need_time_info['name'], need_time_info['desc'])
                            need_time_list.append(need_time_str)
                            need_time_dict[need_time_info['name']] = need_time_str
                        need_time_desc = "若放任不管，经评估当前状态，"+''.join(need_time_list)
                        # advise_info += need_time_desc

                # 如果是刚开始的几个周期，就根据值来判断当前等级，并给其他两个等级为 —— “刚开始追踪，距离/超过时间尚在计算”
                else:
                    events_grades = ["A级", "B级", "C级"]
                    events_scopes = ["区域", "省域", "全国"]
                    warning_signs = ['warning_a', 'warning_b', 'warning_c']
                    # 当前事件所处等级
                    event_grade = thd_grades[events_head_ids.index(events_head_id)]
                    need_time_to_reach = {}
                    already_pass_dict = {}
                    cant_reach = "newly_start"
                    cant_reach_dict = {}
                    # 追踪中
                    if event_grade == " ":
                        event_status = ''
                        event_info = "事件追踪中。"
                        advise_info = "<span style='color: orange'>持续关注，及时应对。</span>"
                        for i in events_grades:
                            cant_reach_dict[i] = "当前处于追踪的前五个采样周期，事件距离<span style='color: orange'>%s</span>性(%s)事件的时间和概率尚在计算中。<br/>一般而言，%s事件的影响程度为：%s"%(events_scopes[events_grades.index(i)], i, i,WSTATUSES[warning_signs[events_grades.index(i)]]['desc'])
                    # 已达到其他等级的事件
                    else:
                        event_status = warning_signs[events_grades.index(event_grade)]
                        status_info = WSTATUSES[event_status]
                        # first_warn_time = datetime.strptime(df_event.loc[df_event[event_status] == 0, 'do_time'].min(),
                        #                                     '%Y-%m-%d %H:%M:%S')
                        # latest_trace_time = datetime.strptime(latest_trace, '%Y-%m-%d %H:%M:%S')
                        # warn_interval = (latest_trace_time - first_warn_time).seconds
                        event_info = "<span style='color: orange'>%s</span>事件，已成为<span style='color: orange'>%s</span>范围内的热点话题。%s" % (status_info['name'], status_info['area'], status_info['desc'])
                        advise_info = status_info['advise']
                        other_grades = [i for i in events_grades if i != event_grade]
                        for i in other_grades:
                            cant_reach_dict[i] = "当前处于追踪的前五个采样周期，事件距离<span style='color: orange'>%s</span>性(%s)事件的时间和概率尚在计算中。<br/>一般而言，%s事件的影响程度为：%s" % (events_scopes[events_grades.index(i)], i, i,WSTATUSES[warning_signs[events_grades.index(i)]]['desc'])

                C_warning_button = {
                    "cols": [{"text": "全国性（C级）事件", "link": "#list:%s/%s/%sC" % (node_code, gov_code_str, events_short_dict[events_head_id])}, {"text": ""},
                             {"text": ""}]}
                B_warning_button = {
                    "cols": [{"text": "省域性（B级）事件", "link": "#list:%s/%s/%sB" % (node_code, gov_code_str, events_short_dict[events_head_id])},
                             {"text": ""}]}
                A_warning_button = {
                    "cols": [{"text": "区域性（A级）事件", "link": "#list:%s/%s/%sA" % (node_code, gov_code_str, events_short_dict[events_head_id])}]}
                TRACE_warning_button = {
                    "cols": [{"text": "正在追踪的事件", "link": "#list:%s/%s/%strace" % (node_code, gov_code_str, events_short_dict[events_head_id])}]}
                tri_buttons_list = [C_warning_button, B_warning_button, A_warning_button, TRACE_warning_button]

                thd_desc = ["C级", "B级", "A级", " "]  # 空格表示追踪中，没到三级预警门限
                thd_scopes = ["全国", "省域", "区域", " "]  # 空格表示追踪中，没到三级预警门限
                thd_dict = {"A级": a_thd, "B级": b_thd, "C级": c_thd}
                color_dict = {"A级": LN_GOLDEN, "B级": LN_YELLOW, "C级": LN_RED, " ": FT_SOBER_BLUE}

                if event_status != '':
                    status_grade = WSTATUSES[event_status]['name']
                    del tri_buttons_list[-1]
                    # grade_type = status_grade[0]
                else:
                    status_grade = " "
                    # grade_type = "trace"
                status_index = thd_desc.index(status_grade)
                # del tri_buttons_list[status_index]["cols"][0]["link"]
                # tri_buttons_list[status_index]["cols"][0]["text"] = "<section style='text-align:center'>"+"本县当前："+tri_buttons_list[status_index]["cols"][0]["text"]+"</section>"
                tri_buttons_list[status_index]["strong"] = True
                # tri_buttons_list[status_index]["color"] = color_dict[status_grade]
                tri_buttons_list[status_index]["cols"][0]["text"] = "<span style='color: %s'>"%(color_dict[status_grade]) + tri_buttons_list[status_index]["cols"][0]["text"] + "</span>"
                tri_buttons_list[status_index]["cols"][0]["link"] = "#list:%s/%s/%svalue"%(node_code, gov_code_str, events_short_dict[events_head_id])
                # "#data:setting_%s_%s_indexes"%(events_short_dict[events_head_id], index_col.split('_')[-1])
                tri_buttons_list[status_index]["cols"][0]["color"] = color_dict[status_grade]
                status_desc = "<span style='color: orange'>事件当前状态</span>：%s<br/><span style='color: orange'>系统建议</span>：%s"%(event_info, advise_info)
                status_desc_line = {"cols":[{"text":"%s"%status_desc}]}  # , "color":color_dict[status_grade]
                tri_buttons_list.insert(status_index+1, status_desc_line)
                # 加了状态位之后，对应事件级数索引后加0
                thd_desc.insert(status_index+1, 0)
                list_desc_data2.extend(tri_buttons_list)

                list_desc_data2.append(null_line)
                word_types = ['sensitive', 'department', 'guanzhi']
                word_names = ['事件关键词', '涉及部门', '涉及官员']
                title_cols = []
                for word_name in word_names:
                    text = {"text": word_name}
                    title_cols.append(text)
                word_title = {"cols": title_cols, "strong": True, "size": 14,
                              "color": FT_ORANGE}

                # 第四~N行：关键词
                words_dict = {}
                words_lines = []
                for word_type in word_types:
                    df_event = df_warning_keywords.loc[
                               (df_warning_keywords.events_head_id == events_head_id) & (df_warning_keywords.type == word_type),
                               :]
                    df_event = df_event.sort_values(by=['freq'], ascending=False).reset_index(drop=True)
                    if df_event.iloc[:, 0].size > max_key_words_num:
                        df_event = df_event[0:max_key_words_num]
                        freq_total = sum(list(df_event['freq']))
                        df_event.loc[:, 'freq'] = df_event.loc[:, 'freq'].apply(lambda x: round(x / freq_total, 4))
                    words_list = []
                    for index, row in df_event.iterrows():
                        word_str = "%s ( %.2f%% )" % (row['word'], row['freq'] * 100)
                        words_list.append(word_str)
                    words_dict[word_type] = words_list

                max_len = max([len(words_dict[i]) for i in words_dict.keys()])
                for j in range(0, max_len):
                    word_cols = [{"text":""}, {"text":""}, {"text":""}]
                    for i in range(0,3):
                        word_type = list(words_dict.keys())[i]
                        if len(words_dict[word_type]) > j:
                            word_cols[i]["text"] = words_dict[word_type][j]
                    word_line = {"cols": word_cols, "color": FT_SOBER_BLUE}
                    words_lines.append(word_line)

                list_desc_data2.append(word_title)
                list_desc_data2.extend(words_lines)

                list_desc[index_id]["datas"] = list_desc_data2

                # 剩下的其它状态
                last_statuses = [i for i in thd_desc if i not in [status_grade, " ", 0]]
                for status in last_statuses:
                    index_id_other = events_short_dict[events_head_id] + status[0]
                    list_desc[index_id_other] = {"title": "", "sub_title": "", "width": "35%"}
                    list_desc_data3 = [gov_info, warn_title_info, warn_info, null_line]
                    other_status_index = thd_desc.index(status)
                    tri_buttons_list_other = deepcopy(tri_buttons_list)
                    tri_buttons_list_other[other_status_index]["cols"][0]["link"] = "#list:%s/%s/%svalue"%(node_code, gov_code_str, events_short_dict[events_head_id])
                    tri_buttons_list_other[other_status_index]["strong"] = True
                    # tri_buttons_list_other[other_status_index]["color"] = color_dict[status]
                    tri_buttons_list_other[other_status_index]["cols"][0]["text"] = "<span style='color: %s'>" % (
                    color_dict[status]) + tri_buttons_list_other[other_status_index]["cols"][0]["text"] + "</span>"
                    # tri_buttons_list_other[other_status_index]["cols"][0]["color"] = color_dict[status]
                    status_desc = ""
                    if cant_reach != '':
                        if status in cant_reach_dict.keys():
                            status_desc += cant_reach_dict[status]
                    if len(need_time_to_reach) != 0:
                        if status in need_time_dict.keys():
                            status_desc += need_time_dict[status]
                    if len(already_pass_dict) != 0:
                        if status in already_pass_dict.keys():
                            status_desc += already_pass_dict[status]
                    status_desc_line = {"cols": [{"text": "%s" % status_desc}]}  # , "color":color_dict[status_grade]
                    tri_buttons_list_other.insert(other_status_index + 1, status_desc_line)
                    list_desc_data3.extend(tri_buttons_list_other)
                    # 关键词
                    list_desc_data3.extend([null_line, word_title])
                    list_desc_data3.extend(words_lines)
                    list_desc[index_id_other]["datas"] = list_desc_data3

        # 微博详情对应的list_desc
        para_dict = warning_dict[node_code]["parameters"]
        w0 = para_dict["K_weibo"]
        wr = para_dict["K_read"]
        wc = para_dict["K_comment"]
        ws = para_dict["K_share"]
        # 事件参与度
        df_warning_details["event_participation"] = w0 * 1 + wr * df_warning_details["count_read"] + wc * \
                                                    df_warning_details["count_comment"] + ws * df_warning_details[
                                                        "count_share"]
        # 博主影响力
        df_warning_details["publisher_impact"] = df_warning_details["followers_count"]
        for events_head_id in events_head_ids:
            index_id = events_short_dict[events_head_id]+'publisher'
            list_desc[index_id] = {"title":"", "sub_title": "", "width": "35%"}
            list_desc_data4 = []
            # 第一行：标题
            gov_title = "<h3><section style='text-align:center'>%s</section></h3>" % gov_names[
                events_head_ids.index(events_head_id)]
            gov_info = {"cols": [{"text": gov_title}]}
            list_desc_data4.append(gov_info)

            # 第二行：按钮返回
            # {"text": "返回", "link": "#data:setting_%s_value_indexes" % events_short_dict[events_head_id]}
            button_line = {"cols": [{"text": "返回", "link": "#back:"}],
                           "strong": True, "size": 14, "bg_color": BT_TIFFANY_BLUE, "color": FT_PURE_WHITE}
            # button_line = {"cols": [{"text": "预警详情", "link": "#data:setting_%s_value_indexes"% events_short_dict[events_head_id]},
            #                         {"text": "<section style='text-align:center'>部分信息来源</section>"},{"text": "返回首页", "link": "#data:setting"}], "bg_color": BT_TIFFANY_BLUE,"color": FT_PURE_WHITE, "strong": True}
            list_desc_data4.append(button_line)

            # 第三行：微博详情；第四~六行：典型评论；第七——：发布者info
            df_event = df_warning_details.loc[(df_warning_details.events_head_id == events_head_id), :]
            df_event = df_event.sort_values(by=["event_participation"], ascending=False).reset_index(drop=True)
            # df_event
            if df_event.iloc[:, 0].size > 20:
                df_event = df_event[0:20]


            # weibo_title = {"cols":[{"text":"<h3><section style='text-align:center'>事件相关微博</section></h3>"}], "strong":True, "color":FT_ORANGE}
            weibo_title = {"cols": [{"text": "部分事件信息"}],"strong": True, "color": FT_ORANGE}
            for i in df_event.index:
                lenth = len(df_event.loc[i, "content"])
                if len(df_event.loc[i, 'content']) >= 10:
                    if lenth <= 80:
                        k = lenth
                    else:
                        k = 80
                    weibo_content = {"cols":[{"text":"%s：%s"%(df_event.loc[i, 'pub_time'], df_event.loc[i, "content"][0:k]+'...')}]}
                    list_desc_data4.extend([weibo_title, weibo_content])
                    break

            # 评论信息
            df_comments = df_warning_weibo_comments.loc[(df_warning_weibo_comments.events_head_id == events_head_id), :]
            # comment_title = {"cols":[{"text":"<h3><section style='text-align:center'>典型评论</section></h3>"}], "strong":True, "color":FT_ORANGE}
            comment_title = {"cols": [{"text": "部分民众评价"}], "strong": True, "color": FT_ORANGE}
            list_desc_data4.append(comment_title)
            comments_list = []
            for i in df_comments['comments_shown']:
                if len(i) == 0:
                    continue
                for j in i:
                    comments_list.append(j)
            if len(comments_list) == 0:
                comments_str = "无。"
                comments_info = {"cols":[{"text":comments_str}]}
                list_desc_data4.append(comments_info)
            else:
                if len(comments_list) <= 6:
                    k = len(comments_list)
                else:
                    k = 6
                for i in range(0, k):
                    comments_info = {"cols":[{"text":comments_list[i]}]}
                    list_desc_data4.append(comments_info)

            publish_title = {"cols":[{"text":"发布者"}, {"text":"事件参与度"}, {"text":"影响力"}], "strong":True, "color":FT_ORANGE}
            list_desc_data4.append(publish_title)
            for index, row in df_event.iterrows():
                publish_info = {"cols":[{"text":row["post_name"]}, {"text":"%s"%row['event_participation']}, {"text":"%s"%row["publisher_impact"]}], "color":FT_SOBER_BLUE}
                list_desc_data4.append(publish_info)
            list_desc[index_id]["datas"] = list_desc_data4
    return setting_list, setting_name_list, list_desc


# 产生前端细节文件
def generate_html_content(gov_code, node_code, df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, monitor_time, info_dict, record_now=True):
    # 有问题？？？
    gov_code_str = str(gov_code)[0:6]
    target_dir_path = client_path + node_code + '/' + str(gov_code)[0:6] + '/'
    if not os.path.exists(target_dir_path):
        os.makedirs(target_dir_path)

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
        setting_list, setting_name_list, list_desc = get_warning_setting_desc_data(gov_code, node_code, df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, monitor_time, info_dict, record_now)
        if len(setting_list)>0:
            # for position in range(len(warning_map_data_list)):
            #     write_client_datafile_json(target_dir_path, warning_map_data_name_list[position], '.json',warning_map_data_list[position])
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
            # print('map & columns')
            # for set in range(len(warning_line_data_list)):
            #     write_client_datafile_json(target_dir_path, warning_line_data_name_list[set], '.json',
            #                                warning_line_data_list[set])
            for position in range(len(warning_map_data_list)):
                write_client_datafile_json(target_dir_path, warning_map_data_name_list[position], '.json',
                                           warning_map_data_list[position])

        # 得到setting和list文件
        setting_list, setting_name_list, list_desc = get_warning_setting_desc_data(gov_code, node_code, df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, monitor_time, info_dict, record_now)
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
                if 'record_now' in i :
                    record_now_str = i.split('record_now:')[-1].strip()
                    record_now = True if record_now_str == 'True' else False
                if 'events_num' in i :
                    events_num = int(i.split('events_num:')[-1].strip())
            break

    return df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, events_num, record_now


# 计算两个更新周期之间的事件信息新增条数
def events_update_info_num_between(monitor_datetime):
    last_time = monitor_datetime - timedelta(hours=interhour)
    sqlstr = "SELECT SUM(update_info) FROM (SELECT events_head_id, MAX(data_num+count_read+count_comment+count_share)-MIN(data_num+count_read+count_comment+count_share) AS update_info, MAX(do_time) AS do_time FROM %s WHERE do_time BETWEEN '%s' AND '%s' GROUP BY events_head_id) b;"%(trace_info_table, last_time, monitor_datetime)

    rows = trace_db_obj.select_data_from_db_one_by_one(trace_db_info["db_name"], sqlstr)
    return rows[0][0]


# 计算近一周追踪事件数
def get_past_week_trace_events_num(monitor_datetime):
    last_time = monitor_datetime - timedelta(days=7)
    sqlstr = "SELECT count(events_head_id) from %s where sync_time between '%s' and '%s'"%(trace_seed_table, last_time, monitor_datetime)

    rows = trace_db_obj.select_data_from_db_one_by_one(trace_db_info["db_name"], sqlstr)
    # print(rows[0][0])
    return rows[0][0]


# 计算近一周追踪事件信息条数
def get_past_week_trace_info_num(monitor_datetime):
    last_time = monitor_datetime - timedelta(days=7)
    sqlstr = "SELECT SUM(update_info) FROM (SELECT events_head_id, MAX(data_num+count_comment)-MIN(data_num+count_comment) AS update_info, MAX(do_time) AS do_time FROM %s WHERE do_time BETWEEN '%s' AND '%s' GROUP BY events_head_id) b;" % (
    trace_info_table, last_time, monitor_datetime)

    rows = trace_db_obj.select_data_from_db_one_by_one(trace_db_info["db_name"], sqlstr)
    # print(rows[0][0])
    return rows[0][0]


# 计算近一周系统入库信息条数
def get_past_week_sys_info_num(monitor_datetime):
    latest_time = (monitor_datetime - timedelta(days=2)).date()
    last_time = (monitor_datetime - timedelta(days=9)).date()
    sqlstr = "SELECT SUM(daily_weibo+daily_comment) as sum from %s where pub_date >= '%s' and  pub_date <= '%s'"%(stats_table, last_time, latest_time)
    conn = database.ConnDB(product_server, product_db)
    conn.switch_to_arithmetic_write_mode()
    ret = conn.read(sqlstr)
    # print(ret.code)
    rows = ret.data
    # print(ret.result)
    # print(rows)
    # print(rows[0]['sum'])
    return rows[0]['sum']


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
    this_time = datetime.strptime(monitor_time, '%Y-%m-%d %H:%M:%S')
    # next_time = this_time + timedelta(hours=interhour)
    info_dict["monitor_aug"] = events_update_info_num_between(this_time)
    info_dict["past_week"]["events_num"] = get_past_week_trace_events_num(this_time)
    info_dict["past_week"]["events_info"] = get_past_week_trace_info_num(this_time)/10000
    info_dict["past_week"]["sys_info"] = get_past_week_sys_info_num(this_time)/1000


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
            df_warning_trace_info, df_warning_keywords, df_warning_details,df_warning_weibo_comments, events_num, record_now  = get_newliest_events_data(node_code)

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

        for prov in provinces:
            reg = '\A' + prov
            df_county_in_province = df_2861_county.filter(regex=reg, axis=0)
            gov_codes = df_county_in_province.index
            # 遍历一个省下的所有县
            # 多进程执行
            p = Pool(10)
            for i in range(len(gov_codes)):
            # for gov_code in gov_codes:
                count += 1
                if gov_codes[i] not in df_2861_county.index.values:
                    continue
                # if gov_code in df_2861_county.index.values:
                p.apply_async(generate_html_content, args=(gov_codes[i], node_code, df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, monitor_time, info_dict, record_now))
                # generate_html_content(gov_codes[i], node_code, df_warning_trace_info, df_warning_keywords, df_warning_details, df_warning_weibo_comments, monitor_time, info_dict, record_now)
                time_loop1 = time.time()
                print('\r当前进度：%.2f%%, 耗时：%.2f秒, 还剩：%.2f秒'%((count*100/2852), (time_loop1-time_loop_start), (time_loop1-time_loop_start)*(2852-count)/count), end="")

            p.close()
            p.join()


        # 当前没有追踪中的事件，就取历史事件，保证历史事件一定有~~
        # else:
        #     trace_records = open('./trace_info_record.txt', encoding='utf-8').readlines()
        #     trace_records.reverse()
        #     # last_events_num = -1
        #     events_type_nums = []
        #     for record in trace_records:
        #         if "events_type: %s"%events_type in record:
        #             # events_type_record.append(record)
        #             events_type_nums.append(int(record.split("events_num: ")[-1]))
        #             # break
        #     # for i in
        #     last_events_num = events_type_nums[0]
        #     last_three_nums = events_type_nums[0:3]
        #
        #     # 上次跑运行中的事件数不为0 / apps里没有数据 / 最近三次跑的事件数都为0   —— 更新，重新提历史数据；PS:问题在于，如果每次选的省不一样，也需要重跑
        #     if last_events_num != 0 or (not os.path.exists(client_path + node_code)) or last_three_nums.count(0) == 3 or (not same_provs):
        #
        #         df_warning_trace_info_before, df_warning_keywords_before, df_warning_details_before, df_warning_weibo_comments_before, events_num_before = get_events_data(monitor_time, events_type, record_now=False, events_limit=history_events_limit)
        #
        #         if len(df_warning_trace_info_before) + len(df_warning_keywords_before) + len(df_warning_details_before) > 0:
        #             # 上次有运行中的事件，这次没有的话，历史数据也要更新 —— 删掉之前的node_code目录；否则就不用删，直接保留上次的目录
        #             node_code_path = client_path + node_code
        #             # a = os.path.exists(node_code_path)
        #             if os.path.exists(node_code_path):
        #                 shutil.rmtree(node_code_path)
        #
        #             for prov in provinces:
        #                 reg = '\A' + prov
        #                 df_county_in_province = df_2861_county.filter(regex=reg, axis=0)
        #                 gov_codes = df_county_in_province.index
        #                 # 遍历一个省下的所有县
        #                 # 多进程执行
        #                 p = Pool(10)
        #                 for i in range(len(gov_codes)):
        #                     count += 1
        #                     if gov_codes[i] not in df_2861_county.index.values:
        #                         continue
        #                     p.apply_async(generate_html_content, args=(gov_codes[i], node_code, df_warning_trace_info_before, df_warning_keywords_before, df_warning_details_before,df_warning_weibo_comments_before, monitor_time, False))
        #                     # generate_html_content(gov_codes[i], node_code, df_warning_trace_info_before, df_warning_keywords_before, df_warning_details_before, df_warning_weibo_comments_before, False)
        #                     time_loop1 = time.time()
        #                     print('\r当前进度：%.2f%%, 耗时：%.2f秒, 还剩：%.2f秒' % ((count * 100 / 2852), (time_loop1 - time_loop_start),(time_loop1 - time_loop_start) * (2852 - count) / count), end="")
        #
        #                 p.close()
        #                 p.join()

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

# 产生前端主界面文件
# sched = BlockingScheduler()
# @sched.scheduled_job('cron', hour='6-23', minute='25')
def generate_datafile(provinces, same_provs=True):
    # provinces = ['1101', '5101']
    current_day = time.strftime('%Y-%m-%d %H:%M:%S')
    web_leaves_datafile(provinces, current_day, same_provs)
    # app_env_date = time.strftime('%Y-%m-%d %H:%M:%S')
    # return app_env_date
    return


if __name__ == "__main__":
    # provinces = ['11', '51', '13']
    provinces_all = [
        '11', '12', '13', '14', '15',
        '21', '22', '23',
        '31', '32', '33', '34', '35', '36', '37',
        '41', '42', '43', '44', '45', '46',
        '50', '51', '52', '53', '54',
        '61', '62', '63', '64', '65'
    ]
    provinces = ['110101', '320623', '130624', '410381']

    # 北京+成都
    provinces = ['1101', '5101']

    # 北京+四川
    provinces = ['11', '51']

    # 游仙
    # provinces = ['110101', '510704']

    # test
    if 1:
        provinces = ['11', '510704']
        # provinces = ['11', '51']
        generate_datafile(provinces_all)

    # 定时跑程序
    if 0:
        scheduler = BlockingScheduler()
        # scheduler.add_job(generate_datafile, 'interval', hours=3)
        scheduler.add_job(generate_datafile, 'cron', hour='7-23', minute='10', args=(provinces, ))
        scheduler.start()


    if 0:
        monitor_datetime = datetime(2018,8,27)
        get_past_week_trace_events_num(monitor_datetime)
        get_past_week_sys_info_num(monitor_datetime)
        get_past_week_trace_info_num(monitor_datetime)

    if 0:
        time_start = datetime(2018,1,1)
        time_end = datetime(2018,8,27)
        rows = get_past_events_info(time_start, time_end)
        df_temp = pd.DataFrame(rows)
        print(df_temp)











