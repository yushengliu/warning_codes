#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2018/8/4 18:21
@Author  : Liu Yusheng
@File    : ServerRunZip.py
@Description:
"""

from datetime import datetime
import traceback
import logging
import paramiko
import time
from apscheduler.schedulers.background import BlockingScheduler


# 按照模块汇总并压缩web数据
def tar_web_by_module():
    print('running copy_t0_web...%s' % datetime.now())

    # module_version_dict = get_valid_module(version)
    module_time_dict = check_web_updates_from_db()
    if module_time_dict:

        # 新建一个ssh客户端对象
        myclient = paramiko.SSHClient()
        # 设置成默认自动接受密钥
        myclient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # 连接远程主机
        myclient.connect(source_dict['ip'], port=source_dict['port'],
                         username=source_dict['uname'],
                         password=source_dict['password'])

        server_src_base = '/home/ftp/ui_client_data/web/'
        server_src_zip_apps = '/home/ftp/ui_client_data/webzip/apps/'
        server_src_zip = '/home/ftp/ui_client_data/webzip/'

        for node_code, dict_time in module_time_dict.items():

            tar_file_name = node_code + '_apps.tar.gz'
            server_tar_file_path = server_src_zip + tar_file_name

            module_version = datetime.strftime(dict_time, '%Y-%m-%d')
            print(node_code + '  ' + module_version)
            server_src = server_src_base + node_code + '/' + module_version + '/*'
            server_src_zip_node = server_src_zip_apps + node_code + '/'

            mkdir_cmd = "mkdir %s" % server_src_zip_node[:-1]
            stdin, stdout, stderr = myclient.exec_command(mkdir_cmd)
            print(stdout.read().decode('utf-8'))

            cp_cmd = "cp -fr %s %s" % (server_src, server_src_zip_node)
            retcode, outdata, errordata = run_command(myclient, cp_cmd)
            print(retcode)

            # zip
            mv_cmd = "cd %s; tar -zcvf %s %s" % (server_src_zip, server_tar_file_path, 'apps/'+node_code)
            retcode, outdata, errordata = run_command(myclient, mv_cmd)
            print(retcode)

        myclient.close()
    else:
        print('no web data updates...')

    # 按照模块上传并解压web数据
    scp_web_by_module(module_time_dict)
    return module_time_dict


# 按照模块上传并解压web数据
def scp_web_by_module(module_time_dict):
    if module_time_dict:

        # 内部服务器data storage
        myclient = paramiko.SSHClient()
        myclient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        myclient.connect(source_dict['ip'], port=source_dict['port'],
                         username=source_dict['uname'],
                         password=source_dict['password'])

        # 阿里云 data storage
        sshclient = paramiko.SSHClient()
        sshclient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        sshclient.connect(target_dict['ip'], port=target_dict['port'],
                          username=target_dict['uname'],
                          password=target_dict['password'])

        #server_des_base = '/home/zhengli/tornado_data/'
        server_des_base = '/ftp_mnt/tornado_data/'
        server_src_zip = '/home/ftp/ui_client_data/webzip/'

        for node_code, dict_time in module_time_dict.items():
            tar_file_name = node_code + '_apps.tar.gz'
            server_tar_file_path = server_src_zip + tar_file_name
            server_des_tar_file_path = server_des_base + tar_file_name

            # 在远程机执行shell命令
            mv_cmd = "scp -r %s zhengli@120.79.142.157:%s" % (server_tar_file_path, server_des_base)
            retcode, outdata, errordata = run_command(myclient, mv_cmd)
            print(retcode)

            # unzip
            mv_cmd = "tar -xzvf %s -C %s" % (server_des_tar_file_path, server_des_base)
            retcode, outdata, errordata = run_command(sshclient, mv_cmd)
            print(retcode)

            update_web_updates_in_db(node_code, dict_time)

        sshclient.close()
        myclient.close()
    else:
        print('no web data to upload...')
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


'''
    scheduler = BlockingScheduler()
    scheduler.add_job(tar_web_by_module, 'cron', second='0', minute='0', hour='1', day='*', month='*') # '06-22/2'
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
'''
