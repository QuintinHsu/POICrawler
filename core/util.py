#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Author: Quintin Xu
# @Date:   2018-07-11 19:04:11
# @Last Modified by:   Quintin Xu
# @Last Modified time: 2018-07-12 21:36:02
# @E-mail: QuintinHsu@gmail.com
# @Description: 
import os
from datetime import datetime, timedelta, timezone
import time
import random

def localtime():
    """
    获取当前时刻的utc+8时间戳
    :return: utc+8时间戳
    """
    utc_dt = datetime.utcnow().replace(tzinfo=timezone.utc)
    cn_dt = utc_dt.astimezone(timezone(timedelta(hours=8)))
    ts = time.mktime(cn_dt.timetuple())
    return ts

def t_2_localtime(t):
    """
    将时间戳转换为utc+8时间戳
    :param t    待转换的时间戳
    :return:    utc-8时间戳
    """
    utc_dt = datetime.utcfromtimestamp(t).replace(tzinfo=timezone.utc)
    cn_dt = utc_dt.astimezone(timezone(timedelta(hours=8)))
    ts = time.mktime(cn_dt.timetuple())
    return ts

def get_fake_user(ip_file, ua_file):
    """
    生成proxy-ua-cookies对
    :param ip_file:     存储ip的文件地址
    :param ua_file:     存储ua的文件地址
    :return: dict   {'counter': 1, 'fu': [{'proxy': {'http': 'http://1.1.1.1:61000'}, 'ua': ua, 'cookies': None, 'counter': 0}]}
    """
    fake_user = {}
    fake_user['counter'] = 0
    fake_user['fu'] = list()
    ips = list()
    uas = list()

    with open(ip_file, mode='r') as f:
        ips = f.readlines()
        ips = [ip.replace("\n", '') for ip in ips]

    with open(ua_file, mode='r') as f:
        uas = f.readlines()
        uas = [ua.replace("\n", '') for ua in uas]

    for ip in ips:
        for i in range(80):
            if i % 10 == 0:
                ua = random.choice(uas)
            fu = {'proxy': {'http': 'http://%s' % ip}, 'ua': ua, 'cookies': None, 'counter': 0}
            fake_user['fu'].append(fu)
    return fake_user


