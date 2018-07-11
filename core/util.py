#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Author: Quintin Xu
# @Date:   2018-07-11 19:04:11
# @Last Modified by:   Quintin Xu
# @Last Modified time: 2018-07-11 19:25:12
# @E-mail: QuintinHsu@gmail.com
# @Description: 

from datetime import datetime, timedelta, timezone
import time

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