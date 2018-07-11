#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Author: Quintin Xu
# @Date:   2018-07-10 17:16:40
# @Last Modified by:   Quintin Xu
# @Last Modified time: 2018-07-11 16:44:43
# @E-mail: QuintinHsu@gmail.com
# @Description: 

import logging
from config.config import *
logger = logging.getLogger(__name__)

class POIRequest(object):
    """docstring for POIRequest"""
    def __init__(self, url, params, callback=None, method='GET', headers=None, 
        need_proxy=False, fail_time=0, timeout=REQUEST_TIMEOUT):
        """
        """
        self.url = url
        self.params = params
        self.callback = callback
        self.need_proxy = need_proxy
        self.fail_time = fail_time
        self.timeout = timeout

    def args2str(self):
        """
        将Request参数转化为字符串
        :return string
        """
        args = {
            'url': self.url,
            'params': self.params,
            'need_proxy': self.need_proxy,
            'fail_time': self.fail_time,
            'timeout': self.timeout
        }
        return str(args)