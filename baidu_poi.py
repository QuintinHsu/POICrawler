#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Author: Quintin Xu
# @Date:   2018-07-10 19:28:40
# @Last Modified by:   Quintin Xu
# @Last Modified time: 2018-07-15 21:56:39
# @E-mail: QuintinHsu@gmail.com
# @Description: 抓取百度POI，百度POI每次请求最多返回50条数据，page_num从0开始，需要设置城市边界，不要设置城市代码，否则在边界区域会搜索不到数据

import time
import json
import random
import logging
import logging.config
import cloghandler
import threading
import requests
import requests.utils
import copy

from core.db import RedisQueue, POIMySQL
from core.request import POIRequest
from core import util

from config.config import *

logging.config.fileConfig('./config/logging_baidu.conf')
logger = logging.getLogger(__name__)

class Spider(threading.Thread):
    """爬虫"""
    def __init__(self, request_queue):
        threading.Thread.__init__(self)
        self.request_queue = request_queue
        self.db = POIMySQL()
        self.redis_key = REDIS_KEY_POIREQUEST_BD

    def run(self):
        while True:
            poi_request = self.request_queue.get(redis_key=self.redis_key)

            if poi_request.callback == None:
                poi_request.callback = self.parse_json
            callback = poi_request.callback
            poi_response, poi_request = self.request(poi_request)

            if poi_response and poi_response.status_code in VALID_STATUSES:
                results = callback(poi_response, poi_request)

                for result in results:
                    if isinstance(result, POIRequest):
                        self.request_queue.put(result, redis_key=self.redis_key)
                    elif isinstance(result, list):
                        self.save_poi(result)
                    elif isinstance(result, tuple):
                        self.request_queue.put(result[0], redis_key=self.redis_key)
                        self.save_poi(result[1])

    def request(self, poi_request):
        """
        执行请求
        :param poi_request: 请求
        :return: 响应
        """
        time.sleep(SLEEP_TIME_BD)

        proxy = None
        headers = {
            'Connection': u'keep-alive',
            'Cache-Control': u'no-cache',
            'Accept-Encoding': u'gzip, deflate',
            'Accept-Language': u'en-US,en;q=0.5',
            'Accept': u'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }       
        try:
            global fake_user
            # 加锁
            if lock.acquire():
                fake_user['counter'] += 1

                # 总的请求大于40W次时，更新fake_user池
                if fake_user['counter'] > 4000000:
                    fake_user = util.get_fake_user(IP_FILE, UA_FILE)

                fu = random.choice(fake_user['fu'])

                # cookies的请求达到最大次数时，更换cookies,并设置使用次数为0
                if fu['counter'] > MAX_COOKIES_TIME:  
                    fu['cookies'] = None
                    fu['counter'] = 0

                headers['User-Agent'] = fu['ua']
                proxy = copy.deepcopy(fu['proxy'])
                cookies = fu['cookies']
                fu['counter'] += 1

                poi_request.headers = headers
                poi_request.cookies = cookies
                poi_request.proxies = proxy

                # 当cookies为None时，从response中获取cookies，并更新fake_user中的cookies
                if not fu['cookies']:
                    response = requests.get(url=poi_request.url, params=poi_request.params, 
                        headers=headers, cookies=cookies, timeout=poi_request.timeout, allow_redirects=False, proxies=proxy)
                    # 随机城市，并添加至cookies中
                    fu['cookies'] = requests.utils.add_dict_to_cookiejar(response.cookies, {'MCITY':'-%s%%3A' % random.randint(100, 360)})

                    # 释放锁
                    lock.release()
                    return response, poi_request
                else:
                    # 释放锁
                    lock.release()            

            response = requests.get(url=poi_request.url, params=poi_request.params, 
                headers=headers, cookies=cookies, timeout=poi_request.timeout, allow_redirects=False, proxies=proxy)

            return response, poi_request
        except Exception as e:
            # 释放锁
            lock.release()
            logger.error('请求失败，\nparams:%s\nua:%s\nproxy:%s' % (poi_request.params, headers['User-Agent'], proxy), 
                exc_info=True)
            # 将该请求重新加入任务队列
            self.error(poi_request)
            return False, poi_request

    def parse_json(self, response, poi_request):
        """
        对返回的数据进行解析
        :param response:        请求响应
        :param poi_request:     请求
        :return                 list: poi数据 or POIRequest: 请求 or tuple(POIRequest, list) 请求和poi数据
        """

        try:
            response_json = json.loads(response.text)
            response.close()

            params = poi_request.params
            
            if response_json and 'result' in response_json:
                data_security = response_json['result']['data_security_filt_res']                        
                if data_security > 0 and poi_request.fail_time < MAX_FAILED_TIME: # 当data_security > 0时，说明百度对结果进行数据筛选，不是完整的结果,将当前请求重新加入任务队列，若连续MAX_FAILED_TIME都是不完整数据，则将该不完整数据存入数据库
                    logger.error('数据不完整，当前请求重新加入任务队列, data_security_filt_res=%s\nparams:%s\nproxies:%s\nheaders:%s\ncookies:%s' % (data_security, poi_request.params, poi_request.proxies, poi_request.headers, poi_request.cookies))
                    self.error(poi_request) # 

                elif 'content' in response_json: # 结果中有content，说明有POI数据，否者说明该区域没有POI数据
                    poi_total = int(response_json['result']['total'])

                    if poi_total > 0 and poi_total <= 50: #只有1页数据，直接将该POI数据返回
                        logger.info('返回第一页的POI数据（只有一页数据）, wd:%s, bound:%s, nn:%s, total:%s' % (params['wd'], params['b'], params['nn'], poi_total))
                        yield response_json['content']

                    elif poi_total > 50 and poi_total < 760: #有多页数据
                        # 剩余多少数据（含本次返回的数据）
                        remain_poi_num = poi_total - params['nn'] 

                        if remain_poi_num > 0 and remain_poi_num <= 50: # 本次请求返回的数据是最后一页数据，直接将该POI数据返回
                            logger.info('返回最后一页的POI数据, wd:%s, bound:%s, nn:%s, total:%s' % (params['wd'], params['b'], params['nn'], poi_total))
                            yield response_json['content']
                        elif remain_poi_num > 50: # 构造下一页数据的请求，并将该请求加入的任务队列
                            logger.info('返回最后一页的POI数据，请求下一页数据, wd:%s, bound:%s, nn:%s, total:%s, remain: %s' % (params['wd'], params['b'], params['nn'], poi_total, remain_poi_num))
                            next_page_params = self.__construct_params(params['wd'], params['b'], params['nn'] + 50)
                            poi_request = POIRequest(url=poi_request.url, params=next_page_params, callback=self.parse_json, 
                                need_proxy=poi_request.need_proxy)
                            yield (poi_request, response_json['content']) # 返回构造的请求和本次请求返回的POI数据

                    else: # 百度在某个区域最多只能搜索到760个POI，如果poi_total=760，则对该区域进行分割，并构造每个子区域的请求
                        logger.info('对bound进行分割，wd:%s, bound:%s, nn:%s, total:%s' % (params['wd'], params['b'], params['nn'], poi_total))
                        sub_area_bounds = self.__calc_subarea(params['b'])
                        for sub_area_bound in sub_area_bounds:
                            sub_params = self.__construct_params(params['wd'], sub_area_bound, 0)
                            poi_request = POIRequest(url=poi_request.url, params=sub_params, callback=self.parse_json, 
                                need_proxy=poi_request.need_proxy)
                            yield poi_request
            else:
                logger.error('Unknow response: %s\nparams:%s\nproxies:%s\nheaders:%s\ncookies:%s' % (response.text, poi_request.params, poi_request.proxies, poi_request.headers, poi_request.cookies))
                self.error(poi_request)
        except Exception as e:
            logger.error('Unknow response: %s\nparams:%s\nproxies:%s\nheaders:%s\ncookies:%s' % (response.text, poi_request.params, poi_request.proxies, poi_request.headers, poi_request.cookies), exc_info=True)
            self.error(poi_request)

    def save_poi(self, data):
        """
        将poi信息保存到数据库中
        :param data：     poi数据列表
        """
        reconstruct_data = list()
        for d in data:
            if 'uid' in d and d['uid'] and d['uid'] != 'null':
                content = json.dumps(d)
                reconstruct_data.append([d['uid'], content, int(time.time())])
        self.db.insert_many('bd_poi', reconstruct_data)


    def __construct_params(self, wd, bound, nn):
        """
        构造请求参数
        :param wd:      搜索关键词
        :param bound:   搜索区域
        :param nn:      poi数据的起始索引（百度每页最多只有50条数据，假设搜索结果中有200个poi，若nn=30，则本次请求返回第30-79这50个poi数据）
        :return:        构造的请求参数
        """
        params = {
            'newmap': 1,
            'reqflag': 'pcmap',
            'biz': 1,
            'from': 'webmap',
            'da_par': 'direct',
            'pcevaname': 'pc4.1',
            'qt': 'spot',
            'from': 'webmap',
            # 'c': 131,  
            'wd': '美食',
            'wd2': '',
            'pn': 0,
            'nn': 0,
            'db': 0,
            'sug': 0,
            'addr': 0,

            'pl_data_type': 'scope',#'cater',
            'pl_sub_type': '',
            'pl_price_section': '0+',
            'pl_sort_type': 'data_type',
            'pl_sort_rule': 0,
            'pl_discount2_section': '0,+',
            'pl_groupon_section': '0,+',
            'pl_cater_book_pc_section': '0,+',
            'pl_hotel_book_pc_section': '0,+',
            'pl_ticket_book_flag_section': '0,+',
            'pl_movie_book_section': '0,+',
            'pl_business_type': 'scope',#'cater',
            'pl_business_id': '',
            'u_loc': '12931406,4885246',
            'ie': 'utf-8',

            'da_src': 'pcmappg.poi.page',
            'on_gel': 1,
            'src': 7,
            'gr': 3,
            'l': 21,#12.639712187500002
            'rn': 50,
            'tn': 'B_NORMAL_MAP',
            'b': '(12925857.482151808,4827933.7494841525;12981969.788073862,4854018.166951579)',
            't': 1529135387071
        }

        params['t'] = '{:.0f}'.format(time.time() * 1000)
        params['wd'] = wd
        params['b'] = bound
        params['nn'] = nn
        # 随机选取地理位置
        params['u_loc'] = '%s,%s' % (random.randint(12834000,13091000), random.randint(4720000,5006000))

        return params

    def __calc_subarea(self, bound, partition_num=SUBBINS, padding=SUBBIN_PADDING):
        """
        对搜索区域进行分割,分割成 partition_num * partition_num 个子区域
        :param bound:               被分割的区域边界
        :param partition_num:       长、宽等分成partition_num个
        :return:                    分割后的各子区域的边界列表
        """
        area_bound = bound[1:-1].split(";")

        # 左下角地理坐标
        lb = list(map(float, area_bound[0].split(',')))
        # 右上角地理坐标
        rt = list(map(float, area_bound[1].split(',')))
        
        # 计算每个子区域的长与宽
        x_bandwidth = (rt[0] - lb[0]) / partition_num
        y_bandwidth = (rt[1] - lb[1]) / partition_num

        # 向上、向右进行扩展，防止由于坐标变形而导致每个区域之间有缝隙
        x_padding = padding if x_bandwidth / 2 > padding else x_bandwidth / 2
        y_padding = padding if y_bandwidth / 2 > padding else y_bandwidth / 2

        # 返回各个子区域的bound
        for i in range(partition_num):
            for j in range(partition_num):
                sub_lb_0 = lb[0] + i * x_bandwidth
                sub_lb_1 = lb[1] + j * y_bandwidth                

                sub_rt_0 = lb[0] + (i+1) * x_bandwidth + x_padding
                sub_rt_1 = lb[1] + (j+1) * y_bandwidth + y_padding

                sub_area_bound = "(%s,%s;%s,%s)" % (sub_lb_0, sub_lb_1, sub_rt_0, sub_rt_1)
                yield sub_area_bound

    def error(self, poi_request):
        """
        错误处理，发生错误时，若该请求的失败次数小于一定的阈值，则将请求重新加入任务队列
        :param poi_request: 请求

        """
        poi_request.fail_time += 1
        
        if poi_request.fail_time <= MAX_FAILED_TIME:
            self.request_queue.put(poi_request, redis_key=self.redis_key)
        else: # 错误次数达到最大值时，不再发送该请求
            logger.error('错误次数达到最大值时，不再发送该请求, request > %s\nparams:%s' % (MAX_FAILED_TIME, poi_request.params))

def init_queue(init_bound, redis_key=REDIS_KEY_POIREQUEST_BD):
    """
    初始化任务队列，注意，若该任务队列存在，则会先清空该任务队列
    :param init_bound:      城市边界
    :param redis_key:       redis key
    """
    request_queue = RedisQueue()
    request_queue.clear(redis_key=redis_key)
    url = 'http://map.baidu.com'

    baidu_poi_wd = list()
    with open('./data/classification_poi', 'r') as f:
        baidu_poi_wd = f.readlines()
        baidu_poi_wd = [wd.replace("\n", '') for wd in baidu_poi_wd]

    for poi_type in  baidu_poi_wd:
        params = {
            'newmap': 1,
            'reqflag': 'pcmap',
            'biz': 1,
            'from': 'webmap',
            'da_par': 'direct',
            'pcevaname': 'pc4.1',
            'qt': 'spot',
            'from': 'webmap',
            # 'c': 131,  
            'wd': '美食',
            'wd2': '',
            'pn': 0,
            'nn': 0,
            'db': 0,
            'sug': 0,
            'addr': 0,
            'pl_data_type': 'scope',#'cater',
            'pl_sub_type': '',
            'pl_price_section': '0+',
            'pl_sort_type': 'data_type',
            'pl_sort_rule': 0,
            'pl_discount2_section': '0,+',
            'pl_groupon_section': '0,+',
            'pl_cater_book_pc_section': '0,+',
            'pl_hotel_book_pc_section': '0,+',
            'pl_ticket_book_flag_section': '0,+',
            'pl_movie_book_section': '0,+',
            'pl_business_type': 'scope',#'cater',
            'pl_business_id': '',
            'u_loc': '12931406,4885246',
            'ie': 'utf-8',

            'da_src': 'pcmappg.poi.page',
            'on_gel': 1,
            'src': 7,
            'gr': 3,
            'l': 21,#12.639712187500002
            'rn': 50,
            'tn': 'B_NORMAL_MAP',
            'b': '(12925857.482151808,4827933.7494841525;12981969.788073862,4854018.166951579)',
            't': 1529135387071
        }

        params['t'] = '{:.0f}'.format(time.time() * 1000)
        params['wd'] = poi_type
        params['b'] = init_bound
        params['nn'] = 0

        poi_request = POIRequest(url, params, callback=None, need_proxy=NEED_PROXY)

        request_queue.put(poi_request, redis_key=redis_key)

def schedule(thread_num=THREAD_NUM):
    """
    任务调度
    :param thread_num 线程个数
    """

    request_queue = RedisQueue()

    threads = list()
    for i in range(thread_num):
        threads.append(Spider(request_queue))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

lock = threading.Lock()
if __name__ == '__main__':

    fake_user = util.get_fake_user(IP_FILE, UA_FILE)

    # 初始化搜索区域
    init_bound = '(12834000.0,4720000.0;13091000.0,5006000.0)'
    # 注意，若该任务队列存在，本操作会先清空该任务队列
    init_queue(init_bound, redis_key=REDIS_KEY_POIREQUEST_BD)

    schedule(thread_num=THREAD_NUM)
