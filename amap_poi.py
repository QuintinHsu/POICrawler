#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Author: Quintin Xu
# @Date:   2018-07-12 08:57:12
# @Last Modified by:   Quintin Xu
# @Last Modified time: 2018-07-14 17:22:58
# @E-mail: QuintinHsu@gmail.com
# @Description: 抓取高德POI，高德POI每次请求最多返回30条数据，page_num从1开始，需要设置城市代码和城市边界

import time
import json
import random
import logging
import logging.config
import cloghandler
import threading
import requests
import copy

from core.db import RedisQueue, POIMySQL
from core.request import POIRequest
from core import util

from config.config import *

logging.config.fileConfig('./config/logging_amap.conf')
logger = logging.getLogger(__name__)

class Spider(threading.Thread):
    """爬虫"""
    def __init__(self, request_queue):
        threading.Thread.__init__(self)
        self.request_queue = request_queue
        self.db = POIMySQL()
        self.redis_key = REDIS_KEY_POIREQUEST_GD

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
        time.sleep(SLEEP_TIME_GD)

        proxy = None
        headers = {
            'upgrade-insecure-requests': 1,
            'Accept-Encoding': u'gzip, deflate, br',
            'Accept-Language': u'zh-CN,zh;q=0.9',
            'Accept': u'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'
        }       
        try:
            # 随机获取 proxy 和 ua
            global fake_user
            # 加锁
            if lock.acquire():
                fake_user['counter'] += 1

                # 总的请求大于40W次时，更新fake_user池
                if fake_user['counter'] > 4000000:
                    fake_user = util.get_fake_user(IP_FILE, UA_FILE)

                fu = random.choice(fake_user['fu'])

                headers['User-Agent'] = fu['ua']
                proxy = copy.deepcopy(fu['proxy'])
                cookies = fu['cookies']
                fu['counter'] += 1

                poi_request.headers = headers
                poi_request.cookies = cookies
                poi_request.proxies = proxy
                
                lock.release()            

            response = requests.get(url=poi_request.url, params=poi_request.params, 
                headers=headers, cookies=cookies, timeout=poi_request.timeout, allow_redirects=False, proxies=proxy)

            return response, poi_request
        except Exception as e:
            logger.error('请求失败，\nparams:%s\nua:%s\nproxy:%s\ncookies:%s' % (poi_request.params, headers['User-Agent'], proxy, cookies), 
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
            
            if response_json and response_json['status'] == '1':
                if response_json['data']['result'] == 'true' and int(response_json['data']['total']) > 0:
                    page_num = int(response_json['searchOpt']['pageIndex'])
                    page_size = int(response_json['searchOpt']['pageSize'])
                    poi_total = int(response_json['data']['total'])

                    if poi_total > 200: #在某个区域搜索POI大于200个，则对该区域进行分割，并构造每个子区域的请求                            
                        logger.info('对bound进行分割，keywords:%s, bound:%s, page_num:%s, total:%s' % (params['keywords'], params['geoobj'], params['pagenum'], poi_total))
                        sub_area_bounds = self.__calc_subarea(params['geoobj'])
                        for sub_area_bound in sub_area_bounds:
                            sub_params = self.__construct_params(params['keywords'], sub_area_bound, params['city'], 1)
                            poi_request = POIRequest(url=poi_request.url, params=sub_params, callback=self.parse_json, 
                                need_proxy=poi_request.need_proxy)
                            yield poi_request
                    else:
                        poi_list = response_json['data']['poi_list']
                        if len(poi_list) < GD_PAGE_SIZE: # 当前返回的数据个数小于page_size，则说明是最后一页数据
                            logger.info('返回最后一页POI数据，keywords:%s, bound:%s, page_num:%s, total:%s' % (params['keywords'], params['geoobj'], params['pagenum'], poi_total))
                            yield poi_list
                        else:                                
                            logger.info('返回最后一页的POI数据，请求下一页数据，keywords:%s, bound:%s, page_num:%s, total:%s' % (params['keywords'], params['geoobj'], params['pagenum'], poi_total))
                            next_page_params = self.__construct_params(params['keywords'], params['geoobj'], params['city'], page_num + 1)
                            poi_request = POIRequest(url=poi_request.url, params=next_page_params, callback=self.parse_json, need_proxy=poi_request.need_proxy)
                            yield (poi_request, poi_list)

                else:
                    logger.info('该区域没有该类型的POI数据，keywords:%s, bound:%s, page_num:%s\nresponse_data:%s' % (params['keywords'], params['geoobj'], params['pagenum'], response_json['data']))
                
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
            if 'id'in d and d['id'] and d['id'] != 'null':
                content = json.dumps(d)
                reconstruct_data.append([d['id'], content, int(time.time())])
        self.db.insert_many('gd_poi', reconstruct_data)


    def __construct_params(self, wd, bound, city, nn):
        """
        构造请求参数
        :param wd:      搜索关键词
        :param bound:   搜索区域
        :param nn:      poi数据的起始索引（百度每页最多只有50条数据，假设搜索结果中有200个poi，若nn=30，则本次请求返回第30-79这50个poi数据）
        :return:        构造的请求参数
        """
        params = {
            'query_type': 'TQUERY',
            'pagesize': GD_PAGE_SIZE,
            'pagenum': 1,
            'qii': 'true',
            'cluster_state': 5,
            'need_utd': 'false',
            'utd_sceneid': 1000,
            'div': 'PC1000',
            'addr_poi_merge': 'true',
            'is_classify': 'true',
            'zoom': 18,
            'city': 110000,
            'geoobj': '115.281974|39.172454|117.590798|41.142945',
            '_src': 'around',
            'SPQ': 'true',
            'keywords': '美食'
        }

        params['keywords'] = wd
        params['geoobj'] = bound
        params['pagenum'] = nn
        params['city'] = city

        return params

    def __calc_subarea(self, bound, partition_num=SUBBINS, padding=SUBBIN_PADDING_GD):
        """
        对搜索区域进行分割,分割成 partition_num * partition_num 个子区域
        :param bound:               被分割的区域边界
        :param partition_num:       长、宽等分成partition_num个
        :return:                    分割后的各子区域的边界列表
        """
        area_bound = list(map(float, bound.split("|")))

        # 左下角地理坐标
        lb = area_bound[0:2]
        # 右上角地理坐标
        rt = area_bound[2:4]
        
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

                sub_area_bound = "%s|%s|%s|%s" % (sub_lb_0, sub_lb_1, sub_rt_0, sub_rt_1)
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

def init_queue(init_city, init_bound, redis_key=REDIS_KEY_POIREQUEST_GD):
    """
    初始化任务队列，注意，若该任务队列存在，则会先清空该任务队列
    :param init_city:       城市代码
    :param init_bound:      城市边界
    :param redis_key:       redis key
    """
    request_queue = RedisQueue()
    request_queue.clear(redis_key=redis_key)
    url = 'https://ditu.amap.com/service/poiInfo'

    baidu_poi_wd = list()
    with open('./data/classification_poi', 'r') as f:
        baidu_poi_wd = f.readlines()
        baidu_poi_wd = [wd.replace("\n", '') for wd in baidu_poi_wd]

    for poi_type in  baidu_poi_wd:
        params = {
            'query_type': 'TQUERY',
            'pagesize': GD_PAGE_SIZE,
            'pagenum': 1,
            'qii': 'true',
            'cluster_state': 5,
            'need_utd': 'false',
            'utd_sceneid': 1000,
            'div': 'PC1000',
            'addr_poi_merge': 'true',
            'is_classify': 'true',
            'zoom': 18,
            'city': 110000,
            'geoobj': '115.281974|39.172454|117.590798|41.142945',
            '_src': 'around',
            'SPQ': 'true',
            'keywords': '美食'
        }

        params['keywords'] = poi_type
        params['geoobj'] = init_bound
        params['pagenum'] = 1
        params['city'] = init_city

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

    # 初始化搜索区域和城市
    init_bound = '115.281974|39.172454|117.590798|41.142945'
    init_city = 110000
    # 注意，若该任务队列存在，本操作会先清空该任务队列
    init_queue(init_city, init_bound, redis_key=REDIS_KEY_POIREQUEST_GD)

    schedule(thread_num=THREAD_NUM)
