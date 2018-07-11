#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Author: Quintin Xu
# @Date:   2018-07-10 15:50:11
# @Last Modified by:   Quintin Xu
# @Last Modified time: 2018-07-11 09:39:24
# @E-mail: QuintinHsu@gmail.com
# @Description: 
import logging
from redis import StrictRedis
import pymysql

from core.request import POIRequest
from config.config import *
logger = logging.getLogger(__name__)

class RedisQueue(object):
    """Redis 任务队列"""
    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD):
        self.db = StrictRedis(host=host, port=port, password=password)

    def put(self, request, redis_key):
        """
        将Request任务存入Redis中
        :param request
        :param redis_key
        :return bool        是否存成功
        """

        if isinstance(request, POIRequest):
            args = request.args2str()
            logging.info('New Task, params: %s' % args)
            return self.db.rpush(redis_key, args)

        return False

    def get(self, redis_key, timeout=None):
        """
        从Redis中获取Request任务
        :param redis_key
        :param timeout      等待时间
        :return POIRequest  Request
        """
        args = self.db.blpop(redis_key, timeout=timeout)[1]
        args = args.decode()

        logger.info('RedisQueue, new task, args:%s' % args)     
        args = eval(args)

        return POIRequest(url=args['url'], params=args['params'], need_proxy=args['need_proxy'], 
            fail_time=args['fail_time'], timeout=args['timeout'])

    def clear(self, redis_key):
        """
        清空任务队列
        :param redis_key
        """
        self.db.delete(redis_key)

    def empty(self, redis_key):
        """
        判断任务队列是否为空
        :param redis_key
        :return bool
        """
        return self.db.llen(redis_key) == 0
        
class POIMySQL(object):
    """Mysql 数据库"""
    def __init__(self, host=MYSQL_HOST, username=MYSQL_USER, password=MYSQL_PASSWORD, 
        port=MYSQL_PORT, database=MYSQL_DATABASE):
        """
        MySQL初始化
        :param host
        :param username
        :param password
        :param port
        :param database
        """
        try:
            self.db = pymysql.connect(host, username, password, database, charset='utf8', port=port)
            self.cursor = self.db.cursor()
        except Exception as e:
            logging.error(e.args, exc_info=True)

    def insert(self, table, data):
        """
        插入数据
        :param table
        :param data
        """
        # sql = 'INSERT INTO `%s`(`uid`, `raw`, `ts`)' % (table) +' values (%s, %s, %s)'
        sql = "INSERT INTO `bd_poi`(`uid`, `raw`, `ts`) VALUES (%s,COMPRESS(%s), %s) ON DUPLICATE KEY UPDATE `raw`=VALUES(`raw`),`ts`=VALUES(`ts`)"
        try:
            self.cursor.execute(sql, data)
            self.db.commit()
        except Exception as e:
            logging.error(e.args, exc_info=True)
            self.db.rollback()

    def insert_many(self, table, data):
        """
        批量插入数据
        :param table
        :param data
        """
        # sql = 'INSERT INTO `%s`(`uid`, `raw`, `ts`)' % (table) +' values (%s, %s, %s)'
        sql = "INSERT INTO `%s`(`uid`, `raw`, `ts`)" % (table) +" VALUES (%s,COMPRESS(%s), %s) ON DUPLICATE KEY UPDATE `raw`=VALUES(`raw`),`ts`=VALUES(`ts`)"
        try:
            self.cursor.executemany(sql, data)
            self.db.commit()
        except Exception as e:
            logging.error(e.args, exc_info=True)
            self.db.rollback()
        
