#-*- coding: utf-8 -*-

import unittest
import json
 
from redistree import *

class TestRedisTreeBasic(unittest.TestCase):

    def setUp(self):
        "set up Test db redis (9)"
        self.redis_inst = redis.Redis(db=9)
        self.redis_inst.flushdb()
        self.tree = RedisTree(redis_instance=self.redis_inst)

    def test_create_node(self):
        """
        Test simple root node creation
        """
        data={'name': 'root', 'type': 'folder'}
        root = self.tree.create_node(
            mount="user#12",
            path="/",
            data=data
        )
        self.assertEqual(root[0], "path:/user#12/ROOT")    
        self.assertEqual(root[1]['name'], data['name'])    



    def test_create_unicode_path(self):
        """
        Test unicode path creation
        """
        root = self.tree.create_node(
            mount=u"user#12",
            path="/é",
            data={}
        )
        self.assertEqual(root[0], u"path:/user#12/é")    
 