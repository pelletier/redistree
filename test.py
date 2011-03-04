#-*- coding: utf-8 -*-

import unittest
import json
 
from redistree import *

class TestRedisTreeCreate(unittest.TestCase):

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
        self.assertEqual(root[0], u"path:/user#12/ROOT/é")    
 
    def test_create_multi_path(self):
        """
        Test long path
        """
        root = self.tree.create_node(
            mount=u"user#12",
            path="/aaa/bbb/ccc/ddd",
            data={}
        )
        self.assertEqual(root[0], u"path:/user#12/ROOT/aaa/bbb/ccc/ddd")    
    

    def test_create_wrong_path(self):
        """
        Test faulty path creation
        """
        try:
            root = self.tree.create_node(
                    mount=u"user#12",
                    path="///",
                    data={}
            )
            print root
        except InvalidPath:
            self.assertTrue(True)
        else:
            self.assertTrue(False)

    def test_create_wrong_path2(self):
        """
        Test faulty path creation (relative not allowed)
        """
        try:
            root = self.tree.create_node(
                    mount=u"user#12",
                    path="/../",
                    data={}
            )
            print root
        except InvalidPath:
            self.assertTrue(True)
        else:
            self.assertTrue(False)


   
class TestRedisTreeGetChildren(unittest.TestCase):
    """
    Test children listing
    """

    def setUp(self):
        "set up Test db redis (9)"
        self.redis_inst = redis.Redis(db=9)
        self.redis_inst.flushdb()
        self.tree = RedisTree(redis_instance=self.redis_inst)


    def test_children_on_long_path(self):
        """
        Test long path
        """
        mount=u"user#12"
        path="/aaa/bbb/ccc/ddd"

        root = self.tree.create_node(
            mount=mount,
            path=path,
            data={}
        )

        children = self.tree.get_children(
            mount=mount,
            path=path
        )


        print children