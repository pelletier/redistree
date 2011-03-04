#-*- coding: utf-8 -*-

import unittest
import json
 
from redistree import *


PATHS_1 = [
    '/',
    '/A',
    '/B',
    '/C',
    '/A/a',
    '/A/b',
    '/A/c',
    '/B/a',
    '/B/a/x',
    '/B/a/y',
    '/B/a/x/ooo',
    '/B/a/x/ooo/m',
    '/B/a/z',
]

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
            mount="user#12",
            path="/",
            data={}
        )
        d1 = self.tree.create_node(
            mount=u"user#12",
            path="/é",
            data={}
        )
        self.assertEqual(d1[0], u"path:/user#12/ROOT/é")    
 
    def test_create_multi_path(self):
        """
        Test long path
        """
        try:
            root = self.tree.create_node(
                mount=u"user#12",
                path="/aaa/bbb/ccc/ddd",
                data={}
            )
        except NoParent:
            self.assertTrue(True)
        else:
            self.assertTrue(False)    

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


    def test_cplx_tree_creation(self):
        """
        Several path
        """
        for p in PATHS_1:
            self.tree.create_node(
                mount=u"user#13",
                path=p,
                data={}
            )
        self.assertTrue(True)


   
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
        mount = "user#13"
        for p in PATHS_1:
            self.tree.create_node(
                mount=mount,
                path=p,
                data={}
            )
  
        # Test children of /aaa/bbb/ccc/ddd is []
        children = self.tree.get_children(
            mount=mount,
            path='/A'
        )
        print children
        self.assertEqual(len(children), 3)

 