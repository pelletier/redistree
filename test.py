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
    '/A/c/hello',
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

    def test_create(self):
        """
        Test simple root node creation
        """
        data={'name': 'root', 'type': 'folder'}
        root = self.tree.create(
            mount="user#12",
            path="/",
            data=data
        )
        self.assertEqual(root[0], "path:/user#12/ROOT")    
        self.assertEqual(root[1]['name'], data['name'])    


    def test_create_double(self):
        data={'name': 'root', 'type': 'folder'}
        root = self.tree.create(
            mount="user#12",
            path="/",
            data=data
        )
        root = self.tree.create(
            mount="user#12",
            path="/",
            data=data
        )
       

    def test_create_unique(self):
        root = self.tree.create(
            mount="user#12",
            path="/",
        )
        a = self.tree.create(
            mount="user#12",
            path="/a",
        )
        a = self.tree.create(
            mount="user#12",
            path="/a",
        )
        self.assertEqual(a[0], "path:/user#12/ROOT/a(1)")

    def test_create_unicode_path(self):
        """
        Test unicode path creation
        """
        root = self.tree.create(
            mount="user#12",
            path="/",
            data={}
        )
        d1 = self.tree.create(
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
            root = self.tree.create(
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
            root = self.tree.create(
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
            root = self.tree.create(
                    mount=u"user#12",
                    path="/../",
                    data={}
            )
        except InvalidPath:
            self.assertTrue(True)
        else:
            self.assertTrue(False)


    def test_cplx_tree_creation(self):
        """
        Several path
        """
        for p in PATHS_1:
            self.tree.create(
                mount=u"user#13",
                path=p,
                data={}
            )
        self.assertTrue(True)


class TestRedisTreeDelete(unittest.TestCase):

    def setUp(self):
        "set up Test db redis (9)"
        self.redis_inst = redis.Redis(db=9)
        self.redis_inst.flushdb()
        self.tree = RedisTree(redis_instance=self.redis_inst)

    def test_delete(self):
        """
        Test simple root node deletion
        """
        mount = "user#14"
        for p in PATHS_1:
            self.tree.create(mount=mount, path=p)

        delete = self.tree.delete(mount, '/')
 
        children = self.tree.get_children(
            mount=mount,
            path='/'
        )
        self.assertEqual(len(children), 3)

        for child in children:
            self.assertEqual(children[child]['visible'], False)

        children = self.tree.get_children(
            mount=mount,
            path='/B/a/x',
        )
        self.assertEqual(len(children), 1)
        for child in children:
            self.assertEqual(children[child]['visible'], False)


    def test_delete_show_deleted(self):
        """
        Test simple root node deletion with show_deleted=False
        """
        mount = "user#14"
        for p in PATHS_1:
            self.tree.create(mount=mount, path=p)

        delete = self.tree.delete(mount, '/')
 
        children = self.tree.get_children(
            mount=mount,
            path='/',
            show_deleted=False,
        )
        self.assertEqual(len(children), 0)
 
        children = self.tree.get_children(
            mount=mount,
            path='/B/a/x',
            show_deleted=False,
        )
        self.assertEqual(len(children), 0)



class TestRedisTreeGetChildren(unittest.TestCase):
    """
    Test children listing
    """

    def setUp(self):
        "set up Test db redis (9)"
        self.redis_inst = redis.Redis(db=9)
        self.redis_inst.flushdb()
        self.tree = RedisTree(redis_instance=self.redis_inst)
 
    def test_children(self):
        mount = "user#13"
        for p in PATHS_1:
            self.tree.create( mount=mount, path=p)

        children = self.tree.get_children(
            mount=mount,
            path='/A'
        )
        self.assertEqual(len(children), 3)

        children = self.tree.get_children(
            mount=mount,
            path='/B/a/x/ooo'
        )
        self.assertEqual(len(children), 1)

        # Does not exist
        try:
            children = self.tree.get_children(
                mount=mount,
                path='/mm'
            )
            self.assertEqual(len(children), 1)

        except NodeDoesNotExist:
            self.assertTrue(True)
        else:
            self.assertTrue(False)
 


class TestRedisTreeMove(unittest.TestCase):
    """
    Test children listing
    """

    def setUp(self):
        "set up Test db redis (9)"
        self.redis_inst = redis.Redis(db=9)
        self.redis_inst.flushdb()
        self.tree = RedisTree(redis_instance=self.redis_inst)

        self.mount = "user#13"
        for p in PATHS_1:
            self.tree.create(mount=self.mount, path=p)

         
    def test_move(self):
        """
        Move /A in /B/a/z
        so the we will end up with /B/a/z/a/  /B/a/z/b/  /B/a/z/c/  /B/a/z/c/hello/
        """
        move = self.tree.move(self.mount, '/A/a', self.mount, '/B/a/z')
        children = self.tree.get_children(
            mount=self.mount,
            path='/B/a/z'
        )
        print children
        self.assertEqual(len(children), 3)
