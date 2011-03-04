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
        move = self.tree.move(self.mount, '/A', self.mount, '/B/a/z')
        children = self.tree.get_children(
            mount=self.mount,
            path='/B/a/z'
        )
        self.assertEqual(len(children), 3)
        
        try:
            children = self.tree.get_children(
                mount=self.mount,
                path='/A'
            )
        except NodeDoesNotExist:
            self.assertTrue(True)
        else:
            self.assertTrue(False)


    def test_move_to_self_children(self):
        """
        Move /A in /A/b
        Should fail
        """
        try:
            move = self.tree.move(self.mount, '/A', self.mount, '/A/b')
        except InvalidMoveOperation:
            self.assertTrue(True)
        else:
            self.assertTrue(False) 


class TestRedisTreeInfo(unittest.TestCase):
    """
    Test the information retrieval.
    """

    def setUp(self):
        self.redis = redis.Redis(db=9)
        self.redis.flushdb()
        self.tree = RedisTree(redis_instance=self.redis)

    def test_get_info(self):
        i = {
            'date': '01/01/2001',
            'author': 'me',
        }

        self.tree.create('user#18', '/', i)
        r = self.tree.get_info('user#18', '/')
        self.assertEqual(r, i)

    def test_get_info_utf8(self):
        i = {
            'date': '01/01/2001',
            'author': u'mé',
        }

        self.tree.create('user#18', '/', i)
        r = self.tree.get_info('user#18', '/')
        self.assertEqual(r, i)


class TestRedisTreeSymlinks(unittest.TestCase):
    """
    Add symlinks for shares support.
    """

    def setUp(self):
        self.redis = redis.Redis(db=9)
        self.redis.flushdb()
        self.tree = RedisTree(redis_instance=self.redis)
        dirs = {
            'user#1': [
                '/',
                '/A',
                '/A/B',
                '/A/B/C',
                '/A/B/C/D'
            ],
            'user#2': [
                '/',
                '/F',
                '/F/G',
            ]
        }
        for m, ds in dirs.iteritems():
            for d in ds:
                self.tree.create(m, d)

        self.p, self.d = self.tree.link('user#2', '/F/H', 'user#1', '/A/B')

    def test_create_link(self):
        """
        user#1/A/B/C/D
        user#2/F/G
              /F/H
        link user#2/F/H -> user#1/A/B
        """ 
        results = len(self.tree.get_children('user#2', '/F/H{path:/user#1/ROOT/A/B}'))
        self.assertEqual(results, 1)

        results = len(self.tree.get_children('user#2', '/F/H{path:/user#1/ROOT/A/B}/C'))
        self.assertEqual(results, 1)

    def test_create_link_type(self):
        self.assertEqual(self.d['type'], 'link')
        self.assertEqual(self.d, json.loads(self.redis.get(self.p)))


    def test_delete_in_link(self):

        self.tree.delete('user#1', '/A/B/C')

        r = self.tree.get_children('user#2', '/F/H{path:/user#1/ROOT/A/B}')

        self.assertEqual(len(r), 1)
        self.assertEqual(r[u'path:/user#2/ROOT/F/H{path:/user#1/ROOT/A/B}/C']['visible'], False)

