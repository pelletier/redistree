from time import time
from unittest import TestCase
import redis
from redistree import RedisTree


class TestCreateRedisTree(TestCase):

    def test_create(self):
        o = RedisTree()
        self.assertTrue(o.r)

    def test_create_with_pool(self):
        rc = redis.Redis()
        o = RedisTree(connection_pool=rc.connection_pool)
        self.assertEqual(rc.connection_pool, o.pool)

class TestInit(TestCase):
    def test_init_fs(self):
        rt = RedisTree()
        rt.r.flushdb()
        rt.init_fs()
        r = rt.r
        self.assertEqual(r.get('NODE_COUNTER'), "-9223372036854775807"),
        self.assertEqual(r.hgetall('NODE:-9223372036854775807'), {'name':'root'})

class InitRedisTreeCase(TestCase):

    def setUp(self):
        self.rt = RedisTree()
        self.rt.r.flushdb()
        self.rt.init_fs()

class TestNodes(InitRedisTreeCase):

    def test_get_root_node_from_path(self):
        self.assertEqual(self.rt.get_node_at_path('/'), self.rt.ROOT_NODE)

    def test_create_child_node(self):
        self.rt.create_child_node('/foo')
        self.rt.create_child_node('/foo/bar')
        node = self.rt.create_child_node('/foo/bar/alice', {'size': '1TB'})
        self.rt.create_child_node('/foo/bob')

        self.assertEqual(self.rt.get_node_at_path('/foo/bar/alice'), node)

    def test_create_deep_child_node(self):
        n = 100
        path = ''
        uid = None

        for i in xrange(n):
            path = path + '/foo'
            uid = self.rt.create_child_node(path)

        # Make sure the look up is not too slow.
        start = time()
        self.assertEqual(self.rt.get_node_at_path(path), uid)
        elapsed = time() - start
        self.assertTrue(elapsed < 0.02)

    def test_create_inside_symlink(self):
        self.rt.create_child_node('/foo')
        self.rt.create_child_node('/foo/bar')
        self.rt.create_symlink('/foo/bar', '/me')

        self.assertEqual({}, self.rt.get_children('/foo/bar'))

        uid = self.rt.create_child_node('/me/bob')

        self.assertEqual({'bob': uid}, self.rt.get_children('/foo/bar'))

    def test_get_root_info(self):
        self.assertEqual(self.rt.get_node_info(self.rt.ROOT_NODE), {'name': 'root'})

    def test_move_leaf_node_same_dir(self):
        self.rt.create_child_node('/foo')
        self.rt.create_child_node('/foo/bar')
        orig_node = self.rt.create_child_node('/foo/bar/bob')

        self.assertEqual(self.rt.get_node_at_path('/foo/bar/bob'), orig_node)

        self.rt.move_node('/foo/bar/bob', '/foo/bar/me')

        self.assertRaises(Exception, self.rt.get_node_at_path, '/foo/bar/bob')
        self.assertEqual(self.rt.get_node_at_path('/foo/bar/me'), orig_node)

    def test_move_leaf_node_parent_dir(self):
        self.rt.create_child_node('/foo')
        self.rt.create_child_node('/foo/bar')
        orig_node = self.rt.create_child_node('/foo/bar/bob')

        self.assertEqual(self.rt.get_node_at_path('/foo/bar/bob'), orig_node)

        self.rt.move_node('/foo/bar/bob', '/foo/me')

        self.assertRaises(Exception, self.rt.get_node_at_path, '/foo/bar/bob')
        self.assertEqual(self.rt.get_node_at_path('/foo/me'), orig_node)

    def test_get_children(self):
        expected = {}
        self.rt.create_child_node('/foo')
        expected['bar'] = self.rt.create_child_node('/foo/bar')
        expected['bob'] = self.rt.create_child_node('/foo/bob')
        expected['alice'] = self.rt.create_child_node('/foo/alice')
        children = self.rt.get_children('/foo')

        self.assertEqual(expected, children)

    def test_get_children_of_missing_node(self):
        self.assertEqual({}, self.rt.get_children('/nobody'))

    def test_get_children_root_init(self):
        # Yay, root exists only when the first child is created (we can't store
        # empty hashes in Redis).
        self.assertEqual(self.rt.get_children('/'), {})
        self.rt.create_child_node('/foo')
        self.assertTrue('foo' in self.rt.get_children('/').keys())

    def test_create_symlink(self):
        self.rt.create_child_node('/foo')
        self.rt.create_child_node('/foo/bar')
        bob_id = self.rt.create_child_node('/foo/bar/bob')
        self.rt.create_symlink('/foo/bar', '/alice')
        self.assertEqual(bob_id, self.rt.get_node_at_path('/alice/bob'))

    def test_create_deep_child_node(self):
        n = 100
        path = ''

        for i in xrange(n):
            path = path + '/foo'
            self.rt.create_child_node(path)

        uid = self.rt.create_child_node(path + '/foo')
        self.rt.create_symlink(path, '/shortcut')

        # Make sure the look up is not too slow.
        start = time()
        self.assertEqual(self.rt.get_node_at_path('/shortcut/foo'), uid)
        elapsed = time() - start
        self.assertTrue(elapsed < 0.002)

    def test_delete_simple(self):
        self.rt.create_child_node('/foo')
        self.rt.create_child_node('/foo/bar')
        self.rt.create_child_node('/foo/bar/bob')
        self.rt.create_child_node('/foo/bar/alice')
        # / /foo /foo/bar
        self.assertEqual(len(self.rt.r.keys("TREE:*")), 3)
        self.assertEqual(len(self.rt.r.keys("NODE:*")), 5)

        self.rt.delete_node('/foo/bar')
        # /foo lost its only child, so Redis deletes the key.
        self.assertEqual(len(self.rt.r.keys("TREE:*")), 1)
        self.assertEqual(len(self.rt.r.keys("NODE:*")), 2)

        self.assertEqual(self.rt.get_children('/foo'), {})

    def test_delete_leaf(self):
        self.rt.create_child_node('/foo')
        self.assertEqual(len(self.rt.r.keys("TREE:*")), 1)
        self.assertEqual(len(self.rt.r.keys("NODE:*")), 2)
        self.rt.delete_node('/foo')
        self.assertEqual(len(self.rt.r.keys("TREE:*")), 0)
        self.assertEqual(len(self.rt.r.keys("NODE:*")), 1)

    def test_delete_deep(self):
        n = 100
        path = ''
        uid = None

        for i in xrange(n):
            path = path + '/foo'
            uid = self.rt.create_child_node(path)

        self.assertEqual(len(self.rt.r.keys("TREE:*")), n)
        self.assertEqual(len(self.rt.r.keys("NODE:*")), n + 1)

        start = time()
        self.rt.delete_node('/foo')
        elapsed = time() - start

        self.assertEqual(len(self.rt.r.keys("TREE:*")), 0)
        self.assertEqual(len(self.rt.r.keys("NODE:*")), 1)

        self.assertTrue(elapsed < 0.06)

    def test_delete_in_symlink(self):
        self.rt.create_child_node('/foo')
        self.rt.create_child_node('/foo/bar')
        self.rt.create_child_node('/foo/bar/bob')
        self.rt.create_symlink('/foo/bar', '/me')

        self.assertEqual(len(self.rt.r.keys("TREE:*")), 3)
        self.assertEqual(len(self.rt.r.keys("NODE:*")), 5)

        self.rt.delete_node('/me/bob')

        self.assertEqual(len(self.rt.r.keys("TREE:*")), 2)
        self.assertEqual(len(self.rt.r.keys("NODE:*")), 4)


    def test_real_path(self):
        self.rt.create_child_node('/foo')
        self.rt.create_child_node('/foo/bar')
        self.rt.create_child_node('/foo/bar/bob')
        self.rt.create_symlink('/foo/bar', '/me')
        self.assertEqual('/foo/bar/bob', self.rt.get_real_path('/foo/bar/bob'))
        self.assertEqual('/foo/bar/bob', self.rt.get_real_path('/me/bob'))

        self.assertEqual('/', self.rt.get_real_path('/'))
        self.assertEqual('/me', self.rt.get_real_path('/me'))


    def test_copy_node(self):
        expected = {}
        self.rt.create_child_node('/foo')
        expected['bar'] = self.rt.create_child_node('/foo/bar')
        expected['alice'] = self.rt.create_child_node('/foo/alice')
        self.rt.create_child_node('/foo/bar/bob')

        self.assertEqual(len(self.rt.r.keys("TREE:*")), 3)
        self.assertEqual(len(self.rt.r.keys("NODE:*")), 5)
        self.assertEqual(self.rt.get_children('/foo'), expected)

        self.rt.copy_path('/foo', '/me')

        self.assertEqual(len(self.rt.r.keys("TREE:*")), 5)
        self.assertEqual(len(self.rt.r.keys("NODE:*")), 9)
        self.assertEqual(self.rt.get_children('/foo'), expected)



class TestSymlinks(InitRedisTreeCase):

    def test_create_symlink(self):
        expected = {}
        expected['foo'] = self.rt.create_child_node('/foo')
        bar_uid = self.rt.create_child_node('/foo/bar')
        expected['me'] = self.rt.create_symlink('/foo/bar', '/me')
        self.assertEqual(expected, self.rt.get_children('/'))
        info = self.rt.get_node_info(expected['me'])
        self.assertEqual(info['target'], '/foo/bar')
        self.assertEqual(info['target_node'], bar_uid)
        self.assertEqual(expected['me'], self.rt.get_node_at_path('/me'))

    def test_get_target(self):
        self.rt.create_child_node('/foo')
        self.rt.create_child_node('/foo/bar')
        self.rt.create_symlink('/foo/bar', '/me')
        self.assertEqual('/foo/bar', self.rt.get_target('/me'))

    def test_is_symlink(self):
        self.rt.create_child_node('/foo')
        self.rt.create_child_node('/foo/bar')
        self.rt.create_symlink('/foo/bar', '/me')
        self.assertTrue(self.rt.is_symlink('/me'))
        self.assertFalse(self.rt.is_symlink('/foo/bar'))

    def test_get_children_symlink(self):
        expected = {}
        self.rt.create_child_node('/foo')
        expected['alice'] = self.rt.create_child_node('/foo/alice')
        expected['bob'] = self.rt.create_child_node('/foo/bob')
        self.rt.create_symlink('/foo', '/bar')
        self.assertEqual(self.rt.get_children('/foo'), expected)
        self.assertEqual(self.rt.get_children('/bar'), {})
