import posixpath
import redis


class RedisTree:
    """
    The Redis Tree implementation.

    Keys namespaces:
        * TREE: Keys part of the hierarchical structure.
        * NODE: Keys representing nodes.

    A link node must have:
        - target (containing the path it links to).
    """

    def __init__(self, connection_pool=None, redis_host='localhost',
                                             redis_port=6379,
                                             redis_db=0):


        self.ROOT_NODE = -9223372036854775808 + 1

        # Attach a connection pool if provided.
        if connection_pool:
            self.pool = connection_pool
        else:
            self.pool = redis.ConnectionPool(host=redis_host,
                                             port=redis_port,
                                             db=redis_db)

        self.r = redis.Redis(connection_pool=self.pool)


    def init_fs(self):
        """Create the root node and the ID counter, if they don't exist.
        By convention, the root node has an node id of 0."""

        # Redis store strings as base-10 64 bit signed integers, so we start at
        # the smallest possible number and we start counting. If the number is
        # an issue, we may decide to use multiple counters.
        self.r.setnx('NODE_COUNTER', "-9223372036854775808")
        self.create_node({'name': 'root'})

    def create_node(self, attributes, uid=None):
        if uid == None:
            uid = self.r.incr('NODE_COUNTER')

        self.r.hmset("NODE:%s" % uid, attributes)
        return uid

    def create_child_node(self, path, attributes=None):
        parent, name = posixpath.split(path)

        if attributes == None:
            attributes = {'name': name}

        new_uid = self.create_node(attributes)
        self.r.hset("TREE:%s" % parent, name, new_uid)
        return str(new_uid)


    def real_node(self, path):
        chunks = path.split('/')
        del chunks[0]
        if chunks[-1] == '':
            chunks.pop()

        current_path = '/'
        current_node = self.ROOT_NODE

        while True:
            # Check if we are arrived.
            if len(chunks) == 0:
                break # current_node contains the node number.

            # Check if the current node is a link.
            target, target_node = self.r.hmget("NODE:%s" % current_node, ['target', 'target_node'])

            if not target == None:
                current_path = target
                current_node = target_node
                continue

            # This is not a target and we are not arrived.
            next_chunk = chunks.pop(0)
            current_node = self.r.hget("TREE:%s" % current_path, next_chunk)
            if current_node == None:
                raise Exception("Broken path")
            if current_path == '/':
                current_path = ''
            current_path = '/'.join([current_path, next_chunk])

        return current_path, current_node

    def get_node_at_path(self, path):
        return self.real_node(path)[1]

    def get_real_path(self, path):
        return self.real_node(path)[0]

    def get_node_info(self, node_id):
        return self.r.hgetall("NODE:%s" % node_id)

    def move_node(self, orig_path, dest_path):
        parent, name = posixpath.split(orig_path)
        dest_parent, dest_name = posixpath.split(dest_path)

        pipe = self.r.pipeline()
        pipe.hget("TREE:%s" % parent, name)
        pipe.hgetall("TREE:%s" % orig_path)
        uid, content = pipe.execute()

        if uid == None:
            raise Exception("Broken path")

        pipe = self.r.pipeline()
        pipe.hdel("TREE:%s" % parent, name)
        pipe.hset("TREE:%s" % dest_parent, dest_name, uid)
        if content:
            pipe.delete("TREE:%s", orig_path)
            pipe.hmset("TREE:%s" % dest_path, content)
        pipe.execute()

    def get_children(self, path):
        result = self.r.hgetall("TREE:%s" % path)
        return result

    def create_symlink(self, target_path, path):
        target_node = self.get_node_at_path(target_path)
        return self.create_child_node(path, {
            'target': target_path,
            'target_node': target_node
        })

    def get_target(self, path):
        parent, name = posixpath.split(path)
        uid = self.r.hget("TREE:%s" % parent, name)
        if not uid:
            raise Exception("Broken path")
        return self.r.hget("NODE:%s" % uid, 'target')

    def is_symlink(self, path):
        return bool(self.get_target(path))

    def delete_node(self, apath):

        rpath = self.get_real_path(apath)

        def perform_delete(path, given_uid):
            parent, name = posixpath.split(path)
            pipe = self.r.pipeline()
            pipe.hget("TREE:%s" % parent, name)
            pipe.hgetall("TREE:%s" % path)
            pipe.delete("TREE:%s" % path)
            if given_uid == None:
                pipe.hdel("TREE:%s" % parent, name)
            res = pipe.execute()
            uid = res[0]
            info = res[1]

            if not uid and not given_uid == None:
                uid = given_uid

            if not uid:
                raise Exception("Broken path")

            # Now that the TREE entry has been deleted, we can remove the NODE entry
            # and the children in a non atomic manner.

            self.r.delete("NODE:%s" % uid)

            for children, node in info.iteritems():
                perform_delete('/'.join([path, children]), node)

        return perform_delete(rpath, None)

    def clone_node(self, uid):
        data = self.r.hgetall("NODE:%s" % uid)
        return self.create_node(data)

    def copy_path(self, source_path, dest_path):
        dparent, dname = posixpath.split(dest_path)
        rs_path, rs_node = self.real_node(source_path)
        rd_path = self.get_real_path(dparent)

        new_uid = self.clone_node(rs_node)
        self.r.hset("TREE:%s" % rd_path, dname, new_uid)

        for children, node in self.get_children(rs_path).iteritems():
            self.copy_path('/'.join([source_path, children]),
                           '/'.join([dest_path, children]))
