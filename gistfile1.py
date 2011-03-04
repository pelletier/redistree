import redis


class RedisTree(object):

    #
    # EXCEPTIONS
    #

    class RedisTreeException(Exception):
        def __init__(self, value):
            self.value = value
        def __str__(self):
            return repr(self.value)

    class NodeAlreadyExists(RedisTreeException):
        def __init__(self, p):
            self.value = "Node at %s already exists."

    class NodeDoesNotExist(RedisTreeException):
        def __init__(self, p):
            self.value = "Node at %s does not exist."

    class IsBeingDeleted(RedisTreeException):
        def __init__(self, path, key):
            self.value = "Cannot create %s because %s is being deleted." % (path, key)


    #
    # PRIVATE METHODS
    #

    def _build_path(self, mount, path=''):
        """
        Compute a normalized path for the given data.
        """
        if path[-1] == '/': # Normalization: we don't want a / at the end
            path = path[:-1]
        return "%spath:/%s/ROOT%s" % (self.redis_prefix, mount, path)



    #
    # PUBLIC METHODS
    #

    def __init__(self, **kwargs):

        defaults = {
            'redis_instance': None,
            'redis_params': {},
            'redis_prefix': '',
        }

        defaults.update(kwargs)

        # Attach a Redis connection instance or create a new one
        self.redis = defaults[redis_instance]
        
        # Example of redis_params:
        #   redis_params = {
        #       'host': 'foo',
        #       'port': 4567,
        #       'db': 42,
        #   }
        if self.redis == None:
            self.redis = redis.Redis(**defaults[redis_params])

        # Append : to prefix if provided to have cleaner keys
        self.redis_prefix = defaults['redis_prefix']
        if self.redis_prefix:
            self.redis_prefix = "%s:" % self.redis_prefix


    def create_node(self, mount, path, data={}):
        """
        Create a node (ie a folder or a path).
        """
        path = self._build_path(mount, path)

        # By default the node is visible. Visible is a required node attribute
        # because we use it on parsing
        visible = data.get('visible', None)
        if visible == None:
            data['visible'] = True

        # We first have to check that the creation does not happen in a path
        # which is being delete.
        delete_keys = self.redis.keys('%s:delete:*' % self.redis_prefix)

        for key in keys:
            if path.startswith(key):
                # This happen during a delete
                raise IsBeingDeleted(path, key)

        # Test if the node does not already exist
        node_already_exists = self.redis.exists(path)
        if node_already_exists:
            
            # We loop to create a new unique path
            counter = 0
            temp_path = path

            while self.redis.exists(temp_path):
                # Increment
                counter += 1
                temp_path = "%s (%s)" % (path, counter)

            path = temp_path

        
        # If it does not, create it and inject data
        self.redis.set(path, data)

        return (path, self.redis.get(path))


    def delete_node(self, mount, path):
        """
        Delete the given node.
        According to the specs, the node is just hide.
        """
        path = self._build_path(mount, path)

        # Test if the node exist
        node_exists = self.redis.exists(path)
        if not node_exists:
            raise NodeDoesNotExist(path)

        # We announce that we are ready to kick some keys in the ass.
        # 60*5 = 5 minutes
        lock_key = '%sdelete:%s' % (self.redis_prefix, path)
        self.redis.setex(lock_key, 'deleting', 60*5)

        # Delete all the keys for the given path
        keys = self.redis.keys("%s*" % path)
        for key in keys:
            self.redis.hset(key, 'visible', False)

        # Finally delete the lock key
        self.redis.delete(lock_key)

        return (path, self.redis.get(path))


    def move(self, mount1, path1, mount2, path2):
        """
        Move all the nodes from path1 to path2.
        """

        path1 = self._build_path(path1, mount1)
        path2 = self._build_path(path2, mount2)
