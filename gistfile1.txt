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


    #
    # PRIVATE METHODS
    #

    def _build_path(self, mount, path='/'):
        """
        Compute a normalized path for the given data.
        """
        return "%s/%s/ROOT%s" % (self.redis_prefix, mount, path)



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

        # Test if the node does not already exist
        node_already_exists = self.redis.exists(path)
        if node_already_exists:
            raise NodeAlreadyExists(path)

        
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

        self.redis.hset(path, 'visible', False)

        return (path, self.redis.get(path))
