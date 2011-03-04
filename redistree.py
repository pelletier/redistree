#-*- coding: utf-8 -*-

import json
import redis


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

class InvalidPath(RedisTreeException):
    def __init__(self, msg):
        self.value = msg



class RedisTree(object):


    #
    # PRIVATE METHODS
    #

    def _build_path(self, mount, path=''):
        """
        Compute a normalized path for the given data.
        """
        mount = mount.decode('utf-8')
        path = path.decode('utf-8')
        if path[-1] == '/': # Normalization: we don't want a / at the end
            path = path[:-1]
        fpath = u"%spath:/%s/ROOT%s" % (self.redis_prefix, mount, path)
        
        if '' in fpath.split('/'):
            raise InvalidPath('Empty path parts')

        if '..' in fpath:
            raise InvalidPath('A path should not contain ..')
        
        return fpath


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
        self.redis = defaults['redis_instance']
        
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

        for key in delete_keys:
            if path.sdecodewith(key):
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
        self.redis.set(path, json.dumps(data))
 
        return (path, json.loads(self.redis.get(path)))


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
            data = self.redis.get(key)
            data = json.loads(data)
            data['visible'] = False
            self.redis.set(key, json.dumps(data))

        # Finally delete the lock key
        self.redis.delete(lock_key)

        return (path, json.loads(self.redis.get(path)))


    def move(self, mount1, path1, mount2, path2):
        """
        Move all the nodes from path1 to path2. We use the nix way to handle
        the path renames.
        """
        path1 = self._build_path(path1, mount1)
        path2 = self._build_path(path2, mount2)
        
        # We first have to check that the first exists
        if not self.redis.exists(path1):
            raise NodeDoesNotExist(path)

        # Then we check the parent of the second path
        parent_path2 = '/'.join(path2.split('/')[:-1])
        if not self.redis.exists(parent_path2):
            raise NodeDoesNotExist(parent_path2)

        # Otherwise we move the keys
        self.redis.delete(path1)
        keys_to_rename = self.redis.keys('%s/*' % path1)
        
        # Create a pipeline for atomicity
        pipe = self.redis.pipeline()

        for key in keys_to_rename:
            new_name = key.replace(path1, path2)
            pipe.rename(key, new_name)

        # Fire changes
        pipe.execute()

        return (path1, path2)

    def get_children(self, mount, path):
        """
        Returns the mounts and paths for all children in the given path
        """
        path = self._build_path(mount, path)

        # We first have to check that the first exists
        if not self.redis.exists(path):
            raise NodeDoesNotExist(path)

        # Get keys
        keys = self.redis.keys('%s/*' % path)

        final_keys = []

        # Group names
        for key in keys:
            key.replace('%s/' % path, '')
            final = '/'.join(path, key[0])
            final_keys.append(final)

        # Remove doubles
        # See http://www.peterbe.com/plog/uniqifiers-benchmark
        return {}.fromkeys(final_keys).keys()
