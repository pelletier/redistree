#-*- coding: utf-8 -*-

import re
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
        self.value = "Node at %s already exists." % p

class NodeDoesNotExist(RedisTreeException):
    def __init__(self, p):
        self.value = "Node at %s does not exist." % p

class IsBeingDeleted(RedisTreeException):
    def __init__(self, path, key):
        self.value = "Cannot create %s because %s is being deleted." % (path, key)

class InvalidPath(RedisTreeException):
    def __init__(self, msg):
        self.value = msg

class NoParent(RedisTreeException):
    def __init__(self, path):
        self.value = "Parent of %s does not exist." % path

class InvalidMoveOperation(RedisTreeException):
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

    def _has_parent(self, path):
        p = path
        if '{' in p:
            p = re.sub(r'\{.*?\}', '', p)
        parent_path = '/'.join(p.split('/')[:-1])
        if not p.split('/')[-1] == 'ROOT':
            if not self.redis.exists(parent_path):
                raise NoParent(path)

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


    def create(self, mount, path, data={}):
        """
        Create a node (ie a folder or a path).
        """

        path = self._build_path(mount, path)

        # By default the node is visible. Visible is a required node attribute
        # because we use it on parsing
        visible = data.get('visible', None)
        if visible == None:
            data['visible'] = True

        self._has_parent(path)

        # We first have to check that the creation does not happen in a path
        # which is being deleted.
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
                temp_path = "%s(%s)" % (path, counter)

            path = temp_path

        
        # If it does not, create it and inject data
        self.redis.set(path, json.dumps(data))
 
        return (path, json.loads(self.redis.get(path)))


    def delete(self, mount, path):
        """
        Delete the given node.
        According to the specs, the node is just hide.
        """
        path = self._build_path(mount, path)
        orig = path

        # Test if the node exist
        node_exists = self.redis.exists(path)
        if not node_exists:
            raise NodeDoesNotExist(path)

        # We announce that we are ready to kick some keys in the ass.
        # 60*5 = 5 minutes
        lock_key = '%sdelete:%s' % (self.redis_prefix, path)
        self.redis.setex(lock_key, 'deleting', 60*5)

        # Delete all the keys for the given path
        p = ':'.join(path.split(':')[1:])
        keys = self.redis.keys("path:*%s*" % p)

        for key in keys:
            data = self.redis.get(key)
            data = json.loads(data)
            data['visible'] = False
            self.redis.set(key, json.dumps(data))

        # symlinks
        for k in self.redis.keys('link:*'):
            v = ':'.join(k.split(':')[1:])
            if v in path:
                path = path.replace(v, '{%s}' % v)
                break

        keys = self.redis.keys("path:*%s*" % path)

        for key in keys:
            data = self.redis.get(key)
            data = json.loads(data)
            data['visible'] = False
            self.redis.set(key, json.dumps(data))


        # Finally delete the lock key
        self.redis.delete(lock_key)

        return (orig, json.loads(self.redis.get(orig)))


    def move(self, mount1, path1, mount2, path2):
        """
        Move all the nodes from path1 to path2. We use the nix way to handle
        the path renames.
        """
        path1 = self._build_path(mount1, path1)
        path2 = self._build_path(mount2, path2)
 
        if not path2.find(path1) == -1:
            raise InvalidMoveOperation("Cannot move %s into its child %s" % (path1, path2))
         
        # We first have to check that the first exists
        if not self.redis.exists(path1):
            raise NodeDoesNotExist(path)

        # Then we check the parent of the second path
        self._has_parent(path2)

        # Otherwise we move the keys
        self.redis.delete(path1)
        keys_to_rename = self.redis.keys('*')
        #TODO: test is it checking all the keys ??

        #keys_to_rename = self.redis.keys('%s/*' % path1)

        # Create a pipeline for atomicity
        pipe = self.redis.pipeline()

        for key in keys_to_rename:
            new_name = key.replace(path1, path2)
            pipe.rename(key, new_name)

        # Fire changes
        pipe.execute()

        return (path1, path2)

    def get_info(self, mount, path):
        """
        Return metadata
        """
        path = self._build_path(mount, path)

        if not self.redis.exists(path):
            raise NodeDoesNotExist(path)

        return json.loads(self.redis.get(path))


    def get_children(self, mount, path, with_info=True, show_deleted=True):
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
            key = key.replace('%s/' % path, '')
            k = key.split('/')[0]
            final = '/'.join([path, k])
            final_keys.append(final)

        # Remove doubles
        # See http://www.peterbe.com/plog/uniqifiers-benchmark
        final_keys = {}.fromkeys(final_keys).keys()

        if not with_info:
            return final_keys

        data = {}

        for key in final_keys:
            d = json.loads(self.redis.get(key))
            
            if show_deleted:
                data[key] = d
            else:
                if d['visible']:
                    data[key] = d
            
        return data

    def link(self, mount1, path1, mount2, path2):
        """
        Symlink the (m1,p1) to (m2,p2).
        """
        p1 = self._build_path(mount1, path1)
        p2 = self._build_path(mount2, path2)
        
        # Path1 should not exist (we create it with a special link type)
        if self.redis.exists(p1):
            raise NodeAlreadyExists(p1)

        # Path2 should exist (we cannot point to nothing)
        if not self.redis.exists(p2):
            raise NodeDoesNotExist(p2)

        # Compute the full path
        full_path = "%s{%s}" % (p1, p2)
        
        # Replicate children
        orig_children = self.redis.keys("*%s*" % p2)

        # Create the link node
        link_node_path = "%s{%s}" % (path1,p2)
        self.create(mount1, link_node_path, {
            'type': 'link',
        })

        # Replicate the original children
        for ch in orig_children:
            data = self.redis.get(ch)
            new_ch = "%s%s" % (full_path, ch.replace(p2, ''))
            self.redis.set(new_ch, data)

        self.redis.set(full_path, json.dumps({'type':'link','visible':True}))


        self.redis.set('link:%s' % p2, full_path)


        return (full_path, json.loads(self.redis.get(full_path)))
