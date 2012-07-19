# encoding: utf-8
import os
import sys
import imp
import riak
import contextlib
import riakimport_config as rc

@contextlib.contextmanager
def riak_bucket_ctx(path):
    """context to inject riak db client into finder and loader"""
    client = riak.RiakClient(rc.HOST, port=rc.PORT, transport_class=riak.RiakPbcTransport)
    if client.is_alive():
        yield client.bucket(path.split('/')[2])
        
        
def python_module_name(module):
    """
        gives the name of the python module
    """
    return module if module.endswith('__init__') else module + '.__init__'

def check_for(module_name, bucket):
    """
        checks if module_name exists
    """
    print 'checking for', module_name
    if bucket.get_binary(module_name).exists():
        return module_name
    full_module_name = python_module_name(module_name)
    print 'checking for', full_module_name
    if bucket.get_binary(full_module_name).exists():
        return full_module_name
    else:
        return None

class RiakFinder(object):
    """
        finds modules in riak bucket
    """
    def __init__(self, path):
        if not path.startswith('/riak'):
            raise ImportError
        try:
            with riak_bucket_ctx(path):
                pass
        except Exception, e:
            raise ImportError(str(e))
        else:
            self.path = path

    def find_module(self, name, path=None):
        path = path or self.path
        print 'finding module', name
        with riak_bucket_ctx(path) as rb:
            key = check_for(name, rb)
            if key:
                print 'got it as', key
                return RiakLoader(path)
        print 'nope, not found'
        return None

class RiakLoader(object):
    """
        loads modules from riak buckets
    """
    def __init__(self, path):
        self.path = path
    
    def load_source(self, name):
        try:
            with riak_bucket_ctx(self.path) as rb:
                key = check_for(name, rb)
                if key:
                    print 'loading source for', key
                    return rb.get_binary(key).get_data()
                    raise ImportError('could not load source code for module %s' % name)
        except Exception, e:
            raise ImportError(str(e))
                    
    def load_module(self, name):
        """loads and imports the module"""
        source = self.load_source(name)
        if name in sys.modules:
            module = sys.modules[name]
        else:
            module = sys.modules.setdefault(name, imp.new_module(name))
        # fullfill PEP 302
        module.__file__ = os.path.join(self.path, name)
        module.__name__ = name
        module.__path__ = self.path
        module.__package__ = '.'.join(name.split('.')[:-1])
        module.__loader__ = self
        exec source in module.__dict__
        print 'over and out'
        return module