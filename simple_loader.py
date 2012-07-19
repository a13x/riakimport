#!/usr/bin/env python
# encoding: utf-8
"""
simple_loader.py

Simple loader to demonstrate loading modules into Riak bucket

Created by Aleksandar Radulovic on 2012-07-13.
Copyright (c) 2012 A13X Networks. All rights reserved.
"""

import sys
import os
import riak
import riakimport

# path is riak slash bucket name
PATH = "/riak/proj1"

client = riak.RiakClient(port=8087, transport_class=riak.RiakPbcTransport)
def load():
    proj = client.bucket('proj1')
    something = """
text = 'something'
    """
    something2 = """
text = 'something'    
    """
    dark = """
text = 'dark'    
    """
    side = """
text = 'side'
    """
    proj.new_binary('something', something).store()
    proj.new_binary('something2', something2).store()
    proj.new_binary('dark.__init__', dark).store()
    proj.new_binary('dark.side', side).store()

def main():
    sys.path_hooks.append(riakimport.RiakFinder)
    sys.path.insert(0, PATH)
    print('Importing package from riak')
    import something
    import dark.side
    print something.text

if __name__ == '__main__':
    print riakimport.python_module_name('prump')
    #load()
    main()

