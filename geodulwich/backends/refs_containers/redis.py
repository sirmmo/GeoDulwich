from binascii import hexlify, unhexlify
import os
import tempfile
import time
import zlib

import threading
from Queue import Queue

# for the object store
from dulwich.object_store import BaseObjectStore, ShaFile, ObjectStoreIterator
from dulwich.objects import Blob
from dulwich.pack import PackData, iter_sha1, write_pack_index_v2, Pack, load_pack_index_file
from cStringIO import StringIO

# for the refstore
from dulwich.repo import RefsContainer, SYMREF

# for the repo
from dulwich.repo import BaseRepo

import logbook
log = logbook.Logger('geodulwich-refs_container-redis')

import redis 

class RefsContainerImplementation(RefsContainer):
    def __init__(self, container, db_num=1):
        self.db = redis.Redis(db=db_num)
        self.container = container
         
        super(RedisRefsContainer, self).__init()
        
    def _calc_ref_path(self, ref):
        return '%s%s' % (self.container, ref)

    def allkeys(self):
        return self.db.keys()
    
    def read_loose_ref(self, name):
        k = self._calc_ref_path(name)
        d = self.db.get(k)
        if d:
            return d
        else:
            return False
    
    def get_packed_refs(self):
        return {}
    
    def set_symbolic_ref(self, name, other):
        k = self._calc_ref_path(name)
        sref = SYMREF + other
        log.debug('setting symbolic ref %s to %r' % (name, sref))
        k = self.db.set(k, sref)
    
    def set_if_equals(self, name, old_ref, new_ref):
        if old_ref is not None and self.read_loose_ref(name) != old_ref:
            return False

        realname, _ = self._follow(name)

        # set ref (set_if_equals is actually the low-level setting function)
        k = self.db.set(self._calc_ref_path(name), new_ref)
        return True
    
    def add_if_new(self, name, ref):
        if None != self.read_loose_ref(name):
            return False

        self.set_if_equals(name, None, ref)
        return True
    
    def remove_if_equals(self, name, old_ref):
        k = self.db.get(self._calc_ref_path(name))
        if None == k: 
            return True

        if old_ref is not None and k != old_ref:
            return False

        self.db.delete(self._calc_ref_path(name))
        return True

    
    