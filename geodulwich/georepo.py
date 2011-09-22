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

from pymongo import Connection

# for the refstore
from dulwich.repo import RefsContainer, SYMREF

# for the repo
from dulwich.repo import BaseRepo

import logbook
log = logbook.Logger('geogit')

from geodulwich.backends.refs_containers.redisrc import RefsContainerImplementation
from geodulwich.backends.object_stores.mongoos import ObjectStoreImplementation

from geodulwich import GeoRepo




class MongoRedisGeoRepo(GeoRepo):
    def __init__(self, db_name, store_name, refs_name, host="localhost", port=27017):
        self.connection = Connection(host, port)
        self.db = self.connection[db_name]
        object_store = ObjectStoreImplementation(self.db[store_name])
        self._named_files = self.connection["named_files"][db_name]
        refs = RefsContainerImplementation(db_name)
        
        super(MongoRedisGeoRepo, self).__init__(object_store, refs)
        
    def _put_named_file(self, path, contents):
        content = self._stringify(contents)
        
        self._named_files.save({'_id':path, "content":content})
        
    def get_named_file(self, path):
        file = self._named_files.find_one({'_id':path})
        if file:
            return self._unstringify(file['content'])
        return None
    
    def open_index(self):
        raise NoIndexPresent()