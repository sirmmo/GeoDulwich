from binascii import hexlify, unhexlify
import os
import tempfile
import time
import uuid
import zlib

import threading
from Queue import Queue

# for the object store
from dulwich.object_store import ShaFile, ObjectStoreIterator
from dulwich.objects import Blob
from dulwich.pack import PackData, iter_sha1, write_pack_index_v2, Pack, load_pack_index_file
from cStringIO import StringIO

from pymongo import Connection

# for the refstore
from dulwich.repo import RefsContainer, SYMREF

# for the repo
from dulwich.repo import BaseRepo


from geodulwich.backends.object_stores import GeoObjectStore
import logbook
log = logbook.Logger('geodulwich-object_store-mongo')



class MongoObjectStore(GeoObjectStore):
    def __init__(self, store_db):
        
        super(MongoObjectStore, self).__init__()
        self._data = store_db

    def contains_loose(self, sha):
        """Check if a particular object is present by SHA1 and is loose."""
        return self._data.find_one({'_id':sha})


    def __iter__(self):
        """Iterate over the SHAs that are present in this store."""
        return self._data.iterkeys()

    def get_raw(self, name):
        """Obtain the raw text for an object.

        :param name: sha for the object.
        :return: tuple with numeric type and object contents.
        """
        obj = self[name]
        return obj.type_num, obj.as_raw_string()

    def __getitem__(self, name):
        return self._data[name]

    def __delitem__(self, name):
        """Delete an object from this store, for testing only."""
        del self._data[name]

    def add_object(self, obj):
        """Add a single object to this object store.

        """
        self._data[obj.id] = obj

    def add_objects(self, objects):
        """Add a set of objects to this object store.

        :param objects: Iterable over a list of objects.
        """
        for obj, path in objects:
            self._data[obj.id] = obj