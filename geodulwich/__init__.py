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

import pickle
import base64


class GeoRepo(BaseRepo):
    def _stringify(self,  obj):
        content = pickle.dumps(obj)
        content = base64.b64encode(content)
        
        return content
        
    def _unstringify(self, string):
        content = base64.b64decode(string)
        content = pickle.loads(content)
        
        return StringIO(content)

