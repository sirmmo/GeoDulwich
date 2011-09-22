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
log = logbook.Logger('geogit')


class GeoObjectStore(BaseObjectStore):
    
    def contains_packed(self, sha):
        """Check if a particular object is present by SHA1 and is packed."""
        return False
    
    @property
    def packs(self):
        """List with pack objects."""
        return []

        