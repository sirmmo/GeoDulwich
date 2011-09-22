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
   

class MongoObjectStore(BaseObjectStore):
    def __init__(self, objs_collection):
        self.coll = objs_collection
        super(MongoObjectStore, self).__init__()
    def add_pack(self):
        fd, path = tempfile.mkstemp(suffix = ".pack")
        f = os.fdopen(fd, 'wb')

        def commit():
            try:
                os.fsync(fd)
                f.close()

                return self.upload_pack_file(path)
            finally:
                os.remove(path)
                log.debug('Removed temporary file %s' % path)
        return f, commit
    def _create_pack(self, path):
        def data_loader():
            # read and writable temporary file
            pack_tmpfile = tempfile.NamedTemporaryFile()

            # download into temporary file
            log.debug('Downloading pack %s into %s' % (path, pack_tmpfile))
            pack_key = self.bucket.new_key('%s.pack' % path)

            # store
            pack_key.get_contents_to_file(pack_tmpfile)
            log.debug('Filesize is %d' % pack_key.size)

            log.debug('Rewinding...')
            pack_tmpfile.flush()
            pack_tmpfile.seek(0)

            return PackData.from_file(pack_tmpfile, pack_key.size)
        def idx_loader():
            index_tmpfile = tempfile.NamedTemporaryFile()

            log.debug('Downloading pack index %s into %s' % (path, index_tmpfile))
            index_key = self.bucket.new_key('%s.idx' % path)

            index_key.get_contents_to_file(index_tmpfile)
            log.debug('Rewinding...')
            index_tmpfile.flush()
            index_tmpfile.seek(0)

            return load_pack_index_file(index_tmpfile.name, index_tmpfile)

        p = Pack(path)

        p._data_load = data_loader
        p._idx_load = idx_loader

        return p
    def contains_loose(self, sha):
        """Check if a particular object is present by SHA1 and is loose."""
        return bool(self.bucket.get_key(calc_object_path(self.prefix, sha)))
    
    def upload_pack_file(self, path):
        p = PackData(path)
        entries = p.sorted_entries()

        # get the sha1 of the pack, same method as dulwich's move_in_pack()
        pack_sha = iter_sha1(e[0] for e in entries)
        key_prefix = calc_pack_prefix(self.prefix, pack_sha)
        pack_key_name = '%s.pack' % key_prefix

        # FIXME: LOCK HERE? Possibly different pack files could
        #        have the same shas, depending on compression?

        log.debug('Uploading %s to %s' % (path, pack_key_name))

        pack_key = self.bucket.new_key(pack_key_name)
        pack_key.set_contents_from_filename(path)
        index_key_name = '%s.idx' % key_prefix

        index_key = self.bucket.new_key(index_key_name)

        index_fd, index_path = tempfile.mkstemp(suffix = '.idx')
        try:
            f = os.fdopen(index_fd, 'wb')
            write_pack_index_v2(f, entries, p.get_stored_checksum())
            os.fsync(index_fd)
            f.close()

            log.debug('Uploading %s to %s' % (index_path, index_key_name))
            index_key.set_contents_from_filename(index_path)
        finally:
            os.remove(index_path)

        p.close()

        return self._create_pack(key_prefix)
    
    def __iter__(self):
        return (k.name[-41:-39] + k.name[-38:] for k in self._s3_keys_iter())
    
    def _pack_cache_stale(self):
        # pack cache is valid for 5 minutes - no fancy checking here
        return time.time() - self._pack_cache_time > 5*60

    def _load_packs(self):
        packs = []

        # return pack objects, replace _data_load/_idx_load
        # when data needs to be fetched
        log.debug('Loading packs...')
        for key in self.bucket.get_all_keys(prefix = '%sobjects/pack/' % self.prefix):
            if key.name.endswith('.pack'):
                log.debug('Found key %r' % key)
                packs.append(self._create_pack(key.name[:-len('.pack')]))

        self._pack_cache_time = time.time()
        return packs
    def add_object(self, obj):
        """Adds object the repository. Adding an object that already exists will
           still cause it to be uploaded, overwriting the old with the same data."""
        self.add_objects([obj])
    
