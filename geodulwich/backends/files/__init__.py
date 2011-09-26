from io import BytesIO

def GitDbFile(filename, backend, mode="rb"):
    if 'a' in mode:
        raise IOError('append mode not supported for Git files')
    if '+' in mode:
        raise IOError('read/write mode not supported for Git files')
    if 'b' not in mode:
        raise IOError('text mode not supported for Git files')
    
    if 'w' in mode:
        return _GitFileStorer(backend, filename, mode)
    else:
        return _GitFileReader(backend, filename, mode)
    

class _GitFileStorer(object):
    PROXY_PROPERTIES = set(['closed', 'encoding', 'errors', 'mode', 'name',
                            'newlines', 'softspace'])
    PROXY_METHODS = ('__iter__', 'flush', 'fileno', 'isatty', 'next', 'read',
                     'readline', 'readlines', 'xreadlines', 'seek', 'tell',
                     'truncate', 'write', 'writelines')
    
    def __init__(self, backend, filename, mode):
        self._filename = filename
        self._lockfilename = '%s.lock' % self._filename
        fd = backend.open(self._lockfilename)
        self._file = BytesIO(fd['data'])
        self._closed = False
        for method in self.PROXY_METHODS:
            setattr(self, method, getattr(self._file, method))

    def abort(self):
        if self._closed:
            return
        self._file.close()
        del self._file
        self._file.remove({'__filename': self._lockfilename})
        self._closed = True
        
    def close(self):
        if self._closed:
            return
        self._file.close()
        self._file.update({'__filename':self._lockfilename},{'$set':{'__filename':self._file}})
        self.abort()

    def __getattr__(self, name):
        """Proxy property calls to the underlying file."""
        if name in self.PROXY_PROPERTIES:
            return getattr(self._file, name)
        raise AttributeError(name)
    
class _GitFileReader(object):
    pass


class GitFileBackend(object):
    def get_file(self, filename):
        return BytesIO(self.get_data(filename))
    def put_file(self, filename, file):
        return BytesIO(self.put_data(filename))
    def get_data(self, filename):
        pass
    def put_data(self, filename, data):
    
    