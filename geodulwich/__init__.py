
from cStringIO import StringIO
import pickle
import base64

def _stringify(self,  obj):
    content = pickle.dumps(obj)
    content = base64.b64encode(content)
    
    return content
    
def _unstringify(self, string):
    content = base64.b64decode(string)
    content = pickle.loads(content)
    
    return StringIO(content)