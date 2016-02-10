from base64 import b64decode, b64encode
from nbformat import from_dict, reads, writes
from traitlets import Unicode, HasTraits
from s3fs import S3FS, S3File

class S3Base(HasTraits):

    NBFORMAT_VERSION = 4
        
    bucket = Unicode(
        config=True
    )

    user = Unicode(
        config=True
    )

    mimes = {
        'text': 'text/plain',
        'json': 'application/json',
        'base64': 'application/octet-stream'
    }
    
    def __init__(self, *args, **kwargs):
        super(S3Base, self).__init__(*args, **kwargs)
        self.fs = S3FS(self.bucket)

    def _mimes_to_type(self, mimetype):
        if mimetype.startswith('text'):
            return 'text'
        else:
            return 'base64'

    def _scoped_path(self, path):
        """
        Scope path with user name
        """
        return '/'.join([self.user,path.strip('/')])

    def _nb_encode_b64(self, nb, version=NBFORMAT_VERSION):
        return b64encode(writes(nb, version=version).encode('utf-8'))

    def _nb_decode_b64(self, nb, as_version=NBFORMAT_VERSION):
        return reads(b64decode(nb).decode('utf-8'), as_version=as_version)

    def write_content(self, content, f):
        self.fs.write(content, f)
