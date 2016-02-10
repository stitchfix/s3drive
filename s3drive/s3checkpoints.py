import os
from traitlets import Unicode
from notebook.services.contents.checkpoints import GenericCheckpointsMixin
from notebook.services.contents.manager import Checkpoints

from s3fs import S3File
from s3base import S3Base

class S3Checkpoints(S3Base, GenericCheckpointsMixin, Checkpoints):

    def __init__(self, *args, **kwargs):
        super(S3Checkpoints, self).__init__(*args, **kwargs)                
        
    def create_notebook_checkpoint(self, nb, path):
        checkpoint_path = self.checkpoint_path('checkpoint', path)
        content = self._nb_encode_b64(nb)
        s3file = S3File(
            self._scoped_path(checkpoint_path),
            None, 'application/octet-stream')
        self.write_content(content, s3file)
        return self.checkpoint_model('checkpoint', checkpoint_path)

    def create_file_checkpoint(self, content, format, path):
        checkpoint_path = self.checkpoint_path('checkpoint', path)
        s3file = S3File(
            self._scoped_path(checkpoint_path),
            None,
            self.mimes[format])
        self.write_content(content, s3file)
        return self.checkpoint_model('checkpoint', checkpoint_path)

    def delete_checkpoint(self, checkpoint_id, path):
        path = path.strip('/')
        cp_path = self.checkpoint_path(checkpoint_id, path)
        self.fs.delete(self._scoped_path(cp_path))

    def get_notebook_checkpoint(self, checkpoint_id, path):
        path = path.strip('/')
        cp_path = self.checkpoint_path(checkpoint_id, path)
        info = self.fs.info(self._scoped_path(path))
        
        return {
            'type': 'notebook',
            'content': self._nb_decode_b64(self.fs.read(info))
        }
        
    def get_file_checkpoint(self, checkpoint_id, path):
        path = path.strip('/')
        cp_path = self.checkpoint_path(checkpoint_id, path)
        info = self.fs.info(self._scoped_path(path))
        
        return {
            'type': 'file',
            'content': self.fs.read(info),
            'format': self._mimes_to_type(info.content_type)
        }
    
    def list_checkpoints(self, path):
        path = path.strip('/')
        cp_path = self.checkpoint_path('checkpoint', path)
        key = self._scoped_path(cp_path)
        if not self.fs.exists(key):
            return []
        else:
            [self.checkpoint_model('checkpoint', cp_path)]

    def rename_checkpoint(self, old_path, new_path):
        old_cp_path = self._scoped_path(self.checkpoint_path(checkpoint_id, old_path))
        new_cp_path = self._scoped_path(self.checkpoint_path(checkpoint_id, new_path))
        self.fs.rename(old_cp_path, new_cp_path)

    def rename_all_checkpoints(self, old_path, new_path):
        checkpoint_id = 'checkpoint'
        new_cp_path = self._scoped_path(self.checkpoint_path(checkpoint_id, new_path))
        old_cp_path = self._scoped_path(self.checkpoint_path(checkpoint_id, old_path))
        
        olds = self.fs.ls(old_cp_path)[1]
        moves = [(old_cp_path+'/'+old, new_cp_path+'/'+old) for old in olds]
        
        [self.fs.rename(old_path, new_path) for (old_path, new_path) in moves]

    def delete_all_checkpoints(self, path):
        path = path.strip('/')
        cp_path = self.checkpoint_path('checkpoint', path)
        key = self._scoped_path(cp_path)
        self.fs.delete(key)

    def checkpoint_model(self, checkpoint_id, path):
        info = self.fs.info(self._scoped_path(path))
        return {'id': checkpoint_id, 'last_modified': info.last_modified}
        
    def checkpoint_path(self, checkpoint_id, path):
        path = path.strip('/')
        parent, name = ('/' + path).rsplit('/', 1)
        parent = parent.strip('/')
        basename, ext = os.path.splitext(name)
        filename = u"{name}-{checkpoint_id}{ext}".format(
            name=basename,
            checkpoint_id=checkpoint_id,
            ext=ext,
        )
        
        cp_path = '/'.join([parent, '.ipynb_checkpoints', filename])
        return cp_path
