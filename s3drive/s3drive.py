import json
import mimetypes
from datetime import datetime
from traitlets import (
    Bool,
    Instance,
    HasTraits,
    Unicode,
)
from nbformat import (
    from_dict,
    reads,
    writes,
)
from notebook.services.contents.manager import ContentsManager

from s3fs import S3FS, S3File
from s3base import S3Base
from s3checkpoints import S3Checkpoints

class S3ContentsManager(S3Base, ContentsManager):

    DUMMY_CREATED_DATE = datetime.fromtimestamp(0)
    
    def __init__(self, *args, **kwargs):
        super(S3ContentsManager, self).__init__(*args, **kwargs)        
        self.fs = S3FS(self.bucket)

    def _checkpoints_class_default(self):
        return S3Checkpoints
    
    def _checkpoints_kwargs_default(self):
        kw = super(S3ContentsManager, self)._checkpoints_kwargs_default()
        kw.update(
            {
                'bucket': self.bucket,
                'user': self.user
            }
        )
        return kw
        
    def _guess_type(self, path, allow_directory=True):
        if path.endswith('.ipynb'):
            return 'notebook'
        elif allow_directory and self.dir_exists(path):
            return 'directory'
        else:
            return 'file'

    def _base_model(self, path):
        """
        Produce base object for jupyter model
        """
        return {
            "name": path.rsplit('/', 1)[-1],
            "path": path,
            "writable": True,
            "last_modified": None,
            "created": None,
            "content": None,
            "format": None,
            "mimetype": None
        }

    def _base_directory_model(self, path):
        m = self._base_model(path)
        m.update(
            type='directory',
            last_modified=self.DUMMY_CREATED_DATE,
            created=self.DUMMY_CREATED_DATE
        )
        return m
                
    def _save_file(self, model, key):
        mimetype = mimetypes.guess_type(key)[0] or self.mimes[model.get('format')]
        s3file = S3File(key, None, mimetype)
        self.write_content(model.get('content'), s3file)

    def _save_notebook(self, model, key):
        nbcontents = from_dict(model['content'])
        self.check_and_sign(nbcontents, key)
        
        content = self._nb_encode_b64(nbcontents)
        s3file = S3File(key, None, 'application/octet-stream')
        self.write_content(content, s3file)
        self.validate_notebook_model(model)
        return model.get('message')
    
    def save(self, model, path):
        if model['type'] == 'notebook':
            self._save_notebook(model, self._scoped_path(path))
        elif model['type'] == 'file':            
            self._save_file(model, self._scoped_path(path))
            
        saved = self.get(path, type=model['type'], content=False)
        saved['format'] = None
        return saved
        
    def get(self, path, content=True, type=None, format=None):
        if type is None:
            type = self._guess_type(path)
        try:
            fn = {
                'directory': self._get_directory,
                'notebook': self._get_notebook,
                'file': self._get_file
            }[type]
        except KeyError:
            raise ValueError("Unknown type passed: '{}'".format(type))
        return fn(path=path, content=content, format=format)
    
    def _get_file(self, path, content, format):
        scoped = self._scoped_path(path)    
        info = self.fs.info(scoped)
        m = self._base_model(path)
        
        m['type'] = 'file'
        m['last_modified'] = m['created'] = info.last_modified

        m['format'] = format            
        m['mimetype'] = info.content_type
        
        if content:
            m['content'] = self.fs.read(info)
            m['format'] = self._mimes_to_type(info.content_type)
            
        return m

    def _get_notebook(self, path, content, format):
        scoped = self._scoped_path(path)    
        info = self.fs.info(scoped)
        m = self._base_model(path)
        
        m['type'] = 'notebook'
        m['last_modified'] = m['created'] = info.last_modified
        if content:
            content = self._nb_decode_b64(self.fs.read(info))            
            self.mark_trusted_cells(content, path)
            m['content'] = content
            m['format'] = 'json'
            self.validate_notebook_model(m)
            
        return m

    def _convert_file_record(self, f):
        ftype = self._guess_type(f.name)
        if ftype == 'notebook':
            return self._get_notebook(f.name, False, None)
        elif ftype == 'file':
            return self._get_file(f.name, False, self._mimes_to_type(f.content_type))        
        return None
        
    def _get_directory(self, path, content, format):
        scoped = self._scoped_path(path)
        listing = self.fs.ls(scoped)        
        m = self._base_directory_model(path)
        
        if content:            
            m['format'] = 'json'
            dir_models = [self._base_directory_model(d) for d in listing[0]]
            file_models = [self._convert_file_record(f) for f in listing[1]]
            m['content'] = file_models + dir_models
            
        return m
            
    def delete_file(self, path):
        self.fs.delete(self._scoped_path(path))

    def rename_file(self, old_path, new_path):
        old_name = self._scoped_path(old_path)
        new_name = self._scoped_path(new_path)
        self.fs.rename(old_name, new_name)

    def file_exists(self, path):
        return self.fs.exists(self._scoped_path(path))

    def dir_exists(self, path):
        if path == '':
            return True
        return self.fs.dir_exists(self._scoped_path(path))
        
    def is_hidden(self, path):
        return False
        
