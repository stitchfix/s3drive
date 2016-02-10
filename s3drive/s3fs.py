import boto3
import botocore

class S3File(object):

    def __init__(self, name, last_modified, content_type):
        self.name = name
        self.last_modified = last_modified
        self.content_type = content_type
    
    @staticmethod
    def from_api(data, prefix=None):
        name = data['Key'][len(prefix):] if prefix else data['Key']
        last_modified = data['LastModified']
        content_type = data['ContentType'] if 'ContentType' in data else 'text/plain'
        return S3File(name, last_modified, content_type)

    def __repr__(self):        
        return "S3File(name={}, last_modified={}, content_type={})".format(
            self.name,
            self.last_modified.strftime("%Y-%m-%d %H:%M:%S") if self.last_modified else None,
            self.content_type)
        
class S3FS(object):

    def __init__(self, bucket):
        self._s3 = boto3.client('s3')
        self._bucket = bucket

    def exists(self, key):
        exists = False
        try:
            self._s3.head_object(Bucket=self._bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                exists = False
            else:
                raise e
        else:
            exists = True
        return exists
    
    def dir_exists(self, prefix):
        ls = self.ls(prefix)
        if len(ls[0]+ls[1]) == 0:
            return False
        return True

    def ls(self, dir):
        q = dir
        if q == '/':
            q = ''
        elif not q.endswith('/'):
            q = q+'/'

        if q.startswith('/'):
            q = q[1:len(q)]
        
        raw = self._s3.list_objects(
            Bucket=self._bucket,
            Prefix=q, MaxKeys=10000,
            Delimiter='/')

        keys = []
        dirs = []
        if 'Contents' in raw:
            keys = [S3File.from_api(r, dir) for r in raw['Contents']]
        if 'CommonPrefixes' in raw:
            dirs = [r['Prefix'] for r in raw['CommonPrefixes'] if r['Prefix'] != dir]

        return (dirs, keys)

    def info(self, key):
        m = self._s3.head_object(Bucket=self._bucket, Key=key)
        if 'Key' not in m:
            m.update(Key=key)
            
        return S3File.from_api(m)
        
    def write(self, contents, f):
        return self._s3.put_object(
            Bucket=self._bucket,
            Key=f.name,
            ContentType=f.content_type,
            Body=contents)

    def read(self, f):
        contents = None
        r = self._s3.get_object(
            Bucket=self._bucket,
            Key=f.name)
        if r:
            contents = r['Body'].read()
        return contents

    def delete(self, key):
        return self._s3.delete_object(
            Bucket=self._bucket,
            Key=key
        )

    def rename(self, old_key, new_key):
        self._s3.copy_object(
            Bucket=self._bucket,
            CopySource='/'.join([self._bucket, old_key]),
            Key=new_key)
        self.delete(old_key)
