from distutils.core import setup

setup(
    name='ipython-s3-drive',
    version='0.1',
    author='Jacob Perkins',
    author_email='jacobperkins@stitchfix.com',
    license='Apache 2.0',
    description='An S3-backed ContentsManager for IPython',
    packages=['s3drive'],
    long_description='An S3-backed ContentsManager for IPython',
    classifiers=[
        'Intended Audience :: Developers',
    ],
    install_requires=[
        'boto3',
        'ipython',
        'jupyter',
        'requests'
    ]
)
