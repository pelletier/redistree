from distutils.core import setup

version = '0.1'

setup(
    name='RedisTree',
    version=version,
    author='Thomas Pelletier',
    author_email='thomas@pelletier.im',
    url='http://github.com/pelletier/redistree/',
    packages=['redistree',],
    license='MIT',
    long_description="A virtual file system on top of Redis.",
    install_requires=[
        'redis'
    ]
)
