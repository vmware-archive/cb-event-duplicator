from distutils.core import setup

setup(
    name='cb-event-duplicator',
    version='1.0',
    packages=['cbopensource', 'cbopensource.eventduplicator', 'cbopensource.eventduplicator.lib'],
    url='https://github.com/carbonblack/cb-event-duplicator',
    license='MIT',
    author='Bit9 + Carbon Black Developer Network',
    author_email='dev-support@bit9.com',
    description='Extract events from one Carbon Black server and send them to another server - useful for demo/testing purposes'
)
