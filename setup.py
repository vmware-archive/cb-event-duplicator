from setuptools import setup

setup(
    name='cb-event-duplicator',
    version='1.2.0',
    packages=['cbopensource', 'cbopensource.tools', 'cbopensource.tools.eventduplicator'],
    url='https://github.com/carbonblack/cb-event-duplicator',
    license='MIT',
    author='Bit9 + Carbon Black Developer Network',
    author_email='dev-support@bit9.com',
    description='Extract events from one Carbon Black server and send them to another server ' +
                '- useful for demo/testing purposes',
    install_requires=[
        'requests==2.9.1',
        'paramiko==1.16.0',
        'psycopg2==2.6.1'
    ],
    entry_points={
        'console_scripts': ['cb-event-duplicator=cbopensource.tools.eventduplicator.data_migration:main']
    },
    classifiers=[
        'Development Status :: 4 - Beta',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
    ],
    keywords='carbonblack',

)
