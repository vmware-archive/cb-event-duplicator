__author__ = 'jgarman'

import os
import hashlib
from utils import get_process_id
import json
import pprint


def get_process_path(proc_guid):
    key = hashlib.md5(proc_guid).hexdigest()
    return os.path.join(key[:2], '%s.json' % proc_guid)

def get_binary_path(md5sum):
    return os.path.join(md5sum[:2], '%s.json' % md5sum)


class FileInputSource(object):
    def __init__(self, pathname):
        self.pathname = pathname

class FileOutputSink(object):
    def __init__(self, pathname):
        self.pathname = pathname
        os.makedirs(pathname, 0755)

        os.makedirs(os.path.join(pathname, 'procs'), 0755)
        os.makedirs(os.path.join(pathname, 'binaries'), 0755)
        os.makedirs(os.path.join(pathname, 'sensors'), 0755)

        # TODO: only create the directories we need
        for dirname in ['procs', 'binaries']:
            for segment in ['%02X' % x for x in range(0,256)]:
                os.makedirs(os.path.join(pathname, dirname, segment), 0755)

    def output_process_doc(self, doc_content):
        proc_guid = get_process_id(doc_content)
        open(os.path.join(self.pathname, 'procs', get_process_path(proc_guid)), 'wb').write(json.dumps(doc_content))

    def output_binary_doc(self, doc_content):
        md5sum = doc_content.get('md5').lower()
        open(os.path.join(self.pathname, 'binaries', get_binary_path(md5sum)), 'wb').write(json.dumps(doc_content))

    def output_sensor_info(self, doc_content):
        print "got sensor info:"
        pprint.pprint(doc_content)

