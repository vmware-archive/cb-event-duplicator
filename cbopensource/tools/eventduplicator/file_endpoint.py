__author__ = 'jgarman'

import os
import hashlib
from utils import get_process_id, json_encode
import json
from collections import defaultdict
import logging


log = logging.getLogger(__name__)


def get_process_path(proc_guid):
    key = hashlib.md5(str(proc_guid)).hexdigest()
    return os.path.join(key[:2].upper(), '%s.json' % proc_guid)

def get_binary_path(md5sum):
    return os.path.join(md5sum[:2].upper(), '%s.json' % md5sum.lower())


class FileInputSource(object):
    def __init__(self, pathname):
        self.pathname = pathname

    def get_version(self):
        return open(os.path.join(self.pathname, 'VERSION'), 'rb').read()

    def get_process_docs(self, query_filter=None):
        # TODO: the query_filter is a code smell... we should push the traversal code into the Source?
        if query_filter:
            return

        for root, dirs, files in os.walk(os.path.join(self.pathname, 'procs')):
            for fn in files:
                yield json.load(open(os.path.join(root, fn), 'rb'))

    def get_feed_doc(self, feed_key):
        pathname = os.path.join(self.pathname, 'feeds', '%s.json' % feed_key)
        try:
            return json.load(open(pathname, 'rb'))
        except Exception as e:
            log.warning("Could not open feed document: %s" % pathname)
            return None

    def get_feed_metadata(self, feed_id):
        pathname = os.path.join(self.pathname, 'feeds', '%s.json' % feed_id)
        try:
            return json.load(open(pathname, 'rb'))
        except Exception as e:
            log.warning("Could not open feed metadata: %s" % pathname)
            return None

    def get_binary_doc(self, md5sum):
        md5sum = md5sum.lower()
        pathname = os.path.join(self.pathname, 'binaries', get_binary_path(md5sum))
        try:
            return json.load(open(pathname, 'rb'))
        except Exception as e:
            log.warning("Could not open binary document: %s" % pathname)
            return None

    def get_sensor_doc(self, sensor_id):
        pathname = os.path.join(self.pathname, 'sensors', '%d.json' % sensor_id)
        try:
            return json.load(open(os.path.join(self.pathname, 'sensors', '%d.json' % sensor_id), 'rb'))
        except Exception as e:
            log.warning("Could not open sensor document: %s" % pathname)
            return None

    def connection_name(self):
        return self.pathname

    def cleanup(self):
        pass


class FileOutputSink(object):
    def __init__(self, pathname):
        self.pathname = pathname
        os.makedirs(pathname, 0755)

        os.makedirs(os.path.join(pathname, 'procs'), 0755)
        os.makedirs(os.path.join(pathname, 'binaries'), 0755)
        os.makedirs(os.path.join(pathname, 'sensors'), 0755)
        os.makedirs(os.path.join(pathname, 'feeds'), 0755)

        # TODO: only create the directories we need
        for dirname in ['procs', 'binaries']:
            for segment in ['%02X' % x for x in range(0,256)]:
                os.makedirs(os.path.join(pathname, dirname, segment), 0755)

        self.written_docs = defaultdict(int)
        self.new_metadata = defaultdict(list)


    def output_process_doc(self, doc_content):
        proc_guid = get_process_id(doc_content)
        pathname = os.path.join(self.pathname, 'procs', get_process_path(proc_guid))
        if os.path.exists(pathname):
            log.warning('process %s already existed, writing twice' % proc_guid)
        open(os.path.join(self.pathname, 'procs', get_process_path(proc_guid)), 'wb').write(json_encode(doc_content))
        self.written_docs['proc'] += 1

    def output_binary_doc(self, doc_content):
        md5sum = doc_content.get('md5').lower()
        open(os.path.join(self.pathname, 'binaries', get_binary_path(md5sum)), 'wb').write(json_encode(doc_content))
        self.written_docs['binary'] += 1

    def output_sensor_info(self, doc_content):
        open(os.path.join(self.pathname, 'sensors', '%s.json' % doc_content['sensor_info']['id']), 'wb').\
            write(json_encode(doc_content))
        self.new_metadata['sensor'].append(doc_content['sensor_info']['computer_name'])

    def output_feed_doc(self, doc_content):
        open(os.path.join(self.pathname, 'feeds', '%s:%s.json' % (doc_content['feed_name'], doc_content['id'])), 'wb').\
            write(json_encode(doc_content))
        self.written_docs['feed'] += 1

    def output_feed_metadata(self, doc_content):
        open(os.path.join(self.pathname, 'feeds', '%s.json' % (doc_content['id'],)), 'wb').\
            write(json_encode(doc_content))
        self.new_metadata['feed'].append(doc_content['name'])

    def set_data_version(self, version):
        open(os.path.join(self.pathname, 'VERSION'), 'wb').write(version)
        return True

    def cleanup(self):
        pass

    def connection_name(self):
        return self.pathname

    def report(self):
        report_data = "Documents saved to %s by type:\n" % (self.pathname,)
        for key in self.written_docs.keys():
            report_data += " %8s: %d\n" % (key, self.written_docs[key])
        for key in self.new_metadata.keys():
            report_data += "New %ss created in %s:\n" % (key, self.pathname)
            for value in self.new_metadata[key]:
                report_data += " %s\n" % value

        return report_data