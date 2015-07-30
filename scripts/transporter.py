__author__ = 'jgarman'

import logging
from utils import get_process_id, get_parent_process_id, split_process_id

from types import GeneratorType

def flatten(*stack):
    stack = list(stack)
    while stack:
        try: x = stack[0].next()
        except StopIteration:
            stack.pop(0)
            continue
        if isinstance(x, GeneratorType): stack.insert(0, x)
        else: yield x


log = logging.getLogger(__name__)

class Transporter(object):
    # TODO: add check for input and output Cb version before executing
    # - this data is found in the file /usr/share/cb/VERSION

    def __init__(self, input_source, output_sink, tree=False):
        self.input_md5set = set()
        self.input_proc_guids = set()

        self.input = input_source
        self.output = output_sink
        self.mungers = [CleanseSolrData()]

        self.seen_sensor_ids = set()
        self.seen_feeds = set()

        self.traverse_tree = tree

    def add_anonymizer(self, munger):
        self.mungers.append(munger)

    def output_process_doc(self, doc):
        for munger in self.mungers:
            doc = munger.munge_document('proc', doc)

        self.output.output_process_doc(doc)

    def output_binary_doc(self, doc):
        for munger in self.mungers:
            # note that the mungers are mutating the data in place, anyway.
            doc = munger.munge_document('binary', doc)

        self.output.output_binary_doc(doc)

    def update_sensors(self, proc):
        sensor_id = proc.get('sensor_id', 0)
        if not sensor_id:
            return []

        if sensor_id and sensor_id not in self.seen_sensor_ids:
            # notify caller that this sensor_id has to be inserted into the target
            self.seen_sensor_ids.add(sensor_id)
            return [sensor_id]

        return []

    def update_md5sums(self, proc):
        md5s = set()
        process_md5 = proc.get('process_md5', None)
        if process_md5 and process_md5 != '0'*32:
            md5s.add(proc.get('process_md5'))
        for modload_complete in proc.get('modload_complete', []):
            fields = modload_complete.split('|')
            md5s.add(fields[1])

        retval = md5s - self.input_md5set
        self.input_md5set |= md5s
        return retval

    def traverse_up(self, guid):
        # TODO: this prompts a larger issue of - how do we handle process segments?
        total = []

        for proc in self.input.get_process_docs('unique_id:%s' % (guid,)):
            process_id = get_process_id(proc)
            if process_id not in self.input_proc_guids:
                self.input_proc_guids.add(process_id)
                total.append(proc)

            parent_process_id = get_parent_process_id(proc)
            if parent_process_id and parent_process_id not in self.input_proc_guids:
                total.extend(self.traverse_up(parent_process_id))

        return total

    def traverse_down(self, guid):
        total = []

        for proc in self.input.get_process_docs('parent_unique_id:%s' % (guid,)):
            process_id = get_process_id(proc)
            if process_id not in self.input_proc_guids:
                self.input_proc_guids.add(process_id)
                total.append(proc)

            total.extend(self.traverse_down(process_id))

        return total

    def traverse_up_down(self, proc):
        # TODO: infinite recursion prevention
        parent_process_id = get_parent_process_id(proc)
        process_id = get_process_id(proc)

        total = []
        # get parents
        if parent_process_id:
            total.extend(self.traverse_up(parent_process_id))

        total.extend(self.traverse_down(process_id))

        for proc in total:
            yield proc

    def get_process_docs(self):
        # TODO: append so that this also grabs process trees when necessary
        for proc in self.input.get_process_docs():
            process_id = get_process_id(proc)
            if process_id not in self.input_proc_guids:
                self.input_proc_guids.add(get_process_id(proc))
                yield proc

            if self.traverse_tree:
                for tree_proc in self.traverse_up_down(proc):
                    yield tree_proc

    def update_feeds(self, doc):
        feed_keys = [k for k in doc.keys() if k.startswith('alliance_data_')]
        feed_lookup = set()

        for key in feed_keys:
            feed_name = key[14:]
            for doc_name in doc[key]:
                feed_lookup.add("%s:%s" % (feed_name, doc_name))

        retval = feed_lookup - self.seen_feeds
        self.seen_feeds |= feed_lookup
        return retval

    def transport(self, debug=False):
        # TODO: multithread this so we have some parallelization

        input_version = self.input.get_version()
        if not self.output.set_data_version(input_version):
            raise Exception("Input and Output versions are incompatible")

        # get process list
        for proc in self.get_process_docs():
            new_md5sums = self.update_md5sums(proc)
            new_sensor_ids = self.update_sensors(proc)
            new_feed_ids = self.update_feeds(proc)

            # output docs, sending binaries & sensors first
            for md5sum in new_md5sums:
                doc = self.input.get_binary_doc(md5sum)
                if not doc:
                    pass
                    # TODO: logging
                    # print "Could not retrieve MD5sum %s" % md5sum
                else:
                    new_feed_ids |= self.update_feeds(doc)
                    self.output_binary_doc(doc)

            # TODO: right now we don't munge sensor or feed documents
            for sensor in new_sensor_ids:
                doc = self.input.get_sensor_doc(sensor)
                self.output.output_sensor_info(doc)

            for feed in new_feed_ids:
                doc = self.input.get_feed_doc(feed)
                self.output.output_feed_doc(doc)

            self.output_process_doc(proc)

        # clean up
        self.input.cleanup()
        self.output.cleanup()


class CleanseSolrData(object):
    def __init__(self):
        pass

    def munge_document(self, doc_type, doc_content):
        doc_content.pop('_version_', None)
        doc_content.pop('last_update', None)
        doc_content.pop('last_server_update', None)
        # TODO: are these commented out for a reason?
        # TODO: erase parent_unique_id if the parent isn't available in our input dataset
        #doc.pop('parent_unique_id', None)
        #doc.pop('terminated', None)

        for key in doc_content.keys():
            if key.endswith('_facet'):
                doc_content.pop(key, None)

        return doc_content


# TODO: implement
class DataAnonymizer(object):
    def __init__(self):
        pass

    @staticmethod
    def translate(s):
        """
        Super dumb translation for anonymizing strings.
        """
        s_new = ''
        for c in s:
            if c == '\\':
                s_new += c
            else:
                c = chr((ord(c)-65 + 13)%26 + 65)
                s_new += c
        return s_new

    def anonymize(self, doc):
        hostname = doc.get('hostname', '')
        hostname_new = DataAnonymizer.translate(hostname)
        username = doc.get('username', '')
        username_new = None

        translation_usernames = {}

        if len(username) > 0:
            if username.lower() != 'system' and username.lower() != 'local service' and username.lower() != 'network service':
                pieces = username.split('\\')
                for piece in pieces:
                    translation_usernames[piece] = DataAnonymizer.translate(piece)

        for field in doc:
            values = doc[field]
            try:
                if not values:
                    continue
                was_list = True
                targets = values
                if not hasattr(values, '__iter__'):
                    was_list = False
                    targets = [values]
                values = []
                for target in targets:
                    target = target.replace(hostname, hostname_new)
                    for key in translation_usernames:
                        target = target.replace(key, translation_usernames.get(key))
                    values.append(target)
                if not was_list:
                    values = values[0]
                doc[field] = values
            except AttributeError:
                pass

        return doc

    def munge_document(self, doc_type, doc_content):
        return self.anonymize(doc_content)
