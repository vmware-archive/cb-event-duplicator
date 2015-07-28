__author__ = 'jgarman'

import logging

log = logging.getLogger(__name__)

class Transporter(object):
    def __init__(self, input_source, output_sink):
        self.input_md5set = set()
        self.output_md5set = set()
        self.input_proc_guids = set()
        self.output_proc_guids = set()

        self.input = input_source
        self.output = output_sink
        self.mungers = [CleanseSolrData()]

        # sensor map hostname -> document
        self.sensors = dict()
        # map source sensor_id to hostname
        self.sensor_map = dict()

    @staticmethod
    def get_process_id(proc):
        old_style_id = proc.get('id', None)
        if old_style_id and old_style_id != '':
            return old_style_id
        else:
            new_style_id = proc.get('unique_id', None)
            if not new_style_id:
                log.warn("Process has no unique_id")
            return new_style_id

    @staticmethod
    def get_parent_process_id(proc):
        old_style_id = proc.get('parent_id', None)
        if old_style_id and old_style_id != '':
            return old_style_id
        else:
            new_style_id = proc.get('parent_unique_id', None)
            if not new_style_id:
                log.warn("Process has no parent_unique_id")
            return new_style_id

    def add_anonymizer(self, munger):
        self.mungers.append(munger)

    def output_process_doc(self, doc):
        for munger in self.mungers:
            doc = munger.munge_document('proc', doc)

        self.output.output_process_doc(doc)

    def update_sensors(self, proc):
        sensor_id = proc.get('sensor_id', 0)
        if not sensor_id:
            return []

        if sensor_id and sensor_id not in self.sensor_map:
            # new sensor, get the data from postgresql
            self.input.get_sensor_info(id=sensor_id)
            # add to our sensor_maps
            # notify caller that this sensor_id has to be inserted into the target
            return [sensor_id]

    def update_md5sums(self, proc):
        md5s = set()
        md5s.add(proc.get('process_md5'))
        for modload_complete in proc.get('modload_complete', []):
            fields = modload_complete.split('|')
            md5s.add(fields[1])

        retval = md5s = self.input_md5set
        self.input_md5set.union(retval)
        return retval

    def get_process_docs(self):
        # TODO: append so that this also grabs process trees when necessary
        for proc in self.input.get_process_docs():
            yield proc

    def transport(self, debug=False):
        # get process list

        for proc in self.get_process_docs():
            # TODO: better way of doing this once we have multiple ways of getting docs...
            self.input_proc_guids.add(Transporter.get_process_id(proc))
            new_md5sums = self.update_md5sums(proc)
            new_sensor_ids = self.update_sensors(proc)

            self.output_process_doc(proc)

        print 'Need md5s: %s' % self.input_md5set


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

        return doc_content


class DataAnonymizer(object):
    def __init__(self):
        pass

    def munge_document(self, doc_type, doc_content):
        return doc_content
