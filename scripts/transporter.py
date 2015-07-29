__author__ = 'jgarman'

import logging
from utils import get_process_id, get_parent_process_id

log = logging.getLogger(__name__)

class Transporter(object):
    # TODO: add check for input and output Cb version before executing
    # - this data is found in the file /usr/share/cb/VERSION

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

    def add_anonymizer(self, munger):
        self.mungers.append(munger)

    def output_process_doc(self, doc):
        for munger in self.mungers:
            doc = munger.munge_document('proc', doc)

        self.output.output_process_doc(doc)

    def output_binary_doc(self, doc):
        for munger in self.mungers:
            doc = munger.munge_document('binary', doc)

        self.output.output_binary_doc(doc)

    def output_sensor_info(self, doc):
        self.output.output_sensor_info(doc)

    def update_sensors(self, proc):
        sensor_id = proc.get('sensor_id', 0)
        if not sensor_id:
            return []

        if sensor_id and sensor_id not in self.sensor_map:
            # notify caller that this sensor_id has to be inserted into the target
            return [sensor_id]

        return []

    def update_md5sums(self, proc):
        md5s = set()
        md5s.add(proc.get('process_md5'))
        for modload_complete in proc.get('modload_complete', []):
            fields = modload_complete.split('|')
            md5s.add(fields[1])

        retval = md5s - self.input_md5set
        self.input_md5set |= md5s
        return retval

    def get_process_docs(self):
        # TODO: append so that this also grabs process trees when necessary
        for proc in self.input.get_process_docs():
            yield proc

    def update_sensor_info(self, sensor_id):
        # FIXME: this will merge multiple sensors with the same hostname together.
        # new sensor, get the data from postgresql
        data = self.input.get_sensor_doc(sensor_id)
        if not data:
            return

        hostname = data['sensor_info'].get('computer_dns_name')
        self.sensor_map[sensor_id] = hostname
        self.sensors[hostname] = data

        return data

    def transport(self, debug=False):
        # TODO: multithread this so we have some parallelization

        input_version = self.input.get_version()
        if not self.output.set_data_version(input_version):
            raise Exception("Input and Output versions are incompatible")

        # get process list
        for proc in self.get_process_docs():
            self.input_proc_guids.add(get_process_id(proc))
            new_md5sums = self.update_md5sums(proc)
            new_sensor_ids = self.update_sensors(proc)

            # output docs, sending binaries & sensors first
            for md5sum in new_md5sums:
                doc = self.input.get_binary_doc(md5sum)
                if not doc:
                    pass
                    # TODO: logging
                    # print "Could not retrieve MD5sum %s" % md5sum
                else:
                    self.output_binary_doc(doc)

            for sensor in new_sensor_ids:
                doc = self.update_sensor_info(sensor)
                self.output_sensor_info(doc)

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

    def munge_document(self, doc_type, doc_content):
        return doc_content
