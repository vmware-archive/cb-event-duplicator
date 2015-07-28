__author__ = 'jgarman'

import pprint


class DummyOutputSink(object):
    def __init__(self):
        pass

    def output_process_doc(self, doc_content):
        # TODO: for testing purposes, ensure that we receive the sensor and binary docs FIRST before this is called
        print "got process document:"
        pprint.pprint(doc_content)

    def output_binary_doc(self, doc_content):
        print "got binary document:"
        pprint.pprint(doc_content)

    def output_sensor_info(self, doc_content):
        print "got sensor info:"
        pprint.pprint(doc_content)

