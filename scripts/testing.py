__author__ = 'jgarman'

import pprint


class DummyOutputSink(object):
    def __init__(self):
        pass

    def output_process_doc(self, doc_content):
        print "got process document:"
        pprint.pprint(doc_content)

