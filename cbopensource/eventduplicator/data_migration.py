__author__ = 'jgarman'

import sys
import argparse
import re

from lib.solr_endpoint import SolrInputSource, SolrOutputSink, LocalConnection

try:
    from lib.ssh_connection import SSHConnection
    ssh_support = True
except ImportError:
    ssh_support = False
from lib.transporter import Transporter, DataAnonymizer
from lib.file_endpoint import FileInputSource, FileOutputSink
from lib import main_log
import requests
import tempfile
import zipfile
import logging


def initialize_logger(verbose):
    if verbose:
        main_log.setLevel(logging.DEBUG)
    else:
        main_log.setLevel(logging.INFO)

    # create console handler and set level to info
    handler = logging.StreamHandler()
    if verbose:
        handler.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)-15s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    main_log.addHandler(handler)


def main():
    if ssh_support:
        ssh_help = ", or a remote Cb server (root@cb5.server:2202)"
    else:
        ssh_help = ""

    parser = argparse.ArgumentParser(description="Transfer data from one Cb server to another")
    parser.add_argument("source", help="Data source - can be a pathname (/tmp/blah), " +
        "a URL referencing a zip package (http://my.server.com/package.zip), the local Cb server (local)%s" % ssh_help)
    parser.add_argument("destination", help="Data destination - can be a filepath (/tmp/blah), " +
                                            "the local Cb server (local)%s" % ssh_help)
    parser.add_argument("-v", "--verbose", help="Increase output verbosity", action="store_true")
    parser.add_argument("--anonymize", help="Anonymize data in transport", action="store_true", default=False)
    parser.add_argument("-q", "--query", help="Source data query (required for server input)", action="store")
    parser.add_argument("--tree", help="Traverse up and down process tree", action="store_true", default=False)

    options = parser.parse_args()

    host_match = re.compile("([^@]+)@([^:]+)(:([\d]*))?")

    source_parts = host_match.match(options.source)
    destination_parts = host_match.match(options.destination)

    if not ssh_support and (source_parts or destination_parts):
        sys.stderr.write("paramiko python package required for SSH support. Install via `pip install paramiko`\n")
        return 2

    initialize_logger(options.verbose)

    if options.source == options.destination:
        sys.stderr.write("Talk to yourself often?\n\n")
        parser.print_usage()
        return 2

    if options.source.startswith(('http://', 'https://')):
        with tempfile.NamedTemporaryFile() as handle:
            response = requests.get(options.source, stream=True)
            if not response.ok:
                raise Exception("Could not retrieve package at %s" % options.source)
            print "Downloading package from %s..." % options.source
            for block in response.iter_content(1024):
                handle.write(block)

            handle.flush()

            print "Done. Unzipping..."
            tempdir = tempfile.mkdtemp()
            z = zipfile.ZipFile(handle.name)
            z.extractall(tempdir)

            input_source = FileInputSource(tempdir)
    elif options.source == 'local':
        input_connection = LocalConnection()
        input_source = SolrInputSource(input_connection, query=options.query)
    elif source_parts:
        port_number = 22
        if source_parts.group(4):
            port_number = int(source_parts.group(4))

        input_connection = SSHConnection(username=source_parts.group(1), hostname=source_parts.group(2),
                                         port=port_number)
        input_source = SolrInputSource(input_connection, query=options.query)
    else:
        # source_parts is a file path
        input_source = FileInputSource(options.source)

    if type(input_source) == SolrInputSource:
        if not options.query:
            sys.stderr.write("Query is required when using Solr as a data source\n\n")
            parser.print_usage()
            return 2

    if options.destination == 'local':
        output_connection = LocalConnection()
        output_sink = SolrOutputSink(output_connection)
    elif destination_parts:
        port_number = 22
        if destination_parts.group(4):
            port_number = int(destination_parts.group(4))
        output_connection = SSHConnection(username=destination_parts.group(1), hostname=destination_parts.group(2),
                                          port=port_number)
        output_sink = SolrOutputSink(output_connection)
    else:
        output_sink = FileOutputSink(options.destination)

    t = Transporter(input_source, output_sink, tree=options.tree)

    if options.anonymize:
        t.add_anonymizer(DataAnonymizer())

    t.transport(debug=options.verbose)
    print "Migration complete!"
    print t.get_report()

# TODO: add exception wrapper so we clean up after ourselves if there's an error
if __name__ == '__main__':
    sys.exit(main())