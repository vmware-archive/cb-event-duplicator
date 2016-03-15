from __future__ import absolute_import, division, print_function
import sys
import argparse
import re
import requests
import tempfile
import zipfile
import logging
import os.path
from cbopensource.tools.eventduplicator.solr_endpoint import SolrInputSource, SolrOutputSink, LocalConnection
from cbopensource.tools.eventduplicator.transporter import Transporter, DataAnonymizer
from cbopensource.tools.eventduplicator.file_endpoint import FileInputSource, FileOutputSink
from cbopensource.tools.eventduplicator import main_log
from cbopensource.tools.eventduplicator.ssh_connection import SSHConnection

__author__ = 'jgarman'


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


def input_from_zip(fn):
    tempdir = tempfile.mkdtemp()
    z = zipfile.ZipFile(fn)
    z.extractall(tempdir)

    return FileInputSource(tempdir)


def main():
    ssh_help = ", or a remote Cb server (root@cb5.server:2202)"
    parser = argparse.ArgumentParser(description="Transfer data from one Cb server to another")
    parser.add_argument("source", help="Data source - can be a pathname (/tmp/blah), " +
                                       "a URL referencing a zip package" +
                                       "(http://my.server.com/package.zip), the local Cb server (local)%s" % ssh_help)
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
    input_source = None

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
            print("Downloading package from %s..." % options.source)
            for block in response.iter_content(1024):
                handle.write(block)

            handle.flush()

            print("Done. Unzipping...")
            input_source = input_from_zip(handle.name)
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
        if not os.path.exists(options.source):
            sys.stderr.write("Cannot find file %s\n\n" % options.source)
            return 2

        if os.path.isdir(options.source):
            input_source = FileInputSource(options.source)
        else:
            print("Unzipping %s into a temporary directory for processing..." % options.source)
            input_source = input_from_zip(options.source)

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

    try:
        t.transport(debug=options.verbose)
    except KeyboardInterrupt:
        print("\nMigration interrupted. Processed:")
        print(t.get_report())
        return 1

    print("Migration complete!")
    print(t.get_report())
    return 0

if __name__ == '__main__':
    main()
