__author__ = 'jgarman'

import os
import sys
import argparse
import re
from ssh_connection import SSHInputSource, SSHOutputSink
from transporter import Transporter, DataAnonymizer
from file_connection import FileInputSource, FileOutputSink
import requests
import tempfile
import zipfile

def main():
    parser = argparse.ArgumentParser(description="Transfer data from one Cb server to another")
    parser.add_argument("source", help="Data source - can be a filepath /tmp/blah.json or a server root@cb5.server:2202")
    parser.add_argument("destination", help="Data destination - can be a filepath /tmp/blah.json or a server root@cb5.server:2202")
    parser.add_argument("-v", "--verbose", help="Increase output verbosity", action="store_true")
    parser.add_argument("--key", help="SSH private key location", action="store",
                        default=os.path.join(os.path.expanduser('~'), '.ssh', 'id_rsa'))
    parser.add_argument("--anonymize", help="Anonymize data in transport", action="store_true", default=False)
    parser.add_argument("-q", "--query", help="Source data query (required for server input)", action="store")

    options = parser.parse_args()

    host_match = re.compile("([^@]+)@([^:]+)(:([\d]*))?")

    source_parts = host_match.match(options.source)
    destination_parts = host_match.match(options.destination)

    if options.source.startswith(('http://', 'https://')):
        with tempfile.NamedTemporaryFile() as handle:
            response = requests.get(options.source, stream=True)
            if not response.ok:
                raise Exception("Could not retrieve package at %s" % options.source)
            for block in response.iter_content(1024):
                handle.write(block)

            tempdir = tempfile.mkdtemp()
            z = zipfile.ZipFile(handle.name)
            z.extractall(tempdir)

            input_source = FileInputSource(tempdir)
    if not source_parts:
        # source_parts is a file path
        input_source = FileInputSource(options.source)
    else:
        port_number = 22
        if source_parts.group(4):
            port_number = int(source_parts.group(4))
        if not options.query:
            sys.stderr.write("Query is required when using SSH source\n\n")
            parser.print_usage()
            return 2

        input_source = SSHInputSource(username=source_parts.group(1), hostname=source_parts.group(2),
                                      port=port_number, private_key=options.key, query=options.query)

    if not destination_parts:
        output_sink = FileOutputSink(options.destination)
    else:
        port_number = 22
        if destination_parts.group(4):
            port_number = destination_parts.group(4)
        output_sink = SSHOutputSink(username=destination_parts.group(1), hostname=destination_parts.group(2),
                                    port=port_number, private_key=options.key)

    t = Transporter(input_source, output_sink)

    if options.anonymize:
        t.add_anonymizer(DataAnonymizer())

    t.transport(debug=options.verbose)

# TODO: add exception wrapper so we clean up after ourselves if there's an error
if __name__ == '__main__':
    sys.exit(main())