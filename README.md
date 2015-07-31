# Event extractor/duplicator for Carbon Black

Extract events from one Carbon Black server and send them to another server - useful for demo/testing purposes

This script is in **BETA** testing and requires some manual setup. The prerequisites are already included on a Cb
server, so you can install it on a Cb server and run without any setup. You can also run it on any other
platform that has Python 2.6+ and the following Python packages:

* paramiko (optional if you want to use the built-in SSH support)
* psycopg2 (already installed on Cb servers; otherwise, requires postgresql-devel and python-devel)

Command line usage:

```
usage: data_migration.py [-h] [-v] [--key KEY] [--anonymize] [-q QUERY]
                         [--tree]
                         source destination

Transfer data from one Cb server to another

positional arguments:
  source                Data source - can be a pathname (/tmp/blah), a URL
                        referencing a zip package
                        (http://my.server.com/package.zip), the local Cb
                        server (local), or a remote Cb server
                        (root@cb5.server:2202)
  destination           Data destination - can be a filepath (/tmp/blah), the
                        local Cb server (local), or a remote Cb server
                        (root@cb5.server:2202)

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         Increase output verbosity
  --key KEY             SSH private key location
  --anonymize           Anonymize data in transport
  -q QUERY, --query QUERY
                        Source data query (required for server input)
  --tree                Traverse up and down process tree
```

Examples:

* `python data_migration.py http://server.com/package.zip local`

  Will import the events from the zip file located at http://server.com/package.zip into your local Cb server.
  The zip file is simply a packaged version of the directory tree created by this tool.

* `python data_migration.py --tree -q "process_name:googleupdate.exe" —anonymize root@172.22.10.7 /tmp/blah`

  Takes all processes that match “process_name:googleupdate.exe” and their parents/children and saves them all to `/tmp/blah`

* `python data_migration.py --tree -q "process_name:googleupdate.exe" —anonymize root@172.22.10.7 root@172.22.5.118`

  Same as above, just copies directly to the 172.22.5.118 server instead of saving the files to the local disk
