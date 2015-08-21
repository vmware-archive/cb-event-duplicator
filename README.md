# Event extractor/duplicator for Carbon Black

Extract events from one Carbon Black server and send them to another server - useful for demo/testing purposes

## Installation Quickstart

If you want to use this on a Carbon Black server, or any other CentOS 6.x, 7.x or Ubuntu based 64 bit Linux platform, 
you can use the pre-built binary available from the releases page. This single binary bundles together all the necessary
bits and will not interfere with your system installed Python, so it's highly recommended to use this binary unless you
plan on modifying the source code.

[Direct Download link v1.1.1](https://github.com/carbonblack/cb-event-duplicator/releases/download/v1.1.1/cb-event-duplicator)

### Source Installation

If you want to install from source, you can install it via:

```
python setup.py install
```

Once the package is installed, you will have a new script in your $PATH: `cb-event-duplicator`.

If you want to enable the optional SSH support to transparently
send or receive events via an SSH tunnel to another Cb server, then you will have to install the Python `paramiko` package.

## Usage

Command line usage:

```
usage: cb-event-duplicator [-h] [-v] [--key KEY] [--anonymize] [-q QUERY]
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

* `cb-event-duplicator http://server.com/package.zip local`

  Will import the events from the zip file located at http://server.com/package.zip into your local Cb server.
  The zip file is simply a packaged version of the directory tree created by this tool.

* `cb-event-duplicator --tree -q "process_name:googleupdate.exe" —anonymize root@172.22.10.7 /tmp/blah`

  Takes all processes that match “process_name:googleupdate.exe” and their parents/children and saves them all to `/tmp/blah`

* `cb-event-duplicator --tree -q "process_name:googleupdate.exe" —anonymize root@172.22.10.7 root@172.22.5.118`

  Same as above, just copies directly to the 172.22.5.118 server instead of saving the files to the local disk
