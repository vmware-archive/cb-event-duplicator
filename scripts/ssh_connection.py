__author__ = 'jgarman'

import paramiko
try:
    import SocketServer
except ImportError:
    import socketserver as SocketServer
import select
import threading
import re
import psycopg2
import psycopg2.extras
import requests
import logging
import pprint
import json

log = logging.getLogger(__name__)

# TODO: replace with proper logging
def verbose(d):
    log.debug(d)


class ForwardServer (SocketServer.ThreadingTCPServer, threading.Thread):
    def __init__(self, *args, **kwargs):
        SocketServer.ThreadingTCPServer.__init__(self, *args, **kwargs)
        threading.Thread.__init__(self)

    daemon_threads = True
    allow_reuse_address = True

    def run(self):
        return self.serve_forever()


def get_request_handler(remote_host, remote_port, transport):
    class SubHandler(Handler):
        chain_host = remote_host
        chain_port = int(remote_port)
        ssh_transport = transport

    return SubHandler


class Handler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
            chan = self.ssh_transport.open_channel('direct-tcpip',
                                                   (self.chain_host, self.chain_port),
                                                   self.request.getpeername())
        except Exception as e:
            verbose('Incoming request to %s:%d failed: %s' % (self.chain_host,
                                                              self.chain_port,
                                                              repr(e)))
            return
        if chan is None:
            verbose('Incoming request to %s:%d was rejected by the SSH server.' %
                    (self.chain_host, self.chain_port))
            return

        verbose('Connected!  Tunnel open %r -> %r -> %r' % (self.request.getpeername(),
                                                            chan.getpeername(), (self.chain_host, self.chain_port)))
        while True:
            r, w, x = select.select([self.request, chan], [], [])
            if self.request in r:
                data = self.request.recv(1024)
                if len(data) == 0:
                    break
                chan.send(data)
            if chan in r:
                data = chan.recv(1024)
                if len(data) == 0:
                    break
                self.request.send(data)

        peername = self.request.getpeername()
        chan.close()
        self.request.close()
        verbose('Tunnel closed from %r' % (peername,))


class SSHBase(object):
    def __init__(self, username, hostname, port, private_key):
        self.ssh_connection = paramiko.SSHClient()
        self.ssh_connection.load_system_host_keys()
        self.ssh_connection.set_missing_host_key_policy(paramiko.WarningPolicy())

        if private_key: # TODO: fill in, if we need it.
            pass

        self.ssh_connection.connect(hostname=hostname, username=username, port=port, look_for_keys=False)
        self.forwarded_connections = []
        self.have_cb_conf = False
        self.get_cb_conf()

        solr_forwarded_port = self.forward_tunnel('127.0.0.1', int(self.get_cb_conf_item('SolrPort', 8080)))
        self.solr_url_base = 'http://127.0.0.1:%d' % solr_forwarded_port

    def forward_tunnel(self, remote_host, remote_port):
        # this is a little convoluted, but lets me configure things for the Handler
        # object.  (SocketServer doesn't give Handlers any way to access the outer
        # server normally.)

        transport = self.ssh_connection.get_transport()

        local_port = 12001
        conn = None
        while not conn and local_port < 65536:
            try:
                conn = ForwardServer(('127.0.0.1', local_port),
                                     get_request_handler(remote_host, remote_port, transport))
            except:
                local_port += 1

        if conn:
            conn.start()
            self.forwarded_connections.append(conn)
            return local_port

        raise Exception("Cannot find open local port")

    def close(self):
        for conn in self.forwarded_connections:
            conn.shutdown()

    def open_file(self, filename, mode='r'):
        return self.ssh_connection.open_sftp().file(filename, mode=mode)

    def get_cb_conf(self):
        fp = self.open_file('/etc/cb/cb.conf')
        self.cb_conf = fp.read()
        self.have_cb_conf = True

    def get_cb_conf_item(self, item, default=None):
        retval = default
        if not self.have_cb_conf:
            self.get_cb_conf()
        re_match = re.compile("%s=([^\\n]+)" % item)
        matches = re_match.search(self.cb_conf)

        if matches:
            retval = matches.group(1)

        return retval

    def connect_database(self):
        """
        :return: database_connection
        :rtype: psycopg2.connection
        """
        url = self.get_cb_conf_item('DatabaseURL', None)
        if not url:
            raise Exception("Could not get DatabaseURL from remote server")

        db_pattern = re.compile('postgresql\\+psycopg2:\\/\\/([^:]+):([^@]+)@([^:]+):(\d+)/(.*)')
        db_match = db_pattern.match(url)
        if db_match:
            username = db_match.group(1)
            password = db_match.group(2)
            hostname = db_match.group(3)
            remote_port = db_match.group(4)
            database_name = db_match.group(5)

            local_port = self.forward_tunnel(remote_host=hostname, remote_port=remote_port)
            conn = psycopg2.connect(user=username, password=password, database=database_name, host='127.0.0.1',
                                    port=local_port)

            return conn
        else:
            raise Exception("Could not connect to database")

    def __del__(self):
        self.close()

    def solr_get(self, path, *args, **kwargs):
        return requests.get('%s%s' % (self.solr_url_base, path), *args, **kwargs)

    def solr_post(self, path, *args, **kwargs):
        return requests.post('%s%s' % (self.solr_url_base, path), *args, **kwargs)


class SSHInputSource(SSHBase):
    def __init__(self, **kwargs):
        self.query = kwargs.pop('query')
        self.pagination_length = 20
        SSHBase.__init__(self, **kwargs)

    def paginated_get(self, query, params, start=0):
        params['rows'] = self.pagination_length
        params['start'] = start
        while True:
            resp = self.solr_get(query, params=params)
            rj = resp.json()
            docs = rj.get('response', {}).get('docs', [])
            if not len(docs):
                break
            for doc in docs:
                yield doc

            params['start'] += len(docs)
            params['rows'] = self.pagination_length

    def get_process_docs(self):
        query = "/solr/0/select"
        params = {
            'q': self.query,
            'sort': 'start asc',
            'wt': 'json'
        }
        for doc in self.paginated_get(query, params):
            yield doc

    def get_binary_doc(self, md5sum):
        query = "/solr/cbmodules/select"
        params = {
            'q': 'md5:%s' % md5sum.upper(),
            'wt': 'json'
        }
        result = self.solr_get(query, params=params)
        if result.status_code != 200:
            return None
        rj = result.json()
        docs = rj.get('response', {}).get('docs', [{}])
        if len(docs) == 0:
            return None
        return docs[0]

    def get_sensor_doc(self, sensor_id):
        try:
            conn = self.connect_database()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute('select * from sensor_registrations where id=%s',(sensor_id,))
            sensor_info = cur.fetchone()
            if not sensor_info:
                return None
            cur.execute('select * from sensor_builds where id=%s', (sensor_info['build_id'],))
            build_info = cur.fetchone()
            cur.execute('select * from sensor_os_environments where id=%s', (sensor_info['os_environment_id'],))
            environment_info = cur.fetchone()
            conn.commit()
        except:
            import traceback
            traceback.print_exc()
            return None

        return {
            'sensor_info': sensor_info,
            'build_info': build_info,
            'os_info': environment_info
        }

    def get_feed_docs(self):
        pass

    def get_alert_docs(self):
        pass

    def cleanup(self):
        pass


class SSHOutputSink(SSHBase):
    def __init__(self, **kwargs):
        SSHBase.__init__(self, **kwargs)
        self.existing_md5s = set()

    # TODO: cut-and-paste violation
    def get_binary_doc(self, md5sum):
        query = "/solr/cbmodules/select"
        params = {
            'q': 'md5:%s' % md5sum.upper(),
            'wt': 'json'
        }
        result = self.solr_get(query, params=params)
        if result.status_code != 200:
            return None
        rj = result.json()
        docs = rj.get('response', {}).get('docs', [{}])
        if len(docs) == 0:
            return None
        return docs[0]

    def output_doc(self, url, doc_content):
        args = {"add": {"commitWithin": 5000, "doc": doc_content}}
        headers = {'content-type': 'application/json; charset=utf8'}
        r = self.solr_post(url, data=json.dumps(args), headers=headers, timeout=60)
        if not r.ok:
            print "ERROR:", r.content
        return r

    def output_binary_doc(self, doc_content):
        md5sum = doc_content.get('md5').upper()
        if md5sum in self.existing_md5s:
            return

        if self.get_binary_doc(md5sum):
            self.existing_md5s.add(md5sum)
            return

        self.output_doc("/solr/cbmodules/update/json", doc_content)

    def output_process_doc(self, doc_content):
        self.output_doc("/solr/0/update", doc_content)

    def output_sensor_info(self, doc_content):
        # we need to first ensure that the sensor build and os_environment are available in the target server

        pprint.pprint(doc_content)

    def cleanup(self):
        headers = {'content-type': 'application/json; charset=utf8'}
        args = {}

        self.solr_post("/solr/0/update?commit=true", data=json.dumps(args), headers=headers, timeout=60)
        self.solr_post("/solr/cbmodules/update/json?commit=true", data=json.dumps(args), headers=headers, timeout=60)
        self.solr_post("/solr/cbfeeds/update/json?commit=true", data=json.dumps(args), headers=headers, timeout=60)


if __name__ == '__main__':
    c = SSHInputSource(username='root', hostname='cb5.wedgie.org', port=2202, private_key=None,
                       query='process_name:chrome.exe')
    conn = c.connect_database()

    from IPython import embed
    embed()
    c.close()