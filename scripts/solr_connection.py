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
import json
from utils import get_process_id, update_sensor_id_refs, update_feed_id_refs
from copy import deepcopy

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


class SolrBase(object):
    def __init__(self, connection):
        self.connection = connection
        self.have_cb_conf = False
        self.dbhandle = None

    def __del__(self):
        self.close()

    def close(self):
        self.connection.close()

    def get_cb_conf(self):
        fp = self.connection.open_file('/etc/cb/cb.conf')
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

    def get_db_parameters(self):
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

            return (username, password, hostname, remote_port, database_name)

        raise Exception("Could not connect to database")

    def dbconn(self):
        """
        :return: database_connection
        :rtype: psycopg2.connection
        """
        if self.dbhandle:
            return self.dbhandle

        username, password, hostname, remote_port, database_name = self.get_db_parameters()
        conn = self.connection.open_db(user=username, password=password, database=database_name, host='127.0.0.1',
                                       port=remote_port)

        self.dbhandle = conn
        return conn

    def solr_get(self, path, *args, **kwargs):
        return self.connection.http_get(path, *args, **kwargs)

    def solr_post(self, path, *args, **kwargs):
        return self.connection.http_post(path, *args, **kwargs)

    def find_db_row_matching(self, table_name, obj):
        obj.pop('id', None)

        cursor = self.dbconn().cursor()
        predicate = ' AND '.join(["%s = %%(%s)s" % (key, key) for key in obj.keys()])

        # FIXME: is there a better way to do this?
        query = 'SELECT id from %s WHERE %s' % (table_name, predicate)
        cursor.execute(query, obj)

        row_id = cursor.fetchone()
        if row_id:
            return row_id[0]
        else:
            return None

    def insert_db_row(self, table_name, obj):
        obj.pop('id', None)

        cursor = self.dbconn().cursor()
        fields = ', '.join(obj.keys())
        values = ', '.join(['%%(%s)s' % x for x in obj])
        query = 'INSERT INTO %s (%s) VALUES (%s) RETURNING id' % (table_name, fields, values)
        try:
            cursor.execute(query, obj)
            self.dbconn().commit()
            row_id = cursor.fetchone()[0]
            return row_id
        except psycopg2.Error as e:
            import traceback
            traceback.print_exc()
            return None


class LocalConnection(object):
    def __init__(self):
        # TODO: if for some reason someone has changed SolrPort on their cb server... this is incorrect
        self.solr_url_base = 'http://127.0.0.1:8080'

    def open_file(self, filename, mode='r'):
        return open(filename, mode)

    def open_db(self, user, password, database, host, port):
        return psycopg2.connect(user=user, password=password, database=database, host=host, port=port)

    def http_get(self, path, *args, **kwargs):
        return requests.get('%s%s' % (self.solr_url_base, path), *args, **kwargs)

    def http_post(self, path, *args, **kwargs):
        return requests.post('%s%s' % (self.solr_url_base, path), *args, **kwargs)

    def close(self):
        pass


class SSHConnection(object):
    def __init__(self, username, hostname, port, private_key):
        self.ssh_connection = paramiko.SSHClient()
        self.ssh_connection.load_system_host_keys()
        self.ssh_connection.set_missing_host_key_policy(paramiko.WarningPolicy())

        if private_key: # TODO: fill in, if we need it.
            pass

        self.ssh_connection.connect(hostname=hostname, username=username, port=port, look_for_keys=False)
        self.forwarded_connections = []

        solr_forwarded_port = self.forward_tunnel('127.0.0.1', 8080)
        self.solr_url_base = 'http://127.0.0.1:%d' % solr_forwarded_port

    def http_get(self, path, *args, **kwargs):
        return requests.get('%s%s' % (self.solr_url_base, path), *args, **kwargs)

    def http_post(self, path, *args, **kwargs):
        return requests.post('%s%s' % (self.solr_url_base, path), *args, **kwargs)

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
            conn.daemon = True
            conn.start()
            self.forwarded_connections.append(conn)
            return local_port

        raise Exception("Cannot find open local port")

    def close(self):
        for conn in self.forwarded_connections:
            conn.shutdown()

    def open_file(self, filename, mode='r'):
        return self.ssh_connection.open_sftp().file(filename, mode=mode)

    def open_db(self, user, password, database, host, port):
        local_port = self.forward_tunnel(remote_host=host, remote_port=port)
        return psycopg2.connect(user=user, password=password, database=database, host=host, port=local_port)


class SolrInputSource(SolrBase):
    def __init__(self, connection, **kwargs):
        self.query = kwargs.pop('query')
        self.pagination_length = 20
        super(SolrInputSource, self).__init__(connection)

    def doc_count_hint(self):
        query = "/solr/0/select"
        params = {
            'q': self.query,
            'sort': 'start asc',
            'wt': 'json',
            'rows': 0
        }
        resp = self.solr_get(query, params=params)
        rj = resp.json()
        return rj.get('response', {}).get('numFound', 0)

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

    def get_process_docs(self, query_filter=None):
        query = "/solr/0/select"
        if not query_filter:
            query_filter = self.query

        params = {
            'q': query_filter,
            'sort': 'start asc',
            'wt': 'json'
        }
        for doc in self.paginated_get(query, params):
            yield doc

    def get_feed_doc(self, feed_key):
        query = "/solr/cbfeeds/select"
        feed_name, feed_id = feed_key.split(':')

        params = {
            'q': 'id:"%s" AND feed_name:%s' % (feed_id, feed_name),
            'wt': 'json'
        }
        result = self.solr_get(query, params=params)
        if not result.ok:
            return None
        rj = result.json()
        docs = rj.get('response', {}).get('docs', [{}])
        if len(docs) == 0:
            return None

        return docs[0]

    def get_feed_metadata(self, feed_id):
        try:
            conn = self.dbconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute('SELECT id,name,display_name,feed_url,summary,icon,provider_url,tech_data,category,icon_small FROM alliance_feeds WHERE id=%s', (feed_id,))
            feed_info = cur.fetchone()
            if not feed_info:
                return None

            conn.commit()
        except:
            import traceback
            traceback.print_exc()
            return None

        return feed_info

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

    def get_version(self):
        return self.connection.open_file('/usr/share/cb/VERSION').read()

    def get_sensor_doc(self, sensor_id):
        try:
            conn = self.dbconn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute('SELECT * FROM sensor_registrations WHERE id=%s',(sensor_id,))
            sensor_info = cur.fetchone()
            if not sensor_info:
                return None
            cur.execute('SELECT * FROM sensor_builds WHERE id=%s', (sensor_info['build_id'],))
            build_info = cur.fetchone()
            cur.execute('SELECT * FROM sensor_os_environments WHERE id=%s', (sensor_info['os_environment_id'],))
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

    def cleanup(self):
        pass


class SolrOutputSink(SolrBase):
    def __init__(self, connection):
        super(SolrOutputSink, self).__init__(connection)
        self.feed_id_map = {}
        self.existing_md5s = set()
        self.sensor_id_map = {}
        self.sensor_os_map = {}
        self.sensor_build_map = {}

    def set_data_version(self, version):
        target_version = self.connection.open_file('/usr/share/cb/VERSION').read()
        if version.strip() != target_version.strip():
            return False

        return True

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

    def output_feed_doc(self, doc_content):
        if doc_content['feed_id'] not in self.feed_id_map:
            print "WARNING: got feed document %s:%s without associated feed metadata" % (doc_content['feed_name'],
                                                                                         doc_content['id'])
        else:
            feed_id = self.feed_id_map[doc_content['feed_id']]
            doc_content = deepcopy(doc_content)
            update_feed_id_refs(doc_content, feed_id)

        self.output_doc("/solr/cbfeeds/update/json", doc_content)

    def output_binary_doc(self, doc_content):
        md5sum = doc_content.get('md5').upper()
        if md5sum in self.existing_md5s:
            return

        if self.get_binary_doc(md5sum):
            self.existing_md5s.add(md5sum)
            return

        self.output_doc("/solr/cbmodules/update/json", doc_content)

    def output_process_doc(self, doc_content):
        # first, update the sensor_id in the process document to match the target settings
        if doc_content['sensor_id'] not in self.sensor_id_map:
            print "WARNING: got process document %s without associated sensor data" % get_process_id(doc_content)
        else:
            sensor_id = self.sensor_id_map[doc_content['sensor_id']]
            doc_content = deepcopy(doc_content)
            update_sensor_id_refs(doc_content, sensor_id)

        self.output_doc("/solr/0/update", doc_content)

    def output_feed_metadata(self, doc_content):
        original_id = doc_content['id']
        feed_id = self.find_db_row_matching('alliance_feeds', {'name': doc_content['name']})
        if feed_id:
            self.feed_id_map[original_id] = feed_id
            return

        doc_content.pop('id', None)
        doc_content['manually_added'] = True
        doc_content['enabled'] = False
        doc_content['display_name'] += ' (added via cb-event-duplicator)'

        feed_id = self.insert_db_row('alliance_feeds', doc_content)

        self.feed_id_map[original_id] = feed_id

    def output_sensor_info(self, doc_content):
        original_id = doc_content['sensor_info']['id']
        sensor_id = self.find_db_row_matching('sensor_registrations',
                                              {'computer_dns_name': doc_content['sensor_info']['computer_dns_name'],
                                               'computer_name': doc_content['sensor_info']['computer_name']})

        if sensor_id:
            # there's already a sensor that matches what we're looking for
            self.sensor_id_map[original_id] = sensor_id
            return

        # we need to first ensure that the sensor build and os_environment are available in the target server
        os_id = self.find_db_row_matching('sensor_os_environments', doc_content['os_info'])
        if not os_id:
            os_id = self.insert_db_row('sensor_os_environments', doc_content['os_info'])

        build_id = self.find_db_row_matching('sensor_builds', doc_content['build_info'])
        if not build_id:
            build_id = self.insert_db_row('sensor_builds', doc_content['build_info'])

        doc_content['sensor_info']['group_id'] = 1         # TODO: mirror groups?
        doc_content['sensor_info']['build_id'] = build_id
        doc_content['sensor_info']['os_environment_id'] = os_id
        sensor_id = self.insert_db_row('sensor_registrations', doc_content['sensor_info'])

        self.sensor_id_map[original_id] = sensor_id

    def cleanup(self):
        headers = {'content-type': 'application/json; charset=utf8'}
        args = {}

        self.solr_post("/solr/0/update?commit=true", data=json.dumps(args), headers=headers, timeout=60)
        self.solr_post("/solr/cbmodules/update/json?commit=true", data=json.dumps(args), headers=headers, timeout=60)
        self.solr_post("/solr/cbfeeds/update/json?commit=true", data=json.dumps(args), headers=headers, timeout=60)


if __name__ == '__main__':
    s = SSHConnection(username='root', hostname='cb5.wedgie.org', port=2202, private_key=None)
    c = SolrInputSource(s, query='process_name:chrome.exe')
    conn = c.dbconn()

    from IPython import embed
    embed()
    c.close()