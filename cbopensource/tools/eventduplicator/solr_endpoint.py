__author__ = 'jgarman'

import re
import psycopg2
import psycopg2.extras
import requests
import json
from utils import get_process_id, update_sensor_id_refs, update_feed_id_refs
from copy import deepcopy
from collections import defaultdict
import logging
import datetime

log = logging.getLogger(__name__)


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
            log.error("Error inserting row into table %s, id %s: %s" % (table_name, obj.get("id", None), e.message))
            return None


class LocalConnection(object):
    def __init__(self):
        # TODO: if for some reason someone has changed SolrPort on their cb server... this is incorrect
        self.solr_url_base = 'http://127.0.0.1:8080'
        self.session = requests.Session()

    def open_file(self, filename, mode='r'):
        return open(filename, mode)

    def open_db(self, user, password, database, host, port):
        return psycopg2.connect(user=user, password=password, database=database, host=host, port=port)

    def http_get(self, path, *args, **kwargs):
        return self.session.get('%s%s' % (self.solr_url_base, path), *args, **kwargs)

    def http_post(self, path, *args, **kwargs):
        return self.session.post('%s%s' % (self.solr_url_base, path), *args, **kwargs)

    def close(self):
        pass

    def __str__(self):
        return "Local Cb datastore"


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
        except Exception as e:
            log.error("Error getting feed metadata for id %s: %s" % (feed_id, e.message))
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
            cur.execute('SELECT * FROM sensor_builds WHERE id=%s', (sensor_info['build_id'],))
            build_info = cur.fetchone()
            cur.execute('SELECT * FROM sensor_os_environments WHERE id=%s', (sensor_info['os_environment_id'],))
            environment_info = cur.fetchone()
            conn.commit()
        except Exception as e:
            log.error("Error getting sensor data for sensor id %s: %s" % (sensor_id, e.message))
            return None

        if not sensor_info or not build_info or not environment_info:
            log.error("Could not get full sensor data for sensor id %d" % sensor_id)
            return None

        return {
            'sensor_info': sensor_info,
            'build_info': build_info,
            'os_info': environment_info
        }

    def connection_name(self):
        return str(self.connection)

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

        self.written_docs = defaultdict(int)
        self.new_metadata = defaultdict(list)
        self.doc_endpoints = {
            'binary': '/solr/cbmodules/update/json',
            'proc': '/solr/0/update',
            'feed': '/solr/cbfeeds/update/json'
        }

        self.now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def set_data_version(self, version):
        target_version = self.connection.open_file('/usr/share/cb/VERSION').read().strip()
        source_major_version = '.'.join(version.split('.')[:2])
        target_major_version = '.'.join(target_version.split('.')[:2])
        if source_major_version != target_major_version:
            log.warning(("Source data was generated from Cb version %s; target is %s. This may not work. "
                         "Continuing anyway." % (version, target_version)))

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

    def output_doc(self, doc_type, doc_content):
        args = {"add": {"commitWithin": 5000, "doc": doc_content}}
        headers = {'content-type': 'application/json; charset=utf8'}
        r = self.solr_post(self.doc_endpoints[doc_type],
                           data=json.dumps(args), headers=headers, timeout=60)

        self.written_docs[doc_type] += 1

        if not r.ok:
            log.error("Error sending document to destination Solr: %s" % r.content)
        return r

    def output_feed_doc(self, doc_content):
        if doc_content['feed_id'] not in self.feed_id_map:
            log.warning("got feed document %s:%s without associated feed metadata" % (doc_content['feed_name'],
                                                                                         doc_content['id']))
        else:
            feed_id = self.feed_id_map[doc_content['feed_id']]
            doc_content = deepcopy(doc_content)
            update_feed_id_refs(doc_content, feed_id)

        self.output_doc("feed", doc_content)

    def output_binary_doc(self, doc_content):
        md5sum = doc_content.get('md5').upper()
        if md5sum in self.existing_md5s:
            return

        if self.get_binary_doc(md5sum):
            self.existing_md5s.add(md5sum)
            return

        self.output_doc("binary", doc_content)

    def output_process_doc(self, doc_content):
        # first, update the sensor_id in the process document to match the target settings
        if doc_content['sensor_id'] not in self.sensor_id_map:
            log.warning("Got process document %s without associated sensor data" % get_process_id(doc_content))
        else:
            sensor_id = self.sensor_id_map[doc_content['sensor_id']]
            doc_content = deepcopy(doc_content)
            update_sensor_id_refs(doc_content, sensor_id)

        # fix up the last_update field
        last_update = doc_content.get("last_update", None) or self.now
        doc_content["last_update"] = {"set": last_update}

        doc_content.pop("last_server_update", None)

        self.output_doc("proc", doc_content)

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
        self.new_metadata['feed'].append(doc_content['name'])

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

        self.new_metadata['sensor'].append(doc_content['sensor_info']['computer_name'])
        self.sensor_id_map[original_id] = sensor_id

    def cleanup(self):
        headers = {'content-type': 'application/json; charset=utf8'}
        args = {}

        for doc_type in self.doc_endpoints.keys():
            self.solr_post(self.doc_endpoints[doc_type] + '?commit=true',
                           data=json.dumps(args), headers=headers, timeout=60)

    def connection_name(self):
        return str(self.connection)

    def report(self):
        report_data = "Documents inserted into %s by type:\n" % (self.connection,)
        for key in self.written_docs.keys():
            report_data += " %8s: %d\n" % (key, self.written_docs[key])
        for key in self.new_metadata.keys():
            report_data += "New %ss created in %s:\n" % (key, self.connection)
            for value in self.new_metadata[key]:
                report_data += " %s\n" % value

        return report_data
