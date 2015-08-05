__author__ = 'jgarman'

import paramiko
try:
    import SocketServer
except ImportError:
    import socketserver as SocketServer
import select
import threading
import requests
import logging
import getpass
import psycopg2
import socket

log = logging.getLogger(__name__)


def get_password(server_name):
    return getpass.getpass("Enter password for %s: " % server_name)


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
            log.debug('Incoming request to %s:%d failed: %s' % (self.chain_host,
                                                              self.chain_port,
                                                              repr(e)))
            return
        if chan is None:
            log.debug('Incoming request to %s:%d was rejected by the SSH server.' %
                    (self.chain_host, self.chain_port))
            return

        log.debug('Connected!  Tunnel open %r -> %r -> %r' % (self.request.getpeername(),
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
        log.debug('Tunnel closed from %r' % (peername,))


class SSHConnection(object):
    def __init__(self, username, hostname, port, password_callback=get_password):
        self.ssh_connection = paramiko.SSHClient()
        self.ssh_connection.load_system_host_keys()
        self.ssh_connection.set_missing_host_key_policy(paramiko.WarningPolicy())
        self.name = "%s@%s:%d" % (username, hostname, port)
        self.session = requests.Session()

        connected = False
        password = None
        while not connected:
            try:
                self.ssh_connection.connect(hostname=hostname, username=username, port=port, look_for_keys=False,
                                            password=password, timeout=2.0, banner_timeout=2.0)
                connected = True
            except paramiko.AuthenticationException as e:
                password = password_callback(self.name)
            except paramiko.SSHException as e:
                log.error("Error logging into %s: %s" % (self.name, e.message))
                raise
            except socket.error as e:
                log.error("Error connecting to %s: %s" % (self.name, e.message))
                raise

        self.forwarded_connections = []

        solr_forwarded_port = self.forward_tunnel('127.0.0.1', 8080)
        self.solr_url_base = 'http://127.0.0.1:%d' % solr_forwarded_port

    def http_get(self, path, *args, **kwargs):
        return self.session.get('%s%s' % (self.solr_url_base, path), *args, **kwargs)

    def http_post(self, path, *args, **kwargs):
        return self.session.post('%s%s' % (self.solr_url_base, path), *args, **kwargs)

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

    def __str__(self):
        return self.name
