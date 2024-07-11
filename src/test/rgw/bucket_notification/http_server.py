#!/usr/bin/env python3
"""
License: MIT License
Copyright (c) 2023 Miel Donkers
Very simple HTTP server in python for logging requests
Usage::
    ./server.py [<port>]
"""
import logging
import threading
import time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import requests
import json
from cloudevents.http import from_http

META_PREFIX = 'x-amz-meta-'


class LogWrapper:
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.errors = 0

    def info(self, msg, *args, **kwargs):
        try:
            self.log.info(msg, *args, **kwargs)
        except BlockingIOError:
            self.errors += 1

    def warning(self, msg, *args, **kwargs):
        try:
            self.log.warning(msg, *args, **kwargs)
        except BlockingIOError:
            self.errors += 1

    def error(self, msg, *args, **kwargs):
        try:
            self.log.error(msg, *args, **kwargs)
        except BlockingIOError:
            self.errors += 1

    def __del__(self):
        if self.errors > 0:
            self.log.error("%d logs were lost", self.errors)


log = LogWrapper()


def verify_s3_records_by_elements(records, keys, exact_match=False, deletions=False):
    """ verify there is at least one record per element """
    for key in keys:
        key_found = False
        if type(records) is list:
            for record_list in records:
                if key_found:
                    break
                for record in record_list['Records']:
                    if record['s3']['bucket']['name'] == key.bucket.name and \
                            record['s3']['object']['key'] == key.name:
                        # Assertion Error needs to be fixed
                        if len(record['s3']['object']['metadata']) > 0:
                            for meta in record['s3']['object']['metadata']:
                                assert (meta['key'].startswith(META_PREFIX))
                        if deletions and record['eventName'].startswith('ObjectRemoved'):
                            key_found = True
                            break
                        elif not deletions and record['eventName'].startswith('ObjectCreated'):
                            key_found = True
                            break
        else:
            for record in records['Records']:
                if record['s3']['bucket']['name'] == key.bucket.name and \
                        record['s3']['object']['key'] == key.name:
                    if len(record['s3']['object']['metadata']) > 0:
                        for meta in record['s3']['object']['metadata']:
                            assert (meta['key'].startswith(META_PREFIX))
                    if deletions and record['eventName'].startswith('ObjectRemoved'):
                        key_found = True
                        break
                    elif not deletions and record['eventName'].startswith('ObjectCreated'):
                        key_found = True
                        break

        if not key_found:
            err = 'no ' + ('deletion' if deletions else 'creation') + ' event found for key: ' + str(key)
            assert False, err

    if not len(records) == len(keys):
        err = 'superfluous records are found'
        log.warning(err)
        if exact_match:
            for record_list in records:
                for record in record_list['Records']:
                    log.error(str(record['s3']['bucket']['name']) + ',' + str(record['s3']['object']['key']))
            assert False, err


class HTTPPostHandler(BaseHTTPRequestHandler):
    """HTTP POST hanler class storing the received events in its http server"""

    def do_POST(self):
        """implementation of POST handler"""
        content_length = int(self.headers['Content-Length'])
        if content_length == 0:
            log.info('HTTP Server received iempty event')
            self.send_response(200)
            self.end_headers()
            return
        body = self.rfile.read(content_length)
        if self.server.cloudevents:
            event = from_http(self.headers, body)
            record = json.loads(body)['Records'][0]
        log.info('HTTP Server received event: %s', str(body))
        self.server.append(json.loads(body))
        if self.headers.get('Expect') == '100-continue':
            self.send_response(100)
        else:
            self.send_response(200)
        if self.server.delay > 0:
            time.sleep(self.server.delay)
        self.end_headers()


class HTTPServerWithEvents(ThreadingHTTPServer):
    """multithreaded HTTP server used by the handler to store events"""

    def __init__(self, addr, delay=0, cloudevents=False):
        self.events = []
        self.delay = delay
        self.cloudevents = cloudevents
        self.addr = addr
        self.request_queue_size = 100
        self.lock = threading.Lock()
        ThreadingHTTPServer.__init__(self, addr, HTTPPostHandler)
        log.info('http server created on %s', self.addr)
        self.proc = threading.Thread(target=self.run)
        self.proc.daemon = True
        self.proc.start()
        retries = 0
        while self.proc.is_alive() == False and retries < 5:
            retries += 1
            time.sleep(5)
            log.warning('http server on %s did not start yet', str(self.addr))
        if not self.proc.is_alive():
            log.error('http server on %s failed to start. closing...', str(self.addr))
            self.close()
            assert False
        # make sure that http handler is able to consume requests
        url = 'http://{}:{}'.format(self.addr[0], self.addr[1])
        response = requests.post(url, {})
        assert response.status_code == 200

    def run(self):
        log.info('http server started on %s', str(self.addr))
        self.serve_forever()
        self.server_close()
        log.info('http server ended on %s', str(self.addr))

    def acquire_lock(self):
        if not self.lock.acquire(timeout=5):
            self.close()
            raise AssertionError('failed to acquire lock in HTTPServerWithEvents')

    def append(self, event):
        self.acquire_lock()
        self.events.append(event)
        self.lock.release()

    def verify_s3_events(self, keys, exact_match=False, deletions=False):
        """verify stored s3 records agains a list of keys"""
        self.acquire_lock()
        log.info('verify_s3_events: http server has %d events', len(self.events))
        try:
            verify_s3_records_by_elements(self.events, keys, exact_match=exact_match, deletions=deletions)
        except AssertionError as err:
            self.close()
            raise err
        finally:
            self.lock.release()
            self.events = []

    def get_and_reset_events(self):
        self.acquire_lock()
        log.info('get_and_reset_events: http server has %d events', len(self.events))
        events = self.events
        self.events = []
        self.lock.release()
        return events

    def close(self, task=None):
        log.info('http server on %s starting shutdown', str(self.addr))
        t = threading.Thread(target=self.shutdown)
        t.start()
        t.join(5)
        retries = 0
        while self.proc.is_alive() and retries < 5:
            retries += 1
            t.join(5)
            log.warning('http server on %s still alive', str(self.addr))
        if self.proc.is_alive():
            log.error('http server on %s failed to shutdown', str(self.addr))
            self.server_close()
        else:
            log.info('http server on %s shutdown ended', str(self.addr))


if __name__ == '__main__':
    from sys import argv

    host = 'localhost'
    if len(argv) == 2:
        port = int(argv[1])
        HTTPServerWithEvents((host, port))
    else:
        HTTPServerWithEvents((host, 1824))
    while True:
        print('server running')
        time.sleep(60)
