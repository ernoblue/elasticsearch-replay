import logging
from urllib import urlencode

from elasticsearch.transport import Transport
from elasticsearch.exceptions import TransportError, SerializationError

from .exceptions import RequestMatchError, ReplayLogExceededError, \
        ReplayFileParseError


logger = logging.getLogger('elasticsearch.replay')

# Markers
IN = '#> '
OUT = '#< '
END = '##\n'


class RecordTransport(Transport):

    def __init__(self, *args, **kwargs):
        recfile = kwargs.pop('recfile', None)
        self.recfile = self.prepare_output_file(recfile)
        super(RecordTransport, self).__init__(*args, **kwargs)

    def prepare_output_file(self, path):
        if isinstance(path, basestring):
            return open(path, 'w') if path else None
        else:
            return path

    def format_request(self, method, url, params, body):
        """Format single request info"""
        # Serialize request body
        body = self.serializer.dumps(body) if body else None

        out_body = ('\n%s' % IN).join(body.split('\n')) if body else '-'
        out_params = urlencode(params) if params else '-'
        output = "%s%s %s %s\n%s%s\n" % (IN, method, url, out_params, IN,
                                         out_body)
        return output

    def format_response(self, status, body):
        "Format single response info"
        body = self.serializer.dumps(body)
        out_body = ('\n%s' % OUT).join(body.split('\n')) if body else '-'
        output = "%s%s\n%s%s\n" % (OUT, status, OUT, out_body)
        return output

    def perform_request(self, method, url, params=None, body=None):
        exception = None
        try:
            status, data = super(RecordTransport, self).perform_request(
                method, url, params, body
            )
        except TransportError as e:
            status = e.args[0]
            data = e.args[1]
            exception = e

        if self.recfile:
            try:
                # Store request
                req = self.format_request(method, url, params, body)
                self.recfile.write(req)

                # Store response
                resp = self.format_response(status, data)
                self.recfile.write(resp)

                # Store single request end marker
                end_mark = "%s%s" % ('' if resp.endswith('\n') else '\n', END)
                self.recfile.write(end_mark)

                self.recfile.flush()
            except Exception:
                logger.exception('Unable to record request')

        if exception:
            raise exception

        return status, data


class ReplayTransport(Transport):

    def __init__(self, *args, **kwargs):
        recfile = kwargs.pop('recfile', None)
        self.recfile = self.prepare_output_file(recfile)
        self.reset_replay_log()
        super(ReplayTransport, self).__init__(*args, **kwargs)

    def reset_replay_log(self):
        self.recfile.seek(0)
        self.replay_iterator = self.create_replay_iterator()

    def prepare_output_file(self, path):
        if isinstance(path, basestring):
            return open(path, 'r') if path else None
        else:
            return path

    def get_whole_request_info(self, req, resp):
        method, url, params = req[0].split(' ')
        body = '\n'.join(req[1:])
        status = resp[0]
        response = '\n'.join(resp[1:])

        deserialized = body
        if deserialized != '-\n':
            try:
                deserialized = self.deserializer.loads(body)
            except SerializationError:
                pass

        retval = {
            'method': method,
            'url': url,
            'params': params,
            'body': deserialized,
            'status': status,
            'response': response,
        }
        for key, value in retval.items():
            if isinstance(value, basestring):
                retval[key] = value.rstrip('\n')

        retval['status'] = int(retval['status'])
        return retval

    def create_replay_iterator(self):
        """
        Returns iterator which yields request/response dicts from replay file
        """
        req = []
        resp = []
        for line in self.recfile:
            if line == END:
                yield self.get_whole_request_info(req, resp)
                req, resp = [], []
            elif line.startswith(IN):
                req.append(line[3:])
            elif line.startswith(OUT):
                resp.append(line[3:])

    def check_match(self, replay, current):
        for key, value in current.items():
            if value != replay[key]:
                logger.error('Request match error %s != %s',
                             repr(value), repr(replay[key]))
                return False

        return True

    def get_next_replay(self, method, url, params, body):
        try:
            data = self.replay_iterator.next()
        except StopIteration:
            raise ReplayLogExceededError('No more entries in replay log')
        except Exception as e:
            raise ReplayFileParseError('Error parsing replay file: %s', e)

        current = {
            'method': method,
            'url': url,
            'params': urlencode(params) if params else '-',
            'body': body if body else '-',
        }
        if not self.check_match(data, current):
            raise RequestMatchError("Current request doesn't match a replay"
                                    "data")

        return data['status'], self.deserializer.loads(data['response'])

    def perform_request(self, method, url, params=None, body=None):
        # The fun with serialization and deserialization below is due to the
        # fact that eg. datetime is serialized but not deserialized. So we put
        # the request body here to the same process as the recorded one (first
        # it's serialized in RecordTransport and later deserialized in
        # ReplayTransport)
        body = self.serializer.dumps(body)
        if body and isinstance(body, basestring):
            try:
                body = self.deserializer.loads(body)
            except SerializationError:
                pass
        status, data = self.get_next_replay(method, url, params, body)

        # Support for exeptions
        if not (200 <= status < 300):
            connection = self.get_connection()
            connection._raise_error(status, data)

        return status, data
