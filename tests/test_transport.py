import os
import StringIO
import unittest

import mock

from elasticsearch_replay.transport import RecordTransport, ReplayTransport, \
        IN, OUT, END, ReplayLogExceededError, ReplayFileParseError, \
        RequestMatchError
import elasticsearch


IN_CONTENTS = """#> GET /myindex -
#> -
#< 200
#< {"key": "value"}
##
#> GET /myindex2 -
#> {"req": "body"}
#< 200
#< {"key": "value2"}
##

"""


class DummyConnection(elasticsearch.Connection):

    def __init__(self, **kwargs):
        super(DummyConnection, self).__init__(**kwargs)
        self.calls = []

    def perform_request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        params = kwargs.get('params', {})
        if 'exception' in params:
            Exception('BOOM!')

        status = params.get('status', 200)
        data = params.get('data', '{"key": "value"}')
        return status, {}, data


class RecordTransportTestCase(unittest.TestCase):

    def setUp(self):
        with mock.patch('__builtin__.open') as mopen:
            mopen.return_value = os.tmpfile()
            self.t = RecordTransport([{}], recfile='dummy',
                                     connection_class=DummyConnection)

    def test_patched(self):
        assert self.t.recfile.name == '<tmpfile>'

    def test_format_request(self):
        rv = self.t.format_request('GET', '/my/url', {'x': 1},
                                   '{"key": "value"}')
        lines = rv.split('\n')
        for line in lines:
            assert line.startswith(IN) or not line
        assert len(lines) == 3
        assert '#> GET /my/url x=1' == lines[0]
        assert '#> {"key": "value"}' == lines[1]
        assert '' == lines[2]  # ensure all ends with a newline

    def test_format_request_with_empty_params_and_body(self):
        rv = self.t.format_request('GET', '/my/url', None, None)
        lines = rv.split('\n')
        for line in lines:
            assert line.startswith(IN) or not line
        assert len(lines) == 3
        assert '#> GET /my/url -' == lines[0]
        assert '#> -' == lines[1]
        assert '' == lines[2]  # ensure all ends with a newline

    def test_format_response(self):
        rv = self.t.format_response(200, '{"key": "value"}')
        lines = rv.split('\n')
        for line in lines:
            assert line.startswith(OUT) or not line
        assert len(lines) == 3
        assert '#< 200' == lines[0]
        assert '#< {"key": "value"}' == lines[1]
        assert '' == lines[2]  # ensure all ends with a newline

    def test_recfile_as_path(self):
        with mock.patch('__builtin__.open') as mopen:
            self.t.prepare_output_file('/path')
            mopen.assert_called_once_with('/path', 'w')

    def test_recfile_as_file_obj(self):
        file_obj = os.tmpfile()
        with mock.patch('__builtin__.open') as mopen:
            rv = self.t.prepare_output_file(file_obj)
            assert not mopen.called
            assert rv == file_obj

    def test_perform_request_all_markers_in(self):
        self.t.format_request = mock.create_autospec(self.t.format_request)
        self.t.format_request.return_value = '#> GET /url -\n#> -\n'
        self.t.format_response = mock.create_autospec(self.t.format_response)
        self.t.format_response.return_value = '#< 200\n#< {"key": "value"}'
        self.t.perform_request('GET', '/url')
        self.t.recfile.seek(0)
        contents = self.t.recfile.read()
        assert IN in contents
        assert OUT in contents
        assert END in contents

class RecordTransportFullTestCase(unittest.TestCase):

    def setUp(self):
        self.name = os.tmpnam()
        self.t = RecordTransport([{}], recfile=self.name,
                                 connection_class=DummyConnection)
        self.args = ('GET', '/myindex', None, None)

    def test_record_works_normally_if_no_file(self):
        self.t.recfile = None
        self.t.perform_request(*self.args)
        calls = self.t.get_connection().calls
        assert len(calls) == 1
        assert calls[0][0] == self.args

    def test_record_works_normally_if_exception_raised_in_logging(self):
        self.t.format_request = mock.Mock()
        self.t.format_request.side_effect = Exception('BOOM!')
        self.t.perform_request(*self.args)
        calls = self.t.get_connection().calls
        assert len(calls) == 1
        assert calls[0][0] == self.args

    def test_request_is_logged(self):
        self.t.perform_request(*self.args)
        calls = self.t.get_connection().calls
        assert len(calls) == 1
        assert calls[0][0] == self.args
        with open(self.name, 'r') as rfile:
            contents = rfile.read()
        assert len(contents), 'Empty replay log contents'
        for item in self.args:
            if item:
                assert item in contents, '%s not found in replay log' % item


class ReplayTransportTestCase(unittest.TestCase):

    def get_instance(self, replay, connection=DummyConnection):
        f = StringIO.StringIO(replay)
        return ReplayTransport([{}], recfile=f, connection_class=connection)

    def test_invlid_file_format(self):
        wrong = '#> some ##< odd but similar \n##\n format'
        t = self.get_instance(wrong)
        with self.assertRaises(ReplayFileParseError):
            t.get_next_replay('GET', '/', None, None)

    def test_different_request(self):
        t = self.get_instance(IN_CONTENTS)
        with self.assertRaises(RequestMatchError):
            t.get_next_replay('GET', '/badreq', None, None)


class ReplayTransportFullTestCase(unittest.TestCase):

    def setUp(self):
        f = StringIO.StringIO(IN_CONTENTS)
        self.t = ReplayTransport([{}], recfile=f,
                                 connection_class=DummyConnection)
        self.args = ('GET', '/myindex')

    def test_simple_fetch(self):
        status, data = self.t.perform_request(*self.args)
        calls = self.t.get_connection().calls
        assert not calls, "Call to connection made"
        assert status == 200
        assert {"key": "value"} == data

    def test_more_requests(self):
        status1, data1 = self.t.perform_request(*self.args)
        status2, data2 = self.t.perform_request(
            'GET', '/myindex2', body='{"req": "body"}'
        )
        assert status1 == 200
        assert {"key": "value"} == data1
        assert status2 == 200
        assert {"key": "value2"} == data2
        with self.assertRaises(ReplayLogExceededError):
            self.t.perform_request(*self.args)

    def test_diffrent_body_same_after_deserialization(self):
        self.t.perform_request(*self.args)
        status, data = self.t.perform_request(
            'GET', '/myindex2', body='{"req": \n "body"}'
        )
        assert status == 200
        assert {"key": "value2"} == data

    def test_body_as_dict(self):
        self.t.perform_request(*self.args)
        status, data = self.t.perform_request(
            'GET', '/myindex2', body={"req": "body"}
        )
        assert status == 200
        assert {"key": "value2"} == data


class FullIntegrationTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.replay_log = os.tmpfile()
        cls.es_rec = elasticsearch.Elasticsearch(
            transport_class=RecordTransport, recfile=cls.replay_log
        )
        cls.es_rep = elasticsearch.Elasticsearch(
            transport_class=ReplayTransport, recfile=cls.replay_log
        )

    @classmethod
    def tearDownClass(cls):
        es = elasticsearch.Elasticsearch()
        try:
            es.indices.delete(index=['test_replay_transport'])
        except Exception:
            pass

    def tearDown(self):
        self.replay_log.seek(0)
        self.replay_log.truncate(0)

    def test_create_index(self):
        rv_rec = self.es_rec.indices.create(index=['test_replay_transport'])
        assert self.replay_log.tell() > 0
        self.es_rep.transport.reset_replay_log()
        rv_rep = self.es_rep.indices.create(index=['test_replay_transport'])
        assert rv_rec == rv_rep

    def test_item_lifecycle(self):
        rec_responses = []
        rep_responses = []
        kwargs = {
            'index': 'test_replay_transport',
            'doc_type': 'document',
            'id': 1,
        }
        body = {'field': 'value', 'integer': 23}

        # Record sequence
        rec_responses.append(self.es_rec.index(body=body, **kwargs))
        rec_responses.append(self.es_rec.get(**kwargs))
        rec_responses.append(self.es_rec.delete(**kwargs))
        assert self.replay_log.tell() > 0

        # Replay requence
        self.es_rep.transport.reset_replay_log()
        rep_responses.append(self.es_rep.index(body=body, **kwargs))
        rep_responses.append(self.es_rep.get(**kwargs))
        rep_responses.append(self.es_rep.delete(**kwargs))
        assert rec_responses == rep_responses

    def test_not_found_should_be_covered(self):
        kwargs = {
            'index': 'test_replay_transport',
            'doc_type': 'document',
            'id': 1,
        }
        with self.assertRaises(elasticsearch.exceptions.NotFoundError):
            self.es_rec.get(**kwargs)

        assert self.replay_log.tell() > 0
        self.es_rep.transport.reset_replay_log()
        with self.assertRaises(elasticsearch.exceptions.NotFoundError):
            self.es_rep.get(**kwargs)
