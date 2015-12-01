import copy
import json

import pytest
import requests_mock
from six import StringIO

from databasin.client import Client
from databasin.exceptions import LoginRequiredError

try:
    from unittest import mock  # Py3
except ImportError:
    import mock  # Py2

try:
    import __builtin__ as builtins
except ImportError:
    import builtins


@pytest.fixture
def tmp_file_data():
    return {
        'uuid': '1234',
        'date': '2015-11-17T22:42:06+00:00',
        'is_image': False,
        'filename': '',
        'url': 'http://example.com/file.txt'
    }


def test_temporary_file_upload(tmp_file_data):
    with requests_mock.mock() as m:
        m.post('http://databasin.org/uploads/upload-temporary-file/', text=json.dumps({'uuid': '1234'}))
        m.get('http://databasin.org/api/v1/uploads/temporary-files/1234/', text=json.dumps(tmp_file_data))

        c = Client()
        c._session.cookies['csrftoken'] = 'abcd'
        tmp_file = c.upload_temporary_file(StringIO('foo'))

        assert tmp_file.uuid == '1234'
        assert tmp_file.filename == ''
        assert m.call_count == 2


def test_temporary_file_upload_login_required():
    with requests_mock.mock() as m:
        m.post('http://databasin.org/uploads/upload-temporary-file/', status_code=401)

        c = Client()
        c._session.cookies['csrftoken'] = 'abcd'

        with pytest.raises(LoginRequiredError):
            c.upload_temporary_file(StringIO('foo'))


def test_temporary_file_upload_from_path(tmp_file_data):
    with requests_mock.mock() as m:
        m.post('http://databasin.org/uploads/upload-temporary-file/', text=json.dumps({'uuid': '1234'}))
        m.get('http://databasin.org/api/v1/uploads/temporary-files/1234/', text=json.dumps(tmp_file_data))

        with mock.patch.object(builtins, 'open', mock.mock_open(read_data='foo')) as open_mock:
            c = Client()
            c._session.cookies['csrftoken'] = 'abcd'
            tmp_file = c.upload_temporary_file('/path/to/foo.txt')

            assert tmp_file.uuid == '1234'
            open_mock.assert_called_once_with('/path/to/foo.txt', 'rb')
            assert 'filename="foo.txt"' in str(m.request_history[0].body)


def test_get_temporary_file(tmp_file_data):
    with requests_mock.mock() as m:
        m.get('http://databasin.org/api/v1/uploads/temporary-files/1234/', text=json.dumps(tmp_file_data))

        c = Client()
        tmp_file = c.get_temporary_file('1234')

        assert m.called
        assert tmp_file.uuid == '1234'


def test_get_temporary_file_login_required(tmp_file_data):
    with requests_mock.mock() as m:
        m.get('http://databasin.org/api/v1/uploads/temporary-files/1234/', status_code=401)

        c = Client()

        with pytest.raises(LoginRequiredError):
            tmp_file = c.get_temporary_file('1234')


def test_list_temporary_files(tmp_file_data):
    data = {
        'meta': {'next': None, 'total_count': 2},
        'objects': [
            tmp_file_data,
            copy.copy(tmp_file_data)
        ]
    }
    data['objects'][1]['uuid'] = 1235

    with requests_mock.mock() as m:
        m.get('http://databasin.org/api/v1/uploads/temporary-files/', text=json.dumps(data))

        c = Client()
        tmp_files = c.list_temporary_files()

        assert len(tmp_files) == 2
        tmp_files = list(tmp_files)
        assert tmp_files[0].uuid == '1234'
        assert tmp_files[1].uuid == '1235'

