import copy
import json
import zipfile

import pytest
import requests_mock
import six
from requests.models import Request

from databasin.client import Client
from databasin.exceptions import DatasetImportError

try:
    from unittest import mock  # Py3
except ImportError:
    import mock  # Py2

try:
    import __builtin__ as builtins
except ImportError:
    import builtins

LOGIN_URL = 'https://databasin.org/auth/api/login/'


@pytest.fixture()
def dataset_import_data():
    return {
        'id': 'a1b2c3',
        'owner_id': 'user',
        'private': False,
        'title': 'Some Import',
        'description': 'This dataset is a dataset.',
        'create_date': '2015-11-17T22:42:06+00:00',
        'modify_date': '2015-11-17T22:42:06+00:00',
        'native': True,
        'tags': ['one', 'two'],
        'credits': None,
        'failed': False,
        'is_dataset_edit': False
    }


@pytest.fixture()
def dataset_data():
    return {
        'id': 'a1b2c3',
        'owner_id': 'user',
        'private': False,
        'title': 'Some Dataset',
        'snippet': 'This dataset is...',
        'create_date': '2015-11-17T22:42:06+00:00',
        'modify_date': '2015-11-17T22:42:06+00:00',
        'native': True,
        'tags': ['one', 'two'],
        'credits': None
    }


@pytest.fixture
def import_job_data():
    return {
        'id': '1234',
        'job_name': 'create_import_job',
        'status': 'succeeded',
        'progress': 100,
        'message': json.dumps({'next_uri': '/datasets/a1b2c3'})
    }


@pytest.fixture
def tmp_file_data():
    return {
        'uuid': 'abcd',
        'date': '2015-11-17T22:42:06+00:00',
        'is_image': False,
        'filename': '',
        'url': 'https://example.com/file.txt'
    }


def test_alternative_host():
    c = Client('example.com:81')

    assert c.base_url == 'https://example.com:81'


def test_https_referer():
    """Django requires all POST requests via HTTPS to have the Referer header set."""

    c = Client()
    r = c._session.prepare_request(Request('POST', LOGIN_URL))
    c._session.get_adapter(LOGIN_URL).add_headers(r)

    assert r.headers['Referer'] == LOGIN_URL


def test_login():
    with requests_mock.mock() as m:
        m.get('https://databasin.org/', cookies={'csrftoken': 'abcd'})
        m.post(LOGIN_URL, cookies={'sessionid': 'asdf'})

        c = Client()
        c.login('foo', 'bar')

        assert m.call_count == 2


def test_login_no_redirect():
    with requests_mock.mock() as m:
        m.get('https://databasin.org/redirect/')
        m.get('https://databasin.org/', cookies={'csrftoken': 'abcd'})
        m.get(LOGIN_URL, cookies={'csrftoken': 'abcd'})
        m.post(
            LOGIN_URL, headers={'Location': 'https://databasin.org/'}, cookies={'sessionid': 'asdf'}, status_code=302
        )

        c = Client()
        c.login('foo', 'bar')

        assert m.call_count == 2
        assert not any(r.url for r in m.request_history if r.url == 'https://databasin.org/redirect/')

def test_import_lpk(import_job_data, dataset_data, tmp_file_data):
    with requests_mock.mock() as m:
        m.post('https://databasin.org/uploads/upload-temporary-file/', text=json.dumps({'uuid': 'abcd'}))
        m.get('https://databasin.org/api/v1/uploads/temporary-files/abcd/', text=json.dumps(tmp_file_data))
        m.post('https://databasin.org/api/v1/jobs/', headers={'Location': 'https://databasin.org/api/v1/jobs/1234/'})
        m.get('https://databasin.org/api/v1/jobs/1234/', text=json.dumps(import_job_data))
        m.get('https://databasin.org/api/v1/datasets/a1b2c3/', text=json.dumps(dataset_data))

        f = six.BytesIO()
        with mock.patch.object(builtins, 'open', mock.Mock(return_value=f)) as open_mock:
            c = Client()
            c._session.cookies['csrftoken'] = 'abcd'
            dataset = c.import_lpk('test.lpk')

            open_mock.assert_called_once_with('test.lpk', 'rb')
            assert m.call_count == 7
            assert dataset.id == 'a1b2c3'
            request_data = json.loads(m.request_history[2].text)
            assert request_data['job_name'] == 'create_import_job'
            assert request_data['job_args']['file'] == 'abcd'
            assert request_data['job_args']['dataset_type'] == 'ArcGIS_Native'

def test_import_netcdf_dataset_with_zip(import_job_data, dataset_data, tmp_file_data):
    with requests_mock.mock() as m:
        m.post('https://databasin.org/uploads/upload-temporary-file/', text=json.dumps({'uuid': 'abcd'}))
        m.get('https://databasin.org/api/v1/uploads/temporary-files/abcd/', text=json.dumps(tmp_file_data))
        m.post('https://databasin.org/api/v1/jobs/', headers={'Location': 'https://databasin.org/api/v1/jobs/1234/'})
        m.get('https://databasin.org/api/v1/jobs/1234/', text=json.dumps(import_job_data))
        m.get('https://databasin.org/api/v1/datasets/a1b2c3/', text=json.dumps(dataset_data))

        f = six.BytesIO()
        with zipfile.ZipFile(f, 'w') as zf:
            zf.writestr('test.nc', '')
            zf.writestr('style.json', '')
        f.seek(0)

        with mock.patch.object(builtins, 'open', mock.Mock(return_value=f)) as open_mock:
            c = Client()
            c._session.cookies['csrftoken'] = 'abcd'
            dataset = c.import_netcdf_dataset('test.zip')

            open_mock.assert_called_once_with('test.zip', 'a+b')
            assert m.call_count == 5
            assert dataset.id == 'a1b2c3'
            request_data = json.loads(m.request_history[2].text)
            assert request_data['job_name'] == 'create_import_job'
            assert request_data['job_args']['file'] == 'abcd'
            assert request_data['job_args']['dataset_type'] == 'NetCDF_Native'


def test_import_netcdf_dataset_with_nc(import_job_data, dataset_data, tmp_file_data):
    with requests_mock.mock() as m:
        m.post('https://databasin.org/uploads/upload-temporary-file/', text=json.dumps({'uuid': 'abcd'}))
        m.get('https://databasin.org/api/v1/uploads/temporary-files/abcd/', text=json.dumps(tmp_file_data))
        m.post('https://databasin.org/api/v1/jobs/', headers={'Location': 'https://databasin.org/api/v1/jobs/1234/'})
        m.get('https://databasin.org/api/v1/jobs/1234/', text=json.dumps(import_job_data))
        m.get('https://databasin.org/api/v1/datasets/a1b2c3/', text=json.dumps(dataset_data))

        with mock.patch.object(zipfile, 'ZipFile', mock.MagicMock()) as zf_mock:
            c = Client()
            c._session.cookies['csrftoken'] = 'abcd'
            dataset = c.import_netcdf_dataset('test.nc', style={'foo': 'bar'})

            zf_mock().write.assert_called_once_with('test.nc', 'test.nc')
            assert m.call_count == 5
            assert dataset.id == 'a1b2c3'
            request_data = json.loads(m.request_history[2].text)
            assert request_data['job_name'] == 'create_import_job'
            assert request_data['job_args']['file'] == 'abcd'
            assert request_data['job_args']['dataset_type'] == 'NetCDF_Native'


def test_import_netcdf_dataset_with_invalid_file():
    c = Client()
    with pytest.raises(ValueError):
        c.import_netcdf_dataset('test.foo')


def test_import_netcdf_dataset_with_no_style():
    f = six.BytesIO()
    with zipfile.ZipFile(f, 'w') as zf:
        zf.writestr('test.nc', '')
    f.seek(0)

    with mock.patch.object(builtins, 'open', mock.Mock(return_value=f)) as open_mock:
        c = Client()
        c._session.cookies['csrftoken'] = 'abcd'

        with pytest.raises(ValueError):
            c.import_netcdf_dataset('test.zip')


def test_import_netcdf_dataset_incomplete(import_job_data, tmp_file_data, dataset_import_data):
    import_job_data = copy.copy(import_job_data)
    import_job_data['message'] = json.dumps({'next_uri': '/datasets/import/a1b2c3/overview/'})

    with requests_mock.mock() as m:
        m.post('https://databasin.org/uploads/upload-temporary-file/', text=json.dumps({'uuid': 'abcd'}))
        m.get('https://databasin.org/api/v1/uploads/temporary-files/abcd/', text=json.dumps(tmp_file_data))
        m.post('https://databasin.org/api/v1/jobs/', headers={'Location': 'https://databasin.org/api/v1/jobs/1234/'})
        m.get('https://databasin.org/api/v1/jobs/1234/', text=json.dumps(import_job_data))
        m.get('https://databasin.org/api/v1/dataset_imports/a1b2c3/', text=json.dumps(dataset_import_data))
        m.delete('https://databasin.org/api/v1/dataset_imports/a1b2c3/')

        f = six.BytesIO()
        with zipfile.ZipFile(f, 'w') as zf:
            zf.writestr('test.nc', '')
            zf.writestr('style.json', '')
        f.seek(0)

        with mock.patch.object(builtins, 'open', mock.Mock(return_value=f)) as open_mock:
            c = Client()
            c._session.cookies['csrftoken'] = 'abcd'

            with pytest.raises(DatasetImportError):
                c.import_netcdf_dataset('test.zip')

            assert m.call_count == 6
