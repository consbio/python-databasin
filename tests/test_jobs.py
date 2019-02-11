from __future__ import absolute_import

import copy
import json

import pytest
import requests_mock

from databasin.client import Client
from databasin.exceptions import LoginRequiredError, ForbiddenError
from .utils import make_api_key_callback

try:
    from unittest import mock  # Py3
except ImportError:
    import mock  # Py2


@pytest.fixture
def job_data():
    return {
        'id': '1234',
        'job_name': 'foo_job',
        'status': 'queued',
        'progress': 0,
        'message': None
    }


def test_create_job(job_data):
    with requests_mock.mock() as m:
        m.post(
            'https://databasin.org/api/v1/jobs/',
            headers={'Location': 'https://databasin.org/api/v1/jobs/1234/'},
            status_code=201
        )
        m.get('https://databasin.org/api/v1/jobs/1234/', text=json.dumps(job_data))

        c = Client()
        job = c.create_job('foo_job', job_args={'foo': 'bar'})

        assert job.id == '1234'
        assert m.call_count == 2
        request_data = json.loads(m.request_history[0].text)
        assert request_data['job_name'] == 'foo_job'
        assert request_data['job_args'] == {'foo': 'bar'}


def test_create_job_login_required():
    with requests_mock.mock() as m:
        m.post('https://databasin.org/api/v1/jobs/', status_code=401)

        c = Client()
        with pytest.raises(LoginRequiredError):
            c.create_job('foo_job')


def test_create_job_with_api_key(job_data):
    key = 'abcde12345'

    with requests_mock.mock() as m:
        m.post(
            'https://databasin.org/api/v1/jobs/',
            headers={'Location': 'https://databasin.org/api/v1/jobs/1234/'},
            status_code=201,
            text=make_api_key_callback('', key)
        )
        m.get(
            'https://databasin.org/api/v1/jobs/1234/',
            text=make_api_key_callback(json.dumps(job_data), key)
        )

        c = Client()
        c.set_api_key('user', key)
        job = c.create_job('foo_job', job_args={'foo': 'bar'})

        assert job.id == '1234'
        assert m.call_count == 2
        request_data = json.loads(m.request_history[0].text)
        assert request_data['job_name'] == 'foo_job'
        assert request_data['job_args'] == {'foo': 'bar'}


def test_create_job_forbidden():
    with requests_mock.mock() as m:
        m.post('https://databasin.org/api/v1/jobs/', status_code=401)

        c = Client()
        c.username = 'foo'
        with pytest.raises(ForbiddenError):
            c.create_job('foo_job')


def test_get_job(job_data):
    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/jobs/1234/', text=json.dumps(job_data))

        c = Client()
        job = c.get_job('1234')

        assert job.id == '1234'
        assert m.called


def test_get_job_with_api_key(job_data):
    key = 'abcde12345'

    with requests_mock.mock() as m:
        m.get(
            'https://databasin.org/api/v1/jobs/1234/',
            text=make_api_key_callback(json.dumps(job_data), key)
        )

        c = Client()
        c.set_api_key('user', key)
        job = c.get_job('1234')

        assert job.id == '1234'
        assert m.called


def test_job_refresh(job_data):
    job_data_2 = copy.copy(job_data)
    job_data_2.update({
        'status': 'running',
        'progress': 50,
        'message': 'Foo'
    })

    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/jobs/1234/', [
            {'text': json.dumps(job_data)},
            {'text': json.dumps(job_data_2)}
        ])

        c = Client()
        job = c.get_job('1234')

        assert job.status == 'queued'
        assert job.progress == 0
        assert job.message is None

        job.refresh()

        assert m.call_count == 2
        assert job.status == 'running'
        assert job.progress == 50
        assert job.message == 'Foo'


def test_job_refresh_with_api_key(job_data):
    key = 'abcde12345'

    job_data_2 = copy.copy(job_data)
    job_data_2.update({
        'status': 'running',
        'progress': 50,
        'message': 'Foo'
    })

    with requests_mock.mock() as m:
        m.get(
            'https://databasin.org/api/v1/jobs/1234/',
            [
                {'text': make_api_key_callback(json.dumps(job_data), key)},
                {'text': make_api_key_callback(json.dumps(job_data_2), key)}
            ]
        )

        c = Client()
        c.set_api_key('user', key)
        job = c.get_job('1234')

        assert job.status == 'queued'
        assert job.progress == 0
        assert job.message is None

        job.refresh()

        assert m.call_count == 2
        assert job.status == 'running'
        assert job.progress == 50
        assert job.message == 'Foo'


def test_job_block(job_data):
    job_data_2 = copy.copy(job_data)
    job_data_2['status'] = 'succeeded'

    with requests_mock.mock() as m:
        m.post(
            'https://databasin.org/api/v1/jobs/',
            headers={'Location': 'https://databasin.org/api/v1/jobs/1234/'},
            status_code=201
        )
        m.get('https://databasin.org/api/v1/jobs/1234/', [
            {'text': json.dumps(job_data)},
            {'text': json.dumps(job_data)},
            {'text': json.dumps(job_data)},
            {'text': json.dumps(job_data_2)}
        ])

        with mock.patch('time.sleep', mock.Mock()) as time_mock:
            # Make sure the test actually stops if things don't work as planned
            time_mock.side_effect = [None, None, None, IOError]

            c = Client()
            job = c.create_job('foo', block=True)

            assert m.call_count == 5
            assert job.status == 'succeeded'
            assert time_mock.call_count == 3


def test_job_join(job_data):
    job_data_2 = copy.copy(job_data)
    job_data_2['status'] = 'succeeded'

    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/jobs/1234/', [
            {'text': json.dumps(job_data)},
            {'text': json.dumps(job_data)},
            {'text': json.dumps(job_data)},
            {'text': json.dumps(job_data_2)}
        ])

        with mock.patch('time.sleep', mock.Mock()) as time_mock:
            # Make sure the test actually stops if things don't work as planned
            time_mock.side_effect = [None, None, None, IOError]

            c = Client()
            job = c.get_job('1234')

            assert job.status == 'queued'
            assert m.call_count == 1

            job.join()

            assert m.call_count == 4
            assert job.status == 'succeeded'
            assert time_mock.call_count == 3
