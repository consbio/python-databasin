import copy
import json

import pytest
import requests_mock

import six

from databasin.client import Client
from databasin.exceptions import LoginRequiredError, ForbiddenError

if six.PY3:
    from unittest import mock
else:
    import mock


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
            'http://databasin.org/api/v1/jobs/',
            headers={'Location': 'http://databasin.org/api/v1/jobs/1234/'},
            status_code=201
        )
        m.get('http://databasin.org/api/v1/jobs/1234/', text=json.dumps(job_data))

        c = Client()
        job = c.create_job('foo_job', job_args={'foo': 'bar'})

        assert job.id == '1234'
        assert m.call_count == 2
        request_data = json.loads(m.request_history[0].text)
        assert request_data['job_name'] == 'foo_job'
        assert request_data['job_args'] == {'foo': 'bar'}


def test_create_job_login_required():
    with requests_mock.mock() as m:
        m.post('http://databasin.org/api/v1/jobs/', status_code=401)

        c = Client()
        with pytest.raises(LoginRequiredError):
            c.create_job('foo_job')


def test_create_job_forbidden():
    with requests_mock.mock() as m:
        m.post('http://databasin.org/api/v1/jobs/', status_code=401)

        c = Client()
        c.username = 'foo'
        with pytest.raises(ForbiddenError):
            c.create_job('foo_job')


def test_get_job(job_data):
    with requests_mock.mock() as m:
        m.get('http://databasin.org/api/v1/jobs/1234/', text=json.dumps(job_data))

        c = Client()
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
        m.get('http://databasin.org/api/v1/jobs/1234/', [
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


def test_job_block(job_data):
    job_data_2 = copy.copy(job_data)
    job_data_2['status'] = 'succeeded'

    with requests_mock.mock() as m:
        m.post(
            'http://databasin.org/api/v1/jobs/',
            headers={'Location': 'http://databasin.org/api/v1/jobs/1234/'},
            status_code=201
        )
        m.get('http://databasin.org/api/v1/jobs/1234/', [
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
        m.get('http://databasin.org/api/v1/jobs/1234/', [
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
