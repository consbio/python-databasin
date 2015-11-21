import copy
import json

import pytest
import requests_mock

from databasin.client import Client
from databasin.exceptions import LoginRequiredError, ForbiddenError


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


def test_get_dataset(dataset_data):
    with requests_mock.mock() as m:
        m.get('http://databasin.org/api/v1/datasets/a1b2c3/', text=json.dumps(dataset_data))

        c = Client()
        dataset = c.get_dataset('a1b2c3')
        dataset.id

        assert m.called
        assert dataset.tags == dataset_data['tags']
        assert dataset.credits is None


def test_get_dataset_login_required():
    with requests_mock.mock() as m:
        m.get('http://databasin.org/api/v1/datasets/a1b2c3/', status_code=401)

        c = Client()
        with pytest.raises(LoginRequiredError):
            c.get_dataset('a1b2c3')

def test_get_dataset_forbidden():
    with requests_mock.mock() as m:
        m.get('http://databasin.org/api/v1/datasets/a1b2c3/', status_code=401)

        c = Client()
        c.username = 'foo'
        with pytest.raises(ForbiddenError):
            c.get_dataset('a1b2c3')


def test_list_datasets(dataset_data):
    with requests_mock.mock() as m:
        data = {
            'meta': {'next': None, 'total_count': 2},
            'objects': [
                dataset_data,
                copy.copy(dataset_data)
            ]
        }
        data['objects'][1]['id'] = 'a1b2c4'
        m.get('http://databasin.org/api/v1/datasets/', text=json.dumps(data))

        c = Client()
        datasets = c.list_datasets()

        assert len(datasets) == 2
        datasets = list(datasets)
        assert len(datasets) == 2
        assert datasets[0].id == 'a1b2c3'
        assert datasets[1].id == 'a1b2c4'


def test_datasets_pagination(dataset_data):
    with requests_mock.mock() as m:
        page1_data = {
            'meta': {'next': '/api/v1/datasets/?limit=2&offset=2', 'total_count': 3},
            'objects': [
                dataset_data,
                copy.copy(dataset_data)
            ]
        }
        page1_data['objects'][1]['id'] = 'a1b2c4'
        page2_data = {
            'meta': {'next': None, 'total_count': 3},
            'objects': [
                copy.copy(dataset_data)
            ]
        }
        page2_data['objects'][0]['id'] = 'a1b2c5'
        m.get('http://databasin.org/api/v1/datasets/', text=json.dumps(page1_data))
        m.get('http://databasin.org/api/v1/datasets/?offset=2', text=json.dumps(page2_data))

        c = Client()
        datasets = c.list_datasets()

        assert len(datasets) == 3
        datasets = list(datasets)
        assert m.call_count == 2
        assert len(datasets) == 3
        assert datasets[0].id == 'a1b2c3'
        assert datasets[1].id == 'a1b2c4'
        assert datasets[2].id == 'a1b2c5'


def test_datasets_with_filter():
    with requests_mock.mock() as m:
        data = {
            'meta': {'next': None, 'total_count': 0},
            'objects': []
        }
        m.get('http://databasin.org/api/v1/datasets/?private=False', text=json.dumps(data))

        c = Client()
        datasets = c.list_datasets({'private': False})
        list(datasets)

        assert m.called
        assert m.request_history[0].qs == {'private': ['false']}


def test_my_datasets():
    with requests_mock.mock() as m:
        data = {
            'meta': {'next': None, 'total_count': 0},
            'objects': []
        }
        m.get('http://databasin.org/api/v1/datasets/?owner_id=foo', text=json.dumps(data))

        c = Client()
        c.username = 'foo'
        datasets = c.list_my_datasets()
        list(datasets)

        assert m.called
        assert m.request_history[0].qs == {'owner_id': ['foo']}

