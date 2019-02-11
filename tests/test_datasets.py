from __future__ import absolute_import

import copy
import json

import pytest
import requests_mock

from databasin.client import Client
from databasin.exceptions import LoginRequiredError, ForbiddenError
from .utils import make_api_key_callback


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


def test_get_dataset(dataset_data):
    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/datasets/a1b2c3/', text=json.dumps(dataset_data))

        c = Client()
        dataset = c.get_dataset('a1b2c3')
        dataset.id

        assert m.called
        assert dataset.tags == dataset_data['tags']
        assert dataset.credits is None


def test_get_dataset_login_required():
    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/datasets/a1b2c3/', status_code=401)

        c = Client()
        with pytest.raises(LoginRequiredError):
            c.get_dataset('a1b2c3')


def test_get_dataset_forbidden():
    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/datasets/a1b2c3/', status_code=401)

        c = Client()
        c.username = 'foo'
        with pytest.raises(ForbiddenError):
            c.get_dataset('a1b2c3')


def test_get_dataset_with_api_key(dataset_data):
    key = 'abcde12345'

    with requests_mock.mock() as m:
        m.get(
            'https://databasin.org/api/v1/datasets/a1b2c3/',
            text=make_api_key_callback(json.dumps(dataset_data), key)
        )

        c = Client()
        c.set_api_key('user', key)
        dataset = c.get_dataset('a1b2c3')
        dataset.id

        assert m.called
        assert dataset.tags == dataset_data['tags']
        assert dataset.credits is None


def test_dataset_make_public(dataset_data):
    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/datasets/a1b2c3/', text=json.dumps(dataset_data))
        m.patch('https://databasin.org/api/v1/datasets/a1b2c3/')

        c = Client()
        dataset = c.get_dataset('a1b2c3')
        dataset.make_public()

        assert m.call_count == 2
        request_data = json.loads(m.request_history[1].text)
        assert request_data['private'] is False


def test_dataset_make_public_with_api_key(dataset_data):
    key = 'abcde12345'

    with requests_mock.mock() as m:
        m.get(
            'https://databasin.org/api/v1/datasets/a1b2c3/',
            text=make_api_key_callback(json.dumps(dataset_data), key)
        )
        m.patch(
            'https://databasin.org/api/v1/datasets/a1b2c3/',
            text=make_api_key_callback('', key)
        )

        c = Client()
        c.set_api_key('user', key)
        dataset = c.get_dataset('a1b2c3')
        dataset.make_public()

        assert m.call_count == 2
        request_data = json.loads(m.request_history[1].text)
        assert request_data['private'] is False


def test_dataset_make_private(dataset_data):
    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/datasets/a1b2c3/', text=json.dumps(dataset_data))
        m.patch('https://databasin.org/api/v1/datasets/a1b2c3/')

        c = Client()
        dataset = c.get_dataset('a1b2c3')
        dataset.make_private()

        assert m.call_count == 2
        request_data = json.loads(m.request_history[1].text)
        assert request_data['private'] is True


def test_dataset_make_private_with_api_key(dataset_data):
    key = 'abcde12345'

    with requests_mock.mock() as m:
        m.get(
            'https://databasin.org/api/v1/datasets/a1b2c3/',
            text=make_api_key_callback(json.dumps(dataset_data), key)
        )
        m.patch(
            'https://databasin.org/api/v1/datasets/a1b2c3/',
            text=make_api_key_callback('', key)
        )

        c = Client()
        c.set_api_key('user', key)
        dataset = c.get_dataset('a1b2c3')
        dataset.make_private()

        assert m.call_count == 2
        request_data = json.loads(m.request_history[1].text)
        assert request_data['private'] is True


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
        m.get('https://databasin.org/api/v1/datasets/', text=json.dumps(data))

        c = Client()
        datasets = c.list_datasets()

        assert len(datasets) == 2
        datasets = list(datasets)
        assert len(datasets) == 2
        assert datasets[0].id == 'a1b2c3'
        assert datasets[1].id == 'a1b2c4'


def test_list_datasets_with_api_key(dataset_data):
    key = 'abcde12345'

    with requests_mock.mock() as m:
        data = {
            'meta': {'next': None, 'total_count': 2},
            'objects': [
                dataset_data,
                copy.copy(dataset_data)
            ]
        }
        data['objects'][1]['id'] = 'a1b2c4'
        m.get(
            'https://databasin.org/api/v1/datasets/',
            text=make_api_key_callback(json.dumps(data), key)
        )

        c = Client()
        c.set_api_key('user', key)
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
        m.get('https://databasin.org/api/v1/datasets/', text=json.dumps(page1_data))
        m.get('https://databasin.org/api/v1/datasets/?offset=2', text=json.dumps(page2_data))

        c = Client()
        datasets = c.list_datasets()

        assert len(datasets) == 3
        datasets = list(datasets)
        assert m.call_count == 2
        assert len(datasets) == 3
        assert datasets[0].id == 'a1b2c3'
        assert datasets[1].id == 'a1b2c4'
        assert datasets[2].id == 'a1b2c5'


def test_datasets_pagination_with_api_key(dataset_data):
    key = 'abcde12345'

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
        m.get(
            'https://databasin.org/api/v1/datasets/',
            text=make_api_key_callback(json.dumps(page1_data), key)
        )
        m.get(
            'https://databasin.org/api/v1/datasets/?offset=2',
            text=make_api_key_callback(json.dumps(page2_data), key)
        )

        c = Client()
        c.set_api_key('user', key)
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
        m.get('https://databasin.org/api/v1/datasets/?private=False', text=json.dumps(data))

        c = Client()
        datasets = c.list_datasets({'private': False})
        list(datasets)

        assert m.called
        assert m.request_history[0].qs['private'] == ['false']


def test_datasets_with_filter_with_api_key():
    key = 'abcde12345'

    with requests_mock.mock() as m:
        data = {
            'meta': {'next': None, 'total_count': 0},
            'objects': []
        }
        m.get(
            'https://databasin.org/api/v1/datasets/?private=False',
            text=make_api_key_callback(json.dumps(data), key)
        )

        c = Client()
        c.set_api_key('user', key)
        datasets = c.list_datasets({'private': False})
        list(datasets)

        assert m.called
        assert m.request_history[0].qs['private'] == ['false']


def test_my_datasets():
    with requests_mock.mock() as m:
        data = {
            'meta': {'next': None, 'total_count': 0},
            'objects': []
        }
        m.get('https://databasin.org/api/v1/datasets/?owner_id=foo', text=json.dumps(data))

        c = Client()
        c.username = 'foo'
        datasets = c.list_my_datasets()
        list(datasets)

        assert m.called
        assert m.request_history[0].qs['owner_id'] == ['foo']


def test_my_datasets_with_api_key():
    key = 'abcde12345'

    with requests_mock.mock() as m:
        data = {
            'meta': {'next': None, 'total_count': 0},
            'objects': []
        }
        m.get(
            'https://databasin.org/api/v1/datasets/?owner_id=foo',
            text=make_api_key_callback(json.dumps(data), key)
        )

        c = Client()
        c.set_api_key('user', key)
        c.username = 'foo'
        datasets = c.list_my_datasets()
        list(datasets)

        assert m.called
        assert m.request_history[0].qs['owner_id'] == ['foo']


def test_get_dataset_import(dataset_import_data):
    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/dataset_imports/a1b2c3/', text=json.dumps(dataset_import_data))

        c = Client()
        dataset_import = c.get_import('a1b2c3')

        assert m.called
        assert dataset_import.id == 'a1b2c3'


def test_get_dataset_import_with_api_key(dataset_import_data):
    key = 'abcde12345'

    with requests_mock.mock() as m:
        m.get(
            'https://databasin.org/api/v1/dataset_imports/a1b2c3/',
            text=make_api_key_callback(json.dumps(dataset_import_data), key)
        )

        c = Client()
        c.set_api_key('user', key)
        dataset_import = c.get_import('a1b2c3')

        assert m.called
        assert dataset_import.id == 'a1b2c3'


def test_get_dataset_import_login_required():
    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/dataset_imports/a1b2c3/', status_code=401)

        c = Client()
        with pytest.raises(LoginRequiredError):
            c.get_import('a1b2c3')


def test_get_dataset_import_forbidden():
    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/dataset_imports/a1b2c3/', status_code=401)

        c = Client()
        c.username = 'foo'
        with pytest.raises(ForbiddenError):
            c.get_import('a1b2c3')


def test_list_dataset_imports(dataset_import_data):
    data = {
        'meta': {'next': None, 'total_count': 2},
        'objects': [
            dataset_import_data,
            copy.copy(dataset_import_data)
        ]
    }
    data['objects'][1]['id'] = 'a1b2c4'

    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/dataset_imports/', text=json.dumps(data))

        c = Client()
        imports = c.list_imports()

        assert m.called
        assert len(imports) == 2
        imports = list(imports)
        assert imports[0].id == 'a1b2c3'
        assert imports[1].id == 'a1b2c4'


def test_list_dataset_imports_with_api_key(dataset_import_data):
    key = 'abcde12345'

    data = {
        'meta': {'next': None, 'total_count': 2},
        'objects': [
            dataset_import_data,
            copy.copy(dataset_import_data)
        ]
    }
    data['objects'][1]['id'] = 'a1b2c4'

    with requests_mock.mock() as m:
        m.get(
            'https://databasin.org/api/v1/dataset_imports/',
            text=make_api_key_callback(json.dumps(data), key)
        )

        c = Client()
        c.set_api_key('user', key)
        imports = c.list_imports()

        assert m.called
        assert len(imports) == 2
        imports = list(imports)
        assert imports[0].id == 'a1b2c3'
        assert imports[1].id == 'a1b2c4'


def test_list_dataset_imports_with_filters():
    data = {'meta': {'next': None, 'total_count': 0}, 'objects': []}

    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/dataset_imports/?private=False', text=json.dumps(data))

        c = Client()
        c.list_imports(filters={'private': False})

        assert m.called
        assert m.request_history[0].qs == {'private': ['false']}


def test_list_dataset_imports_with_filters_with_api_key():
    key = 'abcde12345'
    data = {'meta': {'next': None, 'total_count': 0}, 'objects': []}

    with requests_mock.mock() as m:
        m.get(
            'https://databasin.org/api/v1/dataset_imports/?private=False',
            text=make_api_key_callback(json.dumps(data), key)
        )

        c = Client()
        c.set_api_key('user', key)
        c.list_imports(filters={'private': False})

        assert m.called
        assert m.request_history[0].qs == {'private': ['false']}


def test_dataset_import_cancel(dataset_import_data):
    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/dataset_imports/a1b2c3/', text=json.dumps(dataset_import_data))
        m.delete('https://databasin.org/api/v1/dataset_imports/a1b2c3/', status_code=204)

        c = Client()
        dataset_import = c.get_import('a1b2c3')
        dataset_import.cancel()

        assert m.call_count == 2


def test_dataset_import_cancel_login_required(dataset_import_data):
    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/dataset_imports/a1b2c3/', text=json.dumps(dataset_import_data))
        m.delete('https://databasin.org/api/v1/dataset_imports/a1b2c3/', status_code=401)

        c = Client()
        dataset_import = c.get_import('a1b2c3')
        with pytest.raises(LoginRequiredError):
            dataset_import.cancel()

def test_dataset_import_cancel_with_api_key(dataset_import_data):
    key = 'abcde12345'

    with requests_mock.mock() as m:
        m.get(
            'https://databasin.org/api/v1/dataset_imports/a1b2c3/',
            text=make_api_key_callback(json.dumps(dataset_import_data), key)
        )
        m.delete(
            'https://databasin.org/api/v1/dataset_imports/a1b2c3/',
            status_code=204,
            text=make_api_key_callback('', key)
        )

        c = Client()
        c.set_api_key('user', key)
        dataset_import = c.get_import('a1b2c3')
        dataset_import.cancel()

        assert m.call_count == 2


def test_dataset_import_cancel_forbidden(dataset_import_data):
    with requests_mock.mock() as m:
        m.get('https://databasin.org/api/v1/dataset_imports/a1b2c3/', text=json.dumps(dataset_import_data))
        m.delete('https://databasin.org/api/v1/dataset_imports/a1b2c3/', status_code=401)

        c = Client()
        c.username = 'foo'
        dataset_import = c.get_import('a1b2c3')
        with pytest.raises(ForbiddenError):
            dataset_import.cancel()

