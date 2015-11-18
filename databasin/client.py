import six
from requests import Session
from requests.adapters import HTTPAdapter

import databasin
from databasin.datasets import DatasetResource, DatasetListResource
from databasin.exceptions import LoginError
from databasin.utils import ResourcePaginator

# IDE inspection trips over these as imports
urljoin = six.moves.urllib_parse.urljoin
urlencode = six.moves.urllib_parse.urlencode

DATASET_DETAIL_PATH = '/api/v1/datasets/{id}/'
DATASETS_LIST_PATH = '/api/v1/datasets/'
DEFAULT_HOST = 'databasin.org'
LOGIN_PATH = '/auth/login_iframe/'


class RefererHTTPAdapter(HTTPAdapter):
    def add_headers(self, request, **kwargs):
        request.headers['referer'] = request.url


class Client(object):
    def __init__(self, host=DEFAULT_HOST):
        self._session = Session()
        self.base_url = 'http://{}'.format(host)
        self.base_url_https = 'https://{}'.format(host)
        self._session.headers = {'user-agent': 'python-databasin/{}'.format(databasin.__version__)}
        self._session.mount('https://', RefererHTTPAdapter())

    def build_url(self, path, https=False):
        return urljoin(self.base_url_https if https else self.base_url, path)

    def login(self, username, password):
        url = self.build_url(LOGIN_PATH, https=True)

        # Make a get request first to get the CSRF token cookie
        r = self._session.get(url)
        r.raise_for_status()

        r = self._session.post(url, data={
            'username': username,
            'password': password,
            'csrfmiddlewaretoken': r.cookies['csrftoken']
        }, allow_redirects=False)
        r.raise_for_status()

        if not 'sessionid' in self._session.cookies:
            raise LoginError

    def list_datasets(self, filters={}):
        url = self.build_url(DATASETS_LIST_PATH)
        if filters:
            url += '?{0}'.format(urlencode(filters))

        return ResourcePaginator(DatasetListResource.get(url, session=self._session))

    def get_dataset(self, dataset_id):
        return DatasetResource.get(self.build_url(DATASET_DETAIL_PATH.format(id=dataset_id)), session=self._session)
