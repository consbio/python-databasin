import six
from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from restle.exceptions import HTTPException

import databasin
from databasin.datasets import DatasetResource, DatasetListResource
from databasin.exceptions import LoginError
from databasin.jobs import JobResource
from databasin.utils import ResourcePaginator, raise_for_authorization

# IDE inspection trips over these as imports
urljoin = six.moves.urllib_parse.urljoin
urlencode = six.moves.urllib_parse.urlencode

DATASET_DETAIL_PATH = '/api/v1/datasets/{id}/'
DATASET_LIST_PATH = '/api/v1/datasets/'
DEFAULT_HOST = 'databasin.org'
JOB_CREATE_PATH = '/api/v1/jobs/'
JOB_DETAIL_PATH = '/api/v1/jobs/{id}/'
LOGIN_PATH = '/auth/login_iframe/'


class RefererHTTPAdapter(HTTPAdapter):
    def add_headers(self, request, **kwargs):
        request.headers['referer'] = request.url


class Client(object):
    def __init__(self, host=DEFAULT_HOST):
        self._session = Session()
        self._session.client = self
        self._session.headers = {'user-agent': 'python-databasin/{}'.format(databasin.__version__)}
        self._session.mount('https://', RefererHTTPAdapter())

        self.base_url = 'http://{}'.format(host)
        self.base_url_https = 'https://{}'.format(host)
        self.username = None

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

        if not 'sessionid' in r.cookies:
            raise LoginError

        self.username = username

    def list_datasets(self, filters={}):
        url = self.build_url(DATASET_LIST_PATH)
        if filters:
            url += '?{0}'.format(urlencode(filters))

        return ResourcePaginator(DatasetListResource.get(url, session=self._session, lazy=False))

    def list_my_datasets(self):
        if not self.username:
            return []

        return self.list_datasets({'owner_id': self.username})

    def get_dataset(self, dataset_id):
        try:
            return DatasetResource.get(
                self.build_url(DATASET_DETAIL_PATH.format(id=dataset_id)), session=self._session, lazy=False
            )
        except HTTPException as e:
            raise_for_authorization(e.response, self.username is not None)
            raise

    def create_job(self, name, job_args={}, block=False):
        job = JobResource.create(self.build_url(JOB_CREATE_PATH), name=name, job_args=job_args, session=self._session)
        if block:
            job.join()

        return job

    def get_job(self, job_id):
        try:
            return JobResource.get(
                self.build_url(JOB_DETAIL_PATH.format(id=job_id)), session=self._session, lazy=False
            )
        except HTTPException as e:
            raise_for_authorization(e.response, self.username is not None)
            raise
