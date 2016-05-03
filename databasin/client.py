import json
import os
import re
import zipfile

import six
from requests import Session
from requests.adapters import HTTPAdapter
from restle.exceptions import HTTPException

import databasin
from databasin.datasets import DatasetResource, DatasetListResource, DatasetImportListResource, DatasetImportResource
from databasin.exceptions import LoginError, DatasetImportError
from databasin.jobs import JobResource
from databasin.uploads import TemporaryFileResource, TEMPORARY_FILE_DETAIL_PATH, TemporaryFileListResource
from databasin.utils import ResourcePaginator, raise_for_authorization

# IDE inspection trips over these as imports
urljoin = six.moves.urllib_parse.urljoin
urlencode = six.moves.urllib_parse.urlencode

DATASET_DETAIL_PATH = '/api/v1/datasets/{id}/'
DATASET_IMPORT_DETAIL_PATH = '/api/v1/dataset_imports/{id}/'
DATASET_IMPORT_LIST_PATH = '/api/v1/dataset_imports/'
DATASET_LIST_PATH = '/api/v1/datasets/'
DEFAULT_HOST = 'databasin.org'
JOB_CREATE_PATH = '/api/v1/jobs/'
JOB_DETAIL_PATH = '/api/v1/jobs/{id}/'
LOGIN_PATH = '/auth/api/login/'
TEMPORARY_FILE_LIST_PATH = '/api/v1/uploads/temporary-files/'
TEMPORARY_FILE_UPLOAD_PATH = '/uploads/upload-temporary-file/'

DATASET_IMPORT_ID_RE = re.compile(r'\/import\/([^\/]*)')


class RefererHTTPAdapter(HTTPAdapter):
    def add_headers(self, request, **kwargs):
        request.headers['Referer'] = request.url

        if request.method.lower() not in {'get', 'head'} and 'csrftoken' in request._cookies:
            request.headers['X-CSRFToken'] = request._cookies['csrftoken']


class Client(object):
    def __init__(self, host=DEFAULT_HOST):
        self._session = Session()
        self._session.client = self
        self._session.headers = {'user-agent': 'python-databasin/{}'.format(databasin.__version__)}
        self._session.mount('https://', RefererHTTPAdapter())
        self._session.mount('http://', RefererHTTPAdapter())

        self.base_url = 'https://{}'.format(host)
        self.username = None

    def build_url(self, path):
        return urljoin(self.base_url, path)

    def login(self, username, password):
        url = self.build_url(LOGIN_PATH)

        # Make a get request first to get the CSRF token cookie
        r = self._session.get(self.build_url('/'))
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

    def list_datasets(self, filters={}, items_per_page=100):
        filters['limit'] = items_per_page
        url = '{0}?{1}'.format(self.build_url(DATASET_LIST_PATH), urlencode(filters))

        return ResourcePaginator(DatasetListResource.get(url, session=self._session, lazy=False))

    def list_my_datasets(self, **kwargs):
        if not self.username:
            return []

        filters = kwargs.get('filters', {})
        filters['owner_id'] = self.username
        kwargs['filters'] = filters

        return self.list_datasets(**kwargs)

    def get_dataset(self, dataset_id):
        try:
            return DatasetResource.get(
                self.build_url(DATASET_DETAIL_PATH.format(id=dataset_id)), session=self._session, lazy=False
            )
        except HTTPException as e:
            raise_for_authorization(e.response, self.username is not None)
            raise

    def list_imports(self, filters={}):
        url = self.build_url(DATASET_IMPORT_LIST_PATH)
        if filters:
            url += '?{0}'.format(urlencode(filters))

        return ResourcePaginator(DatasetImportListResource.get(url, session=self._session, lazy=False))

    def get_import(self, import_id):
        try:
            return DatasetImportResource.get(
                self.build_url(DATASET_IMPORT_DETAIL_PATH.format(id=import_id)), session=self._session, lazy=False
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

    def upload_temporary_file(self, f, filename=None):
        return TemporaryFileResource.upload(
            self.build_url(TEMPORARY_FILE_UPLOAD_PATH), f, filename=filename, session=self._session
        )

    def list_temporary_files(self):
        return ResourcePaginator(
            TemporaryFileListResource.get(self.build_url(TEMPORARY_FILE_LIST_PATH), session=self._session, lazy=False)
        )

    def get_temporary_file(self, uuid):
        try:
            return TemporaryFileResource.get(
                self.build_url(TEMPORARY_FILE_DETAIL_PATH.format(uuid=uuid)), session=self._session, lazy=False
            )
        except HTTPException as e:
            raise_for_authorization(e.response, self.username is not None)
            raise

    def import_netcdf_dataset(self, nc_or_zip_file, style=None):
        if nc_or_zip_file.endswith('.zip'):
            f = open(nc_or_zip_file, 'a+b')
            zf = zipfile.ZipFile(f, 'a')
        elif nc_or_zip_file.endswith('.nc'):
            f = six.BytesIO()
            zf = zipfile.ZipFile(f, 'w', zipfile.ZIP_DEFLATED)
            zf.write(nc_or_zip_file, os.path.basename(nc_or_zip_file))
        else:
            raise ValueError('File must be .nc or .zip')

        try:
            if style is not None and isinstance(style, six.string_types):
                style = json.loads(style)

            if style:
                zf.writestr('style.json', json.dumps(style))
            elif not any(name.endswith('style.json') for name in zf.namelist()):
                raise ValueError(
                    'Import must include style information (either in the zip archive or passed in as an argument)'
                )

            zf.close()
            f.seek(0)

            filename = '{0}.zip'.format(os.path.splitext(os.path.basename(nc_or_zip_file))[0])
            tmp_file = self.upload_temporary_file(f, filename=filename)
        finally:
            zf.close()
            f.close()

        job_args = {
            'file': tmp_file.uuid,
            'url': None,
            'dataset_type': 'NetCDF_Native'
        }
        job = self.create_job('create_import_job', job_args=job_args, block=True)

        if job.status != 'succeeded':
            raise DatasetImportError('Import failed: {0}'.format(job.message))

        # For now, we require imports to have all metadata and style info necessary for one-step import. Later we may
        # add support for multi-stage import, wherein the user could incrementally provide necessary information.
        data = json.loads(job.message)
        next_uri = data['next_uri']
        if '/import/' in next_uri:
            dataset_import_id = DATASET_IMPORT_ID_RE.search(next_uri).group(1)
            dataset_import = self.get_import(dataset_import_id)
            dataset_import.cancel()

            raise DatasetImportError(
                'NetCDF imports must have all necessary style and metadata information necessary for one-step import.'
            )

        dataset_id = next_uri.strip('/').split('/')[-1]
        return self.get_dataset(dataset_id)
