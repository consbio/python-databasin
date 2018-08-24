import six
import time
from requests import Session
from restle import fields
from restle.resources import Resource

from databasin.utils import raise_for_authorization

urlparse = six.moves.urllib_parse.urlparse  # IDE inspection trips over this as an import


class JobResource(Resource):
    id = fields.TextField()
    job_name = fields.TextField()
    status = fields.TextField()
    progress = fields.IntegerField()
    message = fields.TextField(required=False)

    @classmethod
    def create(cls, url, name, job_args={}, session=None):
        if session is None:
            session = Session()

        data = {
            'job_name': name,
            'job_args': job_args
        }
        r = session.post(url, json=data)
        raise_for_authorization(r, hasattr(session, 'client') and session.client.username is not None)
        r.raise_for_status()

        location = r.headers['Location']
        location_o = urlparse(location)
        if not location_o.scheme:
            url_o = urlparse(url)
            location = '{}://{}{}'.format(url_o.scheme, url_o.netloc, location_o.path)

        return cls.get(location, session=session)

    def refresh(self):
        job = self.get(self._url, lazy=False, session=self._session)
        for attr in ('status', 'progress', 'message'):
            setattr(self, attr, getattr(job, attr))

    def join(self):
        """Block until the job is complete."""

        while self.status in {'queued', 'running'}:
            time.sleep(1)
            self.refresh()
