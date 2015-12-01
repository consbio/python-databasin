import os

import six
from requests import Session
from restle import fields
from restle.resources import Resource

from databasin.utils import raise_for_authorization

urlparse = six.moves.urllib_parse.urlparse  # IDE inspection trips over this as an import

TEMPORARY_FILE_DETAIL_PATH = '/api/v1/uploads/temporary-files/{uuid}/'


class TemporaryFileResource(Resource):
    uuid = fields.TextField()
    date = fields.TextField()
    is_image = fields.BooleanField()
    filename = fields.TextField()
    url = fields.TextField()

    @classmethod
    def upload(cls, url, f, filename=None, session=None):
        if session is None:
            session = Session()

        should_close = True

        if isinstance(f, six.string_types):
            if not filename:
                filename = os.path.basename(f)

            f = open(f, 'rb')
            should_close = True

        try:
            if 'csrftoken' not in session.cookies:
                session.get('http://databasin.org')

            r = session.post(
                url, data={'csrfmiddlewaretoken': session.cookies['csrftoken']}, files={'file': (filename, f)}
            )
            raise_for_authorization(r, session.client.username is not None)
            r.raise_for_status()

            o = urlparse(url)
            return cls.get(
                '{0}://{1}{2}'.format(o.scheme, o.netloc, TEMPORARY_FILE_DETAIL_PATH.format(uuid=r.json()['uuid'])),
                session=session,
                lazy=False
            )
        finally:
            if should_close:
                f.close()


class TemporaryFileListResource(Resource):
    meta = fields.ObjectField('meta')
    objects = fields.ToManyField(TemporaryFileResource, nest_type='full')
