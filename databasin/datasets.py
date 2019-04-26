from restle import fields
from restle.resources import Resource

from databasin.utils import raise_for_authorization


class DatasetResource(Resource):
    id = fields.TextField()
    owner_id = fields.TextField()
    private = fields.BooleanField()
    title = fields.TextField()
    snippet = fields.TextField(default='')
    create_date = fields.TextField()  # Todo: replace with `DateTimeField` when available
    modify_date = fields.TextField()  # Todo: replace with `DateTimeField` when available
    thumbnail_url = fields.TextField(required=False)
    credits = fields.TextField(required=False)
    citation = fields.TextField(required=False)
    spatial_resolution = fields.TextField(required=False)
    use_constraints = fields.TextField(required=False)
    file_size = fields.IntegerField(required=False)
    uncompressed_size = fields.IntegerField(required=False)
    peer_reviewed = fields.IntegerField(required=False)
    native = fields.BooleanField()
    contact_organization = fields.TextField(required=False)
    contact_persons = fields.TextField(required=False)
    is_aggregate = fields.BooleanField(default=False)
    can_download = fields.BooleanField(default=False)
    allow_anonymous_download = fields.BooleanField(default=False)
    can_comment = fields.BooleanField(default=False)
    tags = fields.ObjectField()
    ext_download_path = fields.TextField(required=False)
    version = fields.IntegerField(default=1)

    def _set_private(self, private):
        r = self._session.patch(self._url, json={'private': private})
        raise_for_authorization(r, hasattr(self._session, 'client') and self._session.client.username is not None)
        r.raise_for_status()

    def make_public(self):
        self._set_private(False)

    def make_private(self):
        self._set_private(True)

    @property
    def data(self):
        """ Returns dataset data as a CSV-formatted string """

        r = self._session.get('{}/data/'.format(self._url.strip('/')))
        raise_for_authorization(r, hasattr(self._session, 'client') and self._session.client.username is not None)
        r.raise_for_status()

        return r.text


class DatasetListResource(Resource):
    meta = fields.ObjectField('meta')
    objects = fields.ToManyField(DatasetResource, nest_type='full', id_field='id', relative_path='{id}/')


class DatasetImportResource(Resource):
    id = fields.TextField()
    owner_id = fields.TextField()
    private = fields.BooleanField()
    title = fields.TextField()
    description = fields.TextField()
    create_date = fields.TextField()  # Todo: replace with `DateTimeField` when available
    modify_date = fields.TextField()  # Todo: replace with `DateTimeField` when available
    thumbnail_url = fields.TextField(required=False)
    credits = fields.TextField(required=False)
    citation = fields.TextField(required=False)
    spatial_resolution = fields.TextField(required=False)
    use_constraints = fields.TextField(required=False)
    file_size = fields.IntegerField(required=False)
    uncompressed_size = fields.IntegerField(required=False)
    peer_reviewed = fields.IntegerField(required=False)
    native = fields.BooleanField()
    contact_organization = fields.TextField(required=False)
    contact_persons = fields.TextField(required=False)
    is_dataset_edit = fields.BooleanField()
    failed = fields.BooleanField()
    can_download = fields.BooleanField(default=False)
    tags = fields.ObjectField()
    ext_download_path = fields.TextField(required=False)
    aggregate_auto_updates = fields.BooleanField('agg_auto_updates', required=False)

    def cancel(self):
        r = self._session.delete(self._url)
        raise_for_authorization(r, hasattr(self._session, 'client') and self._session.client.username is not None)
        r.raise_for_status()


class DatasetImportListResource(Resource):
    meta = fields.ObjectField('meta')
    objects = fields.ToManyField(DatasetImportResource, nest_type='full')
