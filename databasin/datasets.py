from restle import fields
from restle.resources import Resource


class DatasetResource(Resource):
    id = fields.TextField()
    owner_id = fields.TextField()
    private = fields.BooleanField()
    title = fields.TextField()
    snippet = fields.TextField(default='')
    create_date = fields.TextField()  # Todo: replace with `DateTimeField` when available
    modify_date = fields.TextField()  # Todo: replace with `DateTimeField` when available
    subtitle = fields.TextField(required=False)
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


class DatasetListResource(Resource):
    meta = fields.ObjectField('meta')
    objects = fields.ToManyField(DatasetResource, nest_type='full')
