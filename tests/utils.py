import datetime

import dateutil.parser
from dateutil.tz import tzlocal
from django.core.signing import base64_hmac
from django.utils.crypto import constant_time_compare


def make_api_key_callback(response, key):
    class AuthenticationError(Exception):
        pass

    def callback(request, context):
        if not all(h in request.headers for h in ('x-api-user', 'x-api-time', 'x-api-signature')):
            context.status_code = 401
            raise AuthenticationError('API headers are missing')

        request_time = dateutil.parser.parse(request.headers['x-api-time'])
        if request_time > datetime.datetime.now(tzlocal()) + datetime.timedelta(minutes=5):
            raise AuthenticationError('API request date is too old')

        try:
            salt, signature = request.headers['x-api-signature'].split(b':', 1)
            signature = signature.strip(b'=')
        except ValueError:
            raise AuthenticationError('API signature is malformed')

        test_signature = base64_hmac(salt, request.headers['x-api-time'], key)
        if constant_time_compare(test_signature, signature):
            context.status_code = 200
            return response

        context.status_code = 401
        raise AuthenticationError('Key signature is bad ({} != {})'.format(signature, test_signature))

    return callback
