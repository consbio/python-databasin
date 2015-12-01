import requests_mock
from requests.models import Request

from databasin.client import Client

LOGIN_URL = 'https://databasin.org/auth/login_iframe/'


def test_alternative_host():
    c = Client('example.com:81')

    assert c.base_url == 'http://example.com:81'
    assert c.base_url_https == 'https://example.com:81'


def test_https_referer():
    """Django requires all POST requests via HTTPS to have the Referer header set."""

    c = Client()
    r = c._session.prepare_request(Request('POST', LOGIN_URL))
    c._session.get_adapter(LOGIN_URL).add_headers(r)

    assert r.headers['Referer'] == LOGIN_URL


def test_login():
    return  # requests-mock doesn't support response cookies yet (soon)
    with requests_mock.mock() as m:
        m.get(LOGIN_URL, cookies={'csrftoken': 'abcd'})
        m.post(LOGIN_URL, cookies={'sessionid': 'asdf'})

        c = Client()
        c.login('foo', 'bar')

        assert m.call_count == 2


def test_login_no_redirect():
    return  # requests-mock doesn't support response cookies yet (soon)
    with requests_mock.mock() as m:
        m.get('http://databasin.org/')
        m.get(LOGIN_URL, cookies={'csrftoken': 'abcd'})
        m.post(
            LOGIN_URL, headers={'Location': 'http://databasin.org/'}, cookies={'sessionid': 'asdf'}, status_code=302
        )

        c = Client()
        c.login('foo', 'bar')

        assert m.call_count == 2
        assert not any(r.url for r in m.request_history if r.url == 'http://databasin.org/')
