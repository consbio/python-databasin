import six

from databasin.exceptions import LoginRequiredError, ForbiddenError

urlparse = six.moves.urllib_parse.urlparse  # IDE inspection trips over this as an import


class ResourcePaginator(object):
    def __init__(self, resource):
        self.resource = resource
        self.loaded_urls = set()

    def __iter__(self):
        while True:
            for obj in self.resource.objects:
                yield obj

            if not self.resource.meta.next:
                break
            else:
                o = urlparse(self.resource._url)
                url = '{0}://{1}{2}'.format(o.scheme, o.netloc, self.resource.meta.next)
                if url.lower() in self.loaded_urls:
                    break
                self.loaded_urls.add(url.lower())

                self.resource = self.resource.get(url, session=self.resource._session)

    def __len__(self):
        return self.count()

    def count(self):
        return self.resource.meta.total_count


def raise_for_authorization(response, is_logged_in):
    """Raises `LoginRequiredError` or `ForbiddenError` when appropriate"""

    if response.status_code == 401:
        # Data Basin will return 401 in cases where it should return 403. So if we get this HTTP code and are already
        # logged in, then `ForbiddenError` is the correct response.
        if is_logged_in:
            response.status_code = 403
        else:
            raise LoginRequiredError(response=response)

    if response.status_code == 403:
        raise ForbiddenError(response=response)
