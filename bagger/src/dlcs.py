# -*- encoding: utf-8

from json import JSONDecodeError

from requests import get, auth
import settings


def get_image(image_slug, space=None):
    if space is None:
        space = settings.DLCS_SPACE

    url = "{0}spaces/{1}/images/{2}".format(get_customer_url(), space, image_slug)
    response = get(url, auth=get_authorisation())
    try:
        return response.json()
    except JSONDecodeError as err:
        raise ValueError("Error parsing DLCS response for %s as JSON: %r" % (url, err))


def get_authorisation():
    return auth.HTTPBasicAuth(settings.DLCS_API_KEY, settings.DLCS_API_SECRET)


def get_customer_url():
    return "{0}customers/{1}/".format(settings.DLCS_ENTRY, settings.DLCS_CUSTOMER_ID)
