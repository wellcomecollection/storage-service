#!/usr/bin/env python
# -*- encoding: utf-8

import json
import os
import re
import sys
import tempfile
from urllib.request import urlretrieve

import hyperlink
import imagehash
from PIL import Image
import pytest
from recursive_diff import recursive_diff
import requests
from unidecode import unidecode


# Monkey-patch to ensure we don't get truncated URLs in results
sys.modules["recursive_diff.recursive_diff"]._str_trunc = str


def slugify(u):
    # https://leancrew.com/all-this/2014/10/asciifying/
    "Convert Unicode string into blog slug."
    u = re.sub(u'[–—/:;,.]', '-', u)  # replace separating punctuation
    a = unidecode(u).lower()          # best ASCII substitutions, lowercased
    a = re.sub(r'[^a-z0-9 -]', '', a) # delete any other characters
    a = a.replace(' ', '-')           # spaces to hyphens
    a = re.sub(r'-+', '-', a)         # condense repeated hyphens
    return a


def download_image(url, out_path):
    if os.path.exists(out_path):
        return

    tmp_path = tempfile.mktemp()

    try:
        urlretrieve(str(url), tmp_path)
    except Exception:
        print("Error trying to retrieve %s" % url, file=sys.stderr)
        raise

    os.rename(tmp_path, out_path)


def are_these_urls_the_same_image(url1, url2):
    # If we have URLs to two resources in DLCS, e.g.
    #
    #    https://dlcs.io/thumbs/wellcome/5/b30145004_0001.jp2/full/72,100/0/default.jpg
    #    https://dlcs.io/thumbs/wellcome/1/a968db42-c1b9-45ba-91ed-5f89126a084a/full/72,100/0/default.jpg
    #
    # Do these correspond to the same image?
    #
    # Returns True if the images are the same.
    slug_1 = slugify(str(url1))
    slug_2 = slugify(str(url2))

    for ext in (".jpg", ".jp2"):
        if url1.path[-1].endswith(ext) or url2.path[-1].endswith(ext):
            slug_1 += ext
            slug_2 += ext

    os.makedirs("img_cache", exist_ok=True)
    cache_1 = os.path.join("img_cache", slug_1)
    cache_2 = os.path.join("img_cache", slug_2)

    try:
        download_image(url1, cache_1)
        download_image(url2, cache_2)
    except Exception:
        return False

    # If these are IIIF identifiers rather than actual images,
    # e.g. https://dlcs.io/thumbs/wellcome/5/b30145004_0019.jp2/info.json
    # We need to compare them as JSON objects.
    try:
        blob1 = json.load(open(cache_1))
        blob2 = json.load(open(cache_2))

        if blob1 == blob2:
            return True

        return (
            blob1.pop("@id") == url1 and
            blob2.pop("@id") == url2 and
            blob1 == blob2
        )

    except ValueError:
        pass

    try:
        im1 = Image.open(cache_1)
        im2 = Image.open(cache_2)
    except OSError as err:
        print(err)
        return False

    return imagehash.average_hash(im1) == imagehash.average_hash(im2)


@pytest.mark.parametrize('diff, expected_result', [
    (
        "[@id]: https://library-uat.wellcomelibrary.org/iiif/b30145004/manifest != https://wellcomelibrary.org/iiif/b30145004/manifest",
        False
    ),
    ("[aB]: https://example.org/1 != https://example./org/2", False),
    ("[ab]: https://example/1_2 != https://example/1_1", False),
    ("[ab]: https://example/1!2 != https://example/1!1", False),
    ("[ab]: https://example/1,2 != https://example/1,1", False),
    ("[ab]: https://example/A != https://example/B", False),
    ("[ab]: https://example/?a=1 != https://example/?a=2", False),
])
def test_is_different_modulo_images(diff, expected_result):
    assert is_different_modulo_images(diff) == expected_result


def is_different_modulo_images(diff):
    # We're looking for entries of the form:
    #
    #       [structures][1][@id]: https://example.org/1 != https://example.org/2
    #
    # because URLs are expected to differ in certain ways!  So filter them out
    # from the list.
    m = re.search(
        r"^[\[a-zA-Z0-9@\]@]+: (?P<url1>[a-zA-Z0-9:/\-\._!,?=]+) != (?P<url2>[a-zA-Z0-9:/\-\._!,?=]+)",
        diff
    )

    if m is None:
        if "https" in diff:
            print("Why wasn't this matched?")
            print(diff)
            assert 0
        return True

    url1 = m.group("url1")
    url2 = m.group("url2")

    # For example:
    #
    #   [@id]:
    #   https://library-uat.wellcomelibrary.org/iiif/b30145004/manifest !=
    #   https://wellcomelibrary.org/iiif/b30145004/manifest
    #
    # Because the paths are the same, we know these refer to equivalent resources.
    #
    if "library-uat" in url1 and "wellcomelibrary.org" in url2:
        h_url1 = hyperlink.URL.from_text(url1)
        h_url2 = hyperlink.URL.from_text(url1)

        return (
            (h_url1.scheme != h_url2.scheme) or
            (h_url1.path != h_url2.path) or
            (h_url1.query != h_url2.query) or
            (h_url1.fragment != h_url2.fragment)
        )

    if url1.startswith("https://dlcs.io") and url2.startswith("https://dlcs.io"):
        return not are_these_urls_the_same_image(
            hyperlink.URL.from_text(url1),
            hyperlink.URL.from_text(url2)
        )

    print("I DON'T KNOW HOW TO COMPARE THESE URLS:")
    print(diff)
    assert 0




def compare_uat_and_library_manifests(b_number):
    # Start by fetching the manifests from the UAT and library site.
    uat_manifest = requests.get(
        f"https://library-uat.wellcomelibrary.org/iiif/{b_number}/manifest"
    ).json()

    prod_manifest = requests.get(
        f"https://wellcomelibrary.org/iiif/{b_number}/manifest"
    ).json()

    # We expect these responses are different
    assert uat_manifest != prod_manifest

    has_differences = False
    for diff in recursive_diff(uat_manifest, prod_manifest):
        if is_different_modulo_images(diff):
            print(diff)
            has_differences = True

    return has_differences


if __name__ == "__main__":
    try:
        if compare_uat_and_library_manifests(sys.argv[1]):
            print("Manifests match!", file=sys.stderr)
        else:
            print("Manifests differ!", file=sys.stderr)
    except IndexError:
        sys.exit(f"Usage: {__file__} <B_NUMBER>")
