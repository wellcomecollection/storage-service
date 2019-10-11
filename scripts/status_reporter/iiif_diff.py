# -*- encoding: utf-8

import collections

from deepdiff import DeepDiff
import hyperlink


class IIIFDiff:
    def __init__(self, library_iiif, id_mapper):
        self.library_iiif = library_iiif
        self.id_mapper = id_mapper

    def _check_preservica_id_matches(self, old_id, new_id):
        return self.id_mapper.id_matches(old_id, new_id)

    @staticmethod
    def _diff_modulo_hostname(deep_diff):
        """
        Compare URLs such as

            https://library-uat.wellcomelibrary.org/iiif/b18180000/manifest
            https://wellcomelibrary.org/iiif/b18180000/manifest

        """
        for label, diff in list(deep_diff.get("values_changed", {}).items()):
            old_value = diff["old_value"]
            new_value = diff["new_value"]

            if new_value == old_value.replace(
                "https://wellcomelibrary.org/",
                "https://library-uat.wellcomelibrary.org/",
            ):
                del deep_diff["values_changed"][label]

    def _diff_modulo_imageanno(self, deep_diff):
        """
        Compare URLs such as

            https://library-uat.wellcomelibrary.org/iiif/b18180000/imageanno/b18180000_pp_cri_j_1_5_18_1_0251.jp2
            https://wellcomelibrary.org/iiif/b18180000/imageanno/008c8d8a-a576-4487-809f-6dce77c395ad

        """
        for label, diff in list(deep_diff.get("values_changed", {}).items()):
            old_value = diff["old_value"]
            new_value = diff["new_value"]

            if "/imageanno/" not in old_value:
                continue

            old_path, old_id = old_value.rsplit("/", 1)
            new_path, new_id = new_value.rsplit("/", 1)

            paths_match = new_path == old_path.replace(
                "https://wellcomelibrary.org/",
                "https://library-uat.wellcomelibrary.org/",
            )

            ids_match = self._check_preservica_id_matches(old_id, new_id)

            if paths_match and ids_match:
                del deep_diff["values_changed"][label]

    def _diff_modulo_dlcs(self, deep_diff):
        """
        Compare URLs of the form

            https://dlcs.io/iiif-img/wellcome/5/b1234_0001.jp2/full/!1024,1024/0/default.jpg
            https://dlcs.io/thumbs/wellcome/5/b1234_0001.jp2/full/64,/0/default.jpg

        """
        for label, diff in list(deep_diff.get("values_changed", {}).items()):
            old_value = diff["old_value"]
            new_value = diff["new_value"]

            dlcs_prefixes = ("https://dlcs.io/iiif-img/", "https://dlcs.io/thumbs/")

            if not old_value.startswith(dlcs_prefixes):
                continue

            old_url = hyperlink.URL.from_text(old_value)
            new_url = hyperlink.URL.from_text(new_value)

            old_path = old_url.path
            new_path = new_url.path

            suffix_matches = old_path[4:] == new_path[4:]
            prefix_matches = old_path[:2] == new_path[:2]

            scheme_matches = old_url.scheme == new_url.scheme

            host_matches = old_url.host == new_url.host

            old_id = old_path[3]
            new_id = new_path[3]

            ids_match = self._check_preservica_id_matches(old_id, new_id)

            if (
                suffix_matches
                and prefix_matches
                and scheme_matches
                and host_matches
                and ids_match
            ):
                del deep_diff["values_changed"][label]

    @staticmethod
    def _diff_modulo_author_ordering(deep_diff, old_manifest, new_manifest):
        # DDS returns authors in an arbitrary order, semicolon-separated.
        #
        # As long as the authors are the same, even if the order is different,
        # the manifests are equivalent enough for us.
        #
        # See conversation at
        # https://wellcome.slack.com/archives/CBT40CMKQ/p1570734445106800
        for label, diff in list(deep_diff.get("values_changed", {}).items()):
            if label != "root['metadata'][1]['value']":
                continue

            if old_manifest["metadata"][1]["label"] != "Author(s)":
                continue

            old_authors = collections.Counter(diff["old_value"].split(";"))
            new_authors = collections.Counter(diff["new_value"].split(";"))

            if old_authors != new_authors:
                continue

            del deep_diff["values_changed"][label]

    def diff_manifests(self, old_manifest, new_manifest):
        deep_diff = DeepDiff(old_manifest, new_manifest)

        self._diff_modulo_hostname(deep_diff)
        self._diff_modulo_imageanno(deep_diff)
        self._diff_modulo_dlcs(deep_diff)
        self._diff_modulo_author_ordering(
            deep_diff, old_manifest=old_manifest, new_manifest=new_manifest
        )

        if not deep_diff.get("values_changed", True):
            del deep_diff["values_changed"]

        return deep_diff

    def fetch_and_diff(self, bnum):
        new_manifest = self.library_iiif.stage(bnum)
        old_manifest = self.library_iiif.prod(bnum)

        return self.diff_manifests(old_manifest=old_manifest, new_manifest=new_manifest)


if __name__ == "__main__":
    import sys
    from pprint import pprint

    from id_mapper import IDMapper
    from library_iiif import LibraryIIIF

    try:
        b_number = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <B_NUMBER>")

    diff = IIIFDiff(library_iiif=LibraryIIIF(), id_mapper=IDMapper())

    pprint(diff.fetch_and_diff(b_number))
