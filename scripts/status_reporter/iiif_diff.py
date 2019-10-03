from deepdiff import DeepDiff
import hyperlink


class IIIFDiff:
    def __init__(self, library_iif, id_mapper):
        self.library_iif = library_iif
        self.id_mapper = id_mapper

    @staticmethod
    def _diff_modulo_hostname(deep_diff):
        for label, diff in list(deep_diff.get("values_changed", {}).items()):
            old_value = diff["old_value"]
            new_value = diff["new_value"]

            if new_value == old_value.replace(
                "https://wellcomelibrary.org/",
                "https://library-uat.wellcomelibrary.org/",
            ):
                del deep_diff["values_changed"][label]

    def _check_preservica_id_matches(self, old_id, new_id):
        return self.id_mapper.id_matches(old_id, new_id)

    def _diff_modulo_imageanno(self, deep_diff):
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

    # 'new_value': 'https://dlcs.io/iiif-img/wellcome/5/b18180000_pp_cri_j_1_5_18_1_0001.jp2/full/!1024,1024/0/default.jpg'
    # 'old_value': 'https://dlcs.io/iiif-img/wellcome/1/12131081-5006-4ccf-a2b4-243718b19e26/full/!1024,1024/0/default.jpg'

    def _diff_modulo_dlcs(self, deep_diff):
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

    def diff(self, bnum):
        manifest_new = self.library_iif.stage(bnum)
        manifest_old = self.library_iif.prod(bnum)

        deep_diff = DeepDiff(manifest_old, manifest_new)

        self._diff_modulo_hostname(deep_diff)
        self._diff_modulo_imageanno(deep_diff)
        self._diff_modulo_dlcs(deep_diff)

        if not deep_diff.get("values_changed", True):
            del deep_diff["values_changed"]

        return deep_diff
