import os


class Matcher:
    def __init__(self, iiif_diff, storage_client, space='digitised'):
        self.iiif_diff = iiif_diff
        self.storage_client = storage_client
        self.space = space

    def match(self, bnumber):
        self.iiif_diff.fetch_and_diff(bnumber)

        storage_manifest = self.storage_client.get_bag(self.space, bnumber)

        results = self.match_preservica_and_storage_service_files(
            self.iiif_diff.id_mapper,
            storage_manifest
        )

        return results

    @staticmethod
    def match_preservica_and_storage_service_files(
            id_mapper, storage_manifest):
        """
        This function must:
            - Check that every asset described in the old iiif-manifest
              has an entry in the manifest from the Storage Service.
            - Check that every entry in the manifest from the Storage Service
              except the METS file is listed in the iiif-manifest.
        """

        bnumber = storage_manifest['info']['externalIdentifier']
        space = storage_manifest['space']['id']
        storage_manifest_files = storage_manifest['manifest']['files']

        results = {
            'bnumber': bnumber,
            'space': space,
            'files': []
        }

        for entry in storage_manifest_files:
            # Ignore the METS file
            if entry['name'] == f"data/{bnumber}.xml":
                continue

            filename = os.path.basename(entry['name'])

            matching = {
                new_id: old_id
                for new_id, old_id in id_mapper.new_to_old_map.items()
                if new_id.endswith(filename)
            }

            if len(matching) != 1:
                raise ValueError(
                    f"No exact match for {bnumber}/{filename}: {matching}")

            results['files'].append({
                'preservica_guid': list(matching.values())[0],
                'storage_manifest_entry': entry
            })

        known_guids = {f['preservica_guid'] for f in results['files']}

        for old_id, new_id in id_mapper.old_to_new_map.items():
            if old_id not in known_guids:
                raise ValueError(
                    f"No storage manifest file for preservia guid {old_id}:{new_id}")

        return results
