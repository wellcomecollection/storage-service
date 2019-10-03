from pprint import pprint

class ManifestSync:
    def __init__(self, store, iiif_diff, storage_client, dds_client, s3_client, thread_pool, space_id = 'digitised'):
        self.store = store
        self.iiif_diff = iiif_diff
        self.storage_client = storage_client
        self.dds_client = dds_client
        self.s3_client = s3_client
        self.thread_pool = thread_pool
        self.space_id = space_id

    def _ingest(self, bnumber):
        requested = self.dds_client.ingest(bnumber)
        return requested == "requested"

    def _check_manifest_values(self, differences):
        if(len(differences['values']) > 0):
            return differences['values']
        else:
            return []

    def _match_images(self, image_summary, bnumber):
        def _last_segment(path):
            return path.split('/')[-1]

        def _files_from_storage_service(storage_client, bnumber):
            ss_manifest = self.storage_client.get_bag(self.space_id, bnumber)
            files = ss_manifest['manifest']['files']

            # TODO: Need to got size from manifest to compare to stored old & new
            
            return {
                _last_segment(f['path']): f['path'] for f in files if f['name'].startswith('data/objects/')
            }

        def _find_key(path, keyed):
            found = None

            if(path in keyed):
                found = keyed[path]

            if(f"{bnumber}_{path}" in keyed):
                found = keyed[f"{bnumber}_{path}"]

            return found

        keyed = { i['new']:i['old'] for i in image_summary }
        ss_files = _files_from_storage_service(self.storage_client, bnumber)
                
        matched = { path:_find_key(i, keyed) for i,path in ss_files.items() if _find_key(i, keyed) }

        assert len(image_summary) == len(matched), "File list length mismatch"
        
        return matched

    def _get_images_summary(self, differences):
        images_summary = []

        for image in differences['images']:
            if 'https://dlcs.io/iiif-img' in image['new_value'] and not image['new_value'].endswith('default.jpg'):
                old_image_name = image['new_value'].split('/')[-1]
                old_image_id = old_image_name.split('.')[0]

                new_image_id = image['old_value'].split('/')[-1]

                image_summary = {
                    'old': old_image_id,
                    'new': new_image_id
                }

                images_summary.append(image_summary)

        return images_summary

    def _compare_asset_length(self, images_summary, bnumber):
        
        storage_space_id = 'digitised'
        storage_bucket = 'wellcomecollection-storage'
        preservica_bucket = 'wdl-preservica'

        differences = []

        def build_storage_service_key(space, bnumber, path):
            return f"{storage_space_id}/{bnumber}/{path}"

        def get_content_length(bucket, key):
            response = self.s3_client.head_object(Bucket=bucket,Key=key)
            return response['ContentLength']

        def get_storage_service_object_content_length(bnumber, path):
            path = f"{storage_space_id}/{bnumber}/{path}"
            return get_content_length(storage_bucket, path)

        def get_preservica_object_content_length(preservica_uuid):
            return get_content_length(preservica_bucket, preservica_uuid)

        for new_id, old_id in images_summary.items():
            
            def _get_storage_service_object_content_length(ident):
                return get_storage_service_object_content_length(bnumber, ident)
            
            def _get_preservica_object_content_length(ident):
                return get_preservica_object_content_length(ident)
            
            def _try(f, ident):
                try:
                    return f(ident)
                except Exception as e:
                    raise Exception(f"ERROR: {e} looking up {bnumber}:{ident}")
            
            new_content_length = _try(_get_storage_service_object_content_length, new_id)
            old_content_length = _try(_get_preservica_object_content_length, old_id)
                
            size_match = old_content_length == new_content_length

            if(not size_match):
                differences.append({
                    'old': {
                        'id': old_id,
                        'size': old_cl
                    },
                    'new': {
                        'id': new_id,
                        'size': new_cl
                    }
                })

        return differences

    def diff_summary(self, bnumber):
        matched_images = None
        values_differences = None
        
        try:
            differences = self.iiif_diff.diff(bnumber)

            values_differences = self._check_manifest_values(differences)

            images_summary = self._get_images_summary(differences)
            matched_images = self._match_images(images_summary, bnumber)
            images_differences = self._compare_asset_length(matched_images, bnumber)

            return {
                'values_mismatch': values_differences,
                'images_mismatch': images_differences
            }

        except Exception as e:

            return {
                'failed': {
                    'error': str(e),
                    'values_mismatch': values_differences,
                    'matched_images': matched_images
                }
            }

    def _generate_summaries(self, bnumber_batch):
        summaries = {}

        for status in bnumber_batch:
            bnumber = status['bnumber']
            summaries[bnumber] = self.diff_summary(bnumber)

        return summaries

    def _batch_summary(self, status='finished', batch_size = 10):
        for bnumber_batch in self.store.get_status(status, batch_size):
            yield self._generate_summaries(bnumber_batch)

    def diff_summary_all_finished(self, status='finished'):
        finished_count = self.store.count_status(status)

        batch_size = 3

        total_mismatch = 0
        total_seen = 0

        def _has_mismatch(item):
            values_mismatch = bool(item.get('values_mismatch', []))
            images_mismatch = bool(item.get('images_mismatch', []))
            failed_mismatch = 'failed' in item

            return values_mismatch or images_mismatch or failed_mismatch

        for batch_summary in self._batch_summary(status, batch_size):

            mismatched = {
                bnumber:item for bnumber, item in batch_summary.items() if _has_mismatch(item)
            }

            mismatch_count = len(mismatched)

            for bnumber, item in mismatched.items():
                print(f"Found mismatch for {bnumber}!")
                pprint(item)

            total_mismatch = total_mismatch + mismatch_count
            total_seen = total_seen + batch_size

            print(f"Mismatched {mismatch_count} ({total_mismatch}/{finished_count})")

            print(f"Seen: {total_seen}")

    def retry_mismatched_manifests(self, status='finished'):
        finished_count = self.store.count_status(status)

        batch_size = 10

        total_ingested = 0
        total_mismatch = 0
        total_seen = 0

        def _has_mismatch(item):
            if('values_mismatch' in item):
                return bool(item['values_mismatch'])
            else:
                return False

        for batch_summary in self._batch_summary(status, batch_size):
            ingested_count = 0

            bnumbers = [
                bnumber for bnumber, item in batch_summary.items() if _has_mismatch(item)
            ]

            total_seen = total_seen + batch_size
            mismatch_count = len(bnumbers)
            total_mismatch = total_mismatch + mismatch_count

            print(f"Found {mismatch_count} mismatched manifests, total: {total_mismatch}")

            ingest_results = self.thread_pool.map(self._ingest, bnumbers)
            ingested_count = len([result for result in ingest_results if result])
            total_ingested = total_ingested + ingested_count

            print(f"Ingested {ingested_count} ({total_ingested}/{finished_count})")

            print(f"Seen: {total_seen}")
