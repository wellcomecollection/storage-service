
class ManifestSync:
    def __init__(self, store, iiif_diff):
        self.store = store
        self.iiif_diff = iiif_diff

    def get_manifests(self):
        finished_batches = self.store.get_status('finished')

        for batch in finished_batches:
            for status in batch:
                bnumber = status['bnumber']
                differences = self.iiif_diff.diff(bnumber)

                if(len(differences['values']) > 0):
                    print(f"NOT OK: {bnumber}")
                else:
                    print(f"OK: {bnumber}")

