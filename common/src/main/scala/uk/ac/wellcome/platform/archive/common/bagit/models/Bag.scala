package uk.ac.wellcome.platform.archive.common.bagit.models

case class Bag(
                info: BagInfo,
                manifest: BagManifest,
                tagManifest: BagManifest,
                fetch: Option[BagFetch] = None
              )
