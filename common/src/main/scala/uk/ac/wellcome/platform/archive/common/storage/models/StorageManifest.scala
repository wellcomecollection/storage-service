package uk.ac.wellcome.platform.archive.common.storage.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{Bag, BagId, BagInfo, BagManifest}
import uk.ac.wellcome.platform.archive.common.ingests.models.{InfrequentAccessStorageProvider, StorageLocation}
import uk.ac.wellcome.storage.ObjectLocation

case class StorageManifest(
                            space: StorageSpace,
                            info: BagInfo,
                            manifest: BagManifest,
                            tagManifest: BagManifest,
                            locations: List[StorageLocation],
                            createdDate: Instant
) {
  val id = BagId(space, info.externalIdentifier)
}

object StorageManifest {
  def create(
              root: ObjectLocation,
              space: StorageSpace,
              bag: Bag,
              locations: List[StorageLocation]
            ): StorageManifest = {

    StorageManifest(
      space = space,
      info = bag.info,
      manifest = bag.manifest,
      tagManifest = bag.tagManifest,
      List(
        StorageLocation(
          provider = InfrequentAccessStorageProvider,
          location = root
        )
      ),
      createdDate = Instant.now()
    )
  }

}