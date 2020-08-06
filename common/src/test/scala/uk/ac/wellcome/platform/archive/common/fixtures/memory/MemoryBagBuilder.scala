package uk.ac.wellcome.platform.archive.common.fixtures.memory

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagVersion, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.fixtures.BagBuilder
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.storage.services.DestinationBuilder
import uk.ac.wellcome.storage.providers.memory.{MemoryLocation, MemoryLocationPrefix}

trait MemoryBagBuilder
    extends BagBuilder[MemoryLocation, MemoryLocationPrefix, String] {

  override def createBagRoot(
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  )(
    implicit namespace: String
  ): MemoryLocationPrefix =
    MemoryLocationPrefix(
      namespace = namespace,
      path = DestinationBuilder.buildPath(space, externalIdentifier, version)
    )

  override def createBagLocation(
    bagRoot: MemoryLocationPrefix,
    path: String
  ): MemoryLocation =
    MemoryLocation(
      namespace = bagRoot.namespace,
      path = path
    )
}
