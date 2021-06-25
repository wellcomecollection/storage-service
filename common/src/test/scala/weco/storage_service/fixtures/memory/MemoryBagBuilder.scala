package weco.storage_service.fixtures.memory

import weco.storage_service.bagit.models.{BagVersion, ExternalIdentifier}
import weco.storage_service.fixtures.BagBuilder
import weco.storage_service.storage.models.StorageSpace
import weco.storage_service.storage.services.DestinationBuilder
import weco.storage.providers.memory.{MemoryLocation, MemoryLocationPrefix}
import weco.storage.store.TypedStore
import weco.storage.store.memory.MemoryTypedStore

trait MemoryBagBuilder
    extends BagBuilder[MemoryLocation, MemoryLocationPrefix, String] {

  implicit val typedStore: TypedStore[MemoryLocation, String] =
    MemoryTypedStore[MemoryLocation, String]()

  override def createBagRoot(
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  )(
    namespace: String
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
