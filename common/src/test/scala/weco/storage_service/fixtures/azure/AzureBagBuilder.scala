package weco.storage_service.fixtures.azure

import weco.storage_service.bagit.models.{BagVersion, ExternalIdentifier}
import weco.storage_service.fixtures.BagBuilder
import weco.storage_service.storage.models.StorageSpace
import weco.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import weco.storage.fixtures.AzureFixtures
import weco.storage.fixtures.AzureFixtures.Container
import weco.storage.store.TypedStore
import weco.storage.store.azure.AzureTypedStore

trait AzureBagBuilder
    extends BagBuilder[AzureBlobLocation, AzureBlobLocationPrefix, Container]
    with AzureFixtures {
  implicit val typedStore: TypedStore[AzureBlobLocation, String] =
    AzureTypedStore[String]

  override def createBagRoot(
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  )(container: Container): AzureBlobLocationPrefix =
    AzureBlobLocationPrefix(
      container = container.name,
      namePrefix = createBagRootPath(space, externalIdentifier, version)
    )

  override def createBagLocation(
    bagRoot: AzureBlobLocationPrefix,
    path: String
  ): AzureBlobLocation = AzureBlobLocation(bagRoot.container, path)
}
