package uk.ac.wellcome.platform.archive.common.fixtures.azure

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagVersion, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.fixtures.BagBuilder
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.fixtures.AzureFixtures
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.azure.AzureTypedStore

trait AzureBagBuilder
    extends BagBuilder[AzureBlobLocation, AzureBlobLocationPrefix, Container]
    with AzureFixtures {
  implicit val typedStore: TypedStore[AzureBlobLocation, String] = AzureTypedStore[String]

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
