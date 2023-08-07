package weco.storage_service.bagit.services.azure

import weco.fixtures.TestWith
import weco.storage_service.bagit.services.{BagReader, BagReaderTestCases}
import weco.storage_service.fixtures.azure.AzureBagBuilder
import weco.storage.providers.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import weco.storage.fixtures.AzureFixtures.Container
import weco.storage.store.TypedStore

class AzureBagReaderTest
    extends BagReaderTestCases[
      Unit,
      Container,
      AzureBlobLocation,
      AzureBlobLocationPrefix
    ]
    with AzureBagBuilder {
  override def withContext[R](testWith: TestWith[Unit, R]): R = testWith(())

  override def withTypedStore[R](
    testWith: TestWith[TypedStore[AzureBlobLocation, String], R]
  )(implicit context: Unit): R =
    testWith(typedStore)

  override def withBagReader[R](
    testWith: TestWith[BagReader[AzureBlobLocation, AzureBlobLocationPrefix], R]
  )(implicit context: Unit): R = testWith(new AzureBagReader())

  override def withNamespace[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def deleteFile(root: AzureBlobLocationPrefix, path: String)(
    implicit context: Unit
  ): Unit = {
    azureClient
      .getBlobContainerClient(root.container)
      .getBlobClient(s"${root.namePrefix}/$path")
      .delete()
  }
}
