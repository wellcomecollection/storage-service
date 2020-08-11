package uk.ac.wellcome.platform.archive.common.bagit.services.azure

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.services.{
  BagReader,
  BagReaderTestCases
}
import uk.ac.wellcome.platform.archive.common.fixtures.azure.AzureBagBuilder
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.azure.AzureTypedStore

class AzureBagReaderTest
    extends BagReaderTestCases[
      Unit,
      Container,
      AzureBlobLocation,
      AzureBlobLocationPrefix
    ]
    with AzureBagBuilder {
  override def asLocation(
    root: AzureBlobLocationPrefix,
    path: String
  ): AzureBlobLocation = root.asLocation(path)

  override def withContext[R](testWith: TestWith[Unit, R]): R = testWith(())

  override def withTypedStore[R](
    testWith: TestWith[TypedStore[AzureBlobLocation, String], R]
  )(implicit context: Unit): R =
    testWith(AzureTypedStore[String])

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
