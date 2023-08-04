package weco.storage.services.azure

import weco.fixtures.TestWith
import weco.storage.azure.AzureBlobLocation
import weco.storage.fixtures.AzureFixtures
import weco.storage.fixtures.AzureFixtures.Container
import weco.storage.services.{RangedReader, RangedReaderTestCases}
import weco.storage.store.azure.AzureTypedStore

class AzureRangedReaderTest
    extends RangedReaderTestCases[AzureBlobLocation, Container]
    with AzureFixtures {
  override def withNamespace[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def createIdentWith(container: Container): AzureBlobLocation =
    createAzureBlobLocationWith(container)

  override def writeString(
    location: AzureBlobLocation,
    contents: String
  ): Unit =
    AzureTypedStore[String].put(location)(contents).value

  override def withRangedReader[R](
    testWith: TestWith[RangedReader[AzureBlobLocation], R]
  ): R =
    testWith(new AzureRangedReader())
}
