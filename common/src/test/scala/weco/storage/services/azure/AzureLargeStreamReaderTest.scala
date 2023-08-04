package weco.storage.services.azure

import weco.fixtures.TestWith
import weco.storage.azure.AzureBlobLocation
import weco.storage.fixtures.AzureFixtures
import weco.storage.fixtures.AzureFixtures.Container
import weco.storage.services.{
  LargeStreamReader,
  LargeStreamReaderTestCases,
  RangedReader
}
import weco.storage.store.azure.AzureTypedStore

class AzureLargeStreamReaderTest
    extends LargeStreamReaderTestCases[AzureBlobLocation, Container]
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

  override def withLargeStreamReader[R](
    bufferSize: Long,
    rangedReader: RangedReader[AzureBlobLocation]
  )(testWith: TestWith[LargeStreamReader[AzureBlobLocation], R]): R =
    testWith(
      new AzureLargeStreamReader(
        bufferSize = bufferSize,
        new AzureSizeFinder(),
        rangedReader
      )
    )
}
