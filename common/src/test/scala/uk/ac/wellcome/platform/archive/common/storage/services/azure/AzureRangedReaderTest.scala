package uk.ac.wellcome.platform.archive.common.storage.services.azure

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.storage.services.{RangedReader, RangedReaderTestCases}
import uk.ac.wellcome.storage.azure.AzureBlobLocation
import uk.ac.wellcome.storage.fixtures.AzureFixtures
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container
import uk.ac.wellcome.storage.store.azure.AzureTypedStore

class AzureRangedReaderTest extends RangedReaderTestCases[AzureBlobLocation, Container] with AzureFixtures {
  override def withNamespace[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def createIdentWith(container: Container): AzureBlobLocation =
    createAzureBlobLocationWith(container)

  override def writeString(location: AzureBlobLocation, contents: String): Unit =
    AzureTypedStore[String].put(location)(contents).right.value

  override def withRangedReader[R](testWith: TestWith[RangedReader[AzureBlobLocation], R]): R =
    testWith(new AzureRangedReader())
}
