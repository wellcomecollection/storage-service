package weco.storage.services.azure

import weco.fixtures.TestWith
import weco.storage.azure.AzureBlobLocation
import weco.storage.fixtures.AzureFixtures
import weco.storage.fixtures.AzureFixtures.Container
import weco.storage.services.{SizeFinder, SizeFinderTestCases}
import weco.storage.store.azure.AzureTypedStore

class AzureSizeFinderTest
    extends SizeFinderTestCases[AzureBlobLocation, Container]
    with AzureFixtures {
  override def withContext[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def withSizeFinder[R](
    testWith: TestWith[SizeFinder[AzureBlobLocation], R]
  )(implicit context: Container): R = testWith(new AzureSizeFinder())

  override def createIdent(implicit container: Container): AzureBlobLocation =
    createAzureBlobLocationWith(container)

  override def createObject(location: AzureBlobLocation, contents: String)(
    implicit context: Container
  ): Unit = AzureTypedStore[String].put(location)(contents)
}
