package uk.ac.wellcome.platform.archive.common.storage.services.azure

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.storage.services.{
  SizeFinder,
  SizeFinderTestCases
}
import uk.ac.wellcome.storage.azure.AzureBlobLocation
import uk.ac.wellcome.storage.fixtures.AzureFixtures
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container
import uk.ac.wellcome.storage.store.azure.AzureTypedStore

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
