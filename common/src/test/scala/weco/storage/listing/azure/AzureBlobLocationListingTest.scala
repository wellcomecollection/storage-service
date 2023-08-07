package weco.storage.listing.azure

import org.scalatest.Assertion
import weco.fixtures.TestWith
import weco.storage.fixtures.AzureFixtures
import weco.storage.fixtures.AzureFixtures.Container
import weco.storage.generators.StreamGenerators
import weco.storage.listing.ListingTestCases
import weco.storage.providers.azure.{AzureBlobLocation, AzureBlobLocationPrefix}

class AzureBlobLocationListingTest
    extends ListingTestCases[
      AzureBlobLocation,
      AzureBlobLocationPrefix,
      AzureBlobLocation,
      AzureBlobLocationListing,
      Container]
    with AzureFixtures
    with StreamGenerators {

  override def createIdent(implicit container: Container): AzureBlobLocation =
    createAzureBlobLocationWith(container)

  override def extendIdent(location: AzureBlobLocation,
                           extension: String): AzureBlobLocation =
    location.join(extension)

  override def createPrefix: AzureBlobLocationPrefix =
    createAzureBlobLocationPrefix

  override def createPrefixMatching(
    location: AzureBlobLocation): AzureBlobLocationPrefix =
    location.asPrefix

  override def assertResultCorrect(result: Iterable[AzureBlobLocation],
                                   entries: Seq[AzureBlobLocation])(implicit container: Container): Assertion =
    result.toSeq should contain theSameElementsAs entries

  override def withListingContext[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def withListing[R](container: Container,
                              initialEntries: Seq[AzureBlobLocation])(
    testWith: TestWith[AzureBlobLocationListing, R]): R = {
    initialEntries.foreach { location =>
      azureClient
        .getBlobContainerClient(location.container)
        .getBlobClient(location.name)
        .upload(createInputStream(length = 20), 20)
    }

    testWith(
      AzureBlobLocationListing()
    )
  }
}
