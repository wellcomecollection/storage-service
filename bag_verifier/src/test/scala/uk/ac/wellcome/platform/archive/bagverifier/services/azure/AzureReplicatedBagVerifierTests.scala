package uk.ac.wellcome.platform.archive.bagverifier.services.azure

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityListChecker
import uk.ac.wellcome.platform.archive.bagverifier.fixity.azure.{AzureDynamoTags, AzureFixityChecker}
import uk.ac.wellcome.platform.archive.bagverifier.fixity.s3.S3FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.models.VerificationIncompleteSummary
import uk.ac.wellcome.platform.archive.bagverifier.services.{ReplicatedBagVerifier, ReplicatedBagVerifierTestCases}
import uk.ac.wellcome.platform.archive.bagverifier.storage.azure.{AzureLocatable, AzureResolvable}
import uk.ac.wellcome.platform.archive.common.bagit.models.Bag
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.azure.AzureBagReader
import uk.ac.wellcome.platform.archive.common.fixtures.BagBuilder
import uk.ac.wellcome.platform.archive.common.fixtures.azure.AzureBagBuilder
import uk.ac.wellcome.platform.archive.common.storage.models.{EnsureTrailingSlash, IngestShouldRetry}
import uk.ac.wellcome.platform.archive.common.storage.services.azure.AzureSizeFinder
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.fixtures.{AzureFixtures, DynamoFixtures, S3Fixtures}
import uk.ac.wellcome.storage.listing.azure.AzureBlobLocationListing
import uk.ac.wellcome.storage.store.azure.AzureTypedStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.store.{Readable, TypedStore}
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class AzureReplicatedBagVerifierTests
    extends ReplicatedBagVerifierTestCases[
      AzureBlobLocation,
      AzureBlobLocationPrefix,
      Container
    ]
    with DynamoFixtures
    with S3Fixtures
    with AzureFixtures with MockitoSugar{

  override implicit val et: EnsureTrailingSlash[AzureBlobLocationPrefix] =
    EnsureTrailingSlash.azurePrefixTrailingSlash
  val azureTypedStore: AzureTypedStore[String] = AzureTypedStore[String]
  override val bagBuilder
    : BagBuilder[AzureBlobLocation, AzureBlobLocationPrefix, Container] =
    new AzureBagBuilder {}

  override def withTypedStore[R](
    testWith: TestWith[TypedStore[AzureBlobLocation, String], R]
  ): R = testWith(azureTypedStore)

  override def withVerifier[R](primaryBucket: Bucket)(
    testWith: TestWith[
      ReplicatedBagVerifier[AzureBlobLocation, AzureBlobLocationPrefix],
      R
    ]
  ): R =
    withLocalDynamoDbTable { table =>
      testWith(
        AzureReplicatedBagVerifier(
          primaryBucket = primaryBucket.name,
          dynamoConfig = createDynamoConfigWith(table)
        )
      )
    }

  override def writeFile(location: AzureBlobLocation, contents: String): Unit =
    azureTypedStore.put(location)(contents)

  override def createBagReader
    : BagReader[AzureBlobLocation, AzureBlobLocationPrefix] = AzureBagReader()

  override def withNamespace[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def createId(implicit container: Container): AzureBlobLocation =
    createAzureBlobLocationWith(container)

  override def createTable(table: Table): Table =
    createTableWithHashKey(
      table,
      keyName = "id",
      keyType = ScalarAttributeType.S
    )

  it("marks as retriable a TimeoutException when reading from Azure"){
    val space = createStorageSpace
    val externalIdentifier = createExternalIdentifier
    withNamespace { namespace =>
      withBag(space, externalIdentifier)(namespace) {
        case (primaryBucket, bagRoot) =>
            withBagContext(bagRoot) { bagContext =>
              withLocalDynamoDbTable { table =>
                val dynamoConfig = createDynamoConfigWith(table)
                val failingBagReader = mock[Readable[AzureBlobLocation, InputStreamWithLength]]
              // This is a reproduction of an error we see occasionally in the logs:
              // "java.lang.RuntimeException: Unable to read range ClosedByteRange(7801405440,16777216)
              // from azure://containername/digitised/b16800904/v1/data/objects/0055-0000-3808-0000-0-0000-0000-0.mpg:
              // StoreReadError(reactor.core.Exceptions$ReactiveException: java.util.concurrent.TimeoutException:
              // Did not observe any item or terminal signal within 60000ms in 'map' (and no fallback has been configured))"
              when(failingBagReader.get(any[AzureBlobLocation])).thenThrow(new RuntimeException("java.util.concurrent.TimeoutException"))
              implicit val fixityChecker = new AzureFixityChecker(failingBagReader, new AzureSizeFinder(), new AzureDynamoTags(dynamoConfig), new AzureLocatable)
              implicit val fetchDirectoryFixityChecker = S3FixityChecker()
                val srcReader = new S3StreamStore()
                val verifier = new AzureReplicatedBagVerifier(primaryBucket.name, AzureBagReader(), AzureBlobLocationListing(), new AzureResolvable(), new FixityListChecker[AzureBlobLocation, AzureBlobLocationPrefix, Bag](), srcReader)
              val ingestStep = verifier.verify(
                ingestId = createIngestID,
                bagContext = bagContext,
                space = space,
                externalIdentifier = externalIdentifier
              )

              val result = ingestStep.success.get

              result shouldBe a[IngestShouldRetry[_]]
              result.summary shouldBe a[VerificationIncompleteSummary]

              }
  }}}}
}
