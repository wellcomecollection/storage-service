package uk.ac.wellcome.platform.archive.bagverifier.services.azure

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.services.{ReplicatedBagVerifier, ReplicatedBagVerifierTestCases}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.azure.AzureBagReader
import uk.ac.wellcome.platform.archive.common.fixtures.BagBuilder
import uk.ac.wellcome.platform.archive.common.fixtures.azure.AzureBagBuilder
import uk.ac.wellcome.platform.archive.common.storage.models.EnsureTrailingSlash
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.fixtures.{AzureFixtures, DynamoFixtures, S3Fixtures}
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.azure.AzureTypedStore
import uk.ac.wellcome.storage.streaming.Codec._

class AzureReplicatedBagVerifierTests
    extends ReplicatedBagVerifierTestCases[
      AzureBlobLocation,
      AzureBlobLocationPrefix,
      Container
    ]
    with DynamoFixtures
    with S3Fixtures
    with AzureFixtures {

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
}
