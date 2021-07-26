package weco.storage_service.bag_verifier.services.azure

import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import weco.fixtures.TestWith
import weco.storage_service.bag_verifier.services.{
  ReplicatedBagVerifier,
  ReplicatedBagVerifierTestCases
}
import weco.storage_service.bagit.services.BagReader
import weco.storage_service.bagit.services.azure.AzureBagReader
import weco.storage_service.fixtures.BagBuilder
import weco.storage_service.fixtures.azure.AzureBagBuilder
import weco.storage_service.storage.models.EnsureTrailingSlash
import weco.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import weco.storage.fixtures.AzureFixtures.Container
import weco.storage.fixtures.DynamoFixtures.Table
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.fixtures.{AzureFixtures, DynamoFixtures, S3Fixtures}
import weco.storage.store.TypedStore
import weco.storage.store.azure.AzureTypedStore
import weco.storage.streaming.Codec._

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
    : BagReader[AzureBlobLocation, AzureBlobLocationPrefix] =
    new AzureBagReader()

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
