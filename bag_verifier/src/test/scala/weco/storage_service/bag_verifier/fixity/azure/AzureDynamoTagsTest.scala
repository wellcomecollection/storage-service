package weco.storage_service.bag_verifier.fixity.azure

import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import weco.fixtures.TestWith
import weco.storage.azure.AzureBlobLocation
import weco.storage.fixtures.{AzureFixtures, DynamoFixtures}
import weco.storage.fixtures.AzureFixtures.Container
import weco.storage.fixtures.DynamoFixtures.Table
import weco.storage.store.azure.AzureTypedStore
import weco.storage.tags.{Tags, TagsTestCases}

class AzureDynamoTagsTest
    extends TagsTestCases[AzureBlobLocation, Container]
    with AzureFixtures
    with DynamoFixtures {
  override def withTags[R](
    initialTags: Map[AzureBlobLocation, Map[String, String]]
  )(
    testWith: TestWith[Tags[AzureBlobLocation], R]
  ): R =
    withLocalDynamoDbTable { table =>
      val azureTags = new AzureDynamoTags(createDynamoConfigWith(table))

      initialTags.foreach {
        case (location, tags) =>
          AzureTypedStore[String].put(location)(randomAlphanumeric()) shouldBe a[
            Right[_, _]
          ]
          azureTags.update(location) { _ =>
            Right(tags)
          } shouldBe a[Right[_, _]]
      }

      testWith(azureTags)
    }

  override def createIdent(container: Container): AzureBlobLocation =
    createAzureBlobLocationWith(container)

  override def withContext[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def createTable(table: Table): Table =
    createTableWithHashKey(
      table,
      keyName = "id",
      keyType = ScalarAttributeType.S
    )
}
