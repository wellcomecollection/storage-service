package uk.ac.wellcome.platform.archive.bagverifier.fixity.azure

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.azure.AzureBlobLocation
import uk.ac.wellcome.storage.fixtures.{AzureFixtures, DynamoFixtures}
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.store.azure.AzureTypedStore
import uk.ac.wellcome.storage.tags.{Tags, TagsTestCases}

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
          AzureTypedStore[String].put(location)(randomAlphanumeric) shouldBe a[
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
