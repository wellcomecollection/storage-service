package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.dynamodbv2.model.{ScalarAttributeType, ScanRequest}
import org.scalatest.FunSpec
import uk.ac.wellcome.platform.archive.common.config.builders.StorageManifestDaoBuilder
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.S3Fixtures

class StorageManifestDynamoDaoTest extends FunSpec with DynamoFixtures with S3Fixtures with StorageManifestGenerators {
  it("works") {
    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        val vhs = StorageManifestDaoBuilder.buildVHS(
          createDynamoConfigWith(table),
          createS3ConfigWith(bucket)
        )

        val register = new StorageManifestDao(vhs)

        val manifest = createStorageManifest



        register.put(manifest) shouldBe a[Right[_, _]]

        println(dynamoClient.scan(
          new ScanRequest()
            .withTableName(table.name)
        ))

        println(manifest.id.toString)
        println(vhs.store.max(manifest.id.toString))

        register.get(manifest.id, manifest.version) shouldBe Right(manifest)

        println(dynamoClient.scan(
          new ScanRequest()
            .withTableName(table.name)
        ))
      }
    }
  }

  override def createTable(table: Table): Table =
    createTableWithHashRangeKey(
      table,
      hashKeyName = "id",
      hashKeyType = ScalarAttributeType.S,
      rangeKeyName = "version",
      rangeKeyType = ScalarAttributeType.N
    )
}