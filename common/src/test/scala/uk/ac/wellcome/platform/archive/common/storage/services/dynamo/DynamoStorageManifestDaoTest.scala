package uk.ac.wellcome.platform.archive.common.storage.services.dynamo

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import org.scanamo.auto._
import org.scanamo.{Table => ScanamoTable}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.storage.services.{
  StorageManifestDao, StorageManifestDaoTestCases}
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.{DynamoFixtures, S3Fixtures}
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket

class DynamoStorageManifestDaoTest
  extends StorageManifestDaoTestCases[(Table, Bucket)]
    with DynamoFixtures
    with S3Fixtures {
  override def withContext[R](testWith: TestWith[(Table, Bucket), R]): R =
    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        testWith((table, bucket))
      }
    }

  override def withDao[R](testWith: TestWith[StorageManifestDao, R])(
    implicit context: (Table, Bucket)): R = {
    val (table, bucket) = context

    testWith(
      new DynamoStorageManifestDao(
        dynamoConfig = createDynamoConfigWith(table),
        s3Config = createS3ConfigWith(bucket)
      )
    )
  }

  it("encodes the bag ID as a string to use as a table key") {
    val storageManifest = createStorageManifestWith(
      space = StorageSpace("abc"),
      bagInfo = createBagInfoWith(
        externalIdentifier = ExternalIdentifier("123")
      )
    )

    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        implicit val context: (Table, Bucket) = (table, bucket)

        withDao { dao =>
          dao.put(storageManifest) shouldBe a[Right[_, _]]

          case class Identified(id: String)

          val storedIdentifiers = scanamo.exec(ScanamoTable[Identified](table.name).scan())
            .map { _.right.value }
            .map { _.id }

          storedIdentifiers shouldBe Seq("abc:123")
        }
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
