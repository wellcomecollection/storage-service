package uk.ac.wellcome.platform.archive.common.storage.services.dynamo

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import org.scanamo.auto._
import org.scanamo.{Table => ScanamoTable}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.storage.services.{
  StorageManifestDao,
  StorageManifestDaoTestCases
}
import uk.ac.wellcome.platform.archive.common.versioning.dynamo.DynamoID
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

          val storedIdentifiers = scanamo
            .exec(ScanamoTable[Identified](table.name).scan())
            .map { _.right.value }
            .map { _.id }

          storedIdentifiers shouldBe Seq("abc:123")
        }
      }
    }
  }

  describe("it handles errors from AWS when looking up versions") {
    it("if the table rows have the wrong structure") {
      case class BadRow(id: String, version: Int, data: String)

      val bagId = createBagId

      withLocalDynamoDbTable { table =>
        scanamo.exec(
          ScanamoTable[BadRow](table.name).put(
            BadRow(
              id = DynamoID.createId(bagId.space, bagId.externalIdentifier),
              version = randomInt(0, 100),
              data = randomAlphanumeric
            )
          ))

        withLocalS3Bucket { bucket =>
          implicit val context: (Table, Bucket) = (table, bucket)

          withDao { dao =>
            val err = dao.listVersions(bagId).left.value

            err.e.getMessage should startWith("Errors querying DynamoDB")
          }
        }
      }
    }

    it("if the Dynamo row points to a dangling S3 pointer") {
      val storageManifest = createStorageManifest

      withLocalDynamoDbTable { table =>
        withLocalS3Bucket { bucket =>
          implicit val context: (Table, Bucket) = (table, bucket)

          withDao { dao =>
            dao.put(storageManifest)

            listKeysInBucket(bucket).foreach {
              s3Client.deleteObject(bucket.name, _)
            }

            val err = dao.listVersions(storageManifest.id).left.value

            err.e.getMessage should startWith(
              "Errors fetching S3 objects for manifests")
          }
        }
      }
    }

    it("if the S3 objects have the wrong format") {
      val storageManifest = createStorageManifest

      withLocalDynamoDbTable { table =>
        withLocalS3Bucket { bucket =>
          implicit val context: (Table, Bucket) = (table, bucket)

          withDao { dao =>
            dao.put(storageManifest)

            listKeysInBucket(bucket).foreach {
              s3Client.putObject(bucket.name, _, randomAlphanumeric)
            }

            val err = dao.listVersions(storageManifest.id).left.value

            err.e.getMessage should startWith(
              "Errors fetching S3 objects for manifests")
          }
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
