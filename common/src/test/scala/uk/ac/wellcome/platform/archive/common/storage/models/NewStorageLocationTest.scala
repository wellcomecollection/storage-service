package uk.ac.wellcome.platform.archive.common.storage.models

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.S3ObjectLocationPrefix
import uk.ac.wellcome.storage.fixtures.NewS3Fixtures

class NewStorageLocationTest extends AnyFunSpec with Matchers with NewS3Fixtures {
  it("decodes an old-style PrimaryStorageLocation") {
    val jsonString =
      """
        |{
        |    "provider": {
        |        "type": "AmazonS3StorageProvider"
        |    },
        |    "prefix": {
        |        "namespace": "wellcomecollection-storage",
        |        "path": "testing/test_bag"
        |    },
        |    "type": "PrimaryStorageLocation"
        |}
      """.stripMargin

    val location: PrimaryNewStorageLocation = fromJson[PrimaryNewStorageLocation](jsonString).get

    location shouldBe PrimaryS3StorageLocation(
      prefix = S3ObjectLocationPrefix(
        bucket = "wellcomecollection-storage",
        keyPrefix = "testing/test_bag"
      )
    )
  }

  it("decodes a new-style PrimaryStorageLocation") {
    val location = PrimaryS3StorageLocation(
      prefix = createS3ObjectLocationPrefix
    )

    val jsonString = toJson[PrimaryNewStorageLocation](location).get

    println(jsonString)

    fromJson[PrimaryNewStorageLocation](jsonString).get shouldBe location
  }

  it("decodes an old-style SecondaryStorageLocation") {
    val jsonString =
      """
        |{
        |    "provider": {
        |        "type": "AmazonS3StorageProvider"
        |    },
        |    "prefix": {
        |        "namespace": "wellcomecollection-storage-replica-ireland",
        |        "path": "testing/test_bag"
        |    },
        |    "type": "SecondaryStorageLocation"
        |}
      """.stripMargin

    val location: SecondaryNewStorageLocation = fromJson[SecondaryNewStorageLocation](jsonString).get

    location shouldBe SecondaryS3StorageLocation(
      prefix = S3ObjectLocationPrefix(
        bucket = "wellcomecollection-storage-replica-ireland",
        keyPrefix = "testing/test_bag"
      )
    )
  }

  it("decodes a new-style SecondaryStorageLocation") {
    val location = SecondaryS3StorageLocation(
      prefix = createS3ObjectLocationPrefix
    )

    val jsonString = toJson[SecondaryNewStorageLocation](location).get

    println(jsonString)

    fromJson[SecondaryNewStorageLocation](jsonString).get shouldBe location
  }
}
