package uk.ac.wellcome.platform.storage.bag_tagger.services

import org.scalatest.TryValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{AmazonS3StorageProvider, AzureBlobStorageProvider}
import uk.ac.wellcome.platform.archive.common.storage.models.PrimaryStorageLocation
import uk.ac.wellcome.storage.Identified
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.tags.s3.S3Tags

import scala.util.Success

class ApplyTagsTest
    extends AnyFunSpec
    with Matchers
    with TryValues
    with S3Fixtures
    with StorageManifestGenerators {
  val s3Tags = new S3Tags()
  val applyTags = new ApplyTags(s3Tags = s3Tags)

  describe("it updates tags") {
    it("to objects in S3") {
      withLocalS3Bucket { bucket =>
        val prefix = createObjectLocationPrefixWith(bucket.name)

        val file = createStorageManifestFileWith(
          pathPrefix = "space/externalIdentifier/v1",
          name = "b1234.mxf"
        )

        val location = prefix.asLocation(file.path)

        s3Client.putObject(
          location.namespace,
          location.path,
          "<MXF file contents>"
        )
        s3Tags.update(location) { _ =>
          Right(Map("Content-SHA256" -> "4a5a41ebcf5e2c24c"))
        }

        val result = applyTags.applyTags(
          storageLocations = Seq(
            PrimaryStorageLocation(
              provider = AmazonS3StorageProvider,
              prefix = prefix
            )
          ),
          tagsToApply = Map(file -> Map("Content-Type" -> "application/mxf"))
        )

        result shouldBe Success(())

        s3Tags.get(location).right.value shouldBe Identified(location, Map(
          "Content-SHA256" -> "4a5a41ebcf5e2c24c",
          "Content-Type" -> "application/mxf"
        ))
      }
    }
  }

  describe("it returns an error") {
    it("if asked to tag a non-existent object") {
      val prefix = createObjectLocationPrefixWith(createBucketName)

      val file = createStorageManifestFile

      val result = applyTags.applyTags(
        storageLocations = Seq(
          PrimaryStorageLocation(
            provider = AmazonS3StorageProvider,
            prefix = prefix
          )
        ),
        tagsToApply = Map(file -> Map("Content-Type" -> "application/mxf"))
      )

      val err = result.failed.get
      err shouldBe a[Throwable]
      err.getMessage shouldBe "Could not successfully apply tags!"
    }

    it("if asked to tag objects in Azure") {
      val prefix = createObjectLocationPrefixWith(createBucketName)

      val file = createStorageManifestFile

      val result = applyTags.applyTags(
        storageLocations = Seq(
          PrimaryStorageLocation(
            provider = AzureBlobStorageProvider,
            prefix = prefix
          )
        ),
        tagsToApply = Map(file -> Map("Content-Type" -> "application/mxf"))
      )

      val err = result.failed.get
      err shouldBe a[IllegalArgumentException]
      err.getMessage shouldBe "Unsupported provider for tagging: AzureBlobStorageProvider"
    }

    it("if the objects have not been tagged by the verifier") {
      withLocalS3Bucket { bucket =>
        val prefix = createObjectLocationPrefixWith(bucket.name)

        val file = createStorageManifestFileWith(
          pathPrefix = "space/externalIdentifier/v1",
          name = "b1234.mxf"
        )

        val location = prefix.asLocation(file.path)

        s3Client.putObject(
          location.namespace,
          location.path,
          "<MXF file contents>"
        )

        val result = applyTags.applyTags(
          storageLocations = Seq(
            PrimaryStorageLocation(
              provider = AmazonS3StorageProvider,
              prefix = prefix
            )
          ),
          tagsToApply = Map(file -> Map("Content-Type" -> "application/mxf"))
        )

        val err = result.failed.get
        err shouldBe a[Throwable]
        err.getMessage should startWith("No Content-SHA256 tag")
      }
    }
  }
}
