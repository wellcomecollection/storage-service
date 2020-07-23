package uk.ac.wellcome.platform.storage.bag_tagger.services

import org.scalatest.TryValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.{PrimaryS3StorageLocation, SecondaryAzureStorageLocation}
import uk.ac.wellcome.storage.fixtures.NewS3Fixtures
import uk.ac.wellcome.storage.tags.s3.NewS3Tags
import uk.ac.wellcome.storage.{AzureBlobItemLocationPrefix, Identified}

import scala.util.Success

class ApplyTagsTest
    extends AnyFunSpec
    with Matchers
    with TryValues
    with NewS3Fixtures
    with StorageManifestGenerators {
  val s3Tags = new NewS3Tags()
  val applyTags = new ApplyTags(s3Tags = s3Tags)

  describe("it updates tags") {
    it("to objects in S3") {
      withLocalS3Bucket { bucket =>
        val prefix = createS3ObjectLocationPrefixWith(bucket)

        val file = createStorageManifestFileWith(
          pathPrefix = "space/externalIdentifier/v1",
          name = "b1234.mxf"
        )

        val location = prefix.asLocation(file.path)
        putS3Object(location)

        s3Tags.update(location) { _ =>
          Right(Map("Content-SHA256" -> "4a5a41ebcf5e2c24c"))
        }

        val result = applyTags.applyTags(
          storageLocations = Seq(
            PrimaryS3StorageLocation(prefix)
          ),
          tagsToApply = Map(file -> Map("Content-Type" -> "application/mxf"))
        )

        result shouldBe Success(())

        s3Tags.get(location).right.value shouldBe Identified(
          location,
          Map(
            "Content-SHA256" -> "4a5a41ebcf5e2c24c",
            "Content-Type" -> "application/mxf"
          )
        )
      }
    }
  }

  describe("it returns an error") {
    it("if asked to tag a non-existent object") {
      val prefix = createS3ObjectLocationPrefixWith(createBucket)

      val file = createStorageManifestFile

      val result = applyTags.applyTags(
        storageLocations = Seq(
          PrimaryS3StorageLocation(prefix)
        ),
        tagsToApply = Map(file -> Map("Content-Type" -> "application/mxf"))
      )

      val err = result.failed.get
      err shouldBe a[Throwable]
      err.getMessage shouldBe "Could not successfully apply tags!"
    }

    it("if asked to tag objects in Azure") {
      val prefix = AzureBlobItemLocationPrefix(randomAlphanumeric, randomAlphanumeric)

      val file = createStorageManifestFile

      val result = applyTags.applyTags(
        storageLocations = Seq(
          SecondaryAzureStorageLocation(prefix)
        ),
        tagsToApply = Map(file -> Map("Content-Type" -> "application/mxf"))
      )

      val err = result.failed.get
      err shouldBe a[IllegalArgumentException]
      err.getMessage should startWith("Unsupported location for tagging:")
    }

    it("if the objects have not been tagged by the verifier") {
      withLocalS3Bucket { bucket =>
        val prefix = createS3ObjectLocationPrefixWith(bucket)

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
            PrimaryS3StorageLocation(prefix)
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
