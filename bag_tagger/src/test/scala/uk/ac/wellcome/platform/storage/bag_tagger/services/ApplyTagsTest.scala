package uk.ac.wellcome.platform.storage.bag_tagger.services

import org.scalatest.TryValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.{
  PrimaryS3StorageLocation,
  SecondaryAzureStorageLocation
}
import uk.ac.wellcome.storage.Identified
import uk.ac.wellcome.storage.fixtures.{AzureFixtures, S3Fixtures}
import uk.ac.wellcome.storage.tags.azure.AzureBlobMetadata
import uk.ac.wellcome.storage.tags.s3.S3Tags

import scala.util.Success

class ApplyTagsTest
    extends AnyFunSpec
    with Matchers
    with TryValues
    with S3Fixtures
    with AzureFixtures
    with StorageManifestGenerators {

  val s3Tags = new S3Tags()
  val azureMetadata = new AzureBlobMetadata()

  val applyTags = new ApplyTags(s3Tags = s3Tags, azureMetadata = azureMetadata)

  it("updates tags") {
    withLocalS3Bucket { bucket =>
      withAzureContainer { container =>
        val s3Prefix = createS3ObjectLocationPrefixWith(bucket)
        val azurePrefix = createAzureBlobLocationPrefixWith(container)

        val file = createStorageManifestFileWith(
          pathPrefix = "space/externalIdentifier/v1",
          name = "b1234.mxf"
        )

        val s3Location = s3Prefix.asLocation(file.path)
        s3Client.putObject(s3Location.bucket, s3Location.key, randomAlphanumeric)

        val azureLocation = azurePrefix.asLocation(file.path)
        azureClient
          .getBlobContainerClient(azureLocation.container)
          .getBlobClient(azureLocation.name)
          .upload(randomInputStream(length = 10), 10)

        s3Tags.update(s3Location) { _ =>
          Right(Map("Content-SHA256" -> "4a5a41ebcf5e2c24c"))
        }

        azureMetadata.update(azureLocation) { _ =>
          Right(Map("Content-SHA256" -> "4a5a41ebcf5e2c24c"))
        }

        val result = applyTags.applyTags(
          storageLocations = Seq(
            PrimaryS3StorageLocation(s3Prefix),
            SecondaryAzureStorageLocation(azurePrefix)
          ),
          tagsToApply = Map(file -> Map("Content-Type" -> "application/mxf"))
        )

        result shouldBe Success(())

        s3Tags.get(s3Location).right.value shouldBe Identified(
          s3Location,
          Map(
            "Content-SHA256" -> "4a5a41ebcf5e2c24c",
            "Content-Type" -> "application/mxf"
          )
        )

        azureMetadata.get(azureLocation).right.value shouldBe Identified(
          azureLocation,
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

    it("if the objects have not been tagged by the verifier") {
      withLocalS3Bucket { bucket =>
        val prefix = createS3ObjectLocationPrefixWith(bucket)

        val file = createStorageManifestFileWith(
          pathPrefix = "space/externalIdentifier/v1",
          name = "b1234.mxf"
        )

        val location = prefix.asLocation(file.path)
        putStream(location)

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
