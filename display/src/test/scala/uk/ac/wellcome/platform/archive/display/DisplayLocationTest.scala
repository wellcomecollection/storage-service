package uk.ac.wellcome.platform.archive.display

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import uk.ac.wellcome.platform.archive.common.ingests.models.S3SourceLocation
import uk.ac.wellcome.platform.archive.common.storage.models.{
  PrimaryS3StorageLocation,
  SecondaryAzureStorageLocation,
  SecondaryS3StorageLocation
}
import uk.ac.wellcome.storage.azure.AzureBlobLocationPrefix
import uk.ac.wellcome.storage.s3.{
  S3ObjectLocation,
  S3ObjectLocationPrefix
}

class DisplayLocationTest
    extends AnyFunSpec
    with Matchers
    with TableDrivenPropertyChecks {
  val testCases = Table(
    ("internalLocation", "displayLocation"),
    (
      S3SourceLocation(
        location = S3ObjectLocation(
          bucket = "my-bukkit",
          key = "path/to/my/bag.tar.gz"
        )
      ),
      DisplayLocation(
        provider = DisplayProvider("amazon-s3"),
        bucket = "my-bukkit",
        path = "path/to/my/bag.tar.gz"
      )
    )
  )

  it("creates an internal SourceLocation") {
    forAll(testCases) {
      case (internalLocation, displayLocation) =>
        displayLocation.toSourceLocation shouldBe internalLocation
    }
  }

  it("creates a DisplayLocation from an internalSourceLocation") {
    forAll(testCases) {
      case (internalLocation, displayLocation) =>
        DisplayLocation(internalLocation) shouldBe displayLocation
    }
  }

  val storageLocationTestCases = Table(
    ("storageLocation", "displayLocation"),
    (
      PrimaryS3StorageLocation(
        prefix = S3ObjectLocationPrefix(
          bucket = "my-bukkit",
          keyPrefix = "path/to/bags"
        )
      ),
      DisplayLocation(
        provider = DisplayProvider("amazon-s3"),
        bucket = "my-bukkit",
        path = "path/to/bags"
      )
    ),
    (
      SecondaryS3StorageLocation(
        prefix = S3ObjectLocationPrefix(
          bucket = "replica-bukkit",
          keyPrefix = "path/to/bags"
        )
      ),
      DisplayLocation(
        provider = DisplayProvider("amazon-s3"),
        bucket = "replica-bukkit",
        path = "path/to/bags"
      )
    ),
    (
      SecondaryAzureStorageLocation(
        prefix = AzureBlobLocationPrefix(
          container = "replica-container",
          namePrefix = "path/to/blobs"
        )
      ),
      DisplayLocation(
        provider = DisplayProvider("azure-blob-storage"),
        bucket = "replica-container",
        path = "path/to/blobs"
      )
    )
  )

  it("creates a DisplayLocation from a StorageLocation") {
    forAll(storageLocationTestCases) {
      case (storageLocation, displayLocation) =>
        DisplayLocation(storageLocation) shouldBe displayLocation
    }
  }
}
