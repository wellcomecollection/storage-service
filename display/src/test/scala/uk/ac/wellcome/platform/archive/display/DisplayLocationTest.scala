package uk.ac.wellcome.platform.archive.display

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  AzureBlobSourceLocation,
  S3SourceLocation
}
import uk.ac.wellcome.storage.{
  AzureBlobItemLocationPrefix,
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
        prefix = S3ObjectLocationPrefix(
          bucket = "my-bukkit",
          keyPrefix = "path/to/my/bag"
        )
      ),
      DisplayLocation(
        provider = DisplayProvider("amazon-s3"),
        bucket = "my-bukkit",
        path = "path/to/my/bag"
      )
    ),
    (
      AzureBlobSourceLocation(
        prefix = AzureBlobItemLocationPrefix(
          container = "my-container",
          namePrefix = "path/to/my/bag"
        )
      ),
      DisplayLocation(
        provider = DisplayProvider("azure-blob-storage"),
        bucket = "my-container",
        path = "path/to/my/bag"
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
}
