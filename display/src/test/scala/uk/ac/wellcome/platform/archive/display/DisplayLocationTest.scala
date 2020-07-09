package uk.ac.wellcome.platform.archive.display

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import uk.ac.wellcome.platform.archive.common.ingests.models.S3SourceLocation
import uk.ac.wellcome.storage.S3ObjectLocation

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
    ),
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
