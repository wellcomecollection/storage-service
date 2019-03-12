package uk.ac.wellcome.platform.archive.common.models.bagit

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagLocation,
  BagPath
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

class BagLocationTest extends FunSpec with Matchers {
  it("creates a bagLocation") {
    val bagLocation = BagLocation(
      storageNamespace = "bucket",
      storagePrefix = Some("storage"),
      storageSpace = StorageSpace("digitised"),
      bagPath = BagPath("a/bag")
    )
    bagLocation.objectLocation shouldBe ObjectLocation(
      "bucket",
      "storage/digitised/a/bag")
  }

  it("creates a bagLocation with blank prefix") {
    val bagLocation = BagLocation(
      storageNamespace = "bucket",
      storagePrefix = Some(""),
      storageSpace = StorageSpace("digitised"),
      bagPath = BagPath("a/bag")
    )
    bagLocation.objectLocation shouldBe ObjectLocation(
      "bucket",
      "digitised/a/bag")
  }

  it("creates a bagLocation with no prefix") {
    val bagLocation = BagLocation(
      storageNamespace = "bucket",
      storagePrefix = None,
      storageSpace = StorageSpace("digitised"),
      bagPath = BagPath("a/bag")
    )
    bagLocation.objectLocation shouldBe ObjectLocation(
      "bucket",
      "digitised/a/bag")
  }
}
