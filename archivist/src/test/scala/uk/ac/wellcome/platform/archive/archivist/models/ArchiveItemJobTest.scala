package uk.ac.wellcome.platform.archive.archivist.models

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.archivist.fixtures.ZipBagItFixture
import uk.ac.wellcome.platform.archive.archivist.generators.ArchiveJobGenerators
import uk.ac.wellcome.platform.archive.common.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagLocation,
  BagPath
}
import uk.ac.wellcome.storage.ObjectLocation

class ArchiveItemJobTest
    extends FunSpec
    with Matchers
    with ArchiveJobGenerators
    with ZipBagItFixture {
  val uploadNamespace = "upload-bucket"
  val uploadStoragePrefix = "archive"
  val uploadSpace = "space"
  val file = "bag-info.txt"

  val bagIdentifier = "bag-id"

  val bagUploadLocation = BagLocation(
    uploadNamespace,
    uploadStoragePrefix,
    StorageSpace(uploadSpace),
    BagPath(bagIdentifier))

  it("creates an uploadLocation for a zipped bag with no subdirectory") {
    UploadLocationBuilder.create(bagUploadLocation, file) shouldBe
      ObjectLocation(
        uploadNamespace,
        s"$uploadStoragePrefix/$uploadSpace/$bagIdentifier/$file")
  }

  it("creates an uploadLocation for a zipped bag in a subdirectory") {
    val bagParentDirInZip = "bag-parent"
    UploadLocationBuilder.create(
      bagUploadLocation,
      f"$bagParentDirInZip/$file",
      Some(bagParentDirInZip)) shouldBe
      ObjectLocation(
        uploadNamespace,
        s"$uploadStoragePrefix/$uploadSpace/$bagIdentifier/$file")
  }
}
