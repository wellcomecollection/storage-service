package uk.ac.wellcome.platform.archive.archivist.models


import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.archivist.fixtures.ZipBagItFixture
import uk.ac.wellcome.platform.archive.archivist.generators.ArchiveJobGenerators

class ArchiveItemJobTest extends FunSpec with Matchers with ArchiveJobGenerators with ZipBagItFixture {
  val bagIdentifier = "bag-id"
  val file = "bag-info.txt"
  val uploadNamespace = "upload-bucket"
  val uploadStoragePrefix = "archive"
  val uploadSpace = "space"

//  ignore("creates an uploadLocation for a zipped bag") {
//    val file = "bag-info.txt"
//    withZipFile(List(FileEntry(file, "X"))) { zipFile =>
//      val archiveJob = createArchiveJobWithA(
//        bagIdentifier = ExternalIdentifier(bagIdentifier),
//        bagUploadLocation =  BagLocation(
//          storageNamespace = uploadNamespace,
//          storagePrefix = uploadStoragePrefix,
//          storageSpace = StorageSpace(uploadSpace),
//          bagPath = BagPath(bagIdentifier)
//        ),
//        file = zipFile
//      )
//
//      val archiveItemJob = ArchiveItemJob(
//        archiveJob = archiveJob,
//        bagItemPath = BagItemPath(file)
//      )
//
//      archiveItemJob.uploadLocation shouldBe
//        ObjectLocation(
//          uploadNamespace,
//          s"$uploadStoragePrefix/$uploadSpace/$bagIdentifier/$file")
//    }
//  }

//  ignore("creates an uploadLocation for a zipped bag in a subdirectory") {
//    val bagRootPathInZip = "bag/"
//    val bagIdentifier = "bag-id"
//    val file = "bag-info.txt"
//    val filePathInZip = "bag/bag-info.txt"
//    withZipFile(List(FileEntry(filePathInZip, "X"))) { zipFile =>
//      val archiveJob = createArchiveJobWithA(
//        bagIdentifier = ExternalIdentifier(bagIdentifier),
//        file = zipFile,
//        maybeBagRootPathInZip = Some(bagRootPathInZip),
//        bagUploadLocation =  BagLocation(
//            storageNamespace = uploadNamespace,
//            storagePrefix = uploadStoragePrefix,
//            storageSpace = StorageSpace(uploadSpace),
//            bagPath = BagPath(bagIdentifier)
//        )
//      )
//
//      val archiveItemJob = ArchiveItemJob(
//        archiveJob = archiveJob,
//        bagItemPath = BagItemPath(filePathInZip)
//      )
//
//      archiveItemJob.uploadLocation shouldBe ObjectLocation(
//        uploadNamespace,
//        s"$uploadStoragePrefix/$uploadSpace/$bagIdentifier/$file")
//    }
//  }
}
