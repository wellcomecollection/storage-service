package uk.ac.wellcome.platform.archive.common.bagit.services

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.storage.services.S3StreamableInstances
import uk.ac.wellcome.platform.archive.common.verify.ChecksumAlgorithm
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.Try

class BagService()(implicit s3Client: AmazonS3) {

  import S3StreamableInstances._
  import uk.ac.wellcome.platform.archive.common.bagit.models._

  val checksumAlgorithm = ChecksumAlgorithm("sha256")

  def create(root: ObjectLocation): Try[Bag] =
    for {
      bagInfo <- createBagInfo(root)
      fileManifest <- createFileManifest(root)
      tagManifest <- createTagManifest(root)
    } yield
      Bag(
        info = bagInfo,
        manifest = fileManifest,
        tagManifest = tagManifest,
      )

  def createBagInfo(root: ObjectLocation): Try[BagInfo] = for {
    stream <- BagPath("bag-info.txt").from(root)
    bagInfo <- BagInfo.create(stream)
  } yield bagInfo

  val fileManifest = (a: ChecksumAlgorithm) => s"manifest-$a.txt"
  val tagManifest = (a: ChecksumAlgorithm) => s"tagmanifest-$a.txt"

  def createFileManifest(root: ObjectLocation): Try[BagManifest] =
    createManifest(fileManifest(checksumAlgorithm), root)

  def createTagManifest(root: ObjectLocation): Try[BagManifest] =
    createManifest(tagManifest(checksumAlgorithm), root)

  private def createManifest(
                              name: String,
                              root: ObjectLocation
                            ): Try[BagManifest] = for {
    stream <- BagPath(name).from(root)
    manifest <- BagManifest.create(stream, checksumAlgorithm)
  } yield manifest
}

