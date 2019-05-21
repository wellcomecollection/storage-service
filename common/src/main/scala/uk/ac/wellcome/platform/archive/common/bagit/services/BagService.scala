package uk.ac.wellcome.platform.archive.common.bagit.services

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.services.StreamableInstances
import uk.ac.wellcome.platform.archive.common.verify.{HashingAlgorithm, SHA256}
import uk.ac.wellcome.storage.{ObjectLocation, StorageBackend}

import scala.util.{Failure, Try}

class BagService()(implicit storageBackend: StorageBackend) extends Logging {

  import StreamableInstances._
  import uk.ac.wellcome.platform.archive.common.bagit.models._

  val checksumAlgorithm: HashingAlgorithm = SHA256

  private def recoverable[T](tryT: Try[T])(message: String): Try[T] =
    tryT.recoverWith {
      case e => Failure(new RuntimeException(s"$message: ${e.getMessage}"))
    }

  def retrieve(root: ObjectLocation): Try[Bag] = {
    debug(s"BagService attempting to create Bag @ $root")

    val bag = for {
      bagInfo <- recoverable(createBagInfo(root))("Error getting bag info")
      fileManifest <- recoverable(createFileManifest(root))(
        "Error getting file manifest")
      tagManifest <- recoverable(createTagManifest(root))(
        "Error getting tag manifest")
    } yield Bag(bagInfo, fileManifest, tagManifest)

    debug(s"BagService got: $bag")

    bag
  }

  def createBagInfo(root: ObjectLocation): Try[BagInfo] =
    for {
      stream <- BagPath("bag-info.txt").from(root)
      bagInfo <- BagInfo.create(stream)
    } yield bagInfo

  val fileManifest = (a: HashingAlgorithm) => s"manifest-${a.pathRepr}.txt"
  val tagManifest = (a: HashingAlgorithm) => s"tagmanifest-${a.pathRepr}.txt"

  def createFileManifest(root: ObjectLocation): Try[BagManifest] =
    createManifest(fileManifest(checksumAlgorithm), root)

  def createTagManifest(root: ObjectLocation): Try[BagManifest] =
    createManifest(tagManifest(checksumAlgorithm), root)

  private def createManifest(
    name: String,
    root: ObjectLocation
  ): Try[BagManifest] =
    for {
      stream <- BagPath(name).from(root)
      manifest <- BagManifest.create(stream, checksumAlgorithm)
    } yield manifest
}
