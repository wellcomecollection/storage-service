package uk.ac.wellcome.platform.archive.common.bagit.services

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.services.S3StreamableInstances
import uk.ac.wellcome.platform.archive.common.verify.{HashingAlgorithm, SHA256}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

object TryHard {
  implicit class Recoverable[T](tryT: Try[T]) {
    def recoverWithMessage(message: String) = {
      tryT.recoverWith {
        case e => Failure(new RuntimeException(s"$message: ${e.getMessage}"))
      }
    }
  }

  implicit class Available[T](maybeT: Option[T]) {
    def unavailableWithMessage(message: String) =
      Try(maybeT.get).recoverWith {
        case e => Failure(new RuntimeException(message))
      }

  }
}

class BagService()(implicit s3Client: AmazonS3) extends Logging {

  import TryHard._
  import S3StreamableInstances._
  import uk.ac.wellcome.platform.archive.common.bagit.models._

  val checksumAlgorithm = SHA256

  def retrieve(root: ObjectLocation): Try[Bag] = {
    debug(s"BagService attempting to create Bag @ $root")

    val bag = for {
      bagInfo <- createBagInfo(root)
        .recoverWithMessage("Error getting bag info")
      fileManifest <- createFileManifest(root)
        .recoverWithMessage("Error getting file manifest")
      tagManifest <- createTagManifest(root)
        .recoverWithMessage("Error getting tag manifest")
      bagFetch <- createBagFetch(root)
        .recoverWithMessage("Error getting fetch")
    } yield Bag(bagInfo, fileManifest, tagManifest)

    debug(s"BagService got: $bag")

    bag
  }

  val fileManifest = (a: HashingAlgorithm) => s"manifest-${a.pathRepr}.txt"
  val tagManifest = (a: HashingAlgorithm) => s"tagmanifest-${a.pathRepr}.txt"
  val bagFetch = "fetch.txt"

  def createBagFetch(root: ObjectLocation): Try[Option[BagFetch]] =
    BagPath("fetch.txt").from(root).flatMap {
      case Some(inputStream) => BagFetch
        .create(inputStream)
        .map(Some(_))

      case None => Success(None)
    }

  def createFileManifest(root: ObjectLocation): Try[BagManifest] =
    createManifest(fileManifest(checksumAlgorithm), root)

  def createTagManifest(root: ObjectLocation): Try[BagManifest] =
    createManifest(tagManifest(checksumAlgorithm), root)

  private def createManifest(
                              name: String,
                              root: ObjectLocation
                            ): Try[BagManifest] = for {
    maybeStream <- BagPath(name).from(root)
    stream <- maybeStream
      .unavailableWithMessage(s"$name is not available")
    manifest <- BagManifest.create(stream, checksumAlgorithm)
  } yield manifest

  def createBagInfo(root: ObjectLocation): Try[BagInfo] = for {
    maybeStream <- BagPath("bag-info.txt").from(root)
    stream <- maybeStream
      .unavailableWithMessage("bag-info.txt is not available")
    bagInfo <- BagInfo.create(stream)
  } yield bagInfo

}

