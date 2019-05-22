package uk.ac.wellcome.platform.archive.common.bagit.services

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.services.S3StreamableInstances
import uk.ac.wellcome.platform.archive.common.verify.{HashingAlgorithm, SHA256}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

class BagService()(implicit s3Client: AmazonS3) extends Logging {

  type Stream[T] = InputStream => Try[T]

  import S3StreamableInstances._
  import uk.ac.wellcome.platform.archive.common.bagit.models._

  private val bagFetch = BagPath("fetch.txt")
  private val bagInfo = BagPath("bag-info.txt")
  private val fileManifest = (a: HashingAlgorithm) =>
    BagPath(s"manifest-${a.pathRepr}.txt")
  private val tagManifest = (a: HashingAlgorithm) =>
    BagPath(s"tagmanifest-${a.pathRepr}.txt")

  def retrieve(root: ObjectLocation) =
    for {

      bagInfo <- loadRequired[BagInfo](root)(bagInfo)(BagInfo.create)

      fileManifest <- loadRequired[BagManifest](root)(fileManifest(SHA256))(
        BagManifest.create(_, SHA256))

      tagManifest <- loadRequired[BagManifest](root)(tagManifest(SHA256))(
        BagManifest.create(_, SHA256))

      bagFetch <- loadOptional[BagFetch](root)(bagFetch)(BagFetch.create)

    } yield Bag(bagInfo, fileManifest, tagManifest, bagFetch)

  private def loadOptional[T](root: ObjectLocation)(path: BagPath)(
    f: Stream[T]): Either[BagUnavailable, Option[T]] =
    for {
      maybeStream <- path.locateWith(root) match {
        case Right(o) => Right(o)
        case Left(e) =>
          Left(BagUnavailable(s"${path.value} is not available: ${e.msg}"))
      }

      maybeT <- maybeStream match {
        case Some(o) =>
          f(o) match {
            case Success(r) => Right(Some(r))
            case Failure(e) =>
              Left(
                BagUnavailable(s"Error loading ${path.value}: ${e.getMessage}"))
          }
        case None => Right(None)
      }
    } yield maybeT

  private def loadRequired[T](root: ObjectLocation)(path: BagPath)(
    f: Stream[T]) =
    for {
      maybeStream <- path.locateWith(root) match {
        case Right(o) => Right(o)
        case Left(e) =>
          Left(BagUnavailable(s"${path.value} is not available: ${e.msg}"))
      }

      stream <- maybeStream match {
        case Some(stream) => Right(stream)
        case None         => Left(BagUnavailable(s"Error loading ${path.value}"))
      }

      t <- f(stream) match {
        case Success(r) => Right(r)
        case Failure(e) =>
          Left(BagUnavailable(s"Error loading ${path.value}: ${e.getMessage}"))
      }
    } yield t

}

case class BagUnavailable(msg: String) extends Throwable(msg)
