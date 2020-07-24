package uk.ac.wellcome.platform.archive.common.bagit.services

import java.io.InputStream

import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.verify.{HashingAlgorithm, SHA256}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

import scala.util.{Failure, Success, Try}

trait BagReader[BagLocation <: Location, BagPrefix <: Prefix[BagLocation]] {
  protected val bagFetch = BagPath("fetch.txt")
  protected val bagInfo = BagPath("bag-info.txt")
  protected val fileManifest =
    (a: HashingAlgorithm) => BagPath(s"manifest-${a.pathRepr}.txt")
  protected val tagManifest =
    (a: HashingAlgorithm) => BagPath(s"tagmanifest-${a.pathRepr}.txt")

  implicit val readable: Readable[BagLocation, InputStreamWithLength]

  type Stream[T] = InputStream => Try[T]

  def get(bagRoot: BagPrefix): Either[BagUnavailable, Bag] =
    for {
      bagInfo <- loadRequired[BagInfo](bagRoot)(bagInfo)(BagInfoParser.create)

      fileManifest <- loadRequired[PayloadManifest](bagRoot)(
        fileManifest(SHA256)
      )(
        PayloadManifest.create(_, SHA256)
      )

      tagManifest <- loadRequired[TagManifest](bagRoot)(tagManifest(SHA256))(
        TagManifest.create(_, SHA256)
      )

      bagFetch <- loadOptional[BagFetch](bagRoot)(bagFetch)(BagFetch.create)

    } yield Bag(bagInfo, fileManifest, tagManifest, bagFetch)

  def asLocation(prefix: BagPrefix, path: String): BagLocation

  private def loadOptional[T](
    root: BagPrefix
  )(path: BagPath)(f: Stream[T]): Either[BagUnavailable, Option[T]] = {
    val location = asLocation(root, path = path.value)

    readable.get(location) match {
      case Right(stream) =>
        f(stream.identifiedT) match {
          case Success(r) => Right(Some(r))
          case Failure(e) =>
            Left(
              BagUnavailable(s"Error loading ${path.value}: ${e.getMessage}")
            )
        }

      case Left(_: DoesNotExistError) =>
        Right(None)

      case Left(err) =>
        Left(BagUnavailable(s"Error loading ${path.value}: $err"))
    }
  }

  private def loadRequired[T](
    root: BagPrefix
  )(path: BagPath)(f: Stream[T]): Either[BagUnavailable, T] =
    loadOptional[T](root)(path)(f) match {
      case Right(Some(result)) => Right(result)
      case Right(None) =>
        Left(BagUnavailable(s"Error loading ${path.value}: no such file!"))
      case Left(err) => Left(err)
    }
}
