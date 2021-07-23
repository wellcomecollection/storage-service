package weco.storage_service.bagit.services

import java.io.InputStream

import weco.storage_service.bagit.models._
import weco.storage_service.checksum.{ChecksumAlgorithm, SHA256}
import weco.storage._
import weco.storage.store.Readable
import weco.storage.streaming.InputStreamWithLength

import scala.util.{Failure, Success, Try}

trait BagReader[BagLocation <: Location, BagPrefix <: Prefix[BagLocation]] {
  protected val bagFetch = BagPath("fetch.txt")
  protected val bagInfo = BagPath("bag-info.txt")
  protected val fileManifest =
    (a: ChecksumAlgorithm) => BagPath(s"manifest-${a.pathRepr}.txt")
  protected val tagManifest =
    (a: ChecksumAlgorithm) => BagPath(s"tagmanifest-${a.pathRepr}.txt")

  implicit val readable: Readable[BagLocation, InputStreamWithLength]

  type Stream[T] = InputStream => Try[T]

  def get(bagRoot: BagPrefix): Either[BagUnavailable, Bag] =
    for {
      bagInfo <- loadBagInfo(bagRoot)

      payloadManifest <- loadPayloadManifest(bagRoot)
      tagManifest <- loadTagManifest(bagRoot)

      bagFetch <- loadFetch(bagRoot)
    } yield Bag(bagInfo, payloadManifest, tagManifest, bagFetch)

  private def loadBagInfo(bagRoot: BagPrefix): Either[BagUnavailable, BagInfo] =
    loadRequired[BagInfo](bagRoot)(bagInfo)(BagInfoParser.create)

  private def loadPayloadManifest(
    bagRoot: BagPrefix
  ): Either[BagUnavailable, PayloadManifest] =
    loadRequired[PayloadManifest](bagRoot)(fileManifest(SHA256))(
      (inputStream: InputStream) =>
        BagManifestParser.parse(inputStream).map { entries =>
          PayloadManifest(
            checksumAlgorithm = SHA256,
            entries = entries
          )
        }
    )

  private def loadTagManifest(bagRoot: BagPrefix) =
    loadRequired[TagManifest](bagRoot)(tagManifest(SHA256))(
      (inputStream: InputStream) =>
        BagManifestParser.parse(inputStream).map { entries =>
          TagManifest(
            checksumAlgorithm = SHA256,
            entries = entries
          )
        }
    )

  private def loadFetch(
    bagRoot: BagPrefix
  ): Either[BagUnavailable, Option[BagFetch]] =
    loadOptional[BagFetch](bagRoot)(bagFetch)(BagFetch.create)

  private def loadOptional[T](
    root: BagPrefix
  )(path: BagPath)(f: Stream[T]): Either[BagUnavailable, Option[T]] = {
    val location = root.asLocation(path.value)

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
