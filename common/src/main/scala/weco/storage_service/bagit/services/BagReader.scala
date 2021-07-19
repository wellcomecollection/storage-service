package weco.storage_service.bagit.services

import java.io.InputStream
import weco.storage_service.bagit.models._
import weco.storage_service.verify.{ChecksumValue, HashingAlgorithm, MD5, SHA1, SHA256, SHA512}
import weco.storage._
import weco.storage.store.Readable
import weco.storage.streaming.InputStreamWithLength

import scala.util.{Failure, Success, Try}

trait BagReader[BagLocation <: Location, BagPrefix <: Prefix[BagLocation]] {
  protected val bagFetch = BagPath("fetch.txt")
  protected val bagInfo = BagPath("bag-info.txt")
  protected val payloadManifest =
    (a: HashingAlgorithm) => BagPath(s"manifest-${a.pathRepr}.txt")
  protected val tagManifest =
    (a: HashingAlgorithm) => BagPath(s"tagmanifest-${a.pathRepr}.txt")

  implicit val readable: Readable[BagLocation, InputStreamWithLength]

  type Stream[T] = InputStream => Try[T]

  def get(bagRoot: BagPrefix): Either[BagUnavailable, Bag] =
    for {
      bagInfo <- loadRequired[BagInfo](bagRoot)(bagInfo)(BagInfoParser.create)

      payloadManifest <- loadManifests(bagRoot, payloadManifest).map(NewPayloadManifest)
      tagManifest <- loadManifests(bagRoot, tagManifest).map(NewTagManifest)

      bagFetch <- loadOptional[BagFetch](bagRoot)(bagFetch)(BagFetch.create)

    } yield Bag(bagInfo, payloadManifest, tagManifest, bagFetch)

  type ManifestEntries = Map[BagPath, ChecksumValue]

  private def loadManifests(root: BagPrefix, filename: HashingAlgorithm => BagPath): Either[BagUnavailable, Map[BagPath, MultiChecksumValue[ChecksumValue]]] =
    for {
      md5 <- loadOptional[ManifestEntries](root)(filename(MD5))(BagManifestParser.parse)
      sha1 <- loadOptional[ManifestEntries](root)(filename(SHA1))(BagManifestParser.parse)
      sha256 <- loadOptional[ManifestEntries](root)(filename(SHA256))(BagManifestParser.parse)
      sha512 <- loadOptional[ManifestEntries](root)(filename(SHA512))(BagManifestParser.parse)

      // RFC 8493 § 3 says that:
      //
      //    For BagIt 1.0, every payload file MUST be listed in every payload
      //    manifest.  Note that older versions of BagIt allowed payload
      //    files to be listed in just one of the manifests.
      //
      // TODO: Enforce this.
      manifests: Seq[ManifestEntries] = Seq(md5, sha1, sha256, sha512).flatten
      filenames = manifests.head.keys.toSeq

      entries = filenames
        .map { bagPath =>
          bagPath -> MultiChecksumValue(
            md5 = md5.map(_(bagPath)),
            sha1 = sha1.map(_(bagPath)),
            sha256 = sha256.map(_(bagPath)),
            sha512 = sha512.map(_(bagPath)),
          )
        }
        .toMap
    } yield entries

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
