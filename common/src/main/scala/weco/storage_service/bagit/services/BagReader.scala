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

      // RFC 8493 § 3:
      //
      //    For BagIt 1.0, every payload file MUST be listed in every payload
      //    manifest.  Note that older versions of BagIt allowed payload
      //    files to be listed in just one of the manifests.
      //

      payloadManifest <- loadManifestEntries(bagRoot, payloadManifest) match {
        case Right(entries)                            => Right(NewPayloadManifest(entries))
        case Left(ManifestError.CannotBeLoaded(err))   => Left(err)
        case Left(ManifestError.NoManifests)           => Left(BagUnavailable("Could not find any payload manifests in the bag"))
        case Left(ManifestError.InconsistentFilenames) => Left(BagUnavailable("Payload manifests are inconsistent: every payload file must be listed in every payload manifest"))
      }

      // RFC 8493 § 2.2.1:
      //
      //    A bag MAY contain one or more tag manifests, in which case each tag
      //    manifest SHOULD list the same set of tag files.
      //
      // We treat this as a MUST because it makes things simpler elsewhere.
      //
      tagManifest <- loadManifestEntries(bagRoot, tagManifest) match {
        case Right(entries)                            => Right(NewTagManifest(entries))
        case Left(ManifestError.CannotBeLoaded(err))   => Left(err)
        case Left(ManifestError.NoManifests)           => Left(BagUnavailable("Could not find any tag manifests in the bag"))
        case Left(ManifestError.InconsistentFilenames) => Left(BagUnavailable("Tag manifests are inconsistent: each tag manifest should list the same set of tag files"))
      }

      bagFetch <- loadOptional[BagFetch](bagRoot)(bagFetch)(BagFetch.create)

    } yield Bag(bagInfo, payloadManifest, tagManifest, bagFetch)

  type ManifestEntries = Map[BagPath, ChecksumValue]

  private sealed trait ManifestError
  private object ManifestError {
    case class CannotBeLoaded(err: BagUnavailable) extends ManifestError
    case object NoManifests extends ManifestError
    case object InconsistentFilenames extends ManifestError
  }

  private def loadManifestEntries(root: BagPrefix, filename: HashingAlgorithm => BagPath): Either[ManifestError, Map[BagPath, MultiChecksumValue[ChecksumValue]]] =
    for {
      md5 <- loadSingleManifest(root, filename(MD5))
      sha1 <- loadSingleManifest(root, filename(SHA1))
      sha256 <- loadSingleManifest(root, filename(SHA256))
      sha512 <- loadSingleManifest(root, filename(SHA512))

      manifests <- Seq(md5, sha1, sha256, sha512).flatten match {
        case Nil   => Left(ManifestError.NoManifests)
        case other => Right(other)
      }

      filenames <- manifests.map(_.keys.toSeq).distinct match {
        case Seq(names) => Right(names)
        case _          => Left(ManifestError.InconsistentFilenames)
      }

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

  private def loadSingleManifest(root: BagPrefix, path: BagPath): Either[ManifestError.CannotBeLoaded, Option[ManifestEntries]] =
    loadOptional[ManifestEntries](root)(path)(BagManifestParser.parse) match {
      case Right(entries)       => Right(entries)
      case Left(bagUnavailable) => Left(ManifestError.CannotBeLoaded(bagUnavailable))
    }

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
