package weco.storage_service.bagit.services

import java.io.InputStream
import weco.storage_service.bagit.models._
import weco.storage_service.checksum.{
  ChecksumAlgorithm,
  ChecksumAlgorithms,
  ChecksumValue,
  MD5,
  MultiManifestChecksum,
  MultiManifestChecksumException,
  SHA1,
  SHA256,
  SHA512
}
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

      _ <- compareAlgorithms(payloadManifest, tagManifest)

      bagFetch <- loadFetch(bagRoot)
    } yield Bag(bagInfo, payloadManifest, tagManifest, bagFetch)

  private def loadBagInfo(bagRoot: BagPrefix): Either[BagUnavailable, BagInfo] =
    loadRequired[BagInfo](bagRoot)(bagInfo)(BagInfoParser.create)

  private def loadPayloadManifest(
    bagRoot: BagPrefix
  ): Either[BagUnavailable, PayloadManifest] =
    loadManifestEntries(bagRoot)(fileManifest) match {
      case Right((algorithms, entries)) =>
        Right(PayloadManifest(algorithms, entries))
      case Left(ManifestError.CannotBeLoaded(err)) => Left(err)
      case Left(ManifestError.NoManifestsFound) =>
        Left(
          BagUnavailable("Could not find any payload manifests in the bag")
        )
      case Left(ManifestError.InconsistentFilenames) =>
        Left(
          BagUnavailable(
            "Payload manifests are inconsistent: every payload file must be listed in every payload manifest"
          )
        )

      case Left(ManifestError.OnlyDeprecatedChecksums) =>
        // We'll need to update this error message if we add support for other checksum algorithms.
        // A runtime assertion is meant to draw our attention to it, rather than introducing the
        // unnecessary complexity of creating this message dynamically.
        require(
          ChecksumAlgorithms.nonDeprecatedAlgorithms.toSet == Set(
            SHA256,
            SHA512
          )
        )
        Left(
          BagUnavailable(
            "Payload manifests only use deprecated checksums: add a payload manifest using SHA-256 or SHA-512"
          )
        )

      case Left(_) =>
        Left(
          BagUnavailable("Unknown error while trying to read payload manifests")
        )
    }

  private def loadTagManifest(bagRoot: BagPrefix) =
    loadManifestEntries(bagRoot)(tagManifest) match {
      case Right((algorithms, entries)) =>
        Right(TagManifest(algorithms, entries))
      case Left(ManifestError.CannotBeLoaded(err)) => Left(err)
      case Left(ManifestError.NoManifestsFound) =>
        Left(BagUnavailable("Could not find any tag manifests in the bag"))
      case Left(ManifestError.InconsistentFilenames) =>
        Left(
          BagUnavailable(
            "Tag manifests are inconsistent: each tag manifest should list the same set of tag files"
          )
        )

      case Left(ManifestError.OnlyDeprecatedChecksums) =>
        // We'll need to update this error message if we add support for other checksum algorithms.
        // A runtime assertion is meant to draw our attention to it, rather than introducing the
        // unnecessary complexity of creating this message dynamically.
        require(
          ChecksumAlgorithms.nonDeprecatedAlgorithms.toSet == Set(
            SHA256,
            SHA512
          )
        )
        Left(
          BagUnavailable(
            "Tag manifests only use weak checksums: add a tag manifest using SHA-256 or SHA-512"
          )
        )

      case Left(_) =>
        Left(
          BagUnavailable("Unknown error while trying to read tag manifests")
        )
    }

  // Quoting RFC 8493 § 2.2.1 (https://datatracker.ietf.org/doc/html/rfc8493#section-2.2.1):
  //
  //      Tag manifests SHOULD use the same algorithms as the payload manifests
  //      that are present in the bag.
  //
  // We're already stricter than the spec by requiring tag manifests be present (which are
  // optional in the spec); similarly here we treat this SHOULD as a MUST.
  //
  private def compareAlgorithms(
    payloadManifest: PayloadManifest,
    tagManifest: TagManifest
  ): Either[BagUnavailable, Unit] =
    Either.cond(
      payloadManifest.algorithms == tagManifest.algorithms,
      (),
      BagUnavailable(
        "Manifests are inconsistent: tag manifests should use the same algorithms as the payload manifests in the bag"
      )
    )

  private sealed trait ManifestError
  private object ManifestError {
    case class CannotBeLoaded(err: BagUnavailable) extends ManifestError
    case object NoManifestsFound extends ManifestError
    case object InconsistentFilenames extends ManifestError
    case object OnlyDeprecatedChecksums extends ManifestError
    case class UnknownError(t: Throwable) extends ManifestError
  }

  /** Loads all the entries for every instance of a type of manifest.
    *
    * i.e. loads manifest-{algorithm}.txt or tagmanifest-{algorithm}.txt for
    * every supported checksum algorithm
    *
    * Returns a list of checksum algorithms for which the manifest files are defined
    * in the bag, and a list of entries correlated across the manifests.  These are
    * returned separately because the list of entries may be empty (if there's nothing
    * in the payload manifests).
    *
    * This function is responsible for checking consistency within a type of manifest,
    * e.g. does every manifest list the same filenames.  Callers may assume the manifest
    * data is consistent, although we can't easily enforce that in the type system.
    *
    */
  private def loadManifestEntries(
    root: BagPrefix
  )(filename: ChecksumAlgorithm => BagPath): Either[
    ManifestError,
    (Set[ChecksumAlgorithm], Map[BagPath, MultiManifestChecksum])
  ] =
    for {
      md5 <- loadSingleManifest(root, filename(MD5))
      sha1 <- loadSingleManifest(root, filename(SHA1))
      sha256 <- loadSingleManifest(root, filename(SHA256))
      sha512 <- loadSingleManifest(root, filename(SHA512))

      presentManifests <- {
        val readValues = Map(
          MD5 -> md5,
          SHA1 -> sha1,
          SHA256 -> sha256,
          SHA512 -> sha512
        )

        // This assertion is meant to remind us that we need to change this code
        // when we add new checksum algorithms.
        //
        // I did try writing this code in a dynamic way that automatically picks up
        // new checksum algorithms, but I couldn't find a way that doesn't make
        // the code much more complicated.  Given we don't even know if we'll ever
        // add new checksum algorithms, a single runtime assertion feels like
        // better than adding substantial complexity here.
        require(readValues.size == ChecksumAlgorithms.algorithms.size)

        val extantValues: Map[ChecksumAlgorithm, Map[BagPath, ChecksumValue]] =
          readValues
            .collect { case (algorithm, Some(entries)) => (algorithm, entries) }

        Either.cond(
          extantValues.nonEmpty,
          extantValues,
          ManifestError.NoManifestsFound
        )
      }

      algorithms = presentManifests.keys.toSet
      manifests = presentManifests.values.toSeq

      // RFC 8493 § 3 (https://datatracker.ietf.org/doc/html/rfc8493#section-3):
      //
      //      For BagIt 1.0, every payload file MUST be listed in every payload
      //      manifest.  Note that older versions of BagIt allowed payload
      //      files to be listed in just one of the manifests.
      //
      filenames <- manifests.map(_.keys.toSeq).distinct match {
        case Seq(names) => Right(names)
        case _          => Left(ManifestError.InconsistentFilenames)
      }

      entries <- Try {
        filenames.map { bagPath =>
          bagPath -> MultiManifestChecksum(
            md5 = md5.map(_(bagPath)),
            sha1 = sha1.map(_(bagPath)),
            sha256 = sha256.map(_(bagPath)),
            sha512 = sha512.map(_(bagPath))
          )
        }.toMap
      } match {
        case Success(entries) => Right(entries)
        case Failure(MultiManifestChecksumException.OnlyDeprecatedChecksums) =>
          Left(ManifestError.OnlyDeprecatedChecksums)
        case Failure(t) => Left(ManifestError.UnknownError(t))
      }

      // If this isn't the case, we should have caught it with one of the checks above --
      // this is just to catch a programmer error.
      _ = require(
        entries.values.forall(_.definedAlgorithms == algorithms)
      )

    } yield (algorithms, entries)

  private def loadSingleManifest(
    root: BagPrefix,
    path: BagPath
  ): Either[ManifestError.CannotBeLoaded, Option[Map[BagPath, ChecksumValue]]] =
    loadOptional[Map[BagPath, ChecksumValue]](root)(path)(
      BagManifestParser.parse
    ) match {
      case Right(entries) => Right(entries)
      case Left(bagUnavailable) =>
        Left(ManifestError.CannotBeLoaded(bagUnavailable))
    }

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
