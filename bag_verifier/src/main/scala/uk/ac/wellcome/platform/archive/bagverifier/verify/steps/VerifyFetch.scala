package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerifierError
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagFetch,
  BagFetchMetadata,
  BagPath
}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}

trait VerifyFetch[BagLocation <: Location, BagPrefix <: Prefix[BagLocation]] {

  // Check the user hasn't supplied any fetch entries which are in the wrong
  // namespace or path prefix.
  //
  // We only allow fetch entries to refer to previous versions of the same bag; this
  // ensures that a single bag (same space/external identifier) is completely self-contained,
  // and we don't have to worry about interconnected bag dependencies.
  def verifyFetchPrefixes(
    fetch: Option[BagFetch],
    root: S3ObjectLocationPrefix
  ): Either[BagVerifierError, Unit] =
    fetch match {
      case None => Right(())

      case Some(BagFetch(entries)) =>
        val mismatchedEntries =
          entries
            .filterNot {
              case (_, fetchMetadata: BagFetchMetadata) =>
                val fetchLocation = S3ObjectLocation(
                  bucket = fetchMetadata.uri.getHost,
                  key = fetchMetadata.uri.getPath.stripPrefix("/")
                )

                // TODO: This could verify the version prefix as well.
                // TODO: Hard-coding the expected scheme here isn't ideal
                fetchMetadata.uri.getScheme == "s3" &&
                isPrefixOf(fetchLocation, prefix = root)
            }

        val mismatchedPaths = mismatchedEntries.keys.toSeq

        mismatchedPaths match {
          case Nil => Right(())
          case _ =>
            Left(
              BagVerifierError(
                s"fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme: ${mismatchedPaths
                  .mkString(", ")}"
              )
            )
        }
    }

  private def isPrefixOf(
    location: S3ObjectLocation,
    prefix: S3ObjectLocationPrefix
  ): Boolean =
    location.bucket == prefix.bucket && location.key.startsWith(s"${prefix.keyPrefix}/")

  // Check that the user hasn't sent any files in the bag which
  // also have a fetch file entry.
  def verifyNoConcreteFetchEntries(
    fetch: Option[BagFetch],
    root: BagPrefix,
    actualLocations: Seq[BagLocation]
  ): Either[BagVerifierError, Unit] = {
    val bagFetchLocations: Seq[(BagPath, BagLocation)] = fetch match {
      case Some(bagFetch) =>
        bagFetch.paths
          .map { path: BagPath =>
            path -> root.asLocation(path.value)
          }

      case None => Seq.empty
    }

    val concreteFetchLocations =
      bagFetchLocations
        .filter { case (_, location) => actualLocations.contains(location) }

    if (concreteFetchLocations.isEmpty) {
      Right(())
    } else {
      val concretePaths = concreteFetchLocations.collect {
        case (bagPath, _) => bagPath
      }

      Left(
        BagVerifierError(
          "Files referred to in the fetch.txt also appear in the bag: " +
            concretePaths.mkString(", ")
        )
      )
    }
  }
}
