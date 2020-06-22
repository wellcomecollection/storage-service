package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import uk.ac.wellcome.platform.archive.bagverifier.fixity.{FixityListAllCorrect, FixityListResult}
import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerifierError
import uk.ac.wellcome.platform.archive.common.bagit.models.{Bag, BagFetch, BagFetchMetadata, BagPath}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

trait VerifyFetch {

  // Check the user hasn't supplied any fetch entries which are in the wrong
  // namespace or path prefix.
  //
  // We only allow fetch entries to refer to previous versions of the same bag; this
  // ensures that a single bag (same space/external identifier) is completely self-contained,
  // and we don't have to worry about interconnected bag dependencies.
  def verifyFetchPrefixes(
    bag: Bag,
    root: ObjectLocationPrefix
  ): Either[BagVerifierError, Unit] =
    bag.fetch match {
      case None => Right(())

      case Some(BagFetch(entries)) =>
        val mismatchedEntries =
          entries
            .filterNot {
              case (_, fetchMetadata: BagFetchMetadata) =>
                val fetchLocation = ObjectLocation(
                  namespace = fetchMetadata.uri.getHost,
                  path = fetchMetadata.uri.getPath.stripPrefix("/")
                )

                // TODO: This could verify the version prefix as well.
                // TODO: Hard-coding the expected scheme here isn't ideal
                fetchMetadata.uri.getScheme == "s3" &&
                  fetchLocation.namespace == root.namespace &&
                  fetchLocation.path.startsWith(s"${root.path}/")
            }

        val mismatchedPaths = mismatchedEntries.keys.toSeq

        mismatchedPaths match {
          case Nil => Right(())
          case _ =>
            Left(
              BagVerifierError(
                s"fetch.txt refers to paths in a mismatched prefix: ${mismatchedPaths.mkString(", ")}"
              )
            )
        }
    }

  // Check that the user hasn't sent any files in the bag which
  // also have a fetch file entry.
  def verifyNoConcreteFetchEntries(
    bag: Bag,
    root: ObjectLocationPrefix,
    actualLocations: Seq[ObjectLocation],
    verificationResult: FixityListResult
  ): Either[BagVerifierError, Unit] =
    verificationResult match {
      case FixityListAllCorrect(_) =>
        val bagFetchLocations = bag.fetch match {
          case Some(bagFetch) =>
            bagFetch.paths
              .map { path: BagPath =>
                root.asLocation(path.value)
              }

          case None => Seq.empty
        }

        val concreteFetchLocations =
          bagFetchLocations
            .filter { actualLocations.contains(_) }

        if (concreteFetchLocations.isEmpty) {
          Right(())
        } else {
          val messagePrefix =
            "Files referred to in the fetch.txt also appear in the bag: "

          val internalMessage = messagePrefix + concreteFetchLocations.mkString(
            ", "
          )

          val userMessage = messagePrefix +
            concreteFetchLocations
              .map { _.path.stripPrefix(root.path).stripPrefix("/") }
              .mkString(", ")

          Left(
            BagVerifierError(
              new Throwable(internalMessage),
              userMessage = Some(userMessage)
            )
          )
        }

      case _ => Right(())
    }
}
