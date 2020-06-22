package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{FixityListAllCorrect, FixityListResult}
import uk.ac.wellcome.platform.archive.common.bagit.models.UnreferencedFiles
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

trait VerifyNoUnreferencedFiles extends Step with Logging {

  // Files that it's okay not to be referenced by any other manifests/files.
  //
  // We ignore the tag manifests because they're not referred to by the
  // checksum lists in any other manifests:
  //
  //        (tag manifest) -> (file manifest) -> (files)
  //
  // We don't ignore the file manifests (e.g. manifest-md5.txt), because
  // those should be included in the SHA256 tag manifest.  Every tag manifest
  // should include checksums for every file manifest.
  //
  private val excludedFiles = UnreferencedFiles.tagManifestFiles

  // Check that there aren't any files in the bag that aren't referenced in
  // either the file manifest or the tag manifest.
  def verifyNoUnreferencedFiles(
    root: ObjectLocationPrefix,
    actualLocations: Seq[ObjectLocation],
    verificationResult: FixityListResult
  ): InternalResult[Unit] =
    verificationResult match {
      case FixityListAllCorrect(locations) =>
        val expectedLocations = locations.map { _.objectLocation }

        debug(s"Expecting the bag to contain: $expectedLocations")

        val unreferencedFiles = actualLocations
          .filterNot { expectedLocations.contains(_) }
          .filterNot { location =>
            excludedFiles.exists { root.asLocation(_) == location }
          }

        if (unreferencedFiles.isEmpty) {
          Right(())
        } else {
          // For internal logging, we want a message that contains the full
          // S3 locations for easy debugging, e.g.:
          //
          //    Bag contains 5 files which are not referenced in the manifest:
          //    bukkit/ingest-id/bag-id/unreferenced1.txt, ...
          //
          // For the user-facing message, we want to trim the first part,
          // because it's an internal detail of the storage service, e.g.:
          //
          //    Bag contains 5 files which are not referenced in the manifest:
          //    unreferenced1.txt, ...
          //
          val messagePrefix =
          if (unreferencedFiles.size == 1) {
            "Bag contains a file which is not referenced in the manifest: "
          } else {
            s"Bag contains ${unreferencedFiles.size} files which are not referenced in the manifest: "
          }

          val userMessage = messagePrefix +
            unreferencedFiles
              .map { _.path.stripPrefix(root.path) }
              .mkString(", ")

          Left(
            BagVerifierError(
              new Throwable(messagePrefix + unreferencedFiles.mkString(", ")),
              userMessage = Some(userMessage)
            )
          )
        }

      case _ => Right(())
    }
}
