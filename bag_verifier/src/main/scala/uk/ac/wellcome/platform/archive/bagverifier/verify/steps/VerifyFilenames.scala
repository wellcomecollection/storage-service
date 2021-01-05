package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerifierError
import uk.ac.wellcome.platform.archive.common.bagit.models.PayloadManifest

trait VerifyFilenames {
  def verifyAllowedFilenames(
    filenames: Seq[String]
  ): Either[BagVerifierError, Unit] = {
    // Azure blob storage does not support blob names that end with a `.`
    // See https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#resource-names
    val endsWithADot = filenames.filter { _.endsWith(".") }

    // Combine the results into a single message that can be logged.
    val results = Map(
      "Filenames cannot end with a ." -> endsWithADot
    )

    val logMessage = results
      .map {
        case (reason, badFilenames) =>
          if (badFilenames.isEmpty) {
            ""
          } else {
            s"$reason: ${badFilenames.mkString(", ")}"
          }
      }
      .filterNot { _.isEmpty }
      .mkString("; ")

    logMessage match {
      case ""      => Right(())
      case message => Left(BagVerifierError(message))
    }
  }

  // The BagIt spec says that payload files should exclusively be stored
  // in the "data/" directory.
  //
  // See https://tools.ietf.org/html/rfc8493#section-2.1.2
  def verifyPayloadFilenames(manifest: PayloadManifest): Either[BagVerifierError, Unit] = {
    val paths = manifest.entries.map { case (path, _) => path.value }
    val badPaths = paths.filterNot { _.startsWith("data/") }

    if (badPaths.isEmpty) {
      Right(())
    } else {
      Left(
        BagVerifierError(
          s"Not all payload files are in the data/ directory: ${badPaths.mkString(", ")}"
        )
      )
    }
  }
}
