package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerifierError

trait VerifyLegalFilenames {
  def verifyLegalFilenames(filenames: Seq[String]): Either[BagVerifierError, Unit] = {
    // Azure blob storage does not support blob names that end with a `.`
    // See https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#resource-names
    val endsWithADot = filenames.filter { _.endsWith(".") }

    // Combine the results into a single message that can be logged.
    val results = Map(
      "Filenames cannot end with a ." -> endsWithADot,
    )

    val logMessage = results
      .map { case (reason, badFilenames) =>
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
}
