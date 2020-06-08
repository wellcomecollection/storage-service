package uk.ac.wellcome.platform.archive.bagverifier.fixity

import uk.ac.wellcome.storage.ObjectLocation

sealed trait FileFixityResult {
  val expectedFileFixity: ExpectedFileFixity
}

sealed trait FileFixityError extends FileFixityResult {
  val e: Throwable
}

case class FileFixityCorrect(
  expectedFileFixity: ExpectedFileFixity,
  objectLocation: ObjectLocation,

  // We record the size of the files as we verify them, so we can verify
  // the Payload-Oxum in the bag metadata.
  size: Long
) extends FileFixityResult

case class FileFixityMismatch(
  expectedFileFixity: ExpectedFileFixity,
  objectLocation: ObjectLocation,
  e: Throwable
) extends FileFixityError

case class FileFixityCouldNotRead(
  expectedFileFixity: ExpectedFileFixity,
  e: Throwable
) extends FileFixityError
