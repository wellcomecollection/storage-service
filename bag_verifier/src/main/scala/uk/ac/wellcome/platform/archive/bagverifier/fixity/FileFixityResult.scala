package uk.ac.wellcome.platform.archive.bagverifier.fixity

sealed trait FileFixityResult[BagLocation] {
  val expectedFileFixity: ExpectedFileFixity
}

sealed trait FileFixityError[BagLocation] extends FileFixityResult[BagLocation] {
  val e: Throwable
}

case class FileFixityCorrect[BagLocation](
  expectedFileFixity: ExpectedFileFixity,
  objectLocation: BagLocation,
  // We record the size of the files as we verify them, so we can verify
  // the Payload-Oxum in the bag metadata.
  size: Long
) extends FileFixityResult[BagLocation]

case class FileFixityMismatch[BagLocation](
  expectedFileFixity: ExpectedFileFixity,
  objectLocation: BagLocation,
  e: Throwable
) extends FileFixityError[BagLocation]

case class FileFixityCouldNotRead[BagLocation](
  expectedFileFixity: ExpectedFileFixity,
  e: Throwable
) extends FileFixityError[BagLocation]

case class FileFixityCouldNotGetChecksum[BagLocation](
  expectedFileFixity: ExpectedFileFixity,
  objectLocation: BagLocation,
  e: Throwable
) extends FileFixityError[BagLocation]

case class FileFixityCouldNotWriteTag[BagLocation](
  expectedFileFixity: ExpectedFileFixity,
  objectLocation: BagLocation,
  e: Throwable
) extends FileFixityError[BagLocation]
