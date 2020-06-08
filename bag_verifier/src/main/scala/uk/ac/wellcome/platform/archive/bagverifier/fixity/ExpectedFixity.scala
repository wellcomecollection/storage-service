package uk.ac.wellcome.platform.archive.bagverifier.fixity

/** Given some Container of files, get the expected fixity information (size/checksum)
  * for every file in the container.
  *
  */
trait ExpectedFixity[Container] {
  def create(container: Container): Either[CannotCreateExpectedFixity, Seq[ExpectedFileFixity]]
}

case class CannotCreateExpectedFixity(msg: String) extends Throwable(msg)
