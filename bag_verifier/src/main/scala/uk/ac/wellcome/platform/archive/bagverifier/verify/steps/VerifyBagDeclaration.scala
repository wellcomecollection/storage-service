package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerifierError
import weco.storage.store.Readable
import weco.storage.streaming.InputStreamWithLength
import uk.ac.wellcome.storage.{Identified, Location, NotFoundError, Prefix}
import weco.storage.streaming.Codec._

import scala.util.matching.Regex

trait VerifyBagDeclaration[BagLocation <: Location, BagPrefix <: Prefix[
  BagLocation
]] {
  protected val streamReader: Readable[BagLocation, InputStreamWithLength]

  private val declaration = new Regex(
    "BagIt-Version: \\d\\.\\d+\nTag-File-Character-Encoding: UTF-8\n?"
  )

  // Quoting from the BagIt spec (https://tools.ietf.org/html/rfc8493#section-2.1.1):
  //
  //    The "bagit.txt" tag file MUST consist of exactly two lines in this
  //    order:
  //
  //    BagIt-Version: M.N
  //    Tag-File-Character-Encoding: ENCODING
  //
  // The spec requires that M.N be the BagIt version, and ENCODING be any encoding
  // recognised by the IANA.  We are slightly more restrictive -- we require that
  // this be "UTF-8".
  //
  def verifyBagDeclaration(root: BagPrefix): Either[BagVerifierError, Unit] = {
    val location = root.asLocation("bagit.txt")

    streamReader.get(location) match {

      // If the bagit.txt file is too big, something is definitely wrong.
      // We'll be loading the whole file into memory because it should be small;
      // if it's not, don't explode with an out-of-memory.
      case Right(Identified(_, inputStream)) if inputStream.length > 1000 =>
        inputStream.close()
        Left(
          BagVerifierError(
            "Error loading Bag Declaration (bagit.txt): too large"
          )
        )

      case Right(Identified(_, inputStream)) =>
        stringCodec.fromStream(inputStream) match {
          case Left(_) =>
            Left(
              BagVerifierError(
                "Error loading Bag Declaration (bagit.txt): could not be decoded as UTF-8"
              )
            )

          case Right(declaration()) =>
            Right(())

          case Right(_) =>
            Left(
              BagVerifierError(
                "Error loading Bag Declaration (bagit.txt): not correctly formatted"
              )
            )
        }

      case Left(err: NotFoundError) =>
        Left(
          BagVerifierError(
            err.e,
            userMessage =
              Some("Error loading Bag Declaration (bagit.txt): no such file!")
          )
        )

      case Left(err) =>
        Left(
          BagVerifierError(
            err.e,
            userMessage = Some("Error loading Bag Declaration (bagit.txt)")
          )
        )
    }
  }
}
