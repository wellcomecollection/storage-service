package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerifierError
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.streaming.InputStreamWithLength
import uk.ac.wellcome.storage.{Location, Prefix}

trait VerifyBagDeclaration[BagLocation <: Location, BagPrefix <: Prefix[
  BagLocation
]] {
  protected val srcReader: Readable[BagLocation, InputStreamWithLength]

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
    println(location)

//    Right(())
////    srcReader.get(location) match {
////      case Right(stream)
////      case Left(err) =>
////        Left(BagVerifierError(s"Unable to read bagit.txt: $err"))
////    }
//  }
}
