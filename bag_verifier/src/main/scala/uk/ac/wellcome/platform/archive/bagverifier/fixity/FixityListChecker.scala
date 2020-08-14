package uk.ac.wellcome.platform.archive.bagverifier.fixity

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.fixity.s3.S3FixityChecker
import uk.ac.wellcome.storage.azure.AzureBlobLocation
import uk.ac.wellcome.storage.{Location, Prefix}

/** Given some Container of files, get the expected fixity information for every
  * file in the container, then verify the fixity on each of them.
  *
  */
class FixityListChecker[BagLocation <: Location, BagPrefix <: Prefix[
  BagLocation
], Container](
  implicit
  verifiable: ExpectedFixity[Container],
  s3Client: AmazonS3,
  dataDirectoryFixityChecker: FixityChecker[BagLocation, BagPrefix]
) extends Logging {
  val fetchEntriesFixityChecker = new S3FixityChecker()

  def check(container: Container): FixityListResult[BagLocation] = {
    debug(s"Checking the fixity info for $container")
    verifiable.create(container) match {
      case Left(err) => CouldNotCreateExpectedFixityList(err.msg)
      case Right(expectedFileFixities) =>
        expectedFileFixities
          .map {
            case f: FetchFileFixity => fetchEntriesFixityChecker.check(f)
            case d: DataDirectoryFileFixity =>
              dataDirectoryFixityChecker.check(d)
          }
          // Although it's annoying if we can't write a fixity tag for an object,
          // it shouldn't block the verification of the bag.  Adding the fixity tags
          // isn't a required step to store a bag in the archive.
          //
          // Azure does not allow us to write the same fixity tags that we use in S3,
          // so for now we ignore these errors rather than write mismatched tags.
          // See https://github.com/wellcomecollection/platform/issues/4730
          .map {
            case FileFixityCouldNotWriteTag(expectedFileFixity, location: AzureBlobLocation, e, size) =>
              warn(s"Ignoring error writing fixity tags to Azure at $location: $e")
              FileFixityCorrect(
                expectedFileFixity = expectedFileFixity,
                location = location,
                size = size
              )

            case result => result
          }
          .foldLeft[FixityListCheckingResult[BagLocation]](
            FixityListAllCorrect(Nil)
          ) {

            case (
                existingCorrect: FixityListAllCorrect[BagLocation],
                newCorrect: FileFixityCorrect[BagLocation]
                ) =>
              FixityListAllCorrect(newCorrect :: existingCorrect.locations)

            case (
                correct: FixityListAllCorrect[BagLocation],
                err: FileFixityError[BagLocation]
                ) =>
              FixityListWithErrors(
                errors = List(err),
                correct = correct.locations
              )

            case (
                existingErrors: FixityListWithErrors[BagLocation],
                c: FileFixityCorrect[BagLocation]
                ) =>
              FixityListWithErrors(
                existingErrors.errors,
                c :: existingErrors.correct
              )

            case (
                existingErrors: FixityListWithErrors[BagLocation],
                err: FileFixityError[BagLocation]
                ) =>
              FixityListWithErrors(
                errors = err :: existingErrors.errors,
                correct = existingErrors.correct
              )
          }
    }
  }
}
