package uk.ac.wellcome.platform.archive.common.verify

import java.io.InputStream
import java.net.URI

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.LocateFailure
import uk.ac.wellcome.storage.{ObjectLocation, ReadError}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.HasLength

import scala.util.{Failure, Success}

trait Verifier[IS <: InputStream with HasLength] extends Logging {
  protected val streamStore: StreamStore[ObjectLocation, IS]

  def locate(uri: URI): Either[LocateFailure[URI], ObjectLocation]

  def verify(verifiableLocation: VerifiableLocation): VerifiedLocation = {
    debug(s"Attempting to verify: $verifiableLocation")

    val algorithm = verifiableLocation.checksum.algorithm

    val eitherInputStream = for {
      objectLocation <- locate(verifiableLocation.uri) match {
        case Right(l) => Right(l)
        case Left(locateError) => Left(
          VerificationLocationError(verifiableLocation, locateError)
        )
      }

      inputStream <- streamStore.get(objectLocation) match {
        case Right(stream) => Right(stream.identifiedT)
        case Left(readError: ReadError) => Left(
          VerificationReadError(verifiableLocation, readError))
      }

    } yield (inputStream, objectLocation)

    val result = eitherInputStream match {
      case Left(verificationError) => VerifiedFailure(verificationError)

      case Right((inputStream, objectLocation)) =>
        val verifiedLocation = verifiableLocation.length match {
          case Some(expectedLength) =>
            debug(
              "Location specifies an expected length, checking it's correct"
            )

            if (expectedLength == inputStream.length) {
              verifyChecksum(
                verifiableLocation = verifiableLocation,
                objectLocation = objectLocation,
                inputStream = inputStream,
                algorithm = algorithm
              )
            } else {
              VerifiedFailure(
                objectLocation,
                VerificationLengthsDoNotMatch(
                  verifiableLocation = verifiableLocation,
                  expectedLength = expectedLength,
                  actualLength = inputStream.available()
                )
              )
            }

          case None =>
            verifyChecksum(
              verifiableLocation = verifiableLocation,
              objectLocation = objectLocation,
              inputStream = inputStream,
              algorithm = algorithm
            )
        }

        inputStream.close()

        verifiedLocation
    }

    debug(s"Got: $result")
    result
  }

  private def verifyChecksum(
    verifiableLocation: VerifiableLocation,
    objectLocation: ObjectLocation,
    inputStream: IS,
    algorithm: HashingAlgorithm
  ): VerifiedLocation =
    Checksum.create(inputStream, algorithm) match {
      // Failure to create a checksum (parsing/algorithm)
      case Failure(e) =>
        VerifiedFailure(
          objectLocation,
          VerificationChecksumError(
            verifiableLocation,
            FailedChecksumCreation(algorithm, e)
          )
        )

      // Checksum does not match that provided
      case Success(checksum) =>
        if (checksum != verifiableLocation.checksum)
          VerifiedFailure(
            objectLocation,
            VerificationChecksumError(
              verifiableLocation,
              FailedChecksumNoMatch(
                actual = checksum,
                expected = verifiableLocation.checksum
              )
            )
          )
        else
          VerifiedSuccess(
            verifiableLocation,
            objectLocation,
            size = inputStream.length
          )
    }
}
