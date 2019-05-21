package uk.ac.wellcome.platform.archive.bagverifier.services

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.verify._

import scala.util.{Failure, Success}

class S3ObjectVerifier(implicit s3Client: AmazonS3)
    extends Verifier
    with Logging {
  import uk.ac.wellcome.platform.archive.common.storage.services.StreamableInstances._

  private def compareChecksum(a: Checksum, b: Checksum) = {
    debug(s"Comparing $a, $b")

    val result = if (a.value == b.value && a.algorithm == b.algorithm) {
      Success(())
    } else {
      Failure(
        new RuntimeException(s"Checksum values do not match: $a != $b")
      )
    }

    debug(s"Got: $result")

    result
  }

  def verify(location: VerifiableLocation): VerifiedLocation = {
    debug(s"Attempting to verify: $location")
    val tryVerify = for {
      inputStream <- location.objectLocation.toInputStream
      checksum <- Checksum.create(inputStream, location.checksum.algorithm)
      result <- compareChecksum(checksum, location.checksum)
    } yield result

    tryVerify
      .map(_ => VerifiedSuccess(location))
      .recover { case e => VerifiedFailure(location, e) }
      .getOrElse(VerifiedFailure(location, new UnknownError()))
  }
}
