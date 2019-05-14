package uk.ac.wellcome.platform.archive.bagverifier.services

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.verify._

import scala.util.Try

class S3ObjectVerifier(implicit s3Client: AmazonS3) extends Verifier with Logging {
  import uk.ac.wellcome.platform.archive.common.storage.services.S3StreamableInstances._

  def verify(location: VerifiableLocation): VerifiedLocation = {
    val tryVerify = for {
      inputStream <- location.objectLocation.toInputStream
      checksum <- Checksum.create(
        inputStream,
        location.checksum.algorithm)
      result <- Try(assert(checksum == location.checksum))
    } yield result

    tryVerify
      .map(_ => VerifiedSuccess(location))
      .recover { case e => VerifiedFailure(location, e) }
      .getOrElse(VerifiedFailure(location, new UnknownError()))
  }
}
