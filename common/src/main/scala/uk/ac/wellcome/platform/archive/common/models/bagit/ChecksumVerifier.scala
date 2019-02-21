package uk.ac.wellcome.platform.archive.common.models.bagit

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectInputStream
import grizzled.slf4j.Logging
import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.DigestUtils._
import org.apache.commons.codec.digest.MessageDigestAlgorithms
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

object ChecksumVerifier extends Logging {

  def checksum(
    objectLocation: ObjectLocation,
    bagItAlgorithm: String
  )(implicit
    s3Client: AmazonS3): Either[Throwable, String] = {
    val triedChecksum: Try[String] = for {
      digestAlgorithm <- getChecksum(
        bagItAlgorithm
      )

      checksum <- checksumObject(
        objectLocation,
        digestAlgorithm
      )

    } yield checksum

    triedChecksum.toEither
  }

  private def checksumObject(
    objectLocation: ObjectLocation,
    digestAlgorithm: String
  )(implicit s3Client: AmazonS3) =
    for {
      objectContent <- tryObjectContent(
        s3Client,
        objectLocation
      )

      checksum: String <- tryChecksum(
        objectContent,
        digestAlgorithm
      )

    } yield checksum

  private def tryObjectContent(
    s3Client: AmazonS3,
    objectLocation: ObjectLocation
  ): Try[S3ObjectInputStream] = Try {
    s3Client
      .getObject(objectLocation.namespace, objectLocation.key)
      .getObjectContent
  }

  private def tryChecksum(
    objectStream: S3ObjectInputStream,
    digestAlgorithm: String
  ): Try[String] = {
    val triedChecksum = Try {
      Hex.encodeHexString(
        updateDigest(
          getDigest(digestAlgorithm),
          objectStream
        ).digest
      )
    }

    // We are done with the stream here so close it.
    objectStream.close()

    triedChecksum
  }

  private def getChecksum(checksumAlgorithm: String) = {
    checksumAlgorithms.get(checksumAlgorithm) match {
      case Some(algo) => Success(algo)
      case None =>
        Failure(
          new RuntimeException(s"Unknown algorithm: $checksumAlgorithm")
        )
    }
  }

  // Normalised BagIt checksum algorithms
  // to MessageDigestAlgorithms
  private val checksumAlgorithms = Map(
    "sha256" -> MessageDigestAlgorithms.SHA_256
  )
}
