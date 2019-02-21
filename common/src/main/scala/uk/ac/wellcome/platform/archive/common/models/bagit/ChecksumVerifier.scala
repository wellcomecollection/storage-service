package uk.ac.wellcome.platform.archive.common.models.bagit

import com.amazonaws.services.s3.AmazonS3
import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.DigestUtils._
import org.apache.commons.codec.digest.MessageDigestAlgorithms
import uk.ac.wellcome.storage.ObjectLocation

object ChecksumVerifier {
  @throws(classOf[IllegalArgumentException])
  def checksum(objectLocation: ObjectLocation, bagItAlgorithm: String)(
    implicit s3Client: AmazonS3): String = {
    checksumAlgorithms.get(bagItAlgorithm) match {
      case Some(digestAlgorithm) =>
        checksumObject(objectLocation, digestAlgorithm)
      case None =>
        throw new IllegalArgumentException(
          f"unknown algorithm '$bagItAlgorithm'")
    }
  }

  private def checksumObject(
    objectLocation: ObjectLocation,
    digestAlgorithm: String)(implicit s3Client: AmazonS3): String = {
    // TODO: handle exceptions thrown in stream construction and proper stream closure
    val objectStream = s3Client
      .getObject(objectLocation.namespace, objectLocation.key)
      .getObjectContent
    try {
      Hex.encodeHexString(
          updateDigest(getDigest(digestAlgorithm), objectStream)
          .digest)
    } finally {
      objectStream.close()
    }
  }

  // Normalised BagIt checksum algorithms to MessageDigestAlgorithms
  private val checksumAlgorithms = Map(
    "sha256" -> MessageDigestAlgorithms.SHA_256)
}
