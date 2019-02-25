package uk.ac.wellcome.platform.archive.common.storage

import java.io.InputStream

import grizzled.slf4j.Logging
import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.DigestUtils._

import scala.concurrent.{ExecutionContext, Future}

object ChecksumVerifier extends Logging {

  def checksum(
    inputStream: InputStream,
    algorithm: String
  )(implicit ec: ExecutionContext): Future[String] = Future {
    tryChecksum(inputStream, algorithm)
  }

  private def tryChecksum(
    objectStream: InputStream,
    digestAlgorithm: String
  ) = {
    val checksum = Hex.encodeHexString(
      updateDigest(
        getDigest(digestAlgorithm),
        objectStream
      ).digest
    )

    // We are done with the stream here so close it.
    objectStream.close()

    checksum
  }
}
