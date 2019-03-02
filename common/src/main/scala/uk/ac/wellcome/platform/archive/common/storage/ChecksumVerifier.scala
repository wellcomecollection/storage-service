package uk.ac.wellcome.platform.archive.common.storage

import java.io.InputStream

import grizzled.slf4j.Logging
import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.DigestUtils._

import scala.util.Try

object ChecksumVerifier extends Logging {

  def checksum(
    inputStream: InputStream,
    algorithm: String
  ): Try[String] = Try {
     Hex.encodeHexString(
      updateDigest(
        getDigest(algorithm),
        inputStream
      ).digest
    )
  }
}
