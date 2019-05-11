package uk.ac.wellcome.platform.archive.common.storage.services

import java.io.InputStream

import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.DigestUtils._
import uk.ac.wellcome.platform.archive.common.storage.models.ChecksumAlgorithm
import uk.ac.wellcome.platform.archive.common.verify.ChecksumValue

import scala.util.Try

object ChecksumVerifier {

  def checksum(
                inputStream: InputStream,
                algorithm: ChecksumAlgorithm
              ): Try[ChecksumValue] = Try {
    ChecksumValue(
      Hex.encodeHexString(
        updateDigest(
          getDigest(algorithm.value),
          inputStream
        ).digest
      )
    )
  }
}
