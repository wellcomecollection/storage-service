package uk.ac.wellcome.platform.archive.common.verify

import java.io.InputStream

import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.DigestUtils.{getDigest, updateDigest}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.Try


case class VerifiableLocation(
                               objectLocation: ObjectLocation,
                               checksum: Checksum
                             )


case class Checksum(
                     algorithm: ChecksumAlgorithm,
                     value: ChecksumValue
                   )
object Checksum {
  def create(
             inputStream: InputStream,
             algorithm: ChecksumAlgorithm
           ): Try[Checksum] = Try {
    Checksum(
      algorithm,
      ChecksumValue(inputStream, algorithm)
    )
  }
}

case class ChecksumAlgorithm(value: String) {
  override def toString: String = value
}

case class ChecksumValue(value: String)
object ChecksumValue {

  def apply(
                inputStream: InputStream,
                algorithm: ChecksumAlgorithm
              ): ChecksumValue = {
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