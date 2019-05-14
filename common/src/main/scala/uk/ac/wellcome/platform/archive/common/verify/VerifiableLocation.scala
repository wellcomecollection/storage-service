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
                     algorithm: HashingAlgorithm,
                     value: ChecksumValue
                   )
object Checksum {
  def create(
             inputStream: InputStream,
             algorithm: HashingAlgorithm
           ): Try[Checksum] = Try {
    Checksum(
      algorithm,
      ChecksumValue(inputStream, algorithm)
    )
  }
}

sealed trait HashingAlgorithm {
  val value: String
  override def toString: String = value
}

case class ChecksumAlgorithm(value: String) extends HashingAlgorithm

case object SHA256 extends HashingAlgorithm {
  val value = "SHA-256"
}

case object MD5 extends HashingAlgorithm {
  val value = "MD5"
}

case class ChecksumValue(value: String)
object ChecksumValue {
  def apply(
                inputStream: InputStream,
                algorithm: HashingAlgorithm
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