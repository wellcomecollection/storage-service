package weco.storage_service.verify

import grizzled.slf4j.Logging

import java.io.InputStream
import scala.util.Try

case class Checksum(
  algorithm: HashingAlgorithm,
  value: ChecksumValue
) {
  override def toString: String =
    s"${algorithm.pathRepr}:$value"
}

case object Checksum extends Logging {
  def create(
    inputStream: InputStream,
    algorithm: HashingAlgorithm
  ): Try[Checksum] = {
    debug(s"Creating Checksum for $inputStream with $algorithm")
    val checksum = Hasher
      .hash(inputStream)
      .map { _.getChecksumValue(algorithm) }
      .map { Checksum(algorithm, _) }

    debug(s"Got: $checksum")
    checksum
  }
}
