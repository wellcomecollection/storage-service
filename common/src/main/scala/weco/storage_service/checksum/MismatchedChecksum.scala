package weco.storage_service.checksum

case class MismatchedChecksum(
  algorithm: ChecksumAlgorithm,
  expected: ChecksumValue,
  actual: ChecksumValue
)
