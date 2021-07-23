package weco.storage_service.checksum

case class MismatchedChecksum(
  algorithm: ChecksumAlgorithm,
  expected: ChecksumValue,
  actual: ChecksumValue
) {
  require(expected != actual)

  def description: String =
    s"expected ${algorithm.pathRepr}:$expected, saw ${algorithm.pathRepr}:$actual"
}
