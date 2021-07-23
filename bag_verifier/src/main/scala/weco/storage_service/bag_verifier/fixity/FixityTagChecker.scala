package weco.storage_service.bag_verifier.fixity

import weco.storage_service.checksum.{ChecksumAlgorithm, ChecksumValue}

trait FixityTagChecker {

  // e.g. Content-MD5, Content-SHA256
  protected def fixityTagName(algorithm: ChecksumAlgorithm): String =
    s"Content-${algorithm.pathRepr.toUpperCase}"

  protected def fixityTagValue(value: ChecksumValue): String =
    value.toString
}
