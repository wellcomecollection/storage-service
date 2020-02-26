package uk.ac.wellcome.platform.archive.common.verify

import org.apache.commons.codec.digest.MessageDigestAlgorithms

sealed trait HashingAlgorithm {
  val value: String
  val pathRepr: String
  override def toString: String = value
}

/** The BagIt spec requires that BagIt parsers support SHA-256 and SHA-512.
  * It additionally mentions the MD5 and SHA-1 algorithms, but these should
  * not be used for new bags.
  *
  * See https://tools.ietf.org/html/rfc8493#section-2.4
  *
  */
case object SHA512 extends HashingAlgorithm {
  val value: String = MessageDigestAlgorithms.SHA_512
  val pathRepr = "sha512"
}

case object SHA256 extends HashingAlgorithm {
  val value: String = MessageDigestAlgorithms.SHA_256
  val pathRepr = "sha256"
}

case object SHA1 extends HashingAlgorithm {
  val value: String = MessageDigestAlgorithms.SHA_1
  val pathRepr = "sha1"
}

case object MD5 extends HashingAlgorithm {
  val value: String = MessageDigestAlgorithms.MD5
  val pathRepr = "md5"
}
