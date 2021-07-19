package weco.storage_service.bagit.models

import weco.storage_service.verify.{MD5, SHA1, SHA256, SHA512}

sealed trait MultiChecksumException extends RuntimeException

object MultiChecksumException {
  case object NoChecksum extends MultiChecksumException
  case object OnlyWeakChecksums extends MultiChecksumException
}

case class MultiChecksumValue[T <: HasChecksumAlgorithm](
  md5: Option[T],
  sha1: Option[T],
  sha256: Option[T],
  sha512: Option[T]
) {
  if (md5.isEmpty && sha1.isEmpty && sha256.isEmpty && sha512.isEmpty) {
    throw MultiChecksumException.NoChecksum
  }

  // We support MD5 and SHA1 for backwards compatibility (see RFC 8493 § 2.4)
  // but we require that all new bags use at least one of SHA-256 or SHA-512.
  if (sha256.isEmpty && sha512.isEmpty) {
    throw MultiChecksumException.OnlyWeakChecksums
  }

  require(md5.map(_.checksumAlgorithm).contains(MD5))
  require(sha1.map(_.checksumAlgorithm).contains(SHA1))
  require(sha256.map(_.checksumAlgorithm).contains(SHA256))
  require(sha512.map(_.checksumAlgorithm).contains(SHA512))
}
