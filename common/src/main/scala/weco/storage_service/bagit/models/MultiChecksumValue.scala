package weco.storage_service.bagit.models

sealed trait MultiChecksumException extends RuntimeException

object MultiChecksumException {
  case object NoChecksum extends MultiChecksumException
  case object OnlyWeakChecksums extends MultiChecksumException
}

case class MultiChecksumValue[T](
  md5: Option[T] = None,
  sha1: Option[T] = None,
  sha256: Option[T] = None,
  sha512: Option[T] = None
) {
  if (md5.isEmpty && sha1.isEmpty && sha256.isEmpty && sha512.isEmpty) {
    throw MultiChecksumException.NoChecksum
  }

  // We support MD5 and SHA1 for backwards compatibility (see RFC 8493 § 2.4)
  // but we require that all new bags use at least one of SHA-256 or SHA-512.
  if (sha256.isEmpty && sha512.isEmpty) {
    throw MultiChecksumException.OnlyWeakChecksums
  }
}
