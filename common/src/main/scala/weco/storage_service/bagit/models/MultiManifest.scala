package weco.storage_service.bagit.models

import weco.storage_service.storage.models.FileManifest
import weco.storage_service.verify.{MD5, SHA1, SHA256, SHA512}

sealed trait MultiManifestException extends RuntimeException

object MultiManifestException {
  case object NoManifest extends MultiManifestException
  case object OnlyWeakChecksums extends MultiManifestException
}

case class MultiManifest(
  md5: Option[FileManifest],
  sha1: Option[FileManifest],
  sha256: Option[FileManifest],
  sha512: Option[FileManifest]
) {
  if (md5.isEmpty && sha1.isEmpty && sha256.isEmpty && sha512.isEmpty) {
    throw MultiManifestException.NoManifest
  }

  // We support MD5 and SHA1 for backwards compatibility (see RFC 8493 § 2.4)
  // but we require that all new bags use at least one of SHA-256 or SHA-512.
  if (sha256.isEmpty && sha512.isEmpty) {
    throw MultiManifestException.OnlyWeakChecksums
  }

  require(md5.map(_.checksumAlgorithm).contains(MD5))
  require(sha1.map(_.checksumAlgorithm).contains(SHA1))
  require(sha256.map(_.checksumAlgorithm).contains(SHA256))
  require(sha512.map(_.checksumAlgorithm).contains(SHA512))
}
