package weco.storage_service.checksum

sealed trait MultiManifestChecksumException extends RuntimeException

object MultiManifestChecksumException {
  case object NoChecksums extends MultiManifestChecksumException
  case object OnlyDeprecatedChecksums extends MultiManifestChecksumException
}

/** This class records checksum information about a file that has checksums
  * in multiple manifests, e.g. a checksum in an MD5 manifest and a SHA-256 manifest.
  *
  */
case class MultiManifestChecksum(
  md5: Option[ChecksumValue] = None,
  sha1: Option[ChecksumValue] = None,
  sha256: Option[ChecksumValue] = None,
  sha512: Option[ChecksumValue] = None
) {

  def definedAlgorithms: Seq[ChecksumAlgorithm] =
    Seq(
      md5.map(_ => MD5),
      sha1.map(_ => SHA1),
      sha256.map(_ => SHA256),
      sha512.map(_ => SHA512)
    ).flatten

  if (definedAlgorithms.isEmpty) {
    throw MultiManifestChecksumException.NoChecksums
  }

  if (definedAlgorithms.forall(_.isForBackwardsCompatibilityOnly)) {
    throw MultiManifestChecksumException.OnlyDeprecatedChecksums
  }
}
