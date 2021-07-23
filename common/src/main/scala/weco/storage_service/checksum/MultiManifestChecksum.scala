package weco.storage_service.checksum

sealed trait MultiManifestChecksumException extends RuntimeException

object MultiManifestChecksumException {
  case object NoChecksums extends MultiManifestChecksumException
  case object OnlyDeprecatedChecksums extends MultiManifestChecksumException
}

/** This class records the expected checksum of a file in a bag, based on the
  * manifests provided in the bag.  It can record checksums for multiple checksum
  * algorithms, e.g. a file in an MD5 manifest and a SHA-256 manifest.
  *
  * Note: the omission of default values is deliberate here -- it's so that if we add
  * a new checksum algorithm, we remember to add it everywhere we use MultiManifestChecksum.
  *
  */
case class MultiManifestChecksum(
  md5: Option[ChecksumValue],
  sha1: Option[ChecksumValue],
  sha256: Option[ChecksumValue],
  sha512: Option[ChecksumValue]
) {

  def definedAlgorithms: Set[ChecksumAlgorithm] =
    Set(
      md5.map(_ => MD5),
      sha1.map(_ => SHA1),
      sha256.map(_ => SHA256),
      sha512.map(_ => SHA512)
    ).flatten

  def getValue(algorithm: ChecksumAlgorithm): Option[ChecksumValue] =
    algorithm match {
      case MD5    => md5
      case SHA1   => sha1
      case SHA256 => sha256
      case SHA512 => sha512
    }

  if (definedAlgorithms.isEmpty) {
    throw MultiManifestChecksumException.NoChecksums
  }

  if (definedAlgorithms.forall(_.isForBackwardsCompatibilityOnly)) {
    throw MultiManifestChecksumException.OnlyDeprecatedChecksums
  }
}
