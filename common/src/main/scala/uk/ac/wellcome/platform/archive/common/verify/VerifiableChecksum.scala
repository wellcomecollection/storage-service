package uk.ac.wellcome.platform.archive.common.verify

/** This covers the different types of checksum we might expect to see in
  * a bag manifest.
  *
  * In the storage service, we _require_ that the user give us a SHA256 manifest.
  * If there are other manifests in the bag, we might verify them at some later date;
  * for now, just track the checksums internally.
  *
  */
case class VerifiableChecksum(
  md5: Option[ChecksumValue] = None,
  sha1: Option[ChecksumValue] = None,
  sha256: ChecksumValue,
  sha512: Option[ChecksumValue] = None
)
