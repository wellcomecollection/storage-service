package uk.ac.wellcome.platform.archive.bagverifier.fixtures

import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.storage.services.{S3ObjectVerifier, S3Resolvable}
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3

trait VerifyFixture extends S3 with RandomThings {

  implicit val objectVerifier = new S3ObjectVerifier()

  implicit val s3Resolvable = new S3Resolvable()
  import uk.ac.wellcome.platform.archive.common.storage.Resolvable._

  def randomChecksumValue = ChecksumValue(randomAlphanumeric())
  def randomChecksum = Checksum(SHA256, randomChecksumValue)
  def badChecksum = Checksum(MD5, randomChecksumValue)

  def randomObjectLocation = createObjectLocation

  def verifiableLocationWith(location: ObjectLocation, checksum: Checksum) =
    verifiableLocation(location = Some(location), checksum = Some(checksum))
  def verifiableLocationWith(location: ObjectLocation) =
    verifiableLocation(location = Some(location), checksum = None)
  def verifiableLocationWith(checksum: Checksum) =
    verifiableLocation(None, checksum = Some(checksum))

  def verifiableLocation(
    location: Option[ObjectLocation] = None,
    checksum: Option[Checksum] = None
  ) = {
    VerifiableLocation(
      location.getOrElse(randomObjectLocation).resolve,
      checksum.getOrElse(randomChecksum)
    )
  }
}
