package uk.ac.wellcome.platform.archive.bagverifier.fixtures

import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.storage.services.{
  S3ObjectVerifier,
  S3Resolvable
}
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures

trait VerifyFixtures extends S3Fixtures with StorageRandomThings {

  implicit val objectVerifier = new S3ObjectVerifier()

  implicit val s3Resolvable = new S3Resolvable()
  import uk.ac.wellcome.platform.archive.common.storage.Resolvable._

  def randomChecksumValue = ChecksumValue(randomAlphanumericWithLength())
  def randomChecksum = Checksum(SHA256, randomChecksumValue)
  def badChecksum = Checksum(MD5, randomChecksumValue)

  def createVerifiableLocation: VerifiableLocation =
    createVerifiableLocationWith()

  def createVerifiableLocationWith(
    location: ObjectLocation = createObjectLocation,
    checksum: Checksum = randomChecksum,
    length: Option[Long] = None
  ): VerifiableLocation = {
    VerifiableLocation(
      uri = location.resolve,
      checksum = checksum,
      length = length
    )
  }
}
