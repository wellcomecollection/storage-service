package uk.ac.wellcome.platform.archive.common.fixtures

import java.net.URI

import uk.ac.wellcome.platform.archive.common.bagit.models.BagPath
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

trait VerifyFixtures extends StorageRandomThings with ObjectLocationGenerators {
  def randomChecksumValue = ChecksumValue(randomAlphanumericWithLength())
  def randomChecksum = Checksum(SHA256, randomChecksumValue)
  def badChecksum = Checksum(MD5, randomChecksumValue)

  def createVerifiableLocation: VerifiableLocation =
    createVerifiableLocationWith()

  def resolve(location: ObjectLocation): URI

  def createVerifiableLocationWith(
    location: ObjectLocation = createObjectLocation,
    checksum: Checksum = randomChecksum,
    length: Option[Long] = None
  ): VerifiableLocation = {
    VerifiableLocation(
      uri = resolve(location),
      path = BagPath(randomAlphanumeric),
      checksum = checksum,
      length = length
    )
  }
}
