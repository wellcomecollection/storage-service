package uk.ac.wellcome.platform.archive.bagverifier.generators

import java.net.URI

import uk.ac.wellcome.platform.archive.bagverifier.fixity.{
  DataDirectoryFileFixity,
  ExpectedFileFixity
}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagPath
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, MD5, SHA256}
import uk.ac.wellcome.storage.Location

trait FixityGenerators[BagLocation <: Location] extends StorageRandomThings {
  def randomChecksum = Checksum(SHA256, randomChecksumValue)
  def badChecksum = Checksum(MD5, randomChecksumValue)

  def resolve(location: BagLocation): URI

  def createExpectedFileFixity: ExpectedFileFixity =
    createDataDirectoryFileFixityWith()

  def createLocation: BagLocation

  def createDataDirectoryFileFixityWith(
    location: BagLocation = createLocation,
    checksum: Checksum = randomChecksum,
    length: Option[Long] = None
  ): DataDirectoryFileFixity =
    DataDirectoryFileFixity(
      uri = resolve(location),
      path = BagPath(randomAlphanumeric),
      checksum = checksum,
      length = length
    )
}
