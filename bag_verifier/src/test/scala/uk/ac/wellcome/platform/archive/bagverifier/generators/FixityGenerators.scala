package uk.ac.wellcome.platform.archive.bagverifier.generators

import java.net.URI

import uk.ac.wellcome.platform.archive.bagverifier.fixity.{
  DataDirectoryFileFixity,
  ExpectedFileFixity,
  FetchFileFixity
}
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

  def createFetchFileFixityWith(
    location: BagLocation = createLocation,
    checksum: Checksum = randomChecksum,
    length: Option[Long] = None
  ): FetchFileFixity =
    FetchFileFixity(
      uri = resolve(location),
      path = createBagPath,
      checksum = checksum,
      length = length
    )

  def createDataDirectoryFileFixityWith(
    location: BagLocation = createLocation,
    checksum: Checksum = randomChecksum
  ): DataDirectoryFileFixity =
    DataDirectoryFileFixity(
      uri = resolve(location),
      path = createBagPath,
      checksum = checksum
    )

  def createDataDirectoryFileFixity: DataDirectoryFileFixity =
    createDataDirectoryFileFixityWith()
}
