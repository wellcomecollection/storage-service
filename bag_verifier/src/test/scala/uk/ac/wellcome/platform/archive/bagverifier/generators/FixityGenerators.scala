package uk.ac.wellcome.platform.archive.bagverifier.generators

import java.net.URI

import uk.ac.wellcome.platform.archive.bagverifier.fixity.ExpectedFileFixity
import uk.ac.wellcome.platform.archive.common.bagit.models.BagPath
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, MD5, SHA256}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

trait FixityGenerators extends StorageRandomThings with ObjectLocationGenerators {
  def randomChecksum = Checksum(SHA256, randomChecksumValue)
  def badChecksum = Checksum(MD5, randomChecksumValue)

  def createExpectedFileFixityWith(
    location: ObjectLocation = createObjectLocation,
    checksum: Checksum = randomChecksum,
    length: Option[Long] = None
  ): ExpectedFileFixity = {
    ExpectedFileFixity(
      uri = resolve(location),
      path = BagPath(randomAlphanumeric),
      checksum = checksum,
      length = length
    )
  }

  def createExpectedFileFixity: ExpectedFileFixity =
    createExpectedFileFixityWith()

  def resolve(location: ObjectLocation): URI
}
