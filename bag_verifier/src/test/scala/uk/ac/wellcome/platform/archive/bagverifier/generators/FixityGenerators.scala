package uk.ac.wellcome.platform.archive.bagverifier.generators

import java.net.URI

import uk.ac.wellcome.platform.archive.bagverifier.fixity.ExpectedFileFixity
import uk.ac.wellcome.platform.archive.common.bagit.models.BagPath
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

trait FixityGenerators extends StorageRandomThings with ObjectLocationGenerators {
  def randomChecksum = Checksum(SHA256, randomChecksumValue)
  def badChecksum = Checksum(MD5, randomChecksumValue)

  def createStringChecksumPair: (String, Checksum) =
    (
      "HelloWorld",
      chooseFrom(Seq(
        Checksum(MD5, ChecksumValue("68e109f0f40ca72a15e05cc22786f8e6")),
        Checksum(SHA1, ChecksumValue("db8ac1c259eb89d4a131b253bacfca5f319d54f2")),
        Checksum(SHA256, ChecksumValue("872e4e50ce9990d8b041330c47c9ddd11bec6b503ae9386a99da8584e9bb12c4")),
      ))
    )

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
