package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagFile, BagPath}
import uk.ac.wellcome.platform.archive.common.verify.{
  Checksum,
  ChecksumValue,
  HashingAlgorithm,
  SHA256
}
import uk.ac.wellcome.storage.generators.RandomThings

trait BagFileGenerators extends RandomThings {
  def createBagFileWith(
    path: String = randomAlphanumeric,
    checksum: String = randomAlphanumeric,
    checksumAlgorithm: HashingAlgorithm = SHA256
  ): BagFile =
    BagFile(
      checksum = Checksum(
        algorithm = checksumAlgorithm,
        value = ChecksumValue(checksum)
      ),
      path = BagPath(path)
    )

  def createBagFile: BagFile = createBagFileWith()

  def createBagPath: BagPath = BagPath(randomAlphanumeric)

  def createChecksum: Checksum =
    Checksum(
      algorithm = SHA256,
      value = ChecksumValue(randomAlphanumeric)
    )
}
