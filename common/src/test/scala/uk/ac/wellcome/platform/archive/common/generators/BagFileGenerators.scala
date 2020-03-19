package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagFile, BagPath}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, HashingAlgorithm, SHA256}

trait BagFileGenerators extends StorageRandomThings {
  def createBagFileWith(
    path: String = randomAlphanumeric,
    checksum: String = randomAlphanumeric,
    checksumAlgorithm: HashingAlgorithm = SHA256
  ): BagFile =
    BagFile(
      checksum = Checksum(
        algorithm = checksumAlgorithm,
        value = randomChecksumValue
      ),
      path = BagPath(path)
    )

  def createBagFile: BagFile = createBagFileWith()

  def createBagPath: BagPath = BagPath(randomAlphanumeric)

  def createChecksumWith(algorithm: HashingAlgorithm = SHA256): Checksum =
    Checksum(algorithm = algorithm, value = randomChecksumValue)

  def createChecksum: Checksum =
    createChecksumWith()
}
