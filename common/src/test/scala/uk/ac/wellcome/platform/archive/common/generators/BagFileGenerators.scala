package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagFile, BagPath}
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, ChecksumValue, SHA256}
import uk.ac.wellcome.storage.generators.RandomThings

trait BagFileGenerators extends RandomThings {
  def createBagFileWith(
    path: String = randomAlphanumeric,
    checksum: String = randomAlphanumeric
  ): BagFile =
    BagFile(
      checksum = Checksum(
        algorithm = SHA256,
        value = ChecksumValue(checksum)
      ),
      path = BagPath(path)
    )

  def createBagFile: BagFile = createBagFileWith()
}
