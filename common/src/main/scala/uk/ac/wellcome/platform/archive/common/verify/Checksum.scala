package uk.ac.wellcome.platform.archive.common.verify

import uk.ac.wellcome.platform.archive.common.storage.models.ChecksumAlgorithm

case class ChecksumValue(value: String)

case class Checksum(
                     algorithm: ChecksumAlgorithm,
                     value: ChecksumValue
                   )
