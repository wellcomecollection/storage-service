package uk.ac.wellcome.platform.archive.common.verify

import uk.ac.wellcome.platform.archive.common.storage.models.ChecksumAlgorithm
import uk.ac.wellcome.storage.ObjectLocation

case class VerifiableLocation(
                               objectLocation: ObjectLocation,
                               checksum: Checksum
                             )

case class ChecksumValue(value: String)

case class Checksum(
                     algorithm: ChecksumAlgorithm,
                     value: ChecksumValue
                   )
