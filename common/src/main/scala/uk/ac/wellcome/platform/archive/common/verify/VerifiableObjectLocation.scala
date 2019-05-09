package uk.ac.wellcome.platform.archive.common.verify

import uk.ac.wellcome.storage.ObjectLocation

case class VerifiableObjectLocation(
                                     objectLocation: ObjectLocation,
                                     checksum: Checksum
                                   )
