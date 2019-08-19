package uk.ac.wellcome.platform.archive.common.ingests.models

import uk.ac.wellcome.storage.ObjectLocation

case class SourceLocation(provider: StorageProvider, location: ObjectLocation)
