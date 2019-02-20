package uk.ac.wellcome.platform.archive.bagreplicator.storage

import uk.ac.wellcome.storage.ObjectLocation

trait ObjectCopier {
  def copy(src: ObjectLocation, dst: ObjectLocation): Unit
}
