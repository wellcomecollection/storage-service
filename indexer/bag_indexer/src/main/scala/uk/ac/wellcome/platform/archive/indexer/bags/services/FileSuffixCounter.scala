package uk.ac.wellcome.platform.archive.indexer.bags.services

import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifestFile

object FileSuffixCounter {
  def count(files: Seq[StorageManifestFile]): Map[String, Int] = {
    files
      .map(_.name.split("\\."))
      .filter(_.length > 1)
      .map(_.last.toLowerCase)
      .groupBy(identity)
      .mapValues(_.size)
  }
}
