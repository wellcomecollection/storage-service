package uk.ac.wellcome.platform.storage.bag_tagger.services

import uk.ac.wellcome.platform.archive.common.storage.models.{StorageManifest, StorageManifestFile}
import uk.ac.wellcome.platform.archive.common.verify.{ChecksumValue, HashingAlgorithm}

/** Given a storage manifest, decide what tags (if any) should be applied to the
  * objects in the bag.
  *
  */

object TagRules {
  def chooseTags(
    manifest: StorageManifest
  ): Map[StorageManifestFile, Map[String, String]] =
    fixityTagger(manifest)

  private def fixityTagName(algorithm: HashingAlgorithm): String =
    s"Content-${algorithm.pathRepr.toUpperCase}"

  private def fixityTagValue(checksum: ChecksumValue): String =
    checksum.value.toString

  private def fixityTagger(
    manifest: StorageManifest
  ): Map[StorageManifestFile, Map[String, String]] =
    manifest.space.underlying match {
      case "digitised" =>
        manifest.manifest.files
          .map { manifestFile: StorageManifestFile =>
            // code adapted from bag_verifier FixityChecker
            val algorithm = manifest.manifest.checksumAlgorithm
            val checksum = manifestFile.checksum

            val tagName = fixityTagName(algorithm)
            val tagValue = fixityTagValue(checksum)

            val fixityTags = Map(tagName -> tagValue)

            manifestFile -> fixityTags
          }
          .toMap

      case _ => Map.empty
    }
}
