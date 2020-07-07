package uk.ac.wellcome.platform.storage.bag_tagger.services

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.ingests.models.AmazonS3StorageProvider
import uk.ac.wellcome.platform.archive.common.storage.models.{
  StorageLocation,
  StorageManifestFile
}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.tags.s3.NewS3Tags

import scala.util.Try

class ApplyTags(s3Tags: NewS3Tags) extends Logging {
  def applyTags(
    storageLocations: Seq[StorageLocation],
    tagsToApply: Map[StorageManifestFile, Map[String, String]]
  ): Try[Unit] =
    Try {
      val results: Seq[Either[UpdateError, Unit]] =
        storageLocations.flatMap { location =>
          location.provider match {
            case AmazonS3StorageProvider =>
              applyS3Tags(
                prefix = S3ObjectLocationPrefix(location.prefix),
                tagsToApply = tagsToApply
              )
            case provider =>
              throw new IllegalArgumentException(
                s"Unsupported provider for tagging: $provider"
              )
          }
        }

      val failures = results.collect { case Left(updateError) => updateError }

      if (failures.nonEmpty) {
        warn(s"Failures applying tags to $storageLocations: $failures")
        throw new Throwable("Could not successfully apply tags!")
      } else {
        ()
      }
    }

  private def applyS3Tags(
    prefix: S3ObjectLocationPrefix,
    tagsToApply: Map[StorageManifestFile, Map[String, String]]
  ): Iterable[Either[UpdateError, Unit]] =
    tagsToApply
      .map {
        case (storageManifestFile, newTags) =>
          debug(
            s"Applying S3 tags: prefix=$prefix, path=${storageManifestFile.path}, tags=$tagsToApply"
          )
          val location = prefix.asLocation(storageManifestFile.path)

          val result = s3Tags.update(location) { existingTags =>
            // The bag tagger runs after the bag verifier, which means we should see
            // a Content-SHA256 tag here.  If not, we should abort -- either the storage
            // service is broken, or we're waiting for something to happen with S3 consistency.
            existingTags.get("Content-SHA256") match {
              case Some(_) => Right(existingTags ++ newTags)
              case None =>
                throw new Throwable(s"No Content-SHA256 tag on $location")
            }
          }
          debug(s"Result of applying tags: $result")

          result.map { _ =>
            ()
          }
      }
}
