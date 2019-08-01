package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.listing.s3.S3ObjectSummaryListing
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryStreamStoreEntry}
import uk.ac.wellcome.storage.{
  ListingFailure,
  ObjectLocation,
  ObjectLocationPrefix
}

trait SizeFinder {
  def getSizesUnder(prefix: ObjectLocationPrefix)
    : Either[ListingFailure[ObjectLocationPrefix], Map[ObjectLocation, Long]]
}

class MemorySizeFinder(
  memoryStore: MemoryStore[ObjectLocation, MemoryStreamStoreEntry])
    extends SizeFinder {
  override def getSizesUnder(prefix: ObjectLocationPrefix)
    : Either[ListingFailure[ObjectLocationPrefix], Map[ObjectLocation, Long]] =
    Right(
      memoryStore.entries
        .filter {
          case (location, entry) =>
            prefix.namespace == location.namespace && location.path.startsWith(
              prefix.path)
        }
        .map { case (location, entry) => (location, entry.bytes.length.toLong) }
    )
}

class S3SizeFinder(implicit s3Client: AmazonS3) extends SizeFinder {
  val listing = new S3ObjectSummaryListing()

  override def getSizesUnder(prefix: ObjectLocationPrefix)
    : Either[ListingFailure[ObjectLocationPrefix], Map[ObjectLocation, Long]] =
    listing
      .list(prefix)
      .map { iterator =>
        iterator.map { summary =>
          val location = ObjectLocation(
            namespace = summary.getBucketName,
            path = summary.getKey
          )

          location -> summary.getSize
        }.toMap
      }
}
