package uk.ac.wellcome.platform.archive.bag_register.services.s3
import java.net.URI

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bag_register.services.StorageManifestService
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.platform.archive.common.storage.services.s3.S3SizeFinder
import uk.ac.wellcome.storage.{
  ObjectLocationPrefix,
  Prefix,
  S3ObjectLocation,
  S3ObjectLocationPrefix
}
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.store.s3.NewS3StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class S3StorageManifestService(implicit s3Client: AmazonS3)
    extends StorageManifestService[S3ObjectLocation] {
  override def toLocationPrefix(
    prefix: ObjectLocationPrefix
  ): Prefix[S3ObjectLocation] =
    S3ObjectLocationPrefix(prefix)

  override val sizeFinder: SizeFinder[S3ObjectLocation] =
    new S3SizeFinder()

  override implicit val streamReader
    : Readable[S3ObjectLocation, InputStreamWithLength] =
    new NewS3StreamStore()

  override def createLocation(uri: URI): S3ObjectLocation =
    new S3ObjectLocation(
      bucket = uri.getHost,
      key = uri.getPath.stripPrefix("/")
    )
}
