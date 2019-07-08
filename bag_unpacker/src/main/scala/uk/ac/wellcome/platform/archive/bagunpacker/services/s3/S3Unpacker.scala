package uk.ac.wellcome.platform.archive.bagunpacker.services.s3

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bagunpacker.services.Unpacker
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.{
  InputStreamWithLength,
  InputStreamWithLengthAndMetadata
}
import uk.ac.wellcome.storage.{ObjectLocation, StorageError}

class S3Unpacker()(implicit s3Client: AmazonS3) extends Unpacker {
  private val s3StreamStore = new S3StreamStore()

  override def get(
    location: ObjectLocation): Either[StorageError, InputStream] =
    s3StreamStore.get(location).map { _.identifiedT }

  override def put(location: ObjectLocation)(
    inputStream: InputStreamWithLength): Either[StorageError, Unit] =
    s3StreamStore
      .put(location)(
        InputStreamWithLengthAndMetadata(inputStream, metadata = Map.empty)
      )
      .map { _ =>
        ()
      }
}
