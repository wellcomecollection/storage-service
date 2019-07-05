package uk.ac.wellcome.platform.archive.bagunpacker.services.s3

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import org.apache.commons.io.input.CloseShieldInputStream
import uk.ac.wellcome.platform.archive.bagunpacker.services.Unpacker
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.{InputStreamWithLength, InputStreamWithLengthAndMetadata}
import uk.ac.wellcome.storage.{ObjectLocation, StorageError}

class S3Unpacker()(implicit s3Client: AmazonS3) extends Unpacker {
  private val s3StreamStore = new S3StreamStore()

  override def get(location: ObjectLocation): Either[StorageError, InputStream] =
    s3StreamStore.get(location).map { _.identifiedT }

  override def put(location: ObjectLocation)(inputStream: InputStreamWithLength): Either[StorageError, Unit] =
    // The S3 APIs will "helpfully" close the stream for you.  In this case,
    // our InputStream is really a wrapper around the original tar.gz, so
    // we don't want to close it!
    s3StreamStore.put(location)(
      new InputStreamWithLengthAndMetadata(
        new CloseShieldInputStream(inputStream),
        length = inputStream.length,
        metadata = Map.empty
      )
    ).map { _ => () }
}
