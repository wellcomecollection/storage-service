package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

class S3Uploader(implicit s3Client: AmazonS3) {
  val s3StreamStore = new S3StreamStore()

  def putObject(
    inputStream: InputStream,
    streamLength: Long,
    uploadLocation: ObjectLocation
  ): Unit = {
    val stream = new InputStreamWithLengthAndMetadata(
      inputStream,
      length = streamLength,
      metadata = Map.empty
    )

    s3StreamStore.put(uploadLocation)(stream) match {
      case Right(_) => ()
      case Left(err) => throw new Throwable(s"Error from S3 Stream store: $err")
    }
  }
}
