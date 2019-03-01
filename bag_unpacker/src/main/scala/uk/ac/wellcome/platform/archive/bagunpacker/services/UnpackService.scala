package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.nio.file.Paths

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import org.apache.commons.io.input.CloseShieldInputStream
import uk.ac.wellcome.platform.archive.bagunpacker.storage.Unpack
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.platform.archive.common.ConvertibleToInputStream._

import scala.concurrent.ExecutionContext


class UnpackService()(implicit s3Client: AmazonS3, ec: ExecutionContext) {

  def unpack(
              srcLocation: ObjectLocation,
              dstLocation: ObjectLocation
            ) = {
    for {
      packageInputStream <- srcLocation.toInputStream
      result <- Unpack.get(packageInputStream) {
        (inputStream, archiveEntry) =>

          val metadata = new ObjectMetadata()
          metadata.setContentLength(archiveEntry.getSize)

          // The AmazonS3 putObject call
          // disposes of our InputStream,
          // we want to stop it as the Unpack
          // object requires it to remain open
          // to retrieve the next entry.

          val closeShieldInputStream =
            new CloseShieldInputStream(inputStream)

          val request = new PutObjectRequest(dstLocation.namespace,
            Paths.get(
              dstLocation.key,
              archiveEntry.getName
            ).toString,
            closeShieldInputStream,
            metadata
          )

          s3Client.putObject(request)
      }
    } yield result

  }
}
