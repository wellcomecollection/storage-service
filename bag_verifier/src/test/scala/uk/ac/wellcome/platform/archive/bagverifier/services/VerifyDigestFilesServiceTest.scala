package uk.ac.wellcome.platform.archive.bagverifier.services

import org.apache.commons.codec.digest.MessageDigestAlgorithms
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.services.StorageManifestService
import uk.ac.wellcome.storage.fixtures.S3

import scala.concurrent.ExecutionContext.Implicits.global

class VerifyDigestFilesServiceTest extends FunSpec with Matchers with ScalaFutures with S3 {

  implicit val _ = s3Client

  val service = new VerifyDigestFilesService(
    storageManifestService = new StorageManifestService(),
    s3Client = s3Client,
    algorithm = MessageDigestAlgorithms.SHA_256
  )

  it("is philosophically correct") {
    true shouldBe true
  }
}
