package uk.ac.wellcome.platform.archive.bagverifier

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import org.apache.commons.codec.digest.MessageDigestAlgorithms
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.typesafe.{NotificationStreamBuilder, SNSBuilder}
import uk.ac.wellcome.platform.archive.bagverifier.services.{BagVerifierWorkerService, NotificationService, VerifyDigestFilesService}
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.services.StorageManifestService
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      AkkaBuilder.buildActorSystem()
    implicit val executionContext: ExecutionContext =
      AkkaBuilder.buildExecutionContext()
    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client(config)
    implicit val materializer: Materializer =
      AkkaBuilder.buildActorMaterializer()

    val progressSnsWriter =
      SNSBuilder.buildSNSWriter(
        config,
        namespace = "progress"
      )

    val outgoingSnsWriter =
      SNSBuilder.buildSNSWriter(
        config,
        namespace = "outgoing"
      )

    val notificationStream =
      NotificationStreamBuilder
        .buildStream[BagRequest](config)

    val verifyDigestFilesService = new VerifyDigestFilesService(
      storageManifestService = new StorageManifestService(),
      s3Client = s3Client,
      algorithm = MessageDigestAlgorithms.SHA_256
    )

    val notificationService = new NotificationService(
      progressSnsWriter,
      outgoingSnsWriter
    )

    new BagVerifierWorkerService(
      notificationStream,
      verifyDigestFilesService,
      notificationService
    )
  }
}
