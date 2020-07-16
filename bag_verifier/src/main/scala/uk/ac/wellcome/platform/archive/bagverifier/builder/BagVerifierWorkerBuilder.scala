package uk.ac.wellcome.platform.archive.bagverifier.builder

import akka.actor.ActorSystem
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.messaging.sns.SNSConfig
import uk.ac.wellcome.messaging.typesafe.AlpakkaSqsWorkerConfigBuilder
import uk.ac.wellcome.messaging.worker.monitoring.metrics.MetricsMonitoringClient
import uk.ac.wellcome.platform.archive.bagverifier.services.s3.{S3ReplicatedBagVerifier, S3StandaloneBagVerifier}
import uk.ac.wellcome.platform.archive.bagverifier.services.{BagVerifierWorker, ReplicatedBagVerifyContext, StandaloneBagVerifyContext}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.{BagRootPayload, ReplicaResultPayload}
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._
import uk.ac.wellcome.json.JsonUtil._


object BagVerifierWorkerBuilder{
  def buildBagVerifierWorker(config: Config)(ingestUpdater: IngestUpdater[SNSConfig],outgoingPublisher:OutgoingPublisher[SNSConfig])(implicit s3: AmazonS3,mc: MetricsMonitoringClient,
                                                                                                                                     as: ActorSystem,sc: SqsAsyncClient) = {
    val isReplicatedBagMode = config.getBoolean("bag-verifier.mode.replicated_bag")
    val primaryBucket = config.requireString("bag-verifier.primary-storage-bucket")
    isReplicatedBagMode match {
      case false => buildStandaloneVerifierWorker(config)(primaryBucket,ingestUpdater, outgoingPublisher)
      case true => buildReplicaBagVerifierWorker(config)(primaryBucket,ingestUpdater, outgoingPublisher)
    }

  }

  private def buildStandaloneVerifierWorker(config: Config)(primaryBucket: String,ingestUpdater: IngestUpdater[SNSConfig],outgoingPublisher:OutgoingPublisher[SNSConfig])(implicit s3: AmazonS3,mc: MetricsMonitoringClient,
   as: ActorSystem,
   sc: SqsAsyncClient): BagVerifierWorker[BagRootPayload, StandaloneBagVerifyContext[S3ObjectLocation, S3ObjectLocationPrefix], SNSConfig, SNSConfig] ={
    val verifier = new S3StandaloneBagVerifier(primaryBucket)
    new BagVerifierWorker(
      config = AlpakkaSqsWorkerConfigBuilder.build(config),
      ingestUpdater = ingestUpdater,
      outgoingPublisher = outgoingPublisher,
      verifier = verifier,
      metricsNamespace = config.requireString("aws.metrics.namespace"),
     ( payload: BagRootPayload) => StandaloneBagVerifyContext(S3ObjectLocationPrefix(payload.bagRoot))
    )

  }

  private def buildReplicaBagVerifierWorker(config: Config)(primaryBucket: String,ingestUpdater: IngestUpdater[SNSConfig],outgoingPublisher:OutgoingPublisher[SNSConfig])(implicit s3: AmazonS3,mc: MetricsMonitoringClient,
                                                                                                                                                    as: ActorSystem,
                                                                                                                                                    sc: SqsAsyncClient): BagVerifierWorker[ReplicaResultPayload, ReplicatedBagVerifyContext[S3ObjectLocation, S3ObjectLocationPrefix], SNSConfig, SNSConfig] ={
    val verifier = new S3ReplicatedBagVerifier(primaryBucket)
  new BagVerifierWorker(
    config = AlpakkaSqsWorkerConfigBuilder.build(config),
    ingestUpdater = ingestUpdater,
    outgoingPublisher = outgoingPublisher,
    verifier = verifier,
    metricsNamespace = config.requireString("aws.metrics.namespace"),
    ( payload: ReplicaResultPayload) => ReplicatedBagVerifyContext(S3ObjectLocationPrefix(payload.bagRoot), S3ObjectLocationPrefix(payload.replicaResult.storageLocation.prefix))
  )

  }

}
