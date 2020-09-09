package uk.ac.wellcome.platform.archive.bagverifier.fixtures

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagverifier.builder.BagVerifierWorkerBuilder
import uk.ac.wellcome.platform.archive.bagverifier.models.{
  ReplicatedBagVerifyContext,
  StandaloneBagVerifyContext
}
import uk.ac.wellcome.platform.archive.bagverifier.services.BagVerifierWorker
import uk.ac.wellcome.platform.archive.bagverifier.services.s3.S3StandaloneBagVerifier
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.platform.archive.common.{
  BagRootLocationPayload,
  ReplicaCompletePayload
}
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.fixtures.{
  AzureFixtures,
  DynamoFixtures,
  S3Fixtures
}
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}

import scala.concurrent.ExecutionContext.Implicits.global

trait BagVerifierFixtures
    extends AlpakkaSQSWorkerFixtures
    with SQS
    with Akka
    with OperationFixtures
    with S3Fixtures
    with AzureFixtures
    with DynamoFixtures {
  def withStandaloneBagVerifierWorker[R](
    ingests: MemoryMessageSender = new MemoryMessageSender(),
    outgoing: MemoryMessageSender,
    queue: Queue = dummyQueue,
    bucket: Bucket,
    stepName: String = randomAlphanumericWithLength()
  )(
    testWith: TestWith[BagVerifierWorker[
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      StandaloneBagVerifyContext,
      BagRootLocationPayload,
      String,
      String
    ], R]
  ): R =
    withFakeMonitoringClient() { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        val ingestUpdater =
          createIngestUpdaterWith(ingests, stepName = stepName)

        val outgoingPublisher = createOutgoingPublisherWith(outgoing)

        val worker = BagVerifierWorkerBuilder
          .buildStandaloneVerifierWorker(
            primaryBucket = bucket.name,
            metricsNamespace = "bag_verifier",
            alpakkaSqsWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
            ingestUpdater = ingestUpdater,
            outgoingPublisher = outgoingPublisher
          )

        worker.run()

        testWith(worker)
      }
    }

  def withS3ReplicaBagVerifierWorker[R](
    ingests: MemoryMessageSender = new MemoryMessageSender(),
    outgoing: MemoryMessageSender,
    queue: Queue = dummyQueue,
    bucket: Bucket,
    stepName: String = randomAlphanumericWithLength()
  )(
    testWith: TestWith[BagVerifierWorker[
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      ReplicatedBagVerifyContext[
        S3ObjectLocationPrefix
      ],
      ReplicaCompletePayload,
      String,
      String
    ], R]
  ): R =
    withFakeMonitoringClient() { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        val ingestUpdater =
          createIngestUpdaterWith(ingests, stepName = stepName)

        val outgoingPublisher = createOutgoingPublisherWith(outgoing)

        val worker = BagVerifierWorkerBuilder.buildReplicaS3BagVerifierWorker(
          primaryBucket = bucket.name,
          metricsNamespace = "bag_verifier",
          alpakkaSqsWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
          ingestUpdater = ingestUpdater,
          outgoingPublisher = outgoingPublisher
        )

        worker.run()

        testWith(worker)
      }
    }

  def withAzureReplicaBagVerifierWorker[R](
    ingests: MemoryMessageSender = new MemoryMessageSender(),
    outgoing: MemoryMessageSender = new MemoryMessageSender(),
    queue: Queue = dummyQueue,
    bucket: Bucket,
    stepName: String = randomAlphanumericWithLength()
  )(
    testWith: TestWith[BagVerifierWorker[
      AzureBlobLocation,
      AzureBlobLocationPrefix,
      ReplicatedBagVerifyContext[
        AzureBlobLocationPrefix
      ],
      ReplicaCompletePayload,
      String,
      String
    ], R]
  ): R =
    withFakeMonitoringClient() { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        val ingestUpdater =
          createIngestUpdaterWith(ingests, stepName = stepName)

        val outgoingPublisher = createOutgoingPublisherWith(outgoing)

        withLocalDynamoDbTable { table =>
          val worker =
            BagVerifierWorkerBuilder.buildReplicaAzureBagVerifierWorker(
              primaryBucket = bucket.name,
              dynamoConfig = createDynamoConfigWith(table),
              metricsNamespace = "bag_verifier",
              alpakkaSqsWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
              ingestUpdater = ingestUpdater,
              outgoingPublisher = outgoingPublisher
            )

          worker.run()

          testWith(worker)
        }
      }
    }

  def withVerifier[R](
    bucket: Bucket
  )(testWith: TestWith[S3StandaloneBagVerifier, R]): R =
    testWith(
      new S3StandaloneBagVerifier(primaryBucket = bucket.name)
    )

  override def createTable(table: Table): Table =
    createTableWithHashKey(
      table,
      keyName = "id",
      keyType = ScalarAttributeType.S
    )
}
