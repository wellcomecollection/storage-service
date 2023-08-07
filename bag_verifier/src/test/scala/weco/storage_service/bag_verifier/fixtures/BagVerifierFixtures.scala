package weco.storage_service.bag_verifier.fixtures

import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import weco.akka.fixtures.Akka
import weco.fixtures.TestWith
import weco.messaging.fixtures.SQS
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import weco.messaging.memory.MemoryMessageSender
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.bag_verifier.builder.BagVerifierWorkerBuilder
import weco.storage_service.bag_verifier.models.{ReplicatedBagVerifyContext, StandaloneBagVerifyContext}
import weco.storage_service.bag_verifier.services.BagVerifierWorker
import weco.storage_service.bag_verifier.services.s3.{S3BagVerifier, S3StandaloneBagVerifier}
import weco.storage_service.fixtures.OperationFixtures
import weco.storage_service.{BagRootLocationPayload, ReplicaCompletePayload}
import weco.storage.providers.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import weco.storage.fixtures.DynamoFixtures.Table
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.fixtures.{AzureFixtures, DynamoFixtures, S3Fixtures}
import weco.storage.providers.s3.{S3ObjectLocation, S3ObjectLocationPrefix}

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
    stepName: String = createStepName
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
    withActorSystem { implicit actorSystem =>
      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val ingestUpdater =
        createIngestUpdaterWith(ingests, stepName = stepName)

      val outgoingPublisher = createOutgoingPublisherWith(outgoing)

      val worker = BagVerifierWorkerBuilder
        .buildStandaloneVerifierWorker(
          primaryBucket = bucket.name,
          alpakkaSqsWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
          ingestUpdater = ingestUpdater,
          outgoingPublisher = outgoingPublisher
        )

      worker.run()

      testWith(worker)
    }

  def withS3ReplicaBagVerifierWorker[R](
    ingests: MemoryMessageSender = new MemoryMessageSender(),
    outgoing: MemoryMessageSender,
    queue: Queue = dummyQueue,
    bucket: Bucket,
    stepName: String = createStepName
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
    withActorSystem { implicit actorSystem =>
      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val ingestUpdater =
        createIngestUpdaterWith(ingests, stepName = stepName)

      val outgoingPublisher = createOutgoingPublisherWith(outgoing)

      val worker = BagVerifierWorkerBuilder.buildReplicaS3BagVerifierWorker(
        primaryBucket = bucket.name,
        alpakkaSqsWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
        ingestUpdater = ingestUpdater,
        outgoingPublisher = outgoingPublisher
      )

      worker.run()

      testWith(worker)
    }

  def withAzureReplicaBagVerifierWorker[R](
    ingests: MemoryMessageSender = new MemoryMessageSender(),
    outgoing: MemoryMessageSender = new MemoryMessageSender(),
    queue: Queue = dummyQueue,
    bucket: Bucket,
    stepName: String = createStepName
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
    withActorSystem { implicit actorSystem =>
      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val ingestUpdater =
        createIngestUpdaterWith(ingests, stepName = stepName)

      val outgoingPublisher = createOutgoingPublisherWith(outgoing)

      withLocalDynamoDbTable { table =>
        val worker =
          BagVerifierWorkerBuilder.buildReplicaAzureBagVerifierWorker(
            primaryBucket = bucket.name,
            dynamoConfig = createDynamoConfigWith(table),
            alpakkaSqsWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
            ingestUpdater = ingestUpdater,
            outgoingPublisher = outgoingPublisher
          )

        worker.run()

        testWith(worker)
      }
    }

  def withVerifier[R](
    bucket: Bucket
  )(testWith: TestWith[S3StandaloneBagVerifier, R]): R =
    testWith(
      S3BagVerifier.standalone(primaryBucket = bucket.name)
    )

  override def createTable(table: Table): Table =
    createTableWithHashKey(
      table,
      keyName = "id",
      keyType = ScalarAttributeType.S
    )
}
