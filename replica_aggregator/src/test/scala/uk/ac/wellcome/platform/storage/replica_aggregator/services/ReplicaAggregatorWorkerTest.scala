package uk.ac.wellcome.platform.storage.replica_aggregator.services

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.TryValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scanamo.auto._
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.{
  KnownReplicasPayload,
  ReplicaCompletePayload,
  VersionedBagRootPayload
}
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  AmazonS3StorageProvider,
  Ingest
}
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.storage.replica_aggregator.fixtures.ReplicaAggregatorFixtures
import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.dynamo.DynamoSingleVersionStore
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ReplicaAggregatorWorkerTest
    extends AnyFunSpec
    with Matchers
    with PayloadGenerators
    with IngestUpdateAssertions
    with ReplicaAggregatorFixtures
    with DynamoFixtures
    with ScalaFutures
    with TryValues {

  describe("if there are enough replicas") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    val payload = createReplicaCompletePayloadWith(
      provider = AmazonS3StorageProvider
    )

    val expectedKnownReplicas = KnownReplicas(
      location = payload.dstLocation.asInstanceOf[PrimaryStorageLocation],
      replicas = List.empty
    )

    val result =
      withReplicaAggregatorWorker(
        ingests = ingests,
        outgoing = outgoing,
        stepName = "aggregating replicas"
      ) {
        _.processMessage(payload).success.value
      }

    it("returns a ReplicationAggregationComplete") {
      result shouldBe a[IngestStepSucceeded[_]]

      result.summary shouldBe a[ReplicationAggregationComplete]

      val completeAggregation =
        result.summary.asInstanceOf[ReplicationAggregationComplete]
      completeAggregation.replicaPath shouldBe ReplicaPath(
        payload.bagRoot.path
      )
      completeAggregation.knownReplicas shouldBe expectedKnownReplicas
    }

    it("includes a user-facing message") {
      result.maybeUserFacingMessage shouldBe Some("all replicas complete")
    }

    it("sends an outgoing message") {
      val expectedPayload = KnownReplicasPayload(
        context = payload.context,
        version = payload.version,
        knownReplicas = expectedKnownReplicas
      )

      outgoing.getMessages[KnownReplicasPayload] shouldBe Seq(expectedPayload)
    }

    it("updates the ingests monitor") {
      assertTopicReceivesIngestEvents(
        ingestId = payload.ingestId,
        ingests = ingests,
        expectedDescriptions = Seq(
          "Aggregating replicas succeeded - all replicas complete"
        )
      )
    }
  }

  describe("if there are not enough replicas") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    val payload = createReplicaCompletePayload

    val result =
      withReplicaAggregatorWorker(
        ingests = ingests,
        outgoing = outgoing,
        stepName = "aggregating replicas",
        expectedReplicaCount = 3
      ) {
        _.processMessage(payload).success.value
      }

    it("returns a ReplicationAggregationIncomplete") {
      result shouldBe a[IngestStepSucceeded[_]]

      result.summary shouldBe a[ReplicationAggregationIncomplete]

      val incompleteAggregation =
        result.summary.asInstanceOf[ReplicationAggregationIncomplete]
      incompleteAggregation.replicaPath shouldBe ReplicaPath(
        payload.bagRoot.path
      )
      incompleteAggregation.aggregatorRecord shouldBe AggregatorInternalRecord(
        location = Some(
          PrimaryS3ReplicaLocation(
            prefix = S3ObjectLocationPrefix(payload.bagRoot)
          )
        ),
        replicas = List.empty
      )
      incompleteAggregation.counterError shouldBe NotEnoughReplicas(
        expected = 3,
        actual = 1
      )
    }

    it("includes a user-facing message") {
      result.maybeUserFacingMessage shouldBe Some("1 of 3 replicas complete")
    }

    it("does not send an outgoing message") {
      outgoing.getMessages[VersionedBagRootPayload] shouldBe empty
    }

    it("updates the ingests monitor") {
      assertTopicReceivesIngestEvents(
        ingestId = payload.ingestId,
        ingests = ingests,
        expectedDescriptions = Seq(
          "Aggregating replicas succeeded - 1 of 3 replicas complete"
        )
      )
    }
  }

  describe("if there's an error in the replica aggregator") {
    val throwable = new Throwable("BOOM!")

    val brokenStore =
      new MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
        store =
          new MemoryStore[Version[ReplicaPath, Int], AggregatorInternalRecord](
            initialEntries = Map.empty
          ) with MemoryMaxima[ReplicaPath, AggregatorInternalRecord]
      ) {
        override def upsert(
          id: ReplicaPath
        )(t: AggregatorInternalRecord)(f: UpdateFunction): UpdateEither = {
          Left(UpdateUnexpectedError(throwable))
        }
      }

    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    val payload = createReplicaCompletePayload

    val result =
      withReplicaAggregatorWorker(
        ingests = ingests,
        outgoing = outgoing,
        versionedStore = brokenStore,
        stepName = "aggregating replicas"
      ) {
        _.processMessage(payload).success.value
      }

    it("returns an IngestFailed") {
      result shouldBe a[IngestFailed[_]]
    }

    it("returns a ReplicationAggregationFailed") {
      result.summary shouldBe a[ReplicationAggregationFailed]

      val failure = result.summary.asInstanceOf[ReplicationAggregationFailed]
      failure.replicaPath shouldBe ReplicaPath(payload.bagRoot.path)
      failure.e shouldBe throwable
    }

    it("does not send an outgoing message") {
      outgoing.messages shouldBe empty
    }

    it("sends an IngestFailed to the monitor") {
      assertTopicReceivesIngestStatus(
        ingestId = payload.ingestId,
        ingests = ingests,
        status = Ingest.Failed
      ) {
        _.map { _.description } shouldBe Seq("Aggregating replicas failed")
      }
    }
  }

  describe("if there's a retryable error in the replica aggregator") {
    val brokenStore =
      new MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
        store =
          new MemoryStore[Version[ReplicaPath, Int], AggregatorInternalRecord](
            initialEntries = Map.empty
          ) with MemoryMaxima[ReplicaPath, AggregatorInternalRecord]
      ) {
        override def put(
          id: Version[ReplicaPath, Int]
        )(t: AggregatorInternalRecord): WriteEither =
          Left(VersionAlreadyExistsError())
      }

    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    val payload = createReplicaCompletePayload

    val result =
      withReplicaAggregatorWorker(
        ingests = ingests,
        outgoing = outgoing,
        versionedStore = brokenStore,
        stepName = "aggregating replicas"
      ) {
        _.processMessage(payload).success.value
      }

    it("returns an IngestShouldRetry") {
      result shouldBe a[IngestShouldRetry[_]]
    }

    it("returns a ReplicationAggregationFailed") {
      result.summary shouldBe a[ReplicationAggregationFailed]

      val failure = result.summary.asInstanceOf[ReplicationAggregationFailed]
      failure.replicaPath shouldBe ReplicaPath(payload.bagRoot.path)
      failure.e shouldBe a[java.lang.Error]
    }

    it("does not send an outgoing message") {
      outgoing.messages shouldBe empty
    }

    it("sends a retrying message to the ingests monitor") {
      assertTopicReceivesIngestEvents(
        ingestId = payload.ingestId,
        ingests = ingests,
        expectedDescriptions = Seq(
          "Aggregating replicas retrying"
        )
      )
    }
  }

  override def createTable(table: DynamoFixtures.Table): DynamoFixtures.Table =
    createTableWithHashRangeKey(
      table,
      hashKeyName = "id",
      hashKeyType = ScalarAttributeType.S,
      rangeKeyName = "version",
      rangeKeyType = ScalarAttributeType.N
    )

  it("handles ConditionalUpdate errors from DynamoDB") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    val version = createBagVersion
    val path = randomAlphanumeric
    val locations = Seq(
      createPrimaryLocationWith(
        prefix = ObjectLocationPrefix(
          namespace = randomAlphanumeric,
          path = path
        )
      ),
      createSecondaryLocationWith(
        prefix = ObjectLocationPrefix(
          namespace = randomAlphanumeric,
          path = path
        )
      ),
      createSecondaryLocationWith(
        prefix = ObjectLocationPrefix(
          namespace = randomAlphanumeric,
          path = path
        )
      )
    )

    val context = createPipelineContext

    val payloads = locations.map { storageLocation =>
      ReplicaCompletePayload(
        context = context,
        replicaResult = ReplicaResult(
          originalLocation = createS3ObjectLocationPrefix,
          storageLocation = storageLocation
        ),
        version = version
      )
    }

    withLocalDynamoDbTable { table =>
      val versionedStore =
        new DynamoSingleVersionStore[ReplicaPath, AggregatorInternalRecord](
          createDynamoConfigWith(table)
        )

      val future: Future[Seq[IngestStepResult[ReplicationAggregationSummary]]] =
        withReplicaAggregatorWorker(
          versionedStore = versionedStore,
          ingests = ingests,
          outgoing = outgoing,
          expectedReplicaCount = 3
        ) { service =>
          Future.sequence(
            payloads.map { payload =>
              Future.successful(()).flatMap { _ =>
                Future.fromTry(service.processMessage(payload))
              }
            }
          )
        }

      whenReady(future) {
        result: Seq[IngestStepResult[ReplicationAggregationSummary]] =>
          // Exact numbers will vary between different runs; we just want to
          // check that:
          //
          //  - At least one aggregation succeeded
          //  - At least one payload was retried
          //  - None of the payloads were failed
          //
          result.find { _.isInstanceOf[IngestShouldRetry[_]] } shouldBe 'defined
          result.find { _.isInstanceOf[IngestStepSucceeded[_]] } shouldBe 'defined

          result.find { _.isInstanceOf[IngestFailed[_]] } shouldBe None
      }
    }
  }
}
