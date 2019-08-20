package uk.ac.wellcome.platform.storage.replica_aggregator.services
import org.scalatest.{FunSpec, Matchers}

class ReplicaAggregatorWorkerTest extends FunSpec with Matchers {
  describe("if there are enough replicas") {
    it("returns a ReplicationAggregationComplete") {
      true shouldBe false
    }

    it("sends an outgoing message") {
      true shouldBe false
    }

    it("updates the ingests monitor") {
      true shouldBe false
    }
  }

  describe("if there are not enough replicas") {
    it("returns a ReplicationAggregationIncomplete") {
      true shouldBe false
    }

    it("does not send an outgoing message") {
      true shouldBe false
    }

    it("updates the ingests monitor") {
      true shouldBe false
    }
  }

  describe("if there's an error in the replica aggregator") {
    it("returns a ReplicationAggregationFailed") {
      true shouldBe false
    }

    it("does not send an outgoing message") {
      true shouldBe false
    }

    it("sends an IngestFailed to the monitor") {
      true shouldBe false
    }
  }
}
