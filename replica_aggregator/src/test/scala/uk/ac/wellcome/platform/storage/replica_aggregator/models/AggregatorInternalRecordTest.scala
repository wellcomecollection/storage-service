package uk.ac.wellcome.platform.storage.replica_aggregator.models

import org.scalatest.TryValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.StorageLocationGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.SecondaryStorageLocation
import uk.ac.wellcome.storage.generators.RandomThings

class AggregatorInternalRecordTest
    extends AnyFunSpec
    with Matchers
    with TryValues
    with StorageLocationGenerators
    with RandomThings {

  def createReplicas(min: Int = 0): List[SecondaryStorageLocation] =
    (1 to randomInt(min, 10))
      .map(_ => createSecondaryLocation)
      .toList

  describe("creates a record from a single storage location") {

    it("primary location") {
      val location = createPrimaryLocation

      val record = AggregatorInternalRecord(location)

      record shouldBe AggregatorInternalRecord(
        location = Some(location),
        replicas = List.empty
      )
    }

    it("secondary location") {
      val location = createSecondaryLocation

      val record = AggregatorInternalRecord(location)

      record shouldBe AggregatorInternalRecord(
        location = None,
        replicas = List(location)
      )
    }

  }

  describe("can add a storage location to a record") {
    describe("primary location") {
      it("the record has no primary location") {
        val location = createPrimaryLocation

        val record = AggregatorInternalRecord(
          location = None,
          replicas = List.empty
        )

        val updatedRecord = record.addLocation(location).success.value

        updatedRecord.location shouldBe Some(location)
      }

      it("the record already has the same primary location") {
        val location = createPrimaryLocation

        val record = AggregatorInternalRecord(
          location = Some(location),
          replicas = createReplicas()
        )

        val updatedRecord = record.addLocation(location).success.value

        updatedRecord shouldBe record
      }

      it("the record has a different primary location") {
        val location = createPrimaryLocation
        val differentLocation = createPrimaryLocation

        val record = AggregatorInternalRecord(
          location = Some(differentLocation),
          replicas = List.empty
        )

        val err = record.addLocation(location).failed

        err.get.getMessage should startWith(
          "Record already has a different PrimaryStorageLocation"
        )
      }

      it("preserves any secondary locations") {
        val location = createPrimaryLocation
        val replicas = createReplicas(min = 3)

        val record = AggregatorInternalRecord(
          location = None,
          replicas = replicas
        )

        val updatedRecord = record.addLocation(location).success.value

        updatedRecord.replicas should contain theSameElementsAs (replicas)
      }
    }

    describe("secondary location") {
      it("the record has no secondary locations") {
        val location = createSecondaryLocation

        val record = AggregatorInternalRecord(
          location = None,
          replicas = List.empty
        )

        val updatedRecord = record.addLocation(location).success.value

        updatedRecord.replicas shouldBe List(location)
      }

      it("the record has different secondary locations") {
        val location = createSecondaryLocation
        val replicas = createReplicas(min = 3)

        val record = AggregatorInternalRecord(
          location = None,
          replicas = replicas
        )

        val updatedRecord = record.addLocation(location).success.value

        updatedRecord.replicas should contain theSameElementsAs (replicas :+ location)
      }

      it("the record already has this secondary location") {
        val location = createSecondaryLocation
        val replicas = createReplicas(min = 3)

        val record = AggregatorInternalRecord(
          location = None,
          replicas = replicas :+ location
        )

        val updatedRecord = record.addLocation(location).success.value

        updatedRecord.replicas should contain theSameElementsAs (record.replicas)
      }

      it("preserves a primary location") {
        val primaryLocation = createPrimaryLocation
        val location = createSecondaryLocation

        val record = AggregatorInternalRecord(
          location = Some(primaryLocation),
          replicas = List.empty
        )

        val updatedRecord = record.addLocation(location).success.value

        updatedRecord.location shouldBe record.location
      }
    }
  }
}
