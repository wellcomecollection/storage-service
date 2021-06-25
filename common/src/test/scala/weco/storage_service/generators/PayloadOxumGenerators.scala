package weco.storage_service.generators

import weco.storage_service.bagit.models.PayloadOxum

import scala.util.Random

trait PayloadOxumGenerators extends StorageRandomGenerators {
  def createPayloadOxumWith(
    payloadBytes: Long = Random.nextLong().abs,
    numberOfPayloadFiles: Int = randomInt(from = 1, to = 10000)
  ): PayloadOxum =
    PayloadOxum(
      payloadBytes = payloadBytes,
      numberOfPayloadFiles = numberOfPayloadFiles
    )

  def createPayloadOxum: PayloadOxum = createPayloadOxumWith()
}
