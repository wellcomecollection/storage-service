package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.bagit.models.PayloadOxum
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings

import scala.util.Random

trait PayloadOxumGenerators extends StorageRandomThings {
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
