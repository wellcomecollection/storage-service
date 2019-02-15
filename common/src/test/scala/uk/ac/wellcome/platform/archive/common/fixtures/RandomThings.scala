package uk.ac.wellcome.platform.archive.common.fixtures

import java.time.LocalDate
import java.util.UUID

import uk.ac.wellcome.platform.archive.common.models.bagit._
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.util.Random

trait RandomThings {
  def randomAlphanumeric(length: Int = 8) = {
    Random.alphanumeric take length mkString
  }

  def randomAlphanumericWithSpace(length: Int = 8) = {
    val str = randomAlphanumeric(length).toCharArray

    // Randomly choose an index in the string to replace with a space,
    // avoiding the beginning or the end.
    val spaceIndex = Random.nextInt(str.length - 2) + 1
    str.updated(spaceIndex, ' ')
  }

  def randomUUID = UUID.randomUUID()

  def randomSourceOrganisation =
    SourceOrganisation(randomAlphanumeric())

  def randomInternalSenderIdentifier =
    InternalSenderIdentifier(randomAlphanumeric())

  def randomInternalSenderDescription =
    InternalSenderDescription(randomAlphanumeric())

  def randomExternalDescription =
    ExternalDescription(randomAlphanumeric())

  def randomPayloadOxum =
    PayloadOxum(Random.nextLong().abs, Random.nextInt().abs)

  def randomLocalDate = {
    val startRange = -999999999
    val maxValue = 1999999998
    LocalDate.ofEpochDay(startRange + Random.nextInt(maxValue))
  }

  def randomBagPath = BagPath(randomAlphanumeric(15))

  def randomBucket = Bucket(randomAlphanumeric())
}
