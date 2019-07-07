package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestCompleted,
  IngestFailed,
  IngestStepSucceeded
}

trait IngestOperationGenerators extends StorageRandomThings {

  case class TestSummary(description: String) {
    override def toString: String = this.description
  }

  def createTestSummary() = TestSummary(
    randomAlphanumericWithLength()
  )

  def createOperationSuccess() = createOperationSuccessWith()

  def createOperationSuccessWith(summary: TestSummary = createTestSummary()) =
    IngestStepSucceeded(summary)

  def createOperationCompleted() = createOperationCompletedWith()

  def createOperationCompletedWith(summary: TestSummary = createTestSummary()) =
    IngestCompleted(summary)

  def createOperationFailure() = createIngestFailureWith()

  def createIngestFailureWith(summary: TestSummary = createTestSummary(),
                              throwable: Throwable = new RuntimeException(
                                "error"),
                              maybeFailureMessage: Option[String] = None) =
    IngestFailed(summary, throwable, maybeFailureMessage)
}
