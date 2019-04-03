package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.operation.services.{
  IngestCompleted,
  IngestFailed,
  IngestStepSuccess
}

trait OperationGenerators extends RandomThings {

  case class TestSummary(description: String) {
    override def toString: String = this.description
  }

  def createTestSummary() = TestSummary(
    randomAlphanumeric()
  )

  def createOperationSuccess() = createOperationSuccessWith()

  def createOperationSuccessWith(summary: TestSummary = createTestSummary()) =
    IngestStepSuccess(summary)

  def createOperationCompleted() = createOperationCompletedWith()

  def createOperationCompletedWith(summary: TestSummary = createTestSummary()) =
    IngestCompleted(summary)

  def createOperationFailure() = createOperationFailureWith()

  def createOperationFailureWith(summary: TestSummary = createTestSummary(),
                                 throwable: Throwable = new RuntimeException(
                                   "error")) =
    IngestFailed(summary, throwable)
}
