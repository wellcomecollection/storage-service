package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.ingests.operation.{OperationCompleted, OperationFailure, OperationSuccess}

trait OperationGenerators extends RandomThings {

  case class TestSummary(description: String) {
    override def toString: String = this.description
  }

  def createTestSummary() = TestSummary(
    randomAlphanumeric()
  )

  def createOperationSuccess() = createOperationSuccessWith()

  def createOperationSuccessWith(summary: TestSummary = createTestSummary()) =
    OperationSuccess(summary)

  def createOperationCompleted() = createOperationCompletedWith()

  def createOperationCompletedWith(summary: TestSummary = createTestSummary()) =
    OperationCompleted(summary)

  def createOperationFailure() = createOperationFailureWith()

  def createOperationFailureWith(summary: TestSummary = createTestSummary(),
                                 throwable: Throwable = new RuntimeException(
                                   "error")) =
    OperationFailure(summary, throwable)
}
