package uk.ac.wellcome.platform.archive.common.generators

import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.operation.services.{
  OperationCompleted,
  OperationFailure,
  OperationSuccess
}

trait OperationGenerators extends RandomThings {

  case class TestSummary(description: String) {
    override def toString: String = this.description
  }

  def createTestSummary() = TestSummary(
    randomAlphanumeric()
  )

  def createOperationSuccessWith(summary: TestSummary) =
    OperationSuccess(summary)

  def createOperationCompletedWith(summary: TestSummary) =
    OperationCompleted(summary)

  def createOperationFailureWith(summary: TestSummary,
                                 throwable: Throwable = new RuntimeException(
                                   "error")) =
    OperationFailure(summary, throwable)
}
