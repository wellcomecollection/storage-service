package weco.messaging.fixtures.worker

import weco.messaging.worker._
import weco.messaging.worker.models._
import weco.monitoring.Metrics

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait WorkerFixtures {
  type MySummary = String
  type TestResult = Result[MySummary]
  type TestInnerProcess = MyWork => TestResult
  type TestProcess = MyWork => Future[TestResult]

  case class MyMessage(s: String)
  case class MyWork(s: String)

  object MyWork {
    def apply(message: MyMessage): MyWork =
      new MyWork(message.s)
  }

  def parseMessage(shouldFail: Boolean)(message: MyMessage): Try[MyWork] =
    if (shouldFail) {
      Failure(new RuntimeException("BOOM"))
    } else {
      Success(MyWork(message))
    }

  def actionToAction(toActionShouldFail: Boolean)(result: Result[MySummary])(
    implicit ec: ExecutionContext): Future[MyExternalMessageAction] = Future {
    if (toActionShouldFail) {
      throw new RuntimeException("BOOM")
    } else {
      MyExternalMessageAction(result)
    }
  }

  case class MyExternalMessageAction(result: Result[_])

  class MyWorker(
    val metricsNamespace: String,
    testProcess: TestInnerProcess,
    val parseMessage: MyMessage => Try[MyWork]
  )(implicit val ec: ExecutionContext, val metrics: Metrics[Future])
      extends Worker[MyMessage, MyWork, MySummary, MyExternalMessageAction] {
    val callCounter = new CallCounter()

    override val retryAction: MyMessage => MyExternalMessageAction =
      _ =>
        MyExternalMessageAction(
          RetryableFailure[MySummary](failure = new Throwable("BOOM!")))

    override val successfulAction: MyMessage => MyExternalMessageAction =
      _ => MyExternalMessageAction(Successful())

    override val failureAction: MyMessage => MyExternalMessageAction =
      _ =>
        MyExternalMessageAction(
          TerminalFailure[MySummary](failure = new Throwable("BOOM!")))

    override val doWork =
      (work: MyWork) => createResult(testProcess, callCounter)(ec)(work)
  }

  val message = MyMessage("some_content")
  val work = MyWork("some_content")

  class CallCounter() {
    var calledCount = 0
  }

  def createResult(op: TestInnerProcess, callCounter: CallCounter)(
    implicit ec: ExecutionContext): MyWork => Future[TestResult] = {

    (work: MyWork) =>
      {
        callCounter.calledCount = callCounter.calledCount + 1

        Future(op(work))
      }
  }

  val successful = (_: MyWork) =>
    Successful[MySummary](summary = Some("Summary Successful"))

  val retryableFailure = (_: MyWork) =>
    RetryableFailure[MySummary](
      failure = new RuntimeException("RetryableFailure"),
      summary = Some("Summary RetryableFailure"))

  val terminalFailure = (_: MyWork) =>
    TerminalFailure[MySummary](
      failure = new RuntimeException("TerminalFailure"),
      summary = Some("Summary TerminalFailure"))

  val exceptionState = (_: MyWork) => {
    throw new RuntimeException("BOOM")

    Successful[MySummary](Some("exceptionState"))
  }
}
