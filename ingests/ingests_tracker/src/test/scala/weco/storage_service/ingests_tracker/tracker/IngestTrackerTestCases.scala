package weco.storage_service.ingests_tracker.tracker

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Assertion, EitherValues}
import weco.fixtures.TestWith
import weco.storage_service.generators.IngestGenerators
import weco.storage_service.ingests.models.{Ingest, IngestEvent}

trait IngestTrackerTestCases[Context]
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with IngestGenerators
    with TableDrivenPropertyChecks {
  def withContext[R](testWith: TestWith[Context, R]): R

  def withIngestTracker[R](initialIngests: Seq[Ingest] = Seq.empty)(
    testWith: TestWith[IngestTracker, R]
  )(implicit context: Context): R

  private def withIngestTrackerFixtures[R](
    initialIngests: Seq[Ingest] = Seq.empty
  )(testWith: TestWith[IngestTracker, R]): R =
    withContext { implicit context =>
      withIngestTracker(initialIngests) { tracker =>
        testWith(tracker)
      }
    }

  def withBrokenUnderlyingInitTracker[R](testWith: TestWith[IngestTracker, R])(
    implicit context: Context
  ): R
  def withBrokenUnderlyingGetTracker[R](testWith: TestWith[IngestTracker, R])(
    implicit context: Context
  ): R
  def withBrokenUnderlyingUpdateTracker[R](
    testWith: TestWith[IngestTracker, R]
  )(implicit context: Context): R

  describe("init()") {
    it("allows calling init() twice with the same value") {
      withIngestTrackerFixtures() { tracker =>
        val ingest = createIngest
        println(s"@@AWLC ingest = $ingest")
        tracker.init(ingest) shouldBe a[Right[_, _]]
        tracker.init(ingest) shouldBe a[Right[_, _]]
      }
    }
  }

  protected def assertIngestsEqual(
    ingest1: Ingest,
    ingest2: Ingest
  ): Assertion =
    ingest1 shouldBe ingest2

  def assertIngestSeqEqual(
    seq1: Seq[Ingest],
    seq2: Seq[Ingest]
  ): Seq[Assertion] = {
    seq1.size shouldBe seq2.size

    seq1.zip(seq2).map {
      case (ingest1, ingest2) =>
        assertIngestsEqual(ingest1, ingest2)
    }
  }

  protected def assertIngestEventsEqual(
    event1: IngestEvent,
    event2: IngestEvent
  ): Assertion =
    event1 shouldBe event2

  def assertIngestEventSeqEqual(
    seq1: Seq[IngestEvent],
    seq2: Seq[IngestEvent]
  ): Seq[Assertion] = {
    seq1.size shouldBe seq2.size

    seq1.zip(seq2).map {
      case (event1, event2) =>
        assertIngestEventsEqual(event1, event2)
    }
  }
}
