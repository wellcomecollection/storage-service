package uk.ac.wellcome.platform.archive.common.ingest.fixtures

import java.time.{Duration, Instant}

import org.scalatest.{Assertion, Matchers}

trait TimeTestFixture extends Matchers {
  def assertRecent(instant: Instant, recentSeconds: Int = 1): Assertion =
    Duration
      .between(instant, Instant.now)
      .getSeconds should be <= recentSeconds.toLong

  def assertAllRecent(instants: Seq[Instant], recentSeconds: Int = 1): Unit =
    instants.foreach(i => assertRecent(i, recentSeconds))
}
