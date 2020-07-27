package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.time.Instant

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagunpacker.models.UnpackSummary
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.storage.providers.memory.{MemoryLocation, MemoryLocationPrefix}

import scala.util.Random

class UnpackerMessageTest
    extends AnyFunSpec
    with Matchers
    with StorageRandomThings {
  it("handles a single file correctly") {
    val summary = createSummaryWith(fileCount = 1)

    UnpackerMessage.create(summary) should endWith("from 1 file")
  }

  it("handles multiple files correctly") {
    val summary = createSummaryWith(fileCount = 5)

    UnpackerMessage.create(summary) should endWith("from 5 files")
  }

  it("adds a comma to the file counts if appropriate") {
    val summary = createSummaryWith(fileCount = 123456789)

    UnpackerMessage.create(summary) should endWith("from 123,456,789 files")
  }

  it("pretty-prints the file size") {
    val summary = createSummaryWith(bytesUnpacked = 123456789)

    UnpackerMessage.create(summary) should startWith("Unpacked 117 MB")
  }

  def createSummaryWith(
    fileCount: Long = Random.nextLong(),
    bytesUnpacked: Long = Random.nextLong()
  ): UnpackSummary[_, _] =
    UnpackSummary(
      ingestId = createIngestID,
      srcLocation = MemoryLocation(
        namespace = randomAlphanumeric,
        path = randomAlphanumeric
      ),
      dstPrefix = MemoryLocationPrefix(
        namespace = randomAlphanumeric,
        path = randomAlphanumeric
      ),
      fileCount = fileCount,
      bytesUnpacked = bytesUnpacked,
      startTime = Instant.now()
    )
}
