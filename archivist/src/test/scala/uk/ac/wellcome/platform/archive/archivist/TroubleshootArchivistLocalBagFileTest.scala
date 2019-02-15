package uk.ac.wellcome.platform.archive.archivist

import java.io.File

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.platform.archive.archivist.fixtures.ArchivistFixtures

// Useful test to troubleshoot running the archivist using a local bagfile
class TroubleshootArchivistLocalBagFileTest
    extends FunSpec
    with Matchers
    with ArchivistFixtures {

  ignore("downloads, uploads and verifies a known BagIt bag") {
    withArchivist() {
      case (ingestBucket, storageBucket, QueuePair(queue, _), _, _) =>
        val file = new File(
            List(
              System.getProperty("user.home"),
            "Desktop",
            "b30529943.zip"
          ).mkString("/")
        )
        sendBag(file, ingestBucket, queue) { _ =>
          while (true) {
            Thread.sleep(10000)
            println(s"Uploaded: ${listKeysInBucket(storageBucket).size}")
          }
        }
    }
  }
}
