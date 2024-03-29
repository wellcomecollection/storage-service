package weco.storage_service.generators

import java.io.{File, FileOutputStream}
import java.nio.file.Paths
import java.time.LocalDate
import weco.fixtures.RandomGenerators
import weco.messaging.fixtures.SQS.Queue
import weco.storage_service.bagit.models._
import weco.storage_service.ingests.models.{IngestID, StorageProvider}
import weco.storage_service.checksum._

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration._
import scala.util.Random

trait StorageRandomGenerators extends RandomGenerators {

  def randomChecksumValue = ChecksumValue(randomAlphanumeric())

  def randomMultiChecksum: MultiManifestChecksum =
    MultiManifestChecksum(
      md5 = None,
      sha1 = None,
      sha256 = Some(randomChecksumValue),
      sha512 = None
    )

  val dummyQueue: Queue = Queue(
    url = "test://test-q",
    arn = "arn::sqs::test",
    visibilityTimeout = 1 seconds
  )

  def randomPaths(maxDepth: Int = 4, dirs: Int = 4): List[String] = {
    (1 to dirs).map { _ =>
      val depth = Random.nextInt(maxDepth)

      (1 to depth).foldLeft("") { (memo, _) =>
        Paths.get(memo, randomAlphanumeric()).toString
      }
    }.toList
  }

  def randomFilesInDirs(
    fileCount: Int = 10,
    dirs: Int = 4,
    maxDepth: Int = 4,
    minSize: Int = 265,
    maxSize: Int = 1024
  ): Seq[File] = {

    def createFile(name: String) = {
      val fileSize =
        Random.nextInt(maxSize - minSize) + minSize

      randomFile(
        size = fileSize,
        path = name,
        useBytes = true
      )
    }

    val paths = randomPaths(maxDepth, dirs)

    (1 to fileCount).map { _ =>
      val index = Random.nextInt(paths.length)
      val path = paths(index)

      createFile(
        Paths.get(path, s"${randomAlphanumeric()}.test").toString
      )
    }
  }

  val tmpDir: String = System.getProperty("java.io.tmpdir")

  def writeToOutputStream(
    path: String = s"${randomUUID.toString}.test"
  )(writeTo: FileOutputStream => Unit): File = {
    val absolutePath = Paths.get(tmpDir, path)

    val file = absolutePath.toFile
    val parentDir = absolutePath.getParent.toFile

    parentDir.mkdirs()

    val fileOutputStream = new FileOutputStream(file)
    writeTo(fileOutputStream)
    fileOutputStream.close()

    file
  }

  def randomFile(
    size: Int = 256,
    path: String = s"${randomUUID.toString}.test",
    useBytes: Boolean = false
  ): File =
    writeToOutputStream(path) { fileOutputStream =>
      val bytes = if (useBytes) {
        randomBytes(size)
      } else {
        randomAlphanumeric(size).getBytes
      }

      fileOutputStream.write(bytes)
    }

  def createIngestID: IngestID =
    IngestID.random

  def randomSourceOrganisation =
    SourceOrganisation(randomAlphanumeric())

  def randomInternalSenderIdentifier =
    InternalSenderIdentifier(randomAlphanumeric())

  def randomInternalSenderDescription =
    InternalSenderDescription(randomAlphanumeric())

  def randomExternalDescription =
    ExternalDescription(randomAlphanumeric())

  def randomLocalDate: LocalDate = {
    val startRange = LocalDate.of(-10000, 1, 1).toEpochDay
    val endRange = LocalDate.of(10000, 12, 31).toEpochDay
    LocalDate.ofEpochDay(
      startRange + ThreadLocalRandom
        .current()
        .nextLong((endRange - startRange) + 1)
    )
  }

  def createBagVersion: BagVersion =
    BagVersion(randomInt(from = 1, to = 100000))

  def randomChecksumAlgorithm: ChecksumAlgorithm =
    chooseFrom(ChecksumAlgorithms.algorithms: _*)

  def createBagPath: BagPath = BagPath(randomAlphanumeric())

  def createBagPathWithPrefix(prefix: String, name: String): BagPath =
    BagPath(s"$prefix/$name")

  def createChecksumWith(algorithm: ChecksumAlgorithm = SHA256): Checksum =
    Checksum(algorithm = algorithm, value = randomChecksumValue)

  def createChecksum: Checksum =
    createChecksumWith()

  def createProvider: StorageProvider =
    StorageProvider(
      id = chooseFrom(StorageProvider.allowedValues: _*)
    )

  def createStepName: String =
    s"step-${randomAlphanumeric()}"
}
