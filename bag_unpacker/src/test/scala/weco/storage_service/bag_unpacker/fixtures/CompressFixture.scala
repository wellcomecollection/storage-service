package weco.storage_service.bag_unpacker.fixtures

import java.io.{File, _}
import grizzled.slf4j.Logging
import org.apache.commons.compress.archivers.{
  ArchiveEntry,
  ArchiveOutputStream,
  ArchiveStreamFactory
}
import org.apache.commons.compress.compressors.{
  CompressorOutputStream,
  CompressorStreamFactory
}
import org.apache.commons.io.IOUtils
import org.scalatest.matchers.should.Matchers
import weco.fixtures.TestWith
import weco.storage_service.generators.StorageRandomGenerators
import weco.storage.Location
import weco.storage.store.StreamStore
import weco.storage.streaming.InputStreamWithLength

trait CompressFixture[BagLocation <: Location, Namespace]
    extends StorageRandomGenerators
    with Matchers
    with Logging {

  val defaultFileCount = 10

  def createLocationWith(namespace: Namespace, path: String): BagLocation

  def withArchive[R](
    namespace: Namespace,
    archiveFile: File
  )(testWith: TestWith[BagLocation, R])(
    implicit streamStore: StreamStore[BagLocation]
  ): R = {
    val location = createLocationWith(
      namespace = namespace,
      path = archiveFile.getName
    )

    streamStore.put(location)(
      new InputStreamWithLength(
        new FileInputStream(archiveFile),
        length = archiveFile.length()
      )
    ) shouldBe a[Right[_, _]]

    testWith(location)
  }

  def createTgzArchiveWithRandomFiles(
    fileCount: Int = 10,
    maxDepth: Int = 4,
    minSize: Int = 265,
    maxSize: Int = 1024
  ): (File, Seq[File], Set[ArchiveEntry]) =
    createTgzArchiveWithFiles(
      randomFilesInDirs(
        fileCount = fileCount,
        dirs = fileCount / 4,
        maxDepth = maxDepth,
        minSize = minSize,
        maxSize = maxSize
      )
    )

  def createTgzArchiveWithFiles(
    files: Seq[File]
  ): (File, Seq[File], Set[ArchiveEntry]) =
    createArchiveWith(
      archiverName = "tar",
      compressorName = "gz",
      files = files
    )

  def createArchiveWithRandomFiles(
    archiverName: String,
    compressorName: String,
    fileCount: Int = 10
  ): (File, Seq[File], Set[ArchiveEntry]) =
    createArchiveWith(
      archiverName,
      compressorName,
      randomFilesInDirs(
        fileCount,
        fileCount / 4
      )
    )

  def createArchiveWith(
    archiverName: String,
    compressorName: String,
    files: Seq[File]
  ): (File, Seq[File], Set[ArchiveEntry]) = {
    val archiveFile = File.createTempFile(
      randomUUID.toString,
      ".test"
    )

    val fileOutputStream =
      new FileOutputStream(archiveFile)

    val archive = new Archive(
      archiverName,
      compressorName,
      fileOutputStream
    )

    val entries = files.map { file =>
      val entryName = relativeToTmpDir(file)
      debug(s"Archiving ${file.getAbsolutePath} in $entryName")
      archive.addFile(
        file,
        entryName
      )
    } toSet

    archive.finish()
    fileOutputStream.close()

    (archiveFile, files, entries)
  }

  def relativeToTmpDir(file: File): String =
    new File(tmpDir).toURI
      .relativize(file.toURI)
      .getPath

  private[fixtures] class Archive(
    archiverName: String,
    compressorName: String,
    outputStream: OutputStream
  ) {

    private val compress = compressor(compressorName)(_)
    private val pack = packer(archiverName)(_)

    private val compressorOutputStream =
      compress(outputStream)

    private val archiveOutputStream = pack(
      compressorOutputStream
    )

    def finish(): Unit = {
      archiveOutputStream.flush()
      archiveOutputStream.finish()
      compressorOutputStream.flush()
      compressorOutputStream.close()
    }

    def addFile(
      file: File,
      entryName: String
    ): ArchiveEntry = {
      synchronized {

        val entry = archiveOutputStream
          .createArchiveEntry(file, entryName)

        val fileInputStream = new BufferedInputStream(
          new FileInputStream(file)
        )

        archiveOutputStream.putArchiveEntry(entry)

        IOUtils.copy(
          fileInputStream,
          archiveOutputStream
        )

        archiveOutputStream.closeArchiveEntry()

        fileInputStream.close()

        entry
      }
    }

    private def compressor(
      compressorName: String
    )(
      outputStream: OutputStream
    ): CompressorOutputStream = {

      val compressorStreamFactory =
        new CompressorStreamFactory()

      val bufferedOutputStream =
        new BufferedOutputStream(outputStream)

      compressorStreamFactory
        .createCompressorOutputStream(
          compressorName,
          bufferedOutputStream
        )
    }

    private def packer(
      archiverName: String
    )(
      outputStream: OutputStream
    ): ArchiveOutputStream = {

      val archiveStreamFactory =
        new ArchiveStreamFactory()

      val bufferedOutputStream =
        new BufferedOutputStream(outputStream)

      archiveStreamFactory
        .createArchiveOutputStream(
          archiverName,
          bufferedOutputStream
        )

    }
  }
}
