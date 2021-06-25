package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import java.nio.file.{Files, Paths}

import weco.storage.streaming.InputStreamWithLength

trait LocalResources {
  def getResource(name: String): InputStreamWithLength = {
    val uri = getClass.getResource(name)

    new InputStreamWithLength(
      inputStream = getClass.getResourceAsStream(name),
      length = Files.size(Paths.get(uri.getFile))
    )
  }
}
