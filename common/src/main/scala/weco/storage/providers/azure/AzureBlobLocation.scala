package weco.storage.providers.azure

import java.nio.file.Paths

import weco.storage.{Location, Prefix}

case class AzureBlobLocation(
  container: String,
  name: String
) extends Location {
  override def toString: String =
    s"azure://$container/$name"

  // More than one consecutive slash is going to be weird in the console, and
  // is probably indicative of a bug.  Disallow it.
  require(
    !name.contains("//"),
    s"Azure blob name cannot include multiple consecutive slashes: $name"
  )

  require(
    !name.endsWith("/"),
    s"Azure blob name cannot end with a slash: $name"
  )

  // Having a '.' or '..' in a filesystem path usually indicates "current directory"
  // or "parent directory".  An object store isn't the same as a filesystem,
  // so prevent our code from creating objects with such names.
  require(
    Paths.get(name).normalize().toString == name,
    s"Azure blob name cannot contain '.' or '..' entries: $name"
  )

  def join(parts: String*): AzureBlobLocation =
    this.copy(
      name = Paths.get(name, parts: _*).toString
    )

  def asPrefix: AzureBlobLocationPrefix =
    AzureBlobLocationPrefix(
      container = container,
      namePrefix = name
    )
}

case class AzureBlobLocationPrefix(
  container: String,
  namePrefix: String
) extends Prefix[AzureBlobLocation] {
  override def toString: String =
    s"azure://$container/$namePrefix"

  // More than one consecutive slash is going to be weird in the console, and
  // is probably indicative of a bug.  Disallow it.
  require(
    !namePrefix.contains("//"),
    s"Azure blob name prefix cannot include multiple consecutive slashes: $namePrefix"
  )

  // Having a '.' or '..' in a filesystem path usually indicates "current directory"
  // or "parent directory".  An object store isn't the same as a filesystem,
  // so prevent our code from creating objects with such names.
  require(
    Paths.get(namePrefix.stripSuffix("/")).normalize().toString == namePrefix
      .stripSuffix("/"),
    s"Azure blob name prefix cannot contain '.' or '..' entries: $namePrefix"
  )

  def asLocation(parts: String*): AzureBlobLocation =
    AzureBlobLocation(container = container, name = namePrefix).join(parts: _*)

  override def namespace: String = container
  override def pathPrefix: String = namePrefix

  override def parent: Prefix[AzureBlobLocation] =
    this.copy(namePrefix = parentOf(namePrefix))
}
