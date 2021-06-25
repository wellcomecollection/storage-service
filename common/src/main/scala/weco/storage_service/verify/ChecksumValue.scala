package weco.storage_service.verify

import weco.json.{TypedString, TypedStringOps}

class ChecksumValue(val value: String) extends TypedString[ChecksumValue] {
  val underlying: String = value
}

object ChecksumValue extends TypedStringOps[ChecksumValue] {
  override def apply(value: String): ChecksumValue =
    new ChecksumValue(value)

  def create(raw: String): ChecksumValue =
    ChecksumValue(raw.trim)
}
