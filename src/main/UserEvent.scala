package com.github.rajendharmendra.sparkstreaming

import argonaut.Argonaut._
import argonaut.CodecJson

case class UserEvent(id: Int, data: String, isLast: Boolean)
object UserEvent {
  implicit def codec: CodecJson[UserEvent] =
    casecodec3(UserEvent.apply, UserEvent.unapply)("id", "data", "isLast")

  lazy val empty = UserEvent(-1, "", isLast = false)
}
