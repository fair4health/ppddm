package ppddm.manager.exception

final case class AgentCommunicationException(name: Option[String] = None, url: Option[String] = None, reason: String, cause: Throwable = None.orNull)
  extends Exception(reason: String, cause: Throwable) {
}
