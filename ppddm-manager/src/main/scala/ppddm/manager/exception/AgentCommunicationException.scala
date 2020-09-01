package ppddm.manager.exception

final case class AgentCommunicationException(name: String, url: String, reason: String, cause: Throwable = None.orNull)
  extends Exception(reason: String, cause: Throwable) {
}
