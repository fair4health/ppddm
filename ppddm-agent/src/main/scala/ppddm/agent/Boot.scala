package ppddm.agent

/**
 * Entrypoint (the Main function) of the PPDDM-Agent.
 */
object Boot {
  def main(args: Array[String]): Unit = {
    Agent.start()
  }
}
