package ppddm.manager

/**
 * Entrypoint (the Main function) of the PPDDM-Manager.
 */
object Boot {
  def main(args: Array[String]): Unit = {
    Manager.start()
  }
}
