package ppddm.core.util

import java.time.{Duration => JDuration}

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * Converters from/to Java Duration from/to Scala Duration
 */
private[ppddm] object DurationConverters {
  def asFiniteDuration(duration: JDuration): FiniteDuration = duration.asScala

  final implicit class JavaDurationOps(val self: JDuration) extends AnyVal {
    def asScala: FiniteDuration = Duration.fromNanos(self.toNanos)
  }

  final implicit class ScalaDurationOps(val self: Duration) extends AnyVal {
    def asJava: JDuration = JDuration.ofNanos(self.toNanos)
  }
}
