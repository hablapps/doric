package doric
package syntax

import doric.types.TimestampType
import org.apache.spark.sql.{functions => f}

import java.sql.Timestamp

private[syntax] trait TimestampColumns {

  /**
    * Returns the current timestamp at the start of query evaluation as a timestamp column.
    * All calls of current_timestamp within the same query return the same value.
    *
    * @group Timestamp Type
    */
  def currentTimestamp(): TimestampColumn =
    f.current_timestamp().asDoric[Timestamp]

  implicit class TimestampColumnLikeSyntax[T: TimestampType](
      column: DoricColumn[T]
  ) {

    /**
      * Extracts the seconds as an integer from a given timestamp.
      *
      * @group Timestamp Type
      */
    def second: IntegerColumn = column.elem.map(f.second).toDC

    /**
      * Generates tumbling time windows given a timestamp specifying column. Window
      * starts are inclusive but the window ends are exclusive.
      *
      * @param windowDuration A string specifying the width of the window, e.g. `10 minutes`,
      *                       `1 second`. Check `org.apache.spark.unsafe.types.CalendarInterval` for
      *                       valid duration identifiers.
      * @group Timestamp Type
      */
    def window(windowDuration: String): DStructColumn =
      column.elem.map(x => f.window(x, windowDuration)).toDC

    /**
      * Generates tumbling time windows given a timestamp specifying column. Window
      * starts are inclusive but the window ends are exclusive.
      *
      * @param windowDuration A string specifying the width of the window, e.g. `10 minutes`,
      *                       `1 second`. Check `org.apache.spark.unsafe.types.CalendarInterval` for
      *                       valid duration identifiers. Note that the duration is a fixed length of
      *                       time, and does not vary over time according to a calendar. For example,
      *                       `1 day` always means 86,400,000 milliseconds, not a calendar day.
      * @param slideDuration A string specifying the sliding interval of the window, e.g. `1 minute`.
      *                      A new window will be generated every `slideDuration`. Must be less than
      *                      or equal to the `windowDuration`. Check
      *                      `org.apache.spark.unsafe.types.CalendarInterval` for valid duration
      *                      identifiers. This duration is likewise absolute, and does not vary
      *                      according to a calendar.
      * @param startTime The offset with respect to 1970-01-01 00:00:00 UTC with which to start
      *                  window intervals. For example, in order to have hourly tumbling windows that
      *                  start 15 minutes past the hour, e.g. 12:15-13:15, 13:15-14:15... provide
      *                  `startTime` as `15 minutes`.
      * @group Timestamp Type
      */
    def window(
        windowDuration: String,
        slideDuration: String,
        startTime: String = "0 second"
    ): DStructColumn =
      column.elem
        .map(x => f.window(x, windowDuration, slideDuration, startTime))
        .toDC

    /**
      * @group Timestamp Type
      * @return
      *   a Date Column without the hour
      */
    def toDate: DateColumn = column.elem.map(f.to_date).toDC

    /**
      * @group Timestamp Type
      * @return
      *   a LocalDate Column without the hour
      */
    def toLocalDate: LocalDateColumn = column.elem.map(f.to_date).toDC
  }
}
