package org.apache.spark.sql.catalyst.util

import java.time.LocalDate

object BebeDateTimeUtils {

  type SQLDate = Int

  /**
   * Returns first day of the month for the given date. The date is expressed in days
   * since 1.1.1970.
   */
  def getFirstDayOfMonth(date: SQLDate): SQLDate = {
    val localDate = LocalDate.ofEpochDay(date)
    date - localDate.getDayOfMonth + 1
  }

}
