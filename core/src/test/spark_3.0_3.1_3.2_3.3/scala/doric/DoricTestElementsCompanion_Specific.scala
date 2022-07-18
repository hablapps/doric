package doric

import org.apache.spark.sql.internal.SQLConf

trait DoricTestElementsCompanion_Specific {
  lazy val JAVA8APIENABLED = SQLConf.DATETIME_JAVA8API_ENABLED
}
