package doric

import org.apache.spark.sql.internal.SQLConf.buildConf

trait DoricTestElementsCompanion_Specific {
  lazy val JAVA8APIENABLED = buildConf(
    "spark.sql.datetime.java8API.enabled"
  ).booleanConf.createWithDefaultString("false")
}