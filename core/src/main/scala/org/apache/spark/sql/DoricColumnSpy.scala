package org.apache.spark.sql

class DoricColumnSpy {
  def name(column: Column): String = {
    column.named.name
  }

}
