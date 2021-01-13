package com.yotpo.metorikku.input.readers.hive

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class HiveTableInput(name: String,
                     table: String,
                     schema: String,
                     partitions: Option[Seq[Map[String, Seq[String]]]]
                    ) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = {
    sparkSession
      .table(s"$schema.$table")
  }
}
