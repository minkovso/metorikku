package com.yotpo.metorikku.input.readers.hive

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

case class HiveTableInput(name: String,
                     table: String,
                     schema: String,
                     partitions: Option[Seq[Map[String, Seq[String]]]]
                    ) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = {
    val condExpr = partitions match {
      case Some(p) => p.flatMap(x => x.map { case (k, v) => col(k).isInCollection(v) }).reduce((c1, c2) => c1 and c2)
      case None => lit(true)
    }
    sparkSession
      .table(s"$schema.$table")
      .where(condExpr)
  }
}
