package com.yotpo.metorikku.input.readers.hive

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import ru.mail.dwh.common.TableUtils.readTableAsUnionOrcFiles

case class HiveTableAsOrcFilesInput(name: String,
                                    table: String,
                                    schema: String,
                                    partitions: Seq[Map[String, Seq[String]]]
                                   ) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = {
    val condExpr = partitions.flatMap(x => x.map{case (k, v) => col(k).isInCollection(v)}).reduce((c1, c2) => c1 and c2)
    readTableAsUnionOrcFiles(s"$schema.$table", condExpr, sparkSession)
  }
}
