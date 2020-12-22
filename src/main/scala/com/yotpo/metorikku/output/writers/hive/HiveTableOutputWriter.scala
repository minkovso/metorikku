package com.yotpo.metorikku.output.writers.hive

import com.yotpo.metorikku.configuration.job.output.HiveTable
import com.yotpo.metorikku.output.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame

class HiveTableOutputWriter(props: Map[String, Object], outputTable: Option[HiveTable]) extends Writer {

  require(outputTable.isDefined, "Output is mandatory.")

  val log = LogManager.getLogger(this.getClass)

  case class TableOutputProperties(saveMode: Option[String],
                                   tableProperties: Option[Map[String, String]],
                                   extraOptions: Option[Map[String, String]])

  val fileOutputProperties = TableOutputProperties(
    props.get("saveMode").asInstanceOf[Option[String]],
    props.get("tableProperties").asInstanceOf[Option[Map[String, String]]],
    props.get("extraOptions").asInstanceOf[Option[Map[String, String]]])

  override def write(dataFrame: DataFrame): Unit = {
    val writer = dataFrame.write

    fileOutputProperties.saveMode match {
      case Some(saveMode) => writer.mode(saveMode)
      case None =>
    }

    fileOutputProperties.extraOptions match {
      case Some(options) => writer.options(options)
      case None =>
    }

    val tableName = s"${outputTable.get.schema}.${outputTable.get.table}"
    writer.insertInto(tableName)
  }
}
