package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.hive.HiveTableInput

case class HiveTable(table: String,
                     schema: String,
                     partitions: Option[Seq[Map[String, Seq[String]]]]) extends InputConfig {
  override def getReader(name: String): Reader = {
    HiveTableInput(name, table, schema, partitions)
  }
}