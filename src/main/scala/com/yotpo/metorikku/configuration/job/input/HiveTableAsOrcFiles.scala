package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.hive.HiveTableAsOrcFilesInput


case class HiveTableAsOrcFiles(table: String,
                               schema: String,
                               partitions: Seq[Map[String, Seq[String]]]) extends InputConfig {
  override def getReader(name: String): Reader = {
    HiveTableAsOrcFilesInput(name, table, schema, partitions)
  }
}
