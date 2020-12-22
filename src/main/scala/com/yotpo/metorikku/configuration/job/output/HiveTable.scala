package com.yotpo.metorikku.configuration.job.output

case class HiveTable(table: String,
                     schema: String,
                     partitions: Option[Seq[Map[String, Seq[String]]]])
