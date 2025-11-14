package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.io.{BufferedWriter, File, FileWriter}

object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    // TODO

    //createOutput(inds)
  }

  private def createOutput(dataset: Dataset[(String, String)]): Unit = {
    // Collect, stringify and sort INDs
    val inds = dataset.collect().map(ind => ind._1 + " c " + ind._2).sorted

    // Print results to the console
    inds.foreach(println(_))

    // Write results into a result file
    val writer = new BufferedWriter(new FileWriter(new File("result.txt")))
    inds.foreach(s => writer.write(s + "\r\n"))
    writer.close()
  }
}
