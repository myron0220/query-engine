// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.andygrove.kquery.benchmarks

import java.io.File
import java.io.FileWriter
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import io.andygrove.kquery.datasource.InMemoryDataSource
import io.andygrove.kquery.datatypes.RecordBatch
//import io.andygrove.kquery.execution.ExecutionContext

import kotlin.system.measureTimeMillis
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType
import io.andygrove.kquery.datatypes.ArrowTypes
import io.andygrove.kquery.execution.ExecutionContext
import io.andygrove.kquery.logical.*
import io.andygrove.kquery.optimizer.Optimizer

/** Designed to be run from Docker. See top-level benchmarks folder for more info. */
class Benchmarks {
  companion object {
    @JvmStatic
//    fun main(args: Array<String>) {
//
//      println("maxMemory=${Runtime.getRuntime().maxMemory()}")
//      println("totalMemory=${Runtime.getRuntime().totalMemory()}")
//      println("freeMemory=${Runtime.getRuntime().freeMemory()}")
//
//      //    val sql = System.getenv("BENCH_SQL_PARTIAL")
//      //    val sql = System.getenv("BENCH_SQL_FINAL")
//
//      // TODO parameterize
//
//      val sqlPartial =
////          "SELECT passenger_count, " +
//////              "MIN(CAST(fare_amount AS double)) AS min_fare," +
////              "MAX(CAST(fare_amount AS double)) AS max_fare" +
//////              "SUM(CAST(fare_amount AS double)) AS sum_fare " +
////              "FROM tripdata " +
////              "GROUP BY passenger_count"
//          "SELECT MIN(CAST(total_amount AS double)) FROM tripdata"
//
//      val sqlFinal =
////          "SELECT passenger_count, " +
//////              "MIN(max_fare), " +
////              "MAX(min_fare)" +
//////              "SUM(max_fare) " +
////              "FROM tripdata " +
////              "GROUP BY passenger_count"
//        "SELECT MAX(CAST(total_amount AS double)) FROM tripdata"
//
//      val path = System.getenv("BENCH_PATH")
//      val resultFile = System.getenv("BENCH_RESULT_FILE")
//
//      val settings = mapOf(Pair("ballista.csv.batchSize", "1024"))
//
//      // TODO iterations
//
//      sqlAggregate(path, sqlPartial, sqlFinal, resultFile, settings)
//
//      println("maxMemory=${Runtime.getRuntime().maxMemory()}")
//      println("totalMemory=${Runtime.getRuntime().totalMemory()}")
//      println("freeMemory=${Runtime.getRuntime().freeMemory()}")
//    }

    fun main(args: Array<String>) {

      val ctx = ExecutionContext(mapOf())

      // wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv

      /*
      VendorID: Utf8,
      tpep_pickup_datetime: Utf8,
      tpep_dropoff_datetime: Utf8,
      passenger_count: Utf8,
      trip_distance: Utf8,
      RatecodeID: Utf8,
      store_and_fwd_flag: Utf8,
      PULocationID: Utf8,
      DOLocationID: Utf8,
      payment_type: Utf8,
      fare_amount: Utf8,
      extra: Utf8,
      mta_tax: Utf8,
      tip_amount: Utf8,
      tolls_amount: Utf8,
      improvement_surcharge: Utf8,
      total_amount: Utf8,
      congestion_surcharge: Utf8
      */

      val time =
        measureTimeMillis {
          val df =
            ctx.csv("/Users/kidsama/Documents/Current_Courses/query_engine/how-query-engines-work/jvm/benchmark-data/yellow_tripdata_2019-12.csv")
              .aggregate(
                listOf(col("passenger_count")),
                listOf(max(cast(col("fare_amount"), ArrowTypes.FloatType))))

          println("------ Logical Plan ------")
          println("${format(df.logicalPlan())}")
          println("----------------------------")
          println()

          //        var results = ctx.execute(df.logicalPlan())
          //        results.forEach {
          //            println(it.schema)
          //            println(it.toCSV())
          //        }

          val optimizedPlan = Optimizer().optimize(df.logicalPlan())
          println("------ Optimized Plan ------")
          println("${format(optimizedPlan)}")
          println("----------------------------")
          println()

          val results = ctx.execute(optimizedPlan)
          results.forEach {
            println(it.schema)
            println(it.toCSV())
          }
        }

      println("Query took $time ms")
    }
  }
}

private fun getFiles(path: String): List<String> {
  // TODO improve to do recursion
  println("path: " + path)
  val dir = File(path)
  println("dir: " + dir)
  return dir.list().filter { it.endsWith(".csv") }
}

private fun sqlAggregate(
    path: String,
    sqlPartial: String,
    sqlFinal: String,
    resultFile: String,
    settings: Map<String, String>
) {
  val start = System.currentTimeMillis()
  val files = getFiles(path)
  val deferred =
      files.map { file ->
        GlobalScope.async {
          println("Executing query against $file ...")
          val partitionStart = System.currentTimeMillis()
          val result = executeQuery(File(File(path), file).absolutePath, sqlPartial, settings)
          val duration = System.currentTimeMillis() - partitionStart
          println("Query against $file took $duration ms")
          result
        }
      }
  val results: List<RecordBatch> = runBlocking { deferred.flatMap { it.await() } }

  println(results.first().schema)

  val ctx = ExecutionContext(settings)
  ctx.registerDataSource("tripdata", InMemoryDataSource(results.first().schema, results))
  val df = ctx.sql(sqlFinal)
  ctx.execute(df).forEach { println(it) }

  val duration = System.currentTimeMillis() - start
  println("Executed query in $duration ms")

  val w = FileWriter(File(resultFile))
  w.write("iterations,time_millis\n")
  w.write("1,$duration\n")
  w.close()
}

fun executeQuery(path: String, sql: String, settings: Map<String, String>): List<RecordBatch> {
  val ctx = ExecutionContext(settings)
  ctx.registerCsv("tripdata", path)
  val df = ctx.sql(sql)
  return ctx.execute(df).toList()
}
