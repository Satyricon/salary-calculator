import java.io.File
import java.sql.{Date, Timestamp}

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

final case class Person(id: String,
                        name: String,
                        startTime: Timestamp,
                        endTime: Timestamp)

final case class PersonByMorningAndEvening(id: String,
                                           name: String,
                                           date: Date,
                                           dayQHours: Long,
                                           eveningQHoursBeforeDay: Long,
                                           eveningQHoursAfterDay: Long)

final case class DailySalary(id: String,
                             name: String,
                             date: Date,
                             normalSalary: Double,
                             eveningSalary: Double,
                             overtime: Double)

object SalaryCalculator {
  val dailyRate: Double = 3.75
  val eveningRate: Double = dailyRate + 1.15
  val overtime25Multiplier: Double = 1.25
  val overtime50Multiplier: Double = 1.5
  val overtimeDoubleMultiplier: Double = 2

  def main(args: Array[String]) {
    if(args.length != 2) {
      println("You forgot to provide input csv file path and output file path")
      System.exit(0)
    }

    val inputFilePath = args(0)
    val outputFilePath = args(1)

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()

    import sparkSession.implicits._

    val initialDF = sparkSession.read.option("header","true").csv(inputFilePath)

    // DataFrame transformed to DataSet of Person case classes
    // startTime and endTime are both Timestamps
    // endTime will be correctly calculated to next day when needed
    val transformedTimesDS = initialDF.select(
      initialDF("Person ID").alias("id"),
      initialDF("Person Name").alias("name"),
      unix_timestamp(concat(initialDF("Date"), initialDF("Start")), "dd.MM.yyyyHH:mm").cast("timestamp").alias("startTime"),
      when(
        unix_timestamp(concat(initialDF("Date"), initialDF("End")), "dd.MM.yyyyHH:mm").cast("timestamp")
          > unix_timestamp(concat(initialDF("Date"), initialDF("Start")), "dd.MM.yyyyHH:mm").cast("timestamp"),
        unix_timestamp(concat(initialDF("Date"), initialDF("End")), "dd.MM.yyyyHH:mm").cast("timestamp"))
        .otherwise(unix_timestamp(concat(initialDF("Date"), initialDF("End")), "dd.MM.yyyyHH:mm").cast("timestamp") + expr("INTERVAL 1 DAY"))
        .alias("endTime")
    ).as[Person]

    // DataSet transformed to PersonByMorningAndEvening with aggregation by morning and evening
    val personByMorningAndEveningDS = transformedTimesDS.map(row => {
      val beginningOfADayT = beginningOfADay(row.startTime);
      val sixOCLockMorning = getSixOCLock(row.startTime)
      val sixOClockEvening = getSixOCLock(row.startTime, false)
      val nextDaySixOClockMorning = if (getSixOCLock(row.endTime).after(sixOCLockMorning)) getSixOCLock(row.endTime) else getSixOCLock(row.endTime, true, true)
      val nextDaySixOClockEvening = if (getSixOCLock(row.endTime, false).after(nextDaySixOClockMorning)) getSixOCLock(row.endTime, false) else getSixOCLock(row.endTime, false, true)

      if (row.startTime.before(sixOCLockMorning) && (row.endTime.before(sixOCLockMorning) || row.endTime == sixOCLockMorning)) {
        val timeDiffInQuarterHoursEveningBeforeDay = timeDifferenceInQuarterHours(row.endTime, row.startTime)

        PersonByMorningAndEvening(row.id, row.name, beginningOfADayT, 0, timeDiffInQuarterHoursEveningBeforeDay, 0)
      }
      else if(row.startTime.before(sixOCLockMorning) && row.endTime.after(sixOCLockMorning) && (row.endTime.before(sixOClockEvening) || row.endTime == sixOClockEvening)) {
        val timeDiffInQuarterHoursEveningBeforeDay = timeDifferenceInQuarterHours(sixOCLockMorning, row.startTime)
        val timeDiffInQuarterHoursDay = timeDifferenceInQuarterHours(row.endTime, sixOCLockMorning)

        PersonByMorningAndEvening(row.id, row.name, beginningOfADayT, timeDiffInQuarterHoursDay, timeDiffInQuarterHoursEveningBeforeDay, 0)
      }
      else if(row.startTime.before(sixOCLockMorning) && row.endTime.after(sixOClockEvening)) {
        val timeDiffInQuarterHoursEveningBeforeDay = timeDifferenceInQuarterHours(sixOCLockMorning, row.startTime)
        val timeDiffInQuarterHoursEveningAfterDay = timeDifferenceInQuarterHours(row.endTime, sixOClockEvening)
        val timeDiffInQuarterHoursDay = timeDifferenceInQuarterHours(sixOClockEvening, sixOCLockMorning)

        PersonByMorningAndEvening(row.id, row.name, beginningOfADayT, timeDiffInQuarterHoursDay, timeDiffInQuarterHoursEveningBeforeDay, timeDiffInQuarterHoursEveningAfterDay)
      }
      else if((row.startTime.after(sixOCLockMorning) || row.startTime == sixOCLockMorning) && row.startTime.before(sixOClockEvening) && (row.endTime.before(sixOClockEvening) || row.endTime == sixOClockEvening)) {
        val timeDiffInQuarterHoursDay = timeDifferenceInQuarterHours(row.endTime, row.startTime)

        PersonByMorningAndEvening(row.id, row.name, beginningOfADayT, timeDiffInQuarterHoursDay, 0, 0)
      }
      else if((row.startTime.after(sixOCLockMorning) || row.startTime == sixOCLockMorning) && row.startTime.before(sixOClockEvening) && row.endTime.after(sixOClockEvening) && (row.endTime.before(nextDaySixOClockMorning) || row.endTime == nextDaySixOClockMorning)) {
        val timeDiffInQuarterHoursDay = timeDifferenceInQuarterHours(sixOClockEvening, row.startTime)
        val timeDiffInQuarterHoursEveningAfterDay = timeDifferenceInQuarterHours(row.endTime, sixOClockEvening)

        PersonByMorningAndEvening(row.id, row.name, beginningOfADayT, timeDiffInQuarterHoursDay, 0, timeDiffInQuarterHoursEveningAfterDay)
      }
      else if((row.startTime.after(sixOCLockMorning) || row.startTime == sixOCLockMorning) && row.startTime.before(sixOClockEvening) && row.endTime.after(sixOClockEvening) && row.endTime.after(nextDaySixOClockMorning)) {
        val timeDiffInQuarterHoursDay = timeDifferenceInQuarterHours(sixOClockEvening, row.startTime) + timeDifferenceInQuarterHours(row.endTime, nextDaySixOClockMorning)
        val timeDiffInQuarterHoursEveningAfterDay = timeDifferenceInQuarterHours(nextDaySixOClockMorning, sixOClockEvening)

        PersonByMorningAndEvening(row.id, row.name, beginningOfADayT, timeDiffInQuarterHoursDay, 0, timeDiffInQuarterHoursEveningAfterDay)
      }
      else if((row.startTime.after(sixOClockEvening) || row.startTime == sixOClockEvening) && (row.endTime.before(nextDaySixOClockMorning) || row.endTime == nextDaySixOClockMorning)) {
        val timeDiffInQuarterHoursEveningAfterDay = timeDifferenceInQuarterHours(row.endTime, row.startTime)

        PersonByMorningAndEvening(row.id, row.name, beginningOfADayT, 0, 0, timeDiffInQuarterHoursEveningAfterDay)
      }
      else if((row.startTime.after(sixOClockEvening) || row.startTime == sixOClockEvening) && row.endTime.after(nextDaySixOClockMorning) && (row.endTime.before(nextDaySixOClockEvening) || row.endTime == nextDaySixOClockEvening)) {
        val timeDiffInQuarterHoursDay = timeDifferenceInQuarterHours(row.endTime, nextDaySixOClockMorning)
        val timeDiffInQuarterHoursEveningBeforeDay = timeDifferenceInQuarterHours(nextDaySixOClockMorning, row.startTime)

        PersonByMorningAndEvening(row.id, row.name, beginningOfADayT, timeDiffInQuarterHoursDay, timeDiffInQuarterHoursEveningBeforeDay, 0)
      }
      else if((row.startTime.after(sixOClockEvening) || row.startTime == sixOClockEvening) && row.endTime.after(nextDaySixOClockEvening)) {
        val timeDiffInQuarterHoursDay = timeDifferenceInQuarterHours(row.endTime, nextDaySixOClockMorning)
        val timeDiffInQuarterHoursEveningBeforeDay = timeDifferenceInQuarterHours(nextDaySixOClockMorning, row.startTime)
        val timeDiffInQuarterHoursEveningAfterDay = timeDifferenceInQuarterHours(row.endTime, nextDaySixOClockEvening)

        PersonByMorningAndEvening(row.id, row.name, beginningOfADayT, timeDiffInQuarterHoursDay, timeDiffInQuarterHoursEveningBeforeDay, timeDiffInQuarterHoursEveningAfterDay)
      }
      else {
        PersonByMorningAndEvening(row.id, row.name, beginningOfADayT, 0, 0, 0)
      }
    }).groupBy("id", "name", "date")
      .agg(
        sum("dayQHours").as("dayQHours"),
        sum("eveningQHoursBeforeDay").as("eveningQHoursBeforeDay"),
        sum("eveningQHoursAfterDay").as("eveningQHoursAfterDay")
      ).as[PersonByMorningAndEvening]

    // DataFrame contains salary portions by types (normal, evening and overtime)
    val salaryByTypeDF = personByMorningAndEveningDS.map(row => {
      val totalQHours = row.dayQHours + row.eveningQHoursBeforeDay + row.eveningQHoursAfterDay

      if(totalQHours <= 32) {
        DailySalary(row.id, row.name, new Date(row.date.getTime), row.dayQHours * dailyRate / 4, (row.eveningQHoursBeforeDay + row.eveningQHoursAfterDay) * eveningRate / 4, 0)
      }
      else {
        val overtimeTotal = totalQHours - 32
        val overtime25 = if (overtimeTotal > 8) 8 else overtimeTotal
        val overtime50 = if (overtimeTotal > overtime25) {
          if (overtimeTotal - overtime25 > 8) 8 else overtimeTotal - overtime25
        } else 0
        val overtimeDouble = if (overtimeTotal > overtime25 + overtime50) overtimeTotal - overtime25 - overtime50 else 0
        val (dayQHours, eveningQHours): (Long, Long) = if(row.eveningQHoursBeforeDay == 0) {
          if(row.dayQHours >= 32) (32, 0) else (row.dayQHours, 32 - row.dayQHours)
        }
        else {
          if(row.dayQHours >= 32) (32 - row.eveningQHoursBeforeDay, row.eveningQHoursBeforeDay) else {
            if(row.dayQHours + row.eveningQHoursBeforeDay >= 32) (32 - row.eveningQHoursBeforeDay, row.eveningQHoursBeforeDay) else (row.dayQHours, 32 - row.dayQHours)
          }
        }

        DailySalary(
          row.id,
          row.name,
          new Date(row.date.getTime),
          dayQHours * dailyRate / 4,
          eveningQHours * eveningRate / 4,
          overtime25 * overtime25Multiplier * dailyRate / 4 +
            overtime50 * overtime50Multiplier * dailyRate / 4 +
            overtimeDouble * overtimeDoubleMultiplier * dailyRate / 4
        )
      }
    }).groupBy("id", "name").agg(
      sum("normalSalary").as("normalSalary"),
      sum("eveningSalary").as("eveningSalary"),
      sum("overtime").as("overtime")
    )

    // final DataFrame where total salary is calculated and evening and overtime portions are also included
    // rounding is done only on the final numbers
    val finalDF = salaryByTypeDF.select(
      salaryByTypeDF("id").as("Person ID"),
      salaryByTypeDF("name").as("Person Name"),
      round(salaryByTypeDF("normalSalary") + salaryByTypeDF("eveningSalary") + salaryByTypeDF("overtime"), 2).as("Salary"),
      round(salaryByTypeDF("eveningSalary"), 2).as("Evening part"),
      round(salaryByTypeDF("overtime"), 2).as("Overtime part")
    ).orderBy("Person ID")

    // saving DataFrame to file
    saveDfToCsv(finalDF, outputFilePath)
  }

  // returns a Date for a given Timestamp
  def beginningOfADay(t: Timestamp): Date = {
    new Date(t.getTime)
  }

  // return a new Timestamp corresponding to 18:00 of a same day as provided Timestamp
  def getSixOCLock(t: Timestamp, morning: Boolean = true, nextDay: Boolean = false): Timestamp = {
    val six = if(nextDay) new Timestamp(t.getTime + 24*3600*1000) else new Timestamp(t.getTime)
    if (morning) six.setHours(6) else six.setHours(18)
    six.setMinutes(0)
    six.setSeconds(0)
    six
  }

  // returns amount of 15 minutes between 2 given Timestamps
  // assuming that difference between start and end is divisible to 15 minutes without a residue
  def timeDifferenceInQuarterHours(endTime: Timestamp, startTime: Timestamp): Long = {
    (endTime.getTime - startTime.getTime)/(15*60*1000)
  }

  // saves DataFrame to a file and removes all temporary files and directories
  def saveDfToCsv(df: DataFrame, tsvOutput: String): Unit = {
    val tmpParquetDir = "Posts.tmp.parquet"

    df.repartition(1)
      .write
      .mode("overwrite")
      .option("header", true)
      .option("sep", ",")
      .csv(path=tmpParquetDir)

    val dir = new File(tmpParquetDir)
    val resultsFile = dir.listFiles.find(file => FilenameUtils.getExtension(file.toString) == "csv")

    resultsFile match {
      case Some(file) => {
        file.renameTo(new File(tsvOutput))
        dir.listFiles.foreach( f => f.delete )
        dir.delete
      }
      case None => println("Can't find a file with results")
    }
  }
}
