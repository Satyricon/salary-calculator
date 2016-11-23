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
                             normalSalary: Double,
                             eveningSalary: Double,
                             overtime: Double)

final case class DailyHours(normalQHours: Double,
                            eveningQHours: Double,
                            overtime25QHours: Double,
                            overtime50QHours: Double,
                            overtimeDoubleQHours: Double)

object SalaryCalculator {
  // hourly rates
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

    // input and output file paths are provided as arguments
    val inputFilePath = args(0)
    val outputFilePath = args(1)

    // creating a spark session
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
    val personByMorningAndEveningDS = transformedTimesDS.map(getPersonByMorningAndEveningFromInterval)
      .groupBy("id", "name", "date")
      .agg(
        sum("dayQHours").as("dayQHours"),
        sum("eveningQHoursBeforeDay").as("eveningQHoursBeforeDay"),
        sum("eveningQHoursAfterDay").as("eveningQHoursAfterDay")
      ).as[PersonByMorningAndEvening]

    // DataFrame contains salary portions by types (normal, evening and overtime)
    val salaryByTypeDF = personByMorningAndEveningDS.map(person => {
      // if total working hours are less than 8 for the whole day we just count everything in as it is without overtimes
      // otherwise we calculate how many normal hours, evening hours and overtime hours (by type) there should be
      // for the whole day and calculate daily salary
      if(person.dayQHours + person.eveningQHoursBeforeDay + person.eveningQHoursAfterDay <= 32) {
        DailySalary(
          person.id,
          person.name,
          person.dayQHours * dailyRate / 4,
          (person.eveningQHoursBeforeDay + person.eveningQHoursAfterDay) * eveningRate / 4,
          0
        )
      }
      else {
        val dailyHours: DailyHours = calculateDailyHours(person)

        DailySalary(
          person.id,
          person.name,
          dailyHours.normalQHours * dailyRate / 4,
          dailyHours.eveningQHours * eveningRate / 4,
          dailyHours.overtime25QHours * overtime25Multiplier * dailyRate / 4 +
            dailyHours.overtime50QHours * overtime50Multiplier * dailyRate / 4 +
            dailyHours.overtimeDoubleQHours * overtimeDoubleMultiplier * dailyRate / 4
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

  // returns DailyHours constructed based on combination of normal, evening and total hours for a day
  def calculateDailyHours(person: PersonByMorningAndEvening): DailyHours = {
    val totalQHours = person.dayQHours + person.eveningQHoursBeforeDay + person.eveningQHoursAfterDay
    val overtimeTotal = totalQHours - 32
    val overtime25 = if (overtimeTotal > 8) 8 else overtimeTotal
    val overtime50 = if (overtimeTotal > overtime25) {
      if (overtimeTotal - overtime25 > 8) 8 else overtimeTotal - overtime25
    } else 0
    val overtimeDouble = if (overtimeTotal > overtime25 + overtime50) overtimeTotal - overtime25 - overtime50 else 0
    val (dayQHours, eveningQHours): (Long, Long) = if(person.eveningQHoursBeforeDay == 0) {
      if(person.dayQHours >= 32) (32, 0) else (person.dayQHours, 32 - person.dayQHours)
    }
    else {
      if(person.dayQHours >= 32) (32 - person.eveningQHoursBeforeDay, person.eveningQHoursBeforeDay) else {
        if(person.dayQHours + person.eveningQHoursBeforeDay >= 32) (32 - person.eveningQHoursBeforeDay, person.eveningQHoursBeforeDay) else (person.dayQHours, 32 - person.dayQHours)
      }
    }

    DailyHours(dayQHours, eveningQHours, overtime25, overtime50, overtimeDouble)
  }

  // returns PersonByMorningAndEvening constructed based on startTime and endTime under person
  def getPersonByMorningAndEveningFromInterval(person: Person): PersonByMorningAndEvening = {
    val beginningOfADay = new Date(person.startTime.getTime)
    val sixOCLockMorning = getSixOCLock(person.startTime)
    val sixOClockEvening = getSixOCLock(person.startTime, false)
    val nextDaySixOClockMorning = getSixOCLock(person.startTime, true, true)
    val nextDaySixOClockEvening = getSixOCLock(person.startTime, false, true)

    if (person.startTime.before(sixOCLockMorning) && (person.endTime.before(sixOCLockMorning) || person.endTime == sixOCLockMorning)) {
      val timeDiffInQuarterHoursEveningBeforeDay = timeDifferenceInQuarterHours(person.endTime, person.startTime)

      PersonByMorningAndEvening(person.id, person.name, beginningOfADay, 0, timeDiffInQuarterHoursEveningBeforeDay, 0)
    }
    else if(person.startTime.before(sixOCLockMorning) && person.endTime.after(sixOCLockMorning) && (person.endTime.before(sixOClockEvening) || person.endTime == sixOClockEvening)) {
      val timeDiffInQuarterHoursEveningBeforeDay = timeDifferenceInQuarterHours(sixOCLockMorning, person.startTime)
      val timeDiffInQuarterHoursDay = timeDifferenceInQuarterHours(person.endTime, sixOCLockMorning)

      PersonByMorningAndEvening(person.id, person.name, beginningOfADay, timeDiffInQuarterHoursDay, timeDiffInQuarterHoursEveningBeforeDay, 0)
    }
    else if(person.startTime.before(sixOCLockMorning) && person.endTime.after(sixOClockEvening)) {
      val timeDiffInQuarterHoursEveningBeforeDay = timeDifferenceInQuarterHours(sixOCLockMorning, person.startTime)
      val timeDiffInQuarterHoursEveningAfterDay = timeDifferenceInQuarterHours(person.endTime, sixOClockEvening)
      val timeDiffInQuarterHoursDay = timeDifferenceInQuarterHours(sixOClockEvening, sixOCLockMorning)

      PersonByMorningAndEvening(person.id, person.name, beginningOfADay, timeDiffInQuarterHoursDay, timeDiffInQuarterHoursEveningBeforeDay, timeDiffInQuarterHoursEveningAfterDay)
    }
    else if((person.startTime.after(sixOCLockMorning) || person.startTime == sixOCLockMorning) && person.startTime.before(sixOClockEvening) && (person.endTime.before(sixOClockEvening) || person.endTime == sixOClockEvening)) {
      val timeDiffInQuarterHoursDay = timeDifferenceInQuarterHours(person.endTime, person.startTime)

      PersonByMorningAndEvening(person.id, person.name, beginningOfADay, timeDiffInQuarterHoursDay, 0, 0)
    }
    else if((person.startTime.after(sixOCLockMorning) || person.startTime == sixOCLockMorning) && person.startTime.before(sixOClockEvening) && person.endTime.after(sixOClockEvening) && (person.endTime.before(nextDaySixOClockMorning) || person.endTime == nextDaySixOClockMorning)) {
      val timeDiffInQuarterHoursDay = timeDifferenceInQuarterHours(sixOClockEvening, person.startTime)
      val timeDiffInQuarterHoursEveningAfterDay = timeDifferenceInQuarterHours(person.endTime, sixOClockEvening)

      PersonByMorningAndEvening(person.id, person.name, beginningOfADay, timeDiffInQuarterHoursDay, 0, timeDiffInQuarterHoursEveningAfterDay)
    }
    else if((person.startTime.after(sixOCLockMorning) || person.startTime == sixOCLockMorning) && person.startTime.before(sixOClockEvening) && person.endTime.after(sixOClockEvening) && person.endTime.after(nextDaySixOClockMorning)) {
      val timeDiffInQuarterHoursDay = timeDifferenceInQuarterHours(sixOClockEvening, person.startTime) + timeDifferenceInQuarterHours(person.endTime, nextDaySixOClockMorning)
      val timeDiffInQuarterHoursEveningAfterDay = timeDifferenceInQuarterHours(nextDaySixOClockMorning, sixOClockEvening)

      PersonByMorningAndEvening(person.id, person.name, beginningOfADay, timeDiffInQuarterHoursDay, 0, timeDiffInQuarterHoursEveningAfterDay)
    }
    else if((person.startTime.after(sixOClockEvening) || person.startTime == sixOClockEvening) && (person.endTime.before(nextDaySixOClockMorning) || person.endTime == nextDaySixOClockMorning)) {
      val timeDiffInQuarterHoursEveningAfterDay = timeDifferenceInQuarterHours(person.endTime, person.startTime)

      PersonByMorningAndEvening(person.id, person.name, beginningOfADay, 0, 0, timeDiffInQuarterHoursEveningAfterDay)
    }
    else if((person.startTime.after(sixOClockEvening) || person.startTime == sixOClockEvening) && person.endTime.after(nextDaySixOClockMorning) && (person.endTime.before(nextDaySixOClockEvening) || person.endTime == nextDaySixOClockEvening)) {
      val timeDiffInQuarterHoursDay = timeDifferenceInQuarterHours(person.endTime, nextDaySixOClockMorning)
      val timeDiffInQuarterHoursEveningBeforeDay = timeDifferenceInQuarterHours(nextDaySixOClockMorning, person.startTime)

      PersonByMorningAndEvening(person.id, person.name, beginningOfADay, timeDiffInQuarterHoursDay, timeDiffInQuarterHoursEveningBeforeDay, 0)
    }
    else if((person.startTime.after(sixOClockEvening) || person.startTime == sixOClockEvening) && person.endTime.after(nextDaySixOClockEvening)) {
      val timeDiffInQuarterHoursDay = timeDifferenceInQuarterHours(person.endTime, nextDaySixOClockMorning)
      val timeDiffInQuarterHoursEveningBeforeDay = timeDifferenceInQuarterHours(nextDaySixOClockMorning, person.startTime)
      val timeDiffInQuarterHoursEveningAfterDay = timeDifferenceInQuarterHours(person.endTime, nextDaySixOClockEvening)

      PersonByMorningAndEvening(person.id, person.name, beginningOfADay, timeDiffInQuarterHoursDay, timeDiffInQuarterHoursEveningBeforeDay, timeDiffInQuarterHoursEveningAfterDay)
    }
    else {
      PersonByMorningAndEvening(person.id, person.name, beginningOfADay, 0, 0, 0)
    }
  }

  // returns a new Timestamp corresponding to 18:00 of a same day as provided Timestamp
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
