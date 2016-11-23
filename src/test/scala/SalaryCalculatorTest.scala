import java.sql.{Date, Timestamp}

import org.scalatest.FunSuite

class SalaryCalculatorTest extends FunSuite {

  test("timeDifferenceInQuarterHours returns correct difference between two Timestamps expressed in amount of quarter hours") {
    val start = new Timestamp(2016, 11, 24, 8, 15, 0, 0)
    val end = new Timestamp(2016, 11, 24, 13, 0, 0, 0)
    assert(SalaryCalculator.timeDifferenceInQuarterHours(end, start) == 19)
  }

  test("getSixOCLock returns a new Timestamp with date set to provided day and time set to 18:00:00 or 6:00:00 depending on input parameters") {
    val date = new Timestamp(2016, 11, 24, 8, 15, 0, 0)
    val morningSixOClock = new Timestamp(2016, 11, 24, 6, 0, 0, 0)
    val eveningSixOClock = new Timestamp(2016, 11, 24, 18, 0, 0, 0)
    assert(SalaryCalculator.getSixOCLock(date, true) == morningSixOClock)
    assert(SalaryCalculator.getSixOCLock(date, false) == eveningSixOClock)
  }

  test("getSixOCLock returns a new Timestamp with date set to next day from provided day and time set to 18:00:00 or 6:00:00 depending on input parameters") {
    val date = new Timestamp(2016, 11, 24, 8, 15, 0, 0)
    val morningSixOClock = new Timestamp(2016, 11, 25, 6, 0, 0, 0)
    val eveningSixOClock = new Timestamp(2016, 11, 25, 18, 0, 0, 0)
    assert(SalaryCalculator.getSixOCLock(date, true, true) == morningSixOClock)
    assert(SalaryCalculator.getSixOCLock(date, false, true) == eveningSixOClock)
  }

  test("calculateDailyHours returns correct DailyHours") {
    val person1 = PersonByMorningAndEvening("1", "Jack", new Date(2016, 11, 24), 24, 4, 10)
    val result1 = DailyHours(24, 8, 6, 0, 0)
    assert(SalaryCalculator.calculateDailyHours(person1) == result1)

    val person2 = PersonByMorningAndEvening("1", "Jack", new Date(2016, 11, 24), 24, 14, 10)
    val result2 = DailyHours(18, 14, 8, 8, 0)
    assert(SalaryCalculator.calculateDailyHours(person2) == result2)

    val person3 = PersonByMorningAndEvening("1", "Jack", new Date(2016, 11, 24), 40, 0, 0)
    val result3 = DailyHours(32, 0, 8, 0, 0)
    assert(SalaryCalculator.calculateDailyHours(person3) == result3)
  }

  test("getPersonByMorningAndEveningFromInterval returns correct PersonByMorningAndEvening") {
    val start1 = new Timestamp(2016, 11, 24, 8, 15, 0, 0)
    val end1 = new Timestamp(2016, 11, 24, 13, 0, 0, 0)
    val person1 = Person("1", "Jack", start1, end1)
    val result1 = PersonByMorningAndEvening("1", "Jack", new Date(start1.getTime), 19, 0, 0)
    assert(SalaryCalculator.getPersonByMorningAndEveningFromInterval(person1) == result1)

    val start2 = new Timestamp(2016, 11, 24, 4, 30, 0, 0)
    val end2 = new Timestamp(2016, 11, 25, 2, 15, 0, 0)
    val person2 = Person("1", "Jack", start2, end2)
    val result2 = PersonByMorningAndEvening("1", "Jack", new Date(start2.getTime), 48, 6, 33)
    assert(SalaryCalculator.getPersonByMorningAndEveningFromInterval(person2) == result2)
  }
}
