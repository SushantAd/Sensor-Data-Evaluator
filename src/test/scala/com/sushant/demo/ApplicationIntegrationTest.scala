package com.sushant.demo

import cats.effect.unsafe.implicits.global
import com.sushant.demo.model.{SensorData, SensorStatistics}
import fs2.io.file.Path
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.HashMap

class ApplicationIntegrationTest extends AnyWordSpec with Matchers with MockitoSugar{

  "Application" should {
    "read files from a directoru" in{
      val path = Path("src/test/resources/")
      val files = Application.directory(path).compile.toList.unsafeRunSync()
      files.size should be >= 1
      }

    "read and parse a csv file" in{
      val path = Path("src/test/resources/test.csv")
      val parseResult = Application.parser(path).compile.toList.unsafeRunSync()
      parseResult.size should be > 1
      println(parseResult)
    }

    "should process and return map" in{
      val path = Path("src/test/resources/test.csv")
      val parseResult: Seq[SensorData] = Application.parser(path).compile.toList.unsafeRunSync()
      val map = Application.processContent(parseResult, HashMap.empty[String, SensorStatistics])
      map.keys.size should be >=1
    }

    "should evaluate sensor statistics for new valid data" in{
      val sensorData1 = SensorData("s1", "88")
      val sensorStatisticsNew = SensorStatistics(None, None, None)
      val sensorStatistics1 = SensorStatistics(Some(88), Some(88), Some(88), 1, 0)
      val result = Application.evaluateSensorStatistics(sensorData1, sensorStatisticsNew, true)
      result shouldEqual sensorStatistics1
    }

    "should evaluate sensor statistics as failed process for new invalid data" in{
      val sensorData1 = SensorData("s1", "NaN")
      val sensorStatisticsNew = SensorStatistics(None, None, None)
      val sensorStatistics1 = SensorStatistics(None, None, None, 1, 1)
      val result = Application.evaluateSensorStatistics(sensorData1, sensorStatisticsNew, true)
      result shouldEqual sensorStatistics1
    }

    "should evaluate sensor statistics for old valid data" in{
      val sensorData1 = SensorData("s1", "88")
      val sensorStatisticsOld = SensorStatistics(Some(88), Some(88), Some(88), 1)
      val sensorStatistics1 = SensorStatistics(Some(88), Some(176), Some(88), 2, 0)
      val result = Application.evaluateSensorStatistics(sensorData1, sensorStatisticsOld)
      result shouldEqual sensorStatistics1
    }

    "should evaluate sensor statistics as failed process for old invalid data" in{
      val sensorData1 = SensorData("s1", "NaN")
      val sensorStatisticsNew = SensorStatistics(None, None, None, 0, 1)
      val sensorStatistics1 = SensorStatistics(None, None, None, 1, 2)
      val result = Application.evaluateSensorStatistics(sensorData1, sensorStatisticsNew)
      result shouldEqual sensorStatistics1
    }


  }

}
