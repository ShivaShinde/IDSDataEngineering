import java.io.{FileWriter, PrintWriter}

import scala.collection.mutable.LinkedHashMap


case class FecTableSchema(CMTE_ID: String, NAME: String, ZIP_CODE: String, TRANSACTION_DT: String, TRANSACTION_AMT: String, OTHER_ID: String)

case class Record(id: String, fname: String, lname: String, code: String, year: String, var amount: Int)

case class Key(fname: String, lname: String, code: String)

object GenerateRepeatDonorsFile extends App {


  override def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("dude, I need at least one parameter")
    }
    else {

      val filename = args(0).toString

      def readLinesFromFile(filename: String): List[String] = {
        val bufferedSource = scala.io.Source.fromFile(filename)
        val lines = (for (line <- bufferedSource.getLines()) yield line).toList
        bufferedSource.close
        lines
      }

      def findRepeatDonors(selectedFecColumns: List[(String, String, String, String, String, String)]): List[(String, String, String, String, String, String)] = {
        val keys = selectedFecColumns.map {
          case (a, b, c, d, e, f) => (b, c)
        }
        val dups = keys diff keys.distinct
        selectedFecColumns.filter {
          case (a, b, c, d, e, f) => dups.contains((b, c))
        }
      }

      println("Reading percentage.txt File, this file contains percentile value")
      val percentLine = readLinesFromFile(args(1))
      val percentValue = Math.round(percentLine(0).toFloat)

      println("Reading itcont.txt File, this file contains all the donors")
      val linesFromItcontFile = readLinesFromFile(filename)

      println("Loading the itcont.txt file with right schema")
      val fecDonorsTable = linesFromItcontFile.map {
        raw_line =>
          val columns = raw_line.split("\\|")
          FecTableSchema(columns(0),columns(7), columns(10), columns(13), columns(14), columns(15))
      }

      val selectedFecDonorColumns = fecDonorsTable.map(x => (x.CMTE_ID, x.NAME, x.ZIP_CODE, x.TRANSACTION_DT, x.TRANSACTION_AMT, x.OTHER_ID)).filter(_._6.trim.length<1)
      val yearWiseSelectedFecDonorColumns=selectedFecDonorColumns.map(record=>(record._1,record._2,record._3,record._4.takeRight(4),record._5,record._6)).filter(_._3.trim.length > 4).filter(_._4.trim.length==4)
      val latestYear = findRepeatDonors(yearWiseSelectedFecDonorColumns).map(record=>record._4).max
      val repeatDonors = findRepeatDonors(yearWiseSelectedFecDonorColumns).filter(_._4==latestYear)


      val repeatDonorsOutputFormat = repeatDonors.map(record => (record._1, record._2, record._3.substring(0, 5), record._4.takeRight(4), record._5))


      println("Polishing all the unnecessary information from itcont.txt file")
      val polishedDonors = repeatDonorsOutputFormat.map(record => record.toString).map(record => record.split(",").toList).filter(_.length > 5).map(record => (record(0).stripPrefix("("), record(1), record(2), record(3), record(4), record(5).dropRight(1).toInt))

      println("Preparding for aggregate compuatation with linkedHashMap as the data structure")
      val preparedRecords: List[(Key, Record)] = polishedDonors.map {
        case recordTuple@(id, fname, lname, code, year, _) =>
          (Key(id, code, year), Record.tupled(recordTuple))
      }

      def percentileValue(percentValue: Int, streamData: Seq[Int]): Int = {
        if(streamData.length==0){return 0}else{val firstSort = streamData.sorted
          val k = math.ceil((streamData.size - 1) * (percentValue / 100.0)).toInt;
          return firstSort(k).toInt}

      }
      val orderedAmount=selectedFecDonorColumns.map(r=>r._5.toInt).toSeq

      def aggregateDuplicatesWithOrder(
                                        remainingRecords: List[(Key, Record)],
                                        processedRecords: LinkedHashMap[Key, List[Record]]
                                      ): LinkedHashMap[Key, List[Record]] =
        remainingRecords match {

          case (key, record) :: newRemainingRecords => {

            processedRecords.get(key) match {
              case Some(recordList :+ lastRecord) => {
                record.amount = record.amount + lastRecord.amount
                processedRecords.update(key, recordList :+ lastRecord :+ record)
              }
              case None => processedRecords(key) = List(record)
            }

            aggregateDuplicatesWithOrder(newRemainingRecords, processedRecords)
          }

          case Nil => processedRecords
        }

      println("Computing the final result here")
      val result = aggregateDuplicatesWithOrder(
        preparedRecords, LinkedHashMap[Key, List[Record]]()
      ).values.flatMap {
        case _ :: Nil => Nil
        case records =>
          records.zipWithIndex.map {
            case (rec, idx) =>
              var percent=percentileValue(percentValue,orderedAmount)
              List(rec.id, rec.code, rec.year, percent, rec.amount, idx + 1).mkString(",")
          }
      }

      writeIntoFile(args(2), result)

      def writeIntoFile(filePath: String, result: Iterable[String]): Unit = {
        val out = new PrintWriter(new FileWriter(filePath, true))
        try {
          println("writing into file  ")
          result.toList.map(r => r.split(",").toList).foreach { e => out.println(e.mkString("|")) }
        }
        finally {
          out.close()
        }

      }




    }
  }


}

