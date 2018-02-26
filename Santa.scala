import org.scalatest.FunSuite
class Santa extends FunSuite{
  val outputFilePath="./output/repeat_donors.txt"
  val inputFilePath="./input/itcont.txt"
  val percentileFile="./input/percentile.txt"

  def readLinesFromFile(filename: String): List[String] = {
    val bufferedSource = scala.io.Source.fromFile(filename)
    val lines = (for (line <- bufferedSource.getLines()) yield line).toList
    bufferedSource.close
    lines
  }

  def percentileValue(percentValue: Int, streamData: Seq[Int]): Int = {
    if(streamData.length==0){return 0}else{val firstSort = streamData.sorted
      val k = math.ceil((streamData.size - 1) * (percentValue / 100.0)).toInt;
      return firstSort(k).toInt}

  }

  test("Checking the number of columns in the output repeat_donors.txt File"){
      val outputStringLines=readLinesFromFile(outputFilePath)
      val outputSplittedLines=outputStringLines.map(r=>r.split("\\|"))
      assert(outputSplittedLines(0).size===6)
    }
  test("Verifying whether the output file contains less records than the input File"){
    val inputStringLines=readLinesFromFile(inputFilePath)
    val outputStringLines=readLinesFromFile(outputFilePath)
    assert(inputStringLines.size>outputStringLines.size)
  }
  test("Verify that there is no 0 in repeated donor count column"){
    val outputStringLines=readLinesFromFile(outputFilePath)
    val outputSplittedLines=outputStringLines.map(r=>r.split("\\|"))
    val filteredCountOfRepeatDonors=outputSplittedLines.map(r=>r(5).toInt).filter(r=>r<1)
    assert(filteredCountOfRepeatDonors.size===0)
  }
  test("Testing the percentile function"){
    val percentileTestList=List(384,250,230,384,333,384).toSeq
    val getPercentileValue=readLinesFromFile(percentileFile)(0).toInt
    val percent=percentileValue(getPercentileValue,percentileTestList)
    assert(percent===333)
  }
  test("Verifying the latest repeat donor year"){
    val inputStringLines=readLinesFromFile(inputFilePath)
    val inputSplittedLines=inputStringLines.map(r=>r.split("\\|"))
    val inputMaxYear=inputSplittedLines.map(r=>r(13).takeRight(4)).filter(r=>r.trim.length==4).max
    val outputStringLines=readLinesFromFile(outputFilePath)
    val outputSplittedLines=outputStringLines.map(r=>r.split("\\|"))
    val outputMaxYear=outputSplittedLines.map(r=>r(2).toInt).max
    assert(inputMaxYear===outputMaxYear.toString)
  }
  test("Verify output file for two different years"){
    val outputStringLines=readLinesFromFile(outputFilePath)
    val outputSplittedLines=outputStringLines.map(r=>r.split("\\|"))
    val outputYearsList=outputSplittedLines.map(r=>r(2).toInt).distinct
    assert(outputYearsList.size===1)
  }
}
