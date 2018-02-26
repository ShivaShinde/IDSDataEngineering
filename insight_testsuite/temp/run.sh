#!/usr/bin/env bash
#/bin/bash
#Please increase the java heap size based on the dataset and flush the GC before running this code for huge datasets
rm *.class
scalac ./src/GenerateRepeatDonorsFile.scala
scala -J-Xmx4g ./src/GenerateRepeatDonorsFile.scala ./input/itcont.txt ./input/percentile.txt ./output/repeat_donors.txt






#Please uncomment below lines for running unit test cases
#scalac -cp scalatest_2.12-3.0.4.jar:scalactic_2.12-3.0.4.jar:scalatest_2.13.0-M2-3.2.0-SNAP10.jar Santa.scala
#scala -cp scalatest_2.12-3.0.4.jar:scalactic_2.12-3.0.4.jar:scalatest_2.13.0-M2-3.2.0-SNAP10.jar org.scalatest.run Santa