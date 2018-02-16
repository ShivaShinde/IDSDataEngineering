#/bin/bash
#Please increase the java heap size based on the dataset and flush the GC before running this code for huge datasets
scalac ./src/GenerateRepeatDonorsFile.scala
scala -J-Xmx4g ./src/GenerateRepeatDonorsFile.scala ./input/itcont.txt ./input/percentile.txt ./output/repeat_donors.txt
