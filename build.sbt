name := "task_bigDataTeam"

version := "0.1"

scalaVersion := "2.12.1"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.3"
libraryDependencies ++= Seq( "org.apache.spark" %% "spark-core" % "3.0.0",
                            "org.apache.spark" %% "spark-sql" % "3.0.0",
                            "org.apache.spark" %% "spark-hive" % "3.0.0" % "provided")

libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.28.0"