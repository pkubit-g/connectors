/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

name := "examples"
organization := "com.examples"
organizationName := "examples"

scalaVersion := "2.12.8"
version := "0.1.0"

lazy val commonSettings = Seq(
  crossScalaVersions := Seq("2.12.8", "2.11.12"),
  libraryDependencies ++= Seq(
    "io.delta" %% "delta-standalone" % getStandaloneVersion(),
    "org.apache.hadoop" % "hadoop-client" % "3.1.0",
    "org.apache.parquet" % "parquet-hadoop" % "1.10.1"
  )
)

def getStandaloneVersion(): String = {
  val envVars = System.getenv
  if (envVars.containsKey("STANDALONE_VERSION")) {
    val version = envVars.get("STANDALONE_VERSION")
    println("Using Delta version " + version)
    version
  } else {
    "0.3.0"
  }
}

lazy val extraMavenRepo = sys.env.get("EXTRA_MAVEN_REPO").toSeq.map { repo =>
  resolvers += "Delta" at repo
}

lazy val convertToDelta = (project in file("convert-to-delta")) settings (
  name := "convert",
  scalaVersion := "2.12.8",
  commonSettings,
  extraMavenRepo
)

lazy val helloWorld = (project in file("hello-world")) settings (
  name := "hello",
  scalaVersion := "2.12.8",
  commonSettings,
  extraMavenRepo
)
