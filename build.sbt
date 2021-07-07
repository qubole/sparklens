name := "sparklens"
organization := "com.qubole"

scalaVersion := "2.12.10"

crossScalaVersions := Seq("2.10.6", "2.11.8", "2.12.10")

spName := "qubole/sparklens"

sparkVersion := "3.1.1"

spAppendScalaVersion := true


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided"

libraryDependencies +=  "org.apache.hadoop" % "hadoop-client" % "2.6.5" % "provided"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.6" % "provided"

libraryDependencies += "org.apache.httpcomponents" % "httpmime" % "4.5.6" % "provided"

test in assembly := {}

testOptions in Test += Tests.Argument("-oF")

scalacOptions ++= Seq("-target:jvm-1.7")

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

publishMavenStyle := true


licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")


pomExtra :=
  <url>https://github.com/qubole/sparklens</url>
  <scm>
    <url>git@github.com:qubole/sparklens.git</url>
    <connection>scm:git:git@github.com:qubole/sparklens.git</connection>
  </scm>
  <developers>
    <developer>
      <id>iamrohit</id>
      <name>Rohit Karlupia</name>
      <url>https://github.com/iamrohit</url>
    </developer>
    <developer>
      <id>beriaanirudh</id>
      <name>Anirudh Beria</name>
      <url>https://github.com/beriaanirudh</url>
    </developer>
    <developer>
      <id>mayurdb</id>
      <name>Mayur Bhosale</name>
      <url>https://github.com/mayurdb</url>
    </developer>
  </developers>

