addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.12.0")

resolvers += "spark-packages" at "https://repos.spark-packages.org/"

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.4")
