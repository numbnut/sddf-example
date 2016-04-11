name := "sddf-example"

version := "0.1.0"

scalaVersion := "2.10.5"

libraryDependencies += "de.unihamburg.vsis" %% "sddf" % "0.1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.1"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.3.1"

// eclipse plugin
// link source code to the dependencies
EclipseKeys.withSource := true

// add resource scala resource folders to the eclipse source folders
//EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

// Assembly plugin
// skip tests during assembly
test in assembly := {}
