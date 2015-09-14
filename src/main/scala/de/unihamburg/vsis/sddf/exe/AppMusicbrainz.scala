package de.unihamburg.vsis.sddf.exe

import org.joda.time.DateTime
import org.joda.time.Period

import com.rockymadden.stringmetric.similarity.JaccardMetric

import de.unihamburg.vsis.sddf.SddfApp
import de.unihamburg.vsis.sddf.SddfContext.FeatureId
import de.unihamburg.vsis.sddf.SddfContext.Measure
import de.unihamburg.vsis.sddf.SddfContext.Threshold
import de.unihamburg.vsis.sddf.SddfContext.pairToInt
import de.unihamburg.vsis.sddf.classification.PipeClassificationThreshold
import de.unihamburg.vsis.sddf.clustering.PipeAnalyseClustering
import de.unihamburg.vsis.sddf.clustering.PipeClusteringTransitiveClosure
import de.unihamburg.vsis.sddf.indexing.PipeAnalyseIndexer
import de.unihamburg.vsis.sddf.indexing.PipeIndexerSortedNeighborhood
import de.unihamburg.vsis.sddf.indexing.blocking.keygeneration.BlockingKeyBuilderBasic
import de.unihamburg.vsis.sddf.pipe.context.SddfPipeContext
import de.unihamburg.vsis.sddf.pipe.optimize.PipeOptimizePersistAndName
import de.unihamburg.vsis.sddf.preprocessing.PipePreprocessorRemoveRegex
import de.unihamburg.vsis.sddf.preprocessing.PipePreprocessorTrim
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping.Id
import de.unihamburg.vsis.sddf.reading.FeatureIdNameMapping.Ignore
import de.unihamburg.vsis.sddf.reading.PipeReaderOmitHead
import de.unihamburg.vsis.sddf.reading.corpus.PipeReaderTupleCsv
import de.unihamburg.vsis.sddf.reading.corpus.PipeStoreInContextCorpus
import de.unihamburg.vsis.sddf.reading.goldstandard.PipeReaderGoldstandardCluster
import de.unihamburg.vsis.sddf.reading.goldstandard.PipeStoreInContextGoldstandard
import de.unihamburg.vsis.sddf.similarity.PipeSimilarity
import de.unihamburg.vsis.sddf.similarity.measures.MeasureEquality
import de.unihamburg.vsis.sddf.similarity.measures.MeasureWrapperToLower
import de.unihamburg.vsis.sddf.writing.PipeWriterTupleClusterActualDate

object AppMusicbrainz extends SddfApp {
  
  val Number = (0, "number")
  val Title = (1, "title")
  val Length = (2, "length")
  val Artist = (3, "artist")
  val Album = (4, "album")
  val Year = (5, "year")
  val Language = (6, "language")

  val featureIdNameMapping = Map(Number, Title, Length, Artist, Album, Year, Language)

  val featureIdNameMapper = new FeatureIdNameMapping(featureIdNameMapping)

  implicit val featureMeasures: Array[(FeatureId, Measure)] = Array(
    (Number, MeasureEquality)
    , (Title, MeasureWrapperToLower(JaccardMetric(2)))
    , (Length, MeasureEquality)
    , (Artist, MeasureWrapperToLower(JaccardMetric(2)))
    , (Album, MeasureWrapperToLower(JaccardMetric(2)))
    , (Year, MeasureEquality)
  //,(Language, JaccardMetric(2))
  )

  implicit val thresholds: Array[(FeatureId, Threshold)] = featureMeasures.map(pair => {
    (pair._1, 0.8)
  })

  implicit val bkvBuilder = new BlockingKeyBuilderBasic(
    (Title, 0 to 2)
    , (Artist, 0 to 2)
    , (Album, 0 to 2)
  )

  // Parse Tuples
  def inputPipeline() = {
       // Parse Tuples
    val allFields: Seq[Int] = Seq(Number, Title, Length, Artist, Album, Year, Language)
    val allFieldsWithId: Seq[Int] = Ignore +: Id +: Ignore +: allFields
  
    new PipeReaderOmitHead()
      .append(PipeReaderTupleCsv(allFieldsWithId))
      .append(PipePreprocessorTrim(allFields: _*))
      .append(PipePreprocessorRemoveRegex("[^0-9]", Number, Year, Length))
      .append(PipeStoreInContextCorpus())
//      .append(PipePrintHeadCorpus())
  }

  def goldstandardPipe() = {
    PipeReaderOmitHead()
      .append(PipeReaderGoldstandardCluster())
      .append(PipeStoreInContextGoldstandard())
//      .append(PipePrintHeadGoldstandard())
  }

  // Blocking
  def blockingPipe() = {
    PipeIndexerSortedNeighborhood(windowSize = 10)
      .append(PipeAnalyseIndexer())
  }

  // Train and Apply Classification Model
  def similarityPipe() = {
    new PipeSimilarity
  }
  //  val trainingPipe = new PipeTrainingDataGenerator
  //  val classificationPipe = new PipeClassificationDecisionTree
  def classificationPipe() = {
    PipeClassificationThreshold()
  }

  // Clustering
  def clusterPipe() = {
    PipeClusteringTransitiveClosure()
  }

  // Start Pipeline
  var dupDecPipe = blockingPipe
    .append(PipeOptimizePersistAndName("blocked-pairs"))
    .append(similarityPipe)
    .append(classificationPipe)
    .append(PipeOptimizePersistAndName("duplicate-pairs"))
    //    .append(new PipePrintHeadFalsePositives)
    //    .append(new PipePrintHeadFalseNegatives)
    .append(clusterPipe)
    .append(PipeAnalyseClustering())

  // don't do that on huge datasets because it collects the data
  dupDecPipe =  dupDecPipe.append(new PipeWriterTupleClusterActualDate("."))
    
  val startTime = new DateTime()
  val initialRddCount = 32
  val inputFile = "src/main/resources/10_000-0.1.csv"

  val sc = Conf.newSparkContext()
  log.info("Spark Application ID: " + sc.applicationId)
  log.info("Input File: " + inputFile)
  val rawData = sc.textFile(inputFile, initialRddCount)
  implicit val pc = new SddfPipeContext()
  val corpus = inputPipeline().run(rawData)
  val goldstandard = goldstandardPipe().run(rawData)
  val duplicateCluster = dupDecPipe.run(corpus)

  // Log lineage to separate file
  logLineage.info(duplicateCluster.toDebugString)
  
  // destroy the spark context
  sc.stop()
  
  val period = new Period(startTime, new DateTime)
  log.info("Runtime of the app: " + periodFormatter.print(period))
}
