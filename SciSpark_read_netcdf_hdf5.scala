package edu.iiita.mta

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.dia.core.{SciSparkContext, SciTensor}
import scala.collection.mutable.ArrayBuffer

object SciSparkReadTest {

  def main(args: Array[String]): Unit = {

    /** ************args ***************************/
    val masterURL = args(0)             // master url => local/yarn
    val netCdfDirectoryPath = args(1)   // CRD directory path
    val noOfPartitions = args(2).toInt  // number of partitions   =>  3 * number of cpu cores allocated
    val firstAtom = args(3).toInt - 1   // first atom id to find the distance
    val secondAtom = args(4).toInt - 1  // seecond atom id to find the ditance
    val distanceOPPath = args(5)        // distance output path
    /** *****************************************/

    /** calculates the distance b/w the given two atom Ids
     *
     * @param x1 x coordinate of first atom
     * @param y1 y coordinate of first atom
     * @param z1 z coordinate of first atom
     * @param x2 x coordinate of second atom
     * @param y2 y coordinate of second atom
     * @param z2 z coordinate of second atom
     * @return the distance between the given two atomIds
     */
    def distance(x1: Float, y1: Float, z1: Float, x2: Float, y2: Float, z2: Float): Float = {
      val a = scala.math.pow(x2 - x1, 2)
      val b = scala.math.pow(y2 - y1, 2)
      val c = scala.math.pow(z2 - z1, 2)
      scala.math.sqrt(a + b + c).toFloat
    }

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SciSparkContext(masterURL, "SciSpark reading test")
    val spark = new sql.SparkSession.Builder().getOrCreate
    import spark.implicits._

    println("started")
    var startTime = System.currentTimeMillis()

    /** ***************** reading ***************/

    val hdfsFilesTensorRDD = sc.netcdfDFSFiles(netCdfDirectoryPath, List("coordinates", "time"), noOfPartitions)
    val ncFileCRDArrayRDD = hdfsFilesTensorRDD.map(x => (x.variables.get("time").get.data.toStream.toArray,
      x.variables.get("coordinates").get.data.toStream.toArray))
    /**
     * framesRDD is the RDD of frames -> each row contains the tuple of frame time and list of coordinates
     */
    val framesRDD = ncFileCRDArrayRDD.flatMap(crdArr => {
      var numberOfCRDsPerFile = crdArr._2.size
      var numberOfCRDsPerFrame = numberOfCRDsPerFile / crdArr._1.size
      (0 to numberOfCRDsPerFile - 1 by numberOfCRDsPerFrame).map(x => ((crdArr._1(x / numberOfCRDsPerFrame)), crdArr._2.slice(x, x + numberOfCRDsPerFrame)))
    }
    )
    /** ****************************************/
    
//    framesRDD.toDF("frame time", "frame coordinates").show()    //just to see the crds and time mapping
    var framesCount = framesRDD.count()
    var timeAfterRead = System.currentTimeMillis()

    /** *************distance ******************/
    val distanceRDD = framesRDD.map(x => {
      (x._1, distance(x._2(firstAtom * 3).toFloat, x._2(firstAtom * 3 + 1).toFloat, x._2(firstAtom * 3 + 2).toFloat,
        x._2(secondAtom * 3).toFloat, x._2(secondAtom * 3 + 1).toFloat, x._2(secondAtom * 3 + 2).toFloat))
    }
    )
    /** ***************************************/

    /** ***************save output ***************/
    distanceRDD.coalesce(1).toDF().write.mode("overwrite").format("csv").save(distanceOPPath)
    print("adding a comment")
   //comment

    /** ***********************times */
    var endTime = System.currentTimeMillis()
    println("total time in sec " + (endTime - startTime) / 1000)
    println("read time " + (timeAfterRead - startTime) / 1000)
    println("process time in sec " + (endTime - timeAfterRead) / 1000)
    println("no of frames  " + framesCount)
    println("no of partitions " + framesRDD.partitions.size)
    println("Done")
    /** *****************************************/

  }


}
