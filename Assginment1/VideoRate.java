package ml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/**
 * submit to local cluster
 *
 * spark-submit \ --class ml.VideoRate \ --master local [4] \ sparkML.jar \
 * /user/mlyu0556/ \ week5_out/
 */
public class VideoRate {

    public static void main(String[] args) {

        // The program arguments are input and output path
        // Using absolute path is always preferred
        // For windows system, the path value should be something like
        // "C:\\data\\ml-100k\\"
        // For unix system, the path value should something like
        // "/home/user1/data/ml-100k/"
        // For HDFS, the path value should be something like
        // "hdfs://localhost/user/abcd1234/movies/"

        String inputDataPath = args[0], outputDataPath = args[1]; // first input is input path, just path, second is
                                                                  // output path, also just path
        SparkConf conf = new SparkConf(); // configuration

        conf.setAppName("youtube video analysis"); // appName

        JavaSparkContext sc = new JavaSparkContext(conf); // initialize configuration

        JavaRDD<String> videoData = sc.textFile(inputDataPath + "ALLvideos.csv");

        JavaPairRDD<String, String> infoExtraction = videoData.mapToPair(s -> {
            String[] values = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            if (18 == values.length) {
                return new Tuple2<String, String>(values[17] + "; " + values[0], values[1] + "@" + values[8]);
            } else {
                return new Tuple2<String, String>("error", "");
            }
        }).filter(s -> {
            return !s._1.equals("error");
        });
        // the output format here is:
        // (countruID, Tuple2<Date, Views>)

        // flatMapToPair is used because one movie can have multiple genres

        JavaPairRDD<String, Iterable<String>> videoGrouped = infoExtraction.groupByKey();

        JavaPairRDD<String, Double> eachCountryIDPercentage = videoGrouped.mapToPair(s -> {
            String[] spStr;
            String videoDate = "";
            Double videoViews = 0.0;
            Double per = 0.0;
            ArrayList<Tuple2<String, Double>> list = new ArrayList<Tuple2<String, Double>>();
            ArrayList<Tuple2<String, Double>> eachCountryID = new ArrayList<Tuple2<String, Double>>();
            for (String dv : s._2) {
                spStr = dv.split("@");
                videoDate = spStr[0];
                if (spStr[1].matches("-?\\d+(\\.\\d+)?")) {
                    videoViews = Double.parseDouble(spStr[1]);
                } else {
                    return new Tuple2<String, Double>("error", 0.0);
                }
                list.add(new Tuple2<String, Double>(videoDate, videoViews));
            }
            if (list.size() > 1) {
                for (int k = 0; k < 2; k++) {
                    eachCountryID.add(list.get(k));
                }
                if (eachCountryID.get(0)._2 > eachCountryID.get(1)._2) {
                    per = (eachCountryID.get(0)._2 - eachCountryID.get(1)._2) / eachCountryID.get(1)._2;
                } else {
                    per = (eachCountryID.get(1)._2 - eachCountryID.get(0)._2) / eachCountryID.get(0)._2;
                }
                return new Tuple2<String, Double>(s._1, per);
            } else {
                return new Tuple2<String, Double>("error", 0.0);
            }
        }).filter(s -> {
            return !s._1.equals("error");
        });

        // here the output is: (CountryID, eachCountryID*2)
        // inverse key-value
        JavaPairRDD<Double, String> inversed = eachCountryIDPercentage.mapToPair(v -> {
            return new Tuple2<Double, String>(v._2, v._1);
        });

        // sort by "value"
        JavaPairRDD<Double, String> sorted = inversed.sortByKey(false);

        // inverse value-key
        JavaPairRDD<String, Double> inverseBack = sorted.mapToPair(v -> {
            return new Tuple2<String, Double>(v._2, v._1);
        }).filter(s -> {
            if (s._2 > 10.0) {
                return s;
            }
        });

        inverseBack.coalesce(1, true).saveAsTextFile(outputDataPath + "videoRate");
        sc.close();
    }
}
