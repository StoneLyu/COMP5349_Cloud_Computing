package ml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

//import scala.*;
//import scala.Double;

/**
 * Run on latest movie lens data to find out the average rating for each genre.
 *
 *
 * input data : movies.csv (only 1.33MB)
 *
 * format movieId,title,genres. sample data 1,Toy Story
 * (1995),Adventure|Animation|Children|Comedy|Fantasy 102604,"Jeffrey Dahmer
 * Files, The (2012)",Crime|Documentary
 *
 * Genres are a pipe-separated list; Movie titles with comma is enclosed by a
 * pair of quotes.
 *
 * ratings.csv (541.96MB)
 *
 * format userId,movieId,rating,timestamp
 * 
 * sample data 1,253,3.0,900660748
 *
 *
 * submit to a yarn cluster
 *
 * spark-submit \ --class ml.MovieLensLarge \ --master yarn-cluster \
 * sparkML.jar \ /share/movie/small/ \ week5_out/
 *
 *
 * @author Mengyu L
 *
 */
public class MovieLensLarge {

	public static void main(String[] args) {

   	 //The program arguments are input and output path
   	 //Using absolute path is always preferred
   	 //For windows system, the path value should be something like "C:\\data\\ml-100k\\"
   	 //For unix system, the path value should something like "/home/user1/data/ml-100k/"
   	 //For HDFS, the path value should be something like "hdfs://localhost/user/abcd1234/movies/"

    	String inputDataPath = args[0], outputDataPath = args[1]; //first input is input path, just path, second is output path, also just path
    	SparkConf conf = new SparkConf(); //configuration

    	conf.setAppName("youtube video analysis"); //appName

    	JavaSparkContext sc = new JavaSparkContext(conf);  //initialize configuration

    	JavaRDD<String> videoData = sc.textFile(inputDataPath+"ALLvideos.csv");

  	//read ratings.csv and convert it to a key value pair RDD of the following format
    	//movieID -> rating

  	// need: id/ trending_date/ views/ country
  	/*two ways
  	1. JavaPairRDD<Tuple2, Tuple2>
  	2. JavaPairRDD<String, String> divide the two strings later
  	*/
    	JavaPairRDD<String, Tuple2<String, String>> infoExtraction = videoData.mapToPair(s ->
  	{
      	String[] values = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
      	if (18 == values.length){
        	return
          	new Tuple2<String, Tuple2<String, String>>(values[17] + values[0], Tuple2<values[1], values[8]>);
      	}
    	}
  	);
  	//the output format here is:
  	//(countruID, Tuple2<Date, Views>)

    	//read movies.csv and convert it to a key value pair RDD of the following format
    	//movieID, genre
    	//flatMapToPair is used because one movie can have multiple genres

        //考虑Iterable<Text>还是Iterable<String>
        JavaPairRDD<String, Iterable<String>> videoGrouped = infoExtraction.groupByKey();
    
        //对videoGrouped循环，找到单个countryID对应的所有的date和views
        JavaPairRDD<String, Double> eachCountryIDPercentage = videoGrouped.value().mapToPair({s -> 
            {//videoGrouped.value() 的格式为：18.24.49999
                Stirng videoDate = "";
                Double videoViews = 0.0;
                Double per = 0.0;
                ArrayList<Tuple2<String, Double>> list = new ArrayList<Tuple2<String, Double>>();
                ArrayList<Tuple2<Stirng, Double>> eachCountryID = new ArrayList<Tuple2<String, Double>>();
                for (Stirng dv : s){
                    videoDate = s.substring(0,6);
                    videoViews = Double.parseDouble(s.substring(7));
                    list.add(new Tuple2<Stirng, Double>(videoDate, videoViews));
                }
                if (list > 1){
                    for (int k = 0; k < 2; k++){
                        eachCountryID.add(list.get(k));
                    }
                    if (eachCountryID[0]._2.get() > eachCountryID[1]._2.get()){
                        per = (eachCountryID[0]._2.get() - eachCountryID[1]._2.get())/eachCountryID[1]._2.get();
                    }else{
                        per = (eachCountryID[1]._2.get() - eachCountryID[0]._2.get())/eachCountryID[0]._2.get();
                    }
                } 
                return ;
            }
        );
        //here the output is: (CountryID, eachCountryID*2)

        JavaPairRDD<String, Double> 

    	JavaPairRDD<String,String> movieGenres = movieData.flatMapToPair(s->{
   		 String[] values = s.split(",");
   		 String movieID = values[0];
   		 int length = values.length;
   		 ArrayList<Tuple2<String,String>> results = new ArrayList<Tuple2<String,String>>();
   		 if (values.length >=3 ){ // genre list is present
   			 String[] genres = values[length -1].split("\\|"); //genres string is always at the last index
   			 for (String genre: genres){
   					 results.add(new Tuple2<String, String>(movieID, genre));
   			 }
   		 }
   		 return results.iterator();
    	});


    	//join the two RDDs to find the ratings for each genre
    	//join function performs an inner join
    	//The result RDD would have the following format
    	//(movieID, (genre, rating))

    	JavaPairRDD<String, Tuple2<String,Float>> joinResults = movieGenres.join(ratingExtraction);

   	 // System.out.println("There are " + joinResults.count() + " rows after the join.");
    	//Join is based on movieID, which is not useful in our calculation
    	//We only want to retain the value which is (genre, rating) and convert it to a PairRDD
    	JavaPairRDD<String, Float> joinResultsNoID = joinResults.values().mapToPair(v->v);



    	//aggregateByKey operation takes one zero value and two functions:
    	//mergeValue() and mergeCombiner()

    	//mergeValue() function is applied on the given zero and any value belonging to a same key to get a partial result.
    	//Since each partition is processed independently, we can have multiple partial results for the same key.
		//mergeCombiner() function is used to merge partial results.
    	//we only want to have one partition for the result RDD, because the number of key is really small
		//output of aggregateByKey is of format:
		//(genre,<totalRating,NumOfRating>)

    	//The mapToPair operation will calculate the average for each genre
    	//the input of the mapToPair is of the format
    	//<genreID, <totalRating, numOfRating>>
    	//the mapTopair will covert the value to totalRating/numOfRating

    	JavaPairRDD genreRatingAvg = joinResultsNoID.aggregateByKey(
   			 new Tuple2<Float, Integer> (0.0f,0),
   			 1,
   			 (r,v)-> new Tuple2<Float, Integer> (r._1+ v, r._2+1),
   			 (v1,v2) -> new Tuple2<Float,Integer> (v1._1 + v2._1, v1._2 + v2._2))
   			 .mapToPair(
   					 t -> new Tuple2(t._1, (t._2._1 * 1.0 / t._2._2))
   			 );

    	// this is an action

    	genreRatingAvg.saveAsTextFile(outputDataPath + "latest.rating.avg.per.genre");
    	sc.close();
      }
}
