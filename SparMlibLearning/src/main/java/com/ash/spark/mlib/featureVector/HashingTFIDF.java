package com.ash.spark.mlib.featureVector;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import io.netty.util.internal.JavassistTypeParameterMatcherGenerator;
import scala.Function1;
import scala.runtime.BoxedUnit;
public class HashingTFIDF {

	public static void main(String args[]){
		
		SparkConf conf=new SparkConf();
		conf.setAppName("MlibLearning").setMaster("local[1]");
		JavaSparkContext jsc=new JavaSparkContext(conf);
		JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
				//RowFctory.create(Object ...values)
				  RowFactory.create(0, "Hi I heard about Spark"),
				  RowFactory.create(0, "I wish Java could use case classes"),
				  RowFactory.create(1, "Logistic regression models are neat")
				));
		jrdd=jrdd.map(new Function<Row, Row>() {

			public Row call(Row arg0) throws Exception {
				// TODO Auto-generated method stub
				return RowFactory.create(Double.parseDouble(arg0.get(0)+""),arg0.get(1));
				 
			}
			
		});
				StructType schema = new StructType(new StructField[]{
									//name 			DataType	can be null 	metadata
				  new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),//label either 0 or 1
				  new StructField("sentence", DataTypes.StringType, false, Metadata.empty()) //complete scentence
				});
				SQLContext sqlContext=new SQLContext(jsc);
				DataFrame sentenceData = sqlContext.createDataFrame(jrdd, schema); //applied schema on Row RDD
				
				
				Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");//splits each scentence into words
				
				
				DataFrame wordsData = tokenizer.transform(sentenceData);
				wordsData.select("words").show();
				wordsData.select("sentence").show();
				
				int numFeatures = 20;
				HashingTF hashingTF = new HashingTF()
				  .setInputCol("words")
				  .setOutputCol("rawFeatures")
				  .setNumFeatures(numFeatures);
				
				//hashingTF is a transformer which takes set of terms and converts those set into fixed length feature vectors
				// here a set of terms is the bag of words //with input col as words ab outpts the feature in rawFeatures
				
				DataFrame featurizedData = hashingTF.transform(wordsData); //a transformer transforms a data frame of one type tp other type
				
				//featurized Data has the feature vector representing all the sentences  
				featurizedData.show();
				
				IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
				//an IDF is an estimator that fits on a dataset and produces IDF model
				
				IDFModel idfModel = idf.fit(featurizedData);//featurizedData is the data set here on which the idf will fit to create the model
				DataFrame rescaledData = idfModel.transform(featurizedData);
				// the model than transforms the  rawfeatures to features by scalling them down based on the number of feature supplied
				for (Row r : rescaledData.select("features", "label").take(3)) {
				  Vector features = r.getAs(0);
				  Double label = r.getDouble(1);
				  System.out.println(features);
				  System.out.println(label);
				}
	}
}

