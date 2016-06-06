package com.ash.spark.mlib.classification;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class TweetSentimentAnalysisUsingSVM {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SVM Classifier Example").setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		String tweet160000="D:\\MLIB\\SVM Trainning\\training.1600000.processed.noemoticon.csv";
		String tweet160000Test="D:\\MLIB\\SVM Trainning\\testdata.manual.2009.06.14.csv";
		
		String sentiment1400="D:\\MLIB\\SVM Trainning\\Sentiment Analysis Dataset.csv";
		
		JavaRDD<Row> jrdd = sc.textFile(sentiment1400,4).map(new Function<String, Row>() {

			@Override
			public Row call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				String [] values=arg0.split(",");
//				values[0]=values[0].replace("\"", "");
				double value=Double.parseDouble(values[1]);
				if(value==0)
				return RowFactory.create(0d,values[3]);
				else
					return RowFactory.create(1d,values[3]);
			}
			
		});
/*		JavaRDD<Row> jrdd = sc.textFile(tweet160000,4).map(new Function<String, Row>() {

			@Override
			public Row call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				String [] values=arg0.split(",");
				values[0]=values[0].replace("\"", "");
				double value=Double.parseDouble(values[0]);
				if(value==0)
				return RowFactory.create(0d,values[5]);
				else
					return RowFactory.create(1d,values[5]);
			}
			
		});*/
		
		JavaRDD<Row> testjrdd = sc.textFile(tweet160000Test,4).map(new Function<String, Row>() {

			@Override
			public Row call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				String [] values=arg0.split(",");
				values[0]=values[0].replace("\"", "");
				double value=Double.parseDouble(values[0]);
				if(value==0)
				return RowFactory.create(0d,values[5]);
				else
					return RowFactory.create(1d,values[5]);
			}
			
		});
		
		StructType schema = new StructType(new StructField[] {
				// name DataType can be null metadata
						  new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),//label either 0 or 1	
				new StructField("sentence", DataTypes.StringType, false, Metadata.empty())// label
																							// either
																							// 0
																							// or
																							// 1
		});
				
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame sentenceData = sqlContext.createDataFrame(jrdd, schema); // applied
																			// schema
																			// on
																			// Row
																			// RDD
		
		DataFrame testData = sqlContext.createDataFrame(testjrdd, schema); // applied

		
		

		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");// splits
																							// each
																							// scentence
																							// into
																							// words

		DataFrame wordsData = tokenizer.transform(sentenceData);
		
		DataFrame testWordsData = tokenizer.transform(testData);
		
// 65 pe best
		int numFeatures = 65;
		HashingTF hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
				.setNumFeatures(numFeatures);

		// hashingTF is a transformer which takes set of terms and converts
		// those set into fixed length feature vectors
		// here a set of terms is the bag of words //with input col as words ab
		// outpts the feature in rawFeatures

		DataFrame featurizedData = hashingTF.transform(wordsData); // a
																	// transformer
																	// transforms
																	// a data
																	// frame of
																	// one type
																	// tp other
																	// type

		
		DataFrame testfeaturizedData = hashingTF.transform(testWordsData);

		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		// an IDF is an estimator that fits on a dataset and produces IDF model

		IDFModel idfModel = idf.fit(featurizedData);// featurizedData is the
													// data set here on which
													// the idf will fit to
													// create the model
		DataFrame rescaledData = idfModel.transform(featurizedData);
		DataFrame testrescaledData = idfModel.transform(testfeaturizedData);

		JavaRDD<LabeledPoint> training =	rescaledData.select("features", "label").javaRDD().map(new Function<Row, LabeledPoint>() {

			@Override
			public LabeledPoint call(Row arg0) throws Exception {
				// TODO Auto-generated method stub
				
				return new LabeledPoint(arg0.getDouble(1), (org.apache.spark.mllib.linalg.SparseVector)arg0.get(0));
			}
			
		}).cache();
		/**
		 * Loads binary labeled data in the LIBSVM format into an
		 * RDD[LabeledPoint], with number of features determined automatically
		 * and the default number of partitions.
		 */

//		JavaRDD<LabeledPoint> training = MLUtils.loadLibSVMFile(sc.sc(), trainingDatapath).toJavaRDD();

		// Split initial RDD into two... [60% training data, 40% testing data].
		// JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 11L);
//		training.cache();
		// JavaRDD<LabeledPoint> test = data.subtract(training);

		JavaRDD<LabeledPoint> test =	testrescaledData.select("features", "label").javaRDD().map(new Function<Row, LabeledPoint>() {

			@Override
			public LabeledPoint call(Row arg0) throws Exception {
				// TODO Auto-generated method stub
				
				return new LabeledPoint(arg0.getDouble(1), (org.apache.spark.mllib.linalg.SparseVector)arg0.get(0));
			}
			
		}).cache();
//		JavaRDD<LabeledPoint> test = MLUtils.loadLibSVMFile(sc.sc(), testDatapath).toJavaRDD();

		// Run training algorithm to build the model.
		//70 and 63 combination was best
		int numIterations = 100;
		final SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);
		final NaiveBayesModel naivemodel = NaiveBayes.train(training.rdd(), 1.0);
		// :: Experimental :: Clears the threshold so that predict will output
		// raw prediction scores.
		// thrshold the base value with which you will compare the score to say
		// positive or negative
		// Clear the default threshold.
		model.clearThreshold();
		
		// Compute raw scores on the test set.
		JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {
			public Tuple2<Object, Object> call(LabeledPoint p) {
				Double score = model.predict(p.features());
				return new Tuple2<Object, Object>(score, p.label());
			}
		});

		// Get evaluation metrics.
		BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
		double auROC = metrics.areaUnderROC();

		System .out.println("******************");
		System.out.println();
		System.out.println("Area under ROC = " + auROC);
		System.out.println();
		System .out.println("******************");
		
		
		
		JavaPairRDD<Double, Double> predictionAndLabel =
				test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
				    @Override
				    public Tuple2<Double, Double> call(LabeledPoint p) {
				      return new Tuple2<Double, Double>(naivemodel.predict(p.features()), p.label());
				    }
				  });
				double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
				  @Override
				  public Boolean call(Tuple2<Double, Double> pl) {
				    return pl._1().equals(pl._2());
				  }
				}).count() / (double) test.count();
		
		
				System .out.println("******************");
				System.out.println();
				System.out.println("NB accuracy = " + accuracy);
				System.out.println();
				System .out.println("******************");
				
				
		// Save and load model
//		model.save(sc.sc(), "D:\\MLIB\\SVM Trainning\\model\\");
//		SVMModel sameModel = SVMModel.load(sc.sc(), "D:\\MLIB\\SVM Trainning\\model\\");
/*		while (true) {
			Scanner scanner = new Scanner(System.in);
			String statement = scanner.nextLine();
			JavaRDD<Row> jrddlocal=sc.parallelize(Arrays.asList(RowFactory.create(statement)));
			StructType schemalocal = new StructType(new StructField[] {
					// name DataType can be null metadata
							
					new StructField("sentence", DataTypes.StringType, false, Metadata.empty())// label
																								// either
																								// 0
																								// or
																								// 1
			});
			DataFrame sentenceDatalocal = sqlContext.createDataFrame(jrddlocal, schemalocal);
			
					Tokenizer tokenizerlocal = new Tokenizer().setInputCol("sentence").setOutputCol("words");
			DataFrame wordsDatalocal = tokenizerlocal.transform(sentenceDatalocal);

			DataFrame datasetlocal = hashingTF.transform(wordsDatalocal);
			DataFrame rescaledDatalocal = idfModel.transform(datasetlocal);
			rescaledDatalocal.select("features").javaRDD().foreach(new VoidFunction<Row>() {

				@Override
				public void call(Row arg0) throws Exception {
					// TODO Auto-generated method stub
					System.out.println(arg0.get(0));
				}
				
			});
			
			JavaRDD<org.apache.spark.mllib.linalg.Vector> vectRDD=rescaledDatalocal.select("features").javaRDD().flatMap(new FlatMapFunction<Row, org.apache.spark.mllib.linalg.Vector>() {
				@Override
				public Iterable<org.apache.spark.mllib.linalg.Vector> call(Row arg0) throws Exception {
					List<org.apache.spark.mllib.linalg.Vector> vectorReadList=Arrays.asList((org.apache.spark.mllib.linalg.Vector)arg0.get(0));
					System.out.println("The Value going to be added "+arg0.get(0)+ " with list of size "+vectorReadList.size());
					return vectorReadList;
				}
			});
			JavaRDD<org.apache.spark.mllib.linalg.Vector> vectRDD = sc.parallelize( Arrays
					.asList((org.apache.spark.mllib.linalg.Vector)rescaledDatalocal.select("features").take(2)[0].get(0)));
			model.predict(vectRDD).foreach(new VoidFunction<Double>() {
				
				@Override
				public void call(Double arg0) throws Exception {
					// TODO Auto-generated method stub
					System.out.println("*******************");
					System.out.println();
					System.out.println();
					System.out.println(arg0 + "* the predicted value* ");
					System.out.println();
					System.out.println();
					System.out.println("*******************");
				}
			});
			

		}*/
	}
}