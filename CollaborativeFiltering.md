
#### Example that shows you how to implement a ratings recommendation engine for a dataset THAT IS NOT the moivelens dataset(!!!)
#### Please download the files from our dataset branch. For more information refer here: http://www2.informatik.uni-freiburg.de/~cziegler/BX/
#### About this dataset: Some dude named Cai-Nicolas Zeigler collected a whole-buncha book ratings from the Book-Crossing community in 2004.
#### There are 3 files in the dataset: BX-Users.csv (278,858 users), BX-Books.csv (271,379 books), and BX-Ratings.csv (1,149,780 book ratings).

#### NOTE: This tutorial requires Apache Spark. If you do not have this installed, STOP, and get this installed by following the instructions on our 'Appendix - Installations' document.

#### Step 1: Fire up the spark-shell and load in our dataset.
#### In your terminal, fire up spark-shell
```
cd wherever/you/downloaded/spark
./bin/spark-shell

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
```
#### The first import loads the ALS (Alternating Least Squares) library that we will use to build our recommendation engine.
#### The second import loads in a very special RDD type called 'Rating' which is a special form of RDD that requires the follow syntax: (UserID[Int], ProductID[Int], Rating[Double])
#### Essentially, 'Rating' is a dataset of triplets that we will feed into the ALS class to use for predicting new books to read for the readers in our book club! 
```
val rawData = sc.textFile("wherever/you/downloaded/theBookRating/data/BX-Book-Ratings.csv")
val rawRatings = rawData.map(_.split(","))
val ratings = rawRatings.map{ case Array(user, isbn, rating) => Rating(user.toInt, isbn.toInt, rating.toDouble) }

rawData.count
ratings.count
ratings.take(10).foreach(println)
```
#### So the first thig we need to do is load in our dataset which is basically the directory you stored the BX-Ratings.csv file.
#### Next, we are going to 'split' the file 'rawData' into a new dataset called 'rawRatings'.  Note that new columns are distinguished by a ','.
#### Finally, we are going to convert 'rawRatings' dataset into the proper 'ratings' dataset that is needed to run our Alternating Least Squares (ALS) algorithm while at the same time giving names to the features (there is no header on the dataset so this is where we insert the hearder names).  We finally check our work by looking at the counts and then a small sample of the ratings.
#### BUT WAIT, how did you know to convert the transformed dataset, rawRatings, into a new RDD Ratings dataset?  Luckily in Spark you can 'error-out' and see why you error'd. Just type this into your browser and see what happens:
```
ALS. #Press 'TAB' instead of enter
```
#### This will return you a set of parameters needed to run the ALS algorithm in Spark.
#### You should see this: asInstanceOf    isInstanceOf    toString        train           trainImplicit
#### These are the 'variations' of ALS at our disposal.  For this tutorial, we are going to use the 'train' method but very often in business we see the 'trainImplicit' approach because not everyone collects customer ratings.
#### Now, type this in and just press 'ENTER' aftwards:
```
ALS.train
```
#### The error says we didn't specify a 'ratings' RDD which is required to train this algorithm and as such we need to make a 'ratings' dataset.

#### Step 2: Train the algorithm
#### The only inputs required for the model are: 'Rank' - # of hidden features in our model, 'Iterations' - # of iterations to run. Somewhere between 10-20 is usually sufficient for most datasets, and finally, 'Lambda' - ensures we do not over-fit our model; a lambda value of 0.01 is fine for most problems so let's get to training!
```
val bookEngine = ALS.train(ratings, 20, 10, 0.01)
```
#### So in words, we are training our Alternating Least Squares algorithm on 20 hidden features of 10 iterations with a lambda (AKA learning rate) of 0.01.
#### As soon as you press 'ENTER' you will see a ton of logs being run wich is just the model being built and iterations being run.  In the end, you will get a new line of scala which denotes the model is finished running.
#### Most of the time in practice you are going to want more than JUST 20 hidden features but if you are running Spark locally on your computer you will probably tap all of your memory doing more so let's just keep it sweet 'n simple with 20 hidden features for now.

#### Step 3: What does it all mean?
#### So we ran our collaborative filtering model......then what?
#### First off, it's time to test how we did by looking at some case examples!  Let's start off by first picking a person, seeing what books they like and then seeing the recommended books.
