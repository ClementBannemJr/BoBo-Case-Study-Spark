# Databricks notebook source
# DBTITLE 1,BOBO Group assignment
#Name: Aranud Vandelaer, Abdelgelil Omar , Bannem Anatol clement junior
#Year: 2019-2020
#Course: Big Data Tools2

# COMMAND ----------

# DBTITLE 1,Filepath
Subscriptions_filepath = "/FileStore/tables/BDT2_1920_Subscriptions.csv"
Customers_filepath = "/FileStore/tables/BDT2_1920_Customers.csv"
Complaints_filepath = "/FileStore/tables/BDT2_1920_Complaints.csv"
Formula_filepath = "/FileStore/tables/BDT2_1920_Formula.csv"
Delivery_filepath = "/FileStore/tables/BDT2_1920_Delivery.csv"

# COMMAND ----------

# DBTITLE 1,Customers Table
#read in the customers file 
customers = spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.option("escape","\"")\
.load(Customers_filepath)

# COMMAND ----------

#look at the summary of customers
customers.summary()

# COMMAND ----------

#Check a summary of the values of the columns in the customers table.
customers.describe().show()

# COMMAND ----------

#create a new variables for region. for evry region one
from pyspark.sql.functions import isnan, when, count, col
customers = customers.withColumn("region 1", when(customers.Region == 1,1).otherwise(0))
customers = customers.withColumn("region 2", when(customers.Region == 2,1).otherwise(0))
customers = customers.withColumn("region 4", when(customers.Region == 4,1).otherwise(0))
customers = customers.withColumn("region 5", when(customers.Region == 5,1).otherwise(0))
customers = customers.withColumn("region 6", when(customers.Region == 6,1).otherwise(0))
customers = customers.withColumn("region 7", when(customers.Region == 7,1).otherwise(0))
customers = customers.withColumn("region 8", when(customers.Region == 8,1).otherwise(0))
customers = customers.withColumn("region 9", when(customers.Region == 9,1).otherwise(0))
customers = customers.withColumn("region 10", when(customers.Region == 10,1).otherwise(0))
display(customers).discribe()

# COMMAND ----------

#drop the region talbe 
customers2 = customers.drop("Region")
display(customers2).discribe()

# COMMAND ----------

# DBTITLE 1,delivery table
#read delivery table
delivery = spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.option("escape","\"")\
.load(Delivery_filepath)

# COMMAND ----------

#Count the number of nulls per column
from pyspark.sql.functions import isnan, when, count, col
delivery.select([count(when(col(c).isNull(), c)).alias(c) for c in delivery.columns]).show()

# COMMAND ----------

#Our unit of analysis is posterID so count how many unique users there are
from pyspark.sql.functions import col, countDistinct
delivery.agg(countDistinct(col("SubscriptionID"))).show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import isnan, when, count, col

# COMMAND ----------

#import new variables that tell us more about deliveryClass and DeliveryTypeName
from pyspark.sql.functions import isnan, when, count, col
delivery = delivery.withColumn("normal delivery", when(delivery.DeliveryClass == "NOR",1).otherwise(0))
delivery = delivery.withColumn(" abnormal delivery", when(delivery.DeliveryClass == "ABN",1).otherwise(0))
delivery = delivery.withColumn("no delivery info", when(delivery.DeliveryClass.isNull(),1).otherwise(0))
delivery = delivery.withColumn("delivery w/ staff", when(delivery.DeliveryTypeName == "delivery w/ staff",1).otherwise(0))
delivery = delivery.withColumn("delivery w/o staff", when(delivery.DeliveryTypeName == "delivery w/o staff",1).otherwise(0))
delivery = delivery.withColumn("drop off", when(delivery.DeliveryTypeName == "drop off",1).otherwise(0))


# COMMAND ----------

#get the year out of the DeliveryDate
delivery = delivery.withColumn("year of delevery", year(col("DeliveryDate")))


# COMMAND ----------

#create a new variables that tell us in witch year the delivery was
delivery = delivery.withColumn("delivery in 2018", when(col("year of delevery") == 2018 ,1).otherwise(0))
delivery = delivery.withColumn("delivery in 2019", when(col("year of delevery") == 2019 ,1).otherwise(0))
delivery = delivery.withColumn("delivery in 2017", when(col("year of delevery") == 2017 ,1).otherwise(0))
delivery = delivery.withColumn("delivery in 2016", when(col("year of delevery") == 2016 ,1).otherwise(0))
delivery = delivery.withColumn("delivery in 2015", when(col("year of delevery") == 2015 ,1).otherwise(0))
delivery = delivery.withColumn("delivery in 2014", when(col("year of delevery") == 2014 ,1).otherwise(0))


# COMMAND ----------

#change the type of SubscriptionId to a float
delivery = delivery.withColumn("SubscriptionID", delivery.SubscriptionID.cast('float'))


# COMMAND ----------

#drop the variables for the variables that wa remade as a dummy
delivery = delivery.drop("DeliveryID",'DeliveryClass',"DeliveryDate","DeliveryTypeName", "year of delevery")

# COMMAND ----------

#group the delivery table by SubscriptionId so we are able to merge it with compaints table
# and we create some aggregate variables
delivery2 = delivery.groupBy("SubscriptionID").sum()
display(delivery2).discribe()

# COMMAND ----------

#sum the variable sum(SubscriptionID) because it doesn't make sense
delivery2 = delivery2.drop("sum(SubscriptionID)")


# COMMAND ----------

# DBTITLE 1,Formula table
#Read in the Formula table
formula=spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.load(Formula_filepath)

display(formula)
#formula.count()
#we have 621 rows

# COMMAND ----------

#Remove duplicates if any
formula = formula.dropDuplicates()
formula.count()
#no duplicates

# COMMAND ----------

#Remove rows for which every column is na
formula = formula.na.drop("all")
formula.count()
#still there was no row with all columns holding na values

# COMMAND ----------

#Count the number of nulls per column
from pyspark.sql.functions import isnan, when, count, col
formula.select([count(when(col(c).isNull(), c)).alias(c) for c in formula.columns]).show()

# COMMAND ----------

#how many distinct formulaID do we have
formula.select(col("FormulaID")).distinct().count()
#so each row is a different formula

# COMMAND ----------

#pivot the variable formulatype which is either CAM or REG
subs_formula = formula.groupby("FormulaID").pivot("FormulaType").agg(count("FormulaID"))
display(subs_formula)
#subs_formula.count()

# COMMAND ----------

#join the table
formula_cleaned = formula\
  .join(subs_formula,['FormulaID']).drop("FormulaType")

display(formula_cleaned)


# COMMAND ----------

#put the varaibles as integers
from pyspark.sql.types import IntegerType

formula_cleaned = formula_cleaned.withColumn("CAM", formula_cleaned["CAM"].cast(IntegerType()))
formula_cleaned = formula_cleaned.withColumn("REG", formula_cleaned["REG"].cast(IntegerType()))
formula_cleaned = formula_cleaned.fillna(0)

# COMMAND ----------

# DBTITLE 1,Subscription Talbe
# Read in Subscription file

filepath = "/FileStore/tables/BDT2_1920_Subscriptions.csv"

subscriptions=spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.load(Subscriptions_filepath)

# COMMAND ----------

subscriptions_reduced = subscriptions.drop('PaymentType','GrossFormulaPrice','ProductName')
subscriptions_reduced.count()

# COMMAND ----------

#Remove duplicates if any
subscriptions_reduced = subscriptions_reduced.dropDuplicates()
subscriptions_reduced.count()

# COMMAND ----------

#Inspect the table
subscriptions_reduced.show(5,False)
subscriptions_reduced.describe().show()

# COMMAND ----------

#Count the number of nulls per column if any
subscriptions_reduced.select([count(when(col(c).isNull(), c)).alias(c) for c in subscriptions_reduced.columns]).show()

# COMMAND ----------

#Our axis of analysis is CustomerID so check how many unique customers do we have
subscriptions_reduced.select(col("CustomerID")).distinct().count()

# COMMAND ----------

#join the formula table with subscription table on FormulaID
subscription = formula_cleaned.join(subscriptions_reduced,on =['FormulaID'],how = "outer")



# COMMAND ----------

#Create new variable that says how many times the customers paid
subscription = subscription.withColumn("amount of times paid", when(col("PaymentStatus") == "Paid" ,1).otherwise(0))
subscription = subscription.withColumn("amount of times not paid", when(col("PaymentStatus") == "Not Paid" ,1).otherwise(0))

# COMMAND ----------

#drop some variables that have no meaning anymore or where we made some new variables for
subscription = subscription.drop("PaymentStatus", "PaymentDate", "FormulaID")


# COMMAND ----------

#Merge the subscription table with the delivey table based on the subscriptionID
basetable = subscription.join(delivery2,on = ['SubscriptionID'],how = "outer")
#remove the SubscriptionID because it doesn't make sens anymore
#chane the type of RenewalDate as a timestamp
basetable = basetable.withColumn("RenewalDate", col("RenewalDate").cast('timestamp'))


# COMMAND ----------

#create a new variable RenewalDate2 that will show us untill when the customers are client with us. therfore we take RenewalDate + duration of #the contract if they don't have a RenewalDate We gone take the EndDate
basetable = basetable.withColumn("RenewalDate2",when(col("RenewalDate").isNull(),col("EndDate")).otherwise(expr("add_months(RenewalDate, Duration)")))

# COMMAND ----------

#change the type of the some variables to Integers so we can aggregate this variables
basetable = basetable.withColumn("NbrMealsPrice", basetable["NbrMealsPrice"].cast(IntegerType()))
basetable = basetable.withColumn("StartDate", basetable["StartDate"].cast(IntegerType()))
basetable = basetable.withColumn("EndDate", basetable["EndDate"].cast(IntegerType()))
basetable = basetable.withColumn("RenewalDate", basetable["RenewalDate"].cast(IntegerType()))
basetable = basetable.withColumn("RenewalDate2", basetable["RenewalDate2"].cast(IntegerType()))




# COMMAND ----------

# DBTITLE 1,Basetable 1
#group the variables on CustomerID and get de get some min,max of some dates and avg,sum,count of the Discounts and NbrMealsPrice
basetable2 = basetable.groupBy("CustomerID").agg(min("StartDate"),max("EndDate"),sum(col("TotalDiscount")),avg(col("TotalDiscount")),count(when((col("TotalDiscount") != 0) & (col("TotalDiscount") !="NA"),True)).alias("Nbr of Discounts"),min(col("NbrMealsPrice")),max(col("NbrMealsPrice")),avg(col("NbrMealsPrice")),count("NbrMealsPrice").alias("Nbr Meals"),max(col("RenewalDate")),max(col("RenewalDate2")),count(when((col("TotalCredit") != 0) & (col("TotalCredit") !="NA"),True)).alias("Nbr  of Credits"),avg(col("TotalCredit")),sum(col("TotalCredit")),avg(col("NetFormulaPrice")),sum(col("NetFormulaPrice")),avg(col("TotalPrice")),sum(col("TotalPrice")),count("SubscriptionID").alias("SubsCount"),count("RenewalDate").alias("NbrRenews"),sum(col("NbrMeals_REG")).alias("nbr of reg meals"),sum(col("NbrMeals_EXCEP")).alias("nbr of exceptionals meals"),count(when((col("ProductDiscount") != 0) & (col("ProductDiscount") !="NA"),True)).alias("Nbr  of ProductDiscount"),count(when((col("FormulaDiscount") != 0) & (col("FormulaDiscount") !="NA"),True)).alias("Nbr  of FormulaDiscount")).drop("SubscriptionID")


# COMMAND ----------

display(basetable2)

# COMMAND ----------

#Move the dates back to a timestamp type
from pyspark.sql.functions import to_timestamp

basetable2 = basetable2.withColumn("min(StartDate)", col("min(StartDate)").cast('timestamp'))
basetable2 = basetable2.withColumn("max(EndDate)", col("max(EndDate)").cast('timestamp'))
basetable2 = basetable2.withColumn("max(RenewalDate)", col("max(RenewalDate)").cast('timestamp'))
basetable2 = basetable2.withColumn("max(RenewalDate2)", col("max(RenewalDate2)").cast('timestamp'))



# COMMAND ----------

#create a new variable the us gone say how long the customer subscription overall was by substracting the LastSubDate with FirstSUbDate
from pyspark.sql.functions import datediff
basetable2 = basetable2.withColumn("subsriction(days)",datediff("max(EndDate)", "min(StartDate)"))


# COMMAND ----------

# DBTITLE 1,Basetable3
#drop the variables that we used in the previous basetalbe.
basetable3 = basetable.drop("EndDate","StartDate","TotalDiscount","NbrMealsPrice","RenewalDate","RenewalDate2","NetFormulaPrice","TotalCredit","TotalPrice","SubscriptionID","NbrMeals_EXCEP","NbrMeals_REG")
#group the rest of the variables by customerID and take the sum variables
basetable3 = basetable3.withColumn("FormulaDiscount", basetable3["FormulaDiscount"].cast(IntegerType()))
basetable3 = basetable3.withColumn("ProductDiscount", basetable3["ProductDiscount"].cast(IntegerType()))
basetable4 = basetable3.groupBy("CustomerID").sum()


# COMMAND ----------

#drop the variable sum(CustomerID ) because it doesn't make any sense
basetable4 = basetable4.drop("sum(CustomerID)")


# COMMAND ----------

# DBTITLE 1,Basetable 5
#join the 2 different basetables that we made to one new basetable
basetable5 = basetable2.join(basetable4,on =['CustomerID'],how = "outer")


# COMMAND ----------

# DBTITLE 1,Complaints table
#Read in the Complaints table
complaints=spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.load(Complaints_filepath)

complaints.count()

# COMMAND ----------

#extract y,m,d
complaints = complaints.select(expr("*"),
    year("ComplaintDate").alias('year'), 
    month("ComplaintDate").alias('month'), 
    dayofmonth("ComplaintDate").alias('day'),
     datediff(current_date(), col("ComplaintDate")).alias("Recency")                           
)
display(complaints)


# COMMAND ----------

#Remove duplicates if any
complaints = complaints.dropDuplicates()
complaints.count()
#there was no duplicates as the number of rows remained 1623

# COMMAND ----------

#Remove rows for which every column is na
complaints = complaints.na.drop("all")
complaints.count()
#still there was no row with all columns holding na values

# COMMAND ----------

#Count the number of nulls per column
from pyspark.sql.functions import isnan, when, count, col
complaints.select([count(when(col(c).isNull(), c)).alias(c) for c in complaints.columns]).show()

# COMMAND ----------

#Our unit of analysis is customerID so check how many unique users there are
complaints.select(col("CustomerID")).distinct().count()

# COMMAND ----------

#Calculate the total number of complaints per CustomerID
total_complaints = complaints.groupBy("CustomerID").agg(count(col("ComplaintID")).alias("NumOfComplaints"))
total_complaints.sort("NumOfComplaints",ascending = False)
total_complaints.count()


# COMMAND ----------

#create a table with the products that the customers complained about as variables
subs_products = complaints.groupby("CustomerID").pivot("ProductName").agg(count("ProductID")).withColumnRenamed("NA","ProductsNA")
subs_products.count()

# COMMAND ----------

#create a table with the complaints reasons as variables
subs_complaints = complaints.groupby("CustomerID").pivot("ComplaintTypeDesc").agg(count("ComplaintTypeID")).withColumnRenamed("other","OtherComplaints")
subs_complaints.count()

# COMMAND ----------

#change nulls to zeros
subs_complaints2 = subs_complaints.na.fill(0) 


# COMMAND ----------

#create a table with the solutions provided by the company for the complaints raised
subs_solutions = complaints.groupby("CustomerID").pivot("SolutionTypeDesc").agg(count("SolutionTypeID")).withColumnRenamed("NA","SolNA").withColumnRenamed("other","OtherSol")
subs_solutions.count

# COMMAND ----------

#change nulls to zeros
subs_solutions2 = subs_solutions.na.fill(0)


# COMMAND ----------

#create a table with the feedback of the customers regarding the solutions provided by the company
subs_feedback = complaints.groupby("CustomerID").pivot("FeedbackTypeDesc").agg(count("FeedbackTypeID")).withColumnRenamed("NA","FeedbackNA").withColumnRenamed("other","OtherFeeback")
subs_feedback.count()

# COMMAND ----------

#change nulls to zeros
subs_feedback2 = subs_feedback.na.fill(0)


# COMMAND ----------

#most recent comlaints in days
subs_recency = complaints.groupby("CustomerID").agg(min("Recency"))
display(subs_recency)
subs_recency.count()

# COMMAND ----------

#join all tables on customerID
complaints_cleaned = subs_recency\
  .join(subs_feedback,['CustomerID'])\
  .join(subs_solutions,['CustomerID'])\
  .join(subs_complaints,['CustomerID'])\
  .join(subs_products,['CustomerID'])\
  .join(total_complaints,['CustomerID'])

display(complaints_cleaned)
complaints_cleaned.count()

# COMMAND ----------

# DBTITLE 1,Basetable 6
#create basetable6 where the complaint table is joined with the rest of the tables
basetable6 = basetable5.join(complaints_cleaned,on =['CustomerID'],how = "outer")


# COMMAND ----------

#merge the customers2 table with the basetable on customerID
basetable7 = basetable6.join(customers2, on = ["CustomerID"], how = "outer")

# COMMAND ----------

#fill all na with 0 in basetalbe6
basetable7 = basetable7.fillna(0)


# COMMAND ----------

# DBTITLE 1,Target variable
#we desided that evrybody how max(RenewalDAte2) above 2019-01-01 is that they are still client the resed churned away
basetable7 = basetable7.withColumn("churn3",
                                   when((col("max(RenewalDate2)")>lit("2019-01-01")) ,0)
                                   .otherwise(1))


# COMMAND ----------

basetable7 = basetable7.fillna(0)

# COMMAND ----------

# DBTITLE 1,Final_Basetable
#basetable7 is our final basetalbe
final_basetable = basetable7

# COMMAND ----------

display(final_basetable)

# COMMAND ----------

# DBTITLE 1,Label
#Cast type as double and change name to "label"
from pyspark.sql.functions import *
final_basetable = final_basetable.withColumn("label",col("churn3").cast("double")).drop("churn3")
final_basetable.show(3)

# COMMAND ----------

display(basetable7)

# COMMAND ----------

final_basetable = final_basetable.drop("min(StartDate)","max(EndDate)","max(RenewalDate)","max(RenewalDate2)")

# COMMAND ----------

#rename some columns because there name gives problem when we want to use the RFormula funcion
final_basetable = final_basetable.withColumnRenamed("Grub Flexi (excl. staff)", "Grub_Flex(ex_staff)")
final_basetable = final_basetable.withColumnRenamed("Grub Maxi (incl. staff)", "Grub_Maxi(incl_staff)")

# COMMAND ----------

# DBTITLE 1,Spliting between train and test set
#Create a train and test set with a 70% train, 30% test split
basetable_train, basetable_test = final_basetable.randomSplit([0.5, 0.5],seed=123)

print(basetable_train.count())
print(basetable_test.count())

# COMMAND ----------

# DBTITLE 1,create the new vector variable Features
#Transform the tables in a table of label, features format
from pyspark.ml.feature import RFormula

trainBig = RFormula(formula="label ~ . - CustomerID").fit(final_basetable).transform(final_basetable)
train = RFormula(formula="label ~ . - CustomerID").fit(basetable_train).transform(basetable_train)
test = RFormula(formula="label ~ . - CustomerID").fit(basetable_test).transform(basetable_test)
print("trainBig nobs: " + str(trainBig.count()))
print("train nobs: " + str(train.count()))
print("test nobs: " + str(test.count()))

# COMMAND ----------

# DBTITLE 1,features selection by chisqSelection
#from pyspark.ml.feature import ChiSqSelector
#from pyspark.ml.linalg import Vectors
#selector = ChiSqSelector(numTopFeatures=15, featuresCol="features",
 #                        outputCol="selectedFeatures", labelCol="label")

#result = selector.fit(train).transform(train)

#print("ChiSqSelector output with top %d features selected" % selector.getNumTopFeatures(15))
#display(result)

# COMMAND ----------

display(train)

# COMMAND ----------

# DBTITLE 1,LogisticRegression
#Hyperparameter tuning for different hyperparameter values of LR (aka model selection)
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression


#Define pipeline
lr = LogisticRegression()
pipeline = Pipeline().setStages([lr])

#Set param grid
params = ParamGridBuilder()\
  .addGrid(lr.regParam, [0.1])\
  .addGrid(lr.maxIter, [5, 20])\
  .build()
   
  

#Evaluator: uses max(AUC) by default to get the final model
evaluator = BinaryClassificationEvaluator()
#Check params through: evaluator.explainParams()

#Cross-validation of entire pipeline
cv = CrossValidator()\
   .setEstimator(pipeline)\
   .setEstimatorParamMaps(params)\
   .setEvaluator(evaluator)\
   .setNumFolds(2) # Here: 5-fold cross validation

#Run cross-validation, and choose the best set of parameters.
#Spark automatically saves the best model in cvModel.
cvModel = cv.fit(train)

# COMMAND ----------

#run the best model on the test set and select the CustomerID,prediction, label,probability
preds = cvModel.transform(test)\
  .select("CustomerID","prediction", "label","probability")
preds.show()

# COMMAND ----------

#Get model performance on test set
from pyspark.mllib.evaluation import BinaryClassificationMetrics

out = preds.rdd.map(lambda x: (float(x[0]), float(x[1])))
metrics = BinaryClassificationMetrics(out)

print(metrics.areaUnderPR) #area under precision/recall curve
print(metrics.areaUnderROC)#area under Receiver Operating Characteristic curve


# COMMAND ----------

#Cuttoff for linair Regression
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

#Extract second value of probability
probs_churnR = udf(lambda v:float(v[1]),FloatType())
preds_subsetR = preds.select('CustomerID','prediction', probs_churnR('probability')).orderBy(asc("probability"))

#renaming the column
preds_subsetR = preds_subsetR.withColumnRenamed("<lambda>(probability)", 'churn_probability')


# COMMAND ----------

#To achieve a probabilty on the full data set, we gone fit the model on the test set
cvModel2 = cv.fit(test)

# COMMAND ----------

#run the model over the train set
preds2R = cvModel.transform(train)\
  .select("CustomerID","prediction", "label","probability")
preds2R.show()

# COMMAND ----------

#Get model performance on test set
from pyspark.mllib.evaluation import BinaryClassificationMetrics

out = preds2R.rdd.map(lambda x: (float(x[0]), float(x[1])))
metrics2R = BinaryClassificationMetrics(out)

print(metrics2R.areaUnderPR) #area under precision/recall curve
print(metrics2R.areaUnderROC)#area under Receiver Operating Characteristic curve

# COMMAND ----------

#Cuttoff for linair Regression
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

#Extract second value of probability
probs_churnR2 = udf(lambda v:float(v[1]),FloatType())
preds_subsetR2 = preds2R.select('CustomerID','prediction', probs_churnR('probability')).orderBy(asc("probability"))

#renaming the column
preds_subsetR2 = preds_subsetR2.withColumnRenamed("<lambda>(probability)", 'churn_probability')

# COMMAND ----------

#stack preds2R and predsR
churn_prediction = preds_subsetR2.union(preds_subsetR)
churn_prediction.count()


# COMMAND ----------

display(churn_prediction)

# COMMAND ----------

#calculation the cut off point 
five_percentR = churn_prediction.count()*5//100
print("5%",five_percentR)
ten_percentR = churn_prediction.count()*10//100
print("10%",ten_percentR)
fifteen_percentR = churn_prediction.count()*15//100
print("15%",fifteen_percentR)

# COMMAND ----------

#doing the cutoff for 5%, 10%, 15%
five_percent_cutoffR = preds_subsetR.limit(five_percentR)

ten_percent_cutoffR = preds_subsetR.limit(ten_percentR)

fifteen_percent_cutoffR = preds_subsetR.limit(fifteen_percentR)

# COMMAND ----------

display(five_percent_cutoffR)

# COMMAND ----------

display(ten_percent_cutoffR)

# COMMAND ----------

display(fifteen_percent_cutoffR)

# COMMAND ----------


bastable_churn = basetable7.join(churn_prediction,['CustomerID'],'inner')

# COMMAND ----------

display(bastable_churn)

# COMMAND ----------

# DBTITLE 1,Gradiant Boosting classefier
#Hyperparameter tuning for different hyperparameter values of LR (aka model selection)
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import GBTClassifier
#Define pipeline
GBT = GBTClassifier()
pipeline2 = Pipeline().setStages([GBT])

#Set param grid
params = ParamGridBuilder()\
  .addGrid(GBT.maxIter,[5])\
  .build()
  #.addGrid(lr.maxIter, [50, 100,150])\
  

#Evaluator: uses max(AUC) by default to get the final model
evaluator = BinaryClassificationEvaluator()
#Check params through: evaluator.explainParams()

#Cross-validation of entire pipeline
cv2 = CrossValidator()\
  .setEstimator(pipeline2)\
  .setEstimatorParamMaps(params)\
  .setEvaluator(evaluator)\
  .setNumFolds(2) # Here: 5-fold cross validation

#Run cross-validation, and choose the best set of parameters.
#Spark automatically saves the best model in cvModel.
cvModel2 = cv2.fit(train)

# COMMAND ----------

predsGBT = cvModel2.transform(test)\
  .select("CustomerID","prediction", "label","probability")
predsGBT.show()

# COMMAND ----------

#Get model accuracy
#print("accuracy: " + str(evaluator2.evaluate(preds2)))

#Get AUC
metricsGBT = BinaryClassificationMetrics(predsGBT.rdd.map(lambda x: (float(x[0]), float(x[1]))))
print("AUC: " + str(metricsGBT.areaUnderROC))

# COMMAND ----------

#Get model performance on test set
from pyspark.mllib.evaluation import BinaryClassificationMetrics

out2 = predsGBT.rdd.map(lambda x: (float(x[0]), float(x[1])))
metricsGBT = BinaryClassificationMetrics(out2)

print(metricsGBT.areaUnderPR) #area under precision/recall curve
print(metricsGBT.areaUnderROC)#area under Receiver Operating Characteristic curve

# COMMAND ----------

# DBTITLE 1,Random Forest
#Exercise: Train a RandomForest model and tune featureSubsetStrategy between 'auto' and 'sqrt'
  
#RF: Hyperparameters
  #numTrees: number of trees to train
  #featureSubsetStrategy: how many features should be considered at each split? Values: auto, all, sqrt, log2, n 

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

#Define pipeline
rfr = RandomForestRegressor()
rfPipe = Pipeline().setStages([rfr])

#Set param grid
rfParams = ParamGridBuilder()\
  .addGrid(rfr.featureSubsetStrategy, ['auto', 'sqrt'])\
  .build()

#Run cross-validation
rfCv = CrossValidator()\
  .setEstimator(rfPipe)\
  .setEstimatorParamMaps(rfParams)\
  .setEvaluator(RegressionEvaluator())\
  .setNumFolds(2) # Here: 2-fold cross validation

#Run cross-validation, and choose the best set of parameters.
rfrModel = rfCv.fit(train)

# COMMAND ----------

predsRF = rfrModel.transform(test)\
 #.select("CustomerID","prediction", "label","probability")
predsRF.show()

# COMMAND ----------

#Get model performance on test set
from pyspark.mllib.evaluation import BinaryClassificationMetrics

out3 = predsRF.rdd.map(lambda x: (float(x[0]), float(x[1])))
metricsRF = BinaryClassificationMetrics(out3)

print(metricsRF.areaUnderPR) #area under precision/recall curve
print(metricsRF.areaUnderROC)#area under Receiver Operating Characteristic curve

# COMMAND ----------

#Get model accuracy
#print("accuracy: " + str(evaluator2.evaluate(preds2)))

#Get AUC
metrics3 = BinaryClassificationMetrics(predsRF.rdd.map(lambda x: (float(x[0]), float(x[1]))))
print("AUC: " + str(metrics3.areaUnderROC))
