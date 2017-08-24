
# coding: utf-8


from pyspark import SparkContext, SparkConf
sc = SparkContext.getOrCreate()
hpRDD = sc.textFile("Hp.txt").map(lambda x: x.split(';'))
itcRDD = sc.textFile("ITC.txt").map(lambda x: x.split(';'))

#print(hpRDD.collect()+itcRDD.collect())
empRDD = hpRDD.union(itcRDD)
empRDD.collect()
fRDD = empRDD.filter(lambda row:"female" in row)
mRDD = empRDD.filter(lambda row:"male" in row)
print("ALL FEMALE EMPLOYEE")
print(fRDD.collect())
print("ALL MALE EMPLOYEE")
print(mRDD.collect())
#Creating Folder of Female Emp
fRDD.coalesce(1).saveAsTextFile("female_emp")
#Creating Folder of Male Emp
mRDD.coalesce(1).saveAsTextFile("male_emp")


