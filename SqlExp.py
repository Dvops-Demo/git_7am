from pyspark.sql import SparkSession

ssn=SparkSession.builder.appName("JOINS").master("local[*]").getOrCreate()

emp = [(1,"Smith",-1,"2018","10","M",3000),(2,"Rose",1,"2010","20","M",4000),(3,"Williams",1,"2010","10","M",1000),
       (4,"Jones",2,"2005","10","F",2000),(5,"Brown",2,"2010","40","",-1),
       (6,"Brown",2,"2010","50","",-1)]

empColumns =["emp_id","name","superior_emp_id","year_joined", "emp_dept_id","gender","salary"]

empDF = ssn.createDataFrame(emp,empColumns)

dept = [("Finance",10),("Marketing",20),("Sales",30), ("IT",40)]
deptColumns = ["dept_name","dept_id"]
deptDF = ssn.createDataFrame(dept,deptColumns)

empDF.createOrReplaceTempView("emp")
deptDF.createOrReplaceTempView("dept")

sqdf=ssn.sql("select * from emp e INNER JOIN dept d on e.emp_id == d.dept_id")
sqdf.show()