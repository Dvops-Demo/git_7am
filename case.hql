hive> select brand,mfg,price,type,
    > case
    > when price > 50000 then "Premium"
    > when price <= 50000 and price >= 30000 then "MidRange"
    > when price <30000 then "LowRange"
    > Else "NA"
    > END as Segment
    > from mydb.sparkcars;

============= CASE =================
brand	mfg	price	type	segment
vw	2013	45000	hatch	MidRange
audi	2013	55000	sedan	Premium
skoda	2013	23000	hatch	LowRange
bmv	2010	60000	suv	Premium
porsche	2010	50000	suv	MidRange
bently	2010	80000	suv	Premium
bugati	2009	30000	sports	MidRange