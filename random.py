n=int(input("enter number of terms"))

a=0

b=1

s=0

for i in range(n):

   s = a+b
   print("s value", s)

   b = a
   print("b value",b)

   a= s
   print("a value", a)


   print(s)