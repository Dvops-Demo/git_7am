#lst=[0,1,2,3,5,8,11]
#print(lst[1])


lst=[0,1]
n=5
itr=n-len(lst)

def fibnace():

    lelm=lst[-1]
    blelm=lst[-2]
    nfelm=lelm+blelm
    lst.append(nfelm)

for i in range(itr):

    fibnace()

print("list",lst)