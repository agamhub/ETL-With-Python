x = [[1,1],[-1,3],[0,2],[1,5],[-1,4]]
y = []
z = []
v = []

def split(x):
    for x,i in enumerate(x):
        y.append([x,i[0]])
        z.append([x,i[1]]) 

def sorting():
    return sorted(z,key=lambda z:z[1])

split(x)
h = sorting()

for x,i in enumerate(y): 
    if i[1] == 0:
        i[1] = 1
    v.append([i[1],h[x][1]])

print(v)