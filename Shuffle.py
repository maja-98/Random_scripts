

def sfl(s):
    if len(s)==1:
        return (list(s))
    else:
        p=[]
        for i in s:
            p.append(i)
        x=[]
        for i in range(len(s)):
            for j in sfl((p[:i]+p[i+1:])):
                if p[i]+''.join(j) not in x:
                    x.append(p[i]+''.join(j))
        return x

print(sfl(input()))
