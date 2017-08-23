import operator
import pandas as pd

def getDF(filename):
    print("Reading CSV...")
    df = pd.read_csv(filename, delimiter=',')
    yield df

def getDict(df):
    hm = {}
    print("Creating Connections Set HashMap...")
    for index, row in df.iterrows():
        print(index)
        if row[0] in hm.keys():
            val = hm[row[0]]
            val.add(row[1]) 
            hm[row[0]] = val
        else:
            hm[row[0]] = set([row[1]])
    return hm

def getSortedDict(hm):
    cntHM = {}
    for k,v in hm.items():
        cntHM[k] = len(v)

    return sorted(cntHM.items(), key=operator.itemgetter(1), reverse=True)

def getConnIntersections(hm, k1, k2):
    return len(set.intersection(hm.get(k1), hm.get(k2)))

def directlyConnected(hm, k, v):
    return v in hm[k]


df = getDF('../Files/common_connection_200k.csv')
hm = getDict(next(df))
sortedDict = getSortedDict(hm)

Conn2HM = (0,0,0)
for index1, (k1,cnt1) in enumerate(sortedDict):  
    if ((Conn2HM[2] > 0) and (Conn2HM[2] > cnt1)):
        break
    for index2, (k2,cnt2) in enumerate(sortedDict):        
        if ((Conn2HM[2] > 0) and (Conn2HM[2] > cnt2)):
            break
        if ((k1 != k2) and (not directlyConnected(hm, k1, k2))):
            v2 = hm.get(k2)
            connSetLen = getConnIntersections(hm, k1, k2)
            if connSetLen > 0:
                print("{} : k1={} - cnt1={} : {} : k2={} : cnt2={} : {}".format(index1, k1, cnt1, index2, k2, cnt2, connSetLen))
                if connSetLen > Conn2HM[2]:
                    Conn2HM = (k1, k2, connSetLen)
                    print("               Max = {} {} {}".format(Conn2HM[0], Conn2HM[1], Conn2HM[2]))
                    
print("Max = {} {} {}".format(Conn2HM[0], Conn2HM[1], Conn2HM[2]))
