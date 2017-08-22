import pandas as pd

def getDF(filename):
    print("Reading CSV...")
    df = pd.read_csv(filename, delimiter=',')
    return df

def getHMap(df):
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

def getConnIntersections(set1, set2):
    return set.intersection(set1, set2)

def directlyConnected(hm, k, v):
    return v in hm[k]


df = getDF('../Files/common_connection_200k.csv')
hm = getHMap(df)

Conn2HM = ()
for index1, (k1,v1) in enumerate(hm.items()):     # k1 = member_id
    for index2, k2 in enumerate(v1):              # k2 = connected_member_id: 1st level of k1
        v2 = hm.get(k2, {})                         
        for index3, k3 in enumerate(v2):          # k3: 1st level of k2, second-level of k1
            if ((k1 != k3) and (not directlyConnected(hm, k1, k3))):
                v3 = hm.get(k3, {})
                connSet = getConnIntersections(v1, v3)
                if len(connSet) > 0:
                    print("{} : {} - {} : {} : {}".format(index1, k1, index2, k3, len(connSet)))
                    if len(connSet) > Conn2HM[2]:
                        Conn2HM = (k1, k3, len(connSet))
                    print("               Max = {} {} {}".format(Conn2HM[0], Conn2HM[1], Conn2HM[2]))
                    
print("Max = {} {} {}".format(Conn2HM[0], Conn2HM[1], Conn2HM[2]))
