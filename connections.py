import pandas as pd
import csv

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

def writeCSV(df, outFile):
    print("Writing CSV....")
    df.to_csv(outFile, sep=',', encoding='utf-8', index=False)

def writeHM(hm, outfile):
    print("Writing hashmap....")
    with open(outfile, 'w') as f:
        w = csv.writer(f)
        w.writerows(hm.items())
    f.close()

def directlyConnected(hm, k, v):
    return v in hm[k]


df = getDF('../../Files/common_connection_200k.csv')

hm = getHMap(df)
writeHM(hm, 'hmout.csv')

Conn2HM = {}
for index1, (k1,v1) in enumerate(hm.items()):     # k1 = member_id
    for index2, k2 in enumerate(v1):              # k2 = connected_member_id: 1st level of k1
        v2 = hm.get(k2, {})                         
        for index3, k3 in enumerate(v2):          # k3: 1st level of k2, second-level of k1
            if ((k1 != k3) and (not directlyConnected(hm, k1, k3))):
                print("{} : {} - {} : {}".format(index1, k1, index2, k3))
                v3 = hm.get(k3, {})
                connSet = getConnIntersections(v1, v3)
                Conn2HM[(k1,k3)] = len(connSet)

writeHM(Conn2HM, 'Conn2DF.csv')
cntDF = pd.DataFrame(list(Conn2HM.items()), columns=['tuple', 'count'])
cntDF.sort_values('count', ascending=False)
writeCSV(cntDF, 'cntdf.csv')
