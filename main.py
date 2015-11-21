import json
import os
from pprint import pprint
from collections import defaultdict
from os import listdir
from os.path import isfile,join,basename,splitext


#with open('input_short.json') as data_file:
#    data = json.load(data_file)
#pprint(data)

#with open('input_short.json') as data_file:
#    data = json.loads(data_file.read())

directory = "data"

def saveToFile(name, list):
    if not os.path.exists(directory):
        os.makedirs(directory)
    path = "%s/%s.txt" % (directory, name)
    with open(path, 'w') as f:
        for item in list:
            f.write("%s" % item)

def processInput():
    d = {}
    with open('input1000000.json') as f:
        for line in f:
            json_line = json.loads(line)
            catkey =  json_line['categorykey']
            if not d.has_key(catkey):
                d[catkey] = []
            d[catkey].append(line)
        for catkey in d.keys():
            saveToFile(catkey, d[catkey])

def readJson(jsonFile):
    d = []
    maxLines = 5000
    with open(jsonFile) as f:
        for i,line in enumerate(f):
            if (i < maxLines):
                d.append(json.loads(line))
#        for line in f:
#            d.append(json.loads(line))
    return d;

def shingling(text, shingleLength):
    tokens_desc = [text[i:i+shingleLength] for i in range(len(text) - shingleLength + 1) if len(text[i]) < shingleLength + 1]
    return tokens_desc

def getSet(d):
    hash_set = set()
    shingleLength = 9
    for i in range(0,len(d)):
        tokens = []
        if "ad__headline" in d[i].keys():
            tokens = shingling(d[i]["ad__headline"], shingleLength)
        if "ad__description" in d[i].keys():
            tokens += shingling(d[i]["ad__description"], shingleLength)
        hashes = [hash(token) & 0xffffffff for token in tokens]
        hash_set.update(hashes)
    return hash_set

def jac(set1, set2):
    return float(len(set1 & set2)) / len(set1 | set2)

def readAllInput(mypath):
    output = {}
    onlyfiles = [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]
    for f in onlyfiles:
        output[splitext(f)[0]] = mypath + "/" + f
    return output
    
def categories():
    inp = readAllInput(directory)
    categories = {}
    for item in inp.items():
        print "processing category " + item[0]
        categories[item[0]] = readJson(item[1])
    return categories

def computation():
    sizeLimit = 4100
    cat = categories()
    tuples = []
    for item1 in cat.items():
        if (len(item1[1]) < sizeLimit):
            continue
        category = item1[0]    
        corpus = getSet(item1[1][1000:1200])
        test = getSet(item1[1][4100:4150])
        tupl = {}
        tupl[0] = category
        tupl[1] = corpus
        tupl[2] = test
        print "extracted sets from catkey" + category
        tuples.append(tupl)

    for corpus in tuples:
        maxSim= -1
        category = "xxx"
        for test in tuples:
            sim = jac(corpus[1], test[2])
            if sim > maxSim:
                maxSim = sim
                category = test[0]
        print "most similar category for %s is %s with similarity %f" % (corpus[0], category, maxSim)

computation()
