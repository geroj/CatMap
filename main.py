import json
import os
from pprint import pprint
from collections import defaultdict
from os import listdir
from os.path import isfile,join,basename,splitext
import shutil


#with open('input_short.json') as data_file:
#    data = json.load(data_file)
#pprint(data)

#with open('input_short.json') as data_file:
#    data = json.loads(data_file.read())

directory = "data"
TRAIN_DIR = "train"
TEST_DIR = "test"

def saveToFile(name, list):
    if not os.path.exists(directory):
        os.makedirs(directory)
    path = "%s/%s.txt" % (directory, name)
    with open(path, 'w') as f:
        for item in list:
            f.write("%s" % item)

def processInput(inputFolder):
    d = {}
    if os.path.exists(directory):
        shutil.rmtree(directory)
    onlyfiles = [ filename for filename in listdir(inputFolder) if isfile(join(inputFolder,filename)) ]
    for filename in onlyfiles:
        fullpath = inputFolder + "/" + filename
        print fullpath
        with open(fullpath) as f:
            for line in f:
                json_line = json.loads(line)
                catkey =  json_line['categorykey']
                if not d.has_key(catkey):
                    d[catkey] = []
                d[catkey].append(line)
            for catkey in d.keys():
                saveToFile(catkey, d[catkey])

def createTrainTest(sources):
    inp = readAllInput(directory)
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    os.makedirs(TEST_DIR)

    if os.path.exists(TRAIN_DIR):
        shutil.rmtree(TRAIN_DIR)
    os.makedirs(TRAIN_DIR)

    for source in sources:
        if not os.path.exists(TEST_DIR+"/"+source):
            os.makedirs(TEST_DIR+"/"+source)
    for item in inp.items():
        with open(item[1]) as f:
            for i,line in enumerate(f):
                d = json.loads(line)
                if (d["sourcename"] in sources):
                    path = TEST_DIR + "/" + d["sourcename"] + "/" + item[0] + ".txt"
                    with open(path, 'a') as g:
                        g.write(line)
                else:
                    path = TRAIN_DIR + "/" + item[0] + ".txt"
                    with open(path, 'a') as g:
                        g.write(line)

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
    shingleLength = 7
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
    sizeLimit = 100
    cat = categories()
    tuples = []
    for item1 in cat.items():
        if (len(item1[1]) < sizeLimit):
            continue
        category = item1[0]    
        corpus = getSet(item1[1][0:50])
        test = getSet(item1[1][50:100])
        tupl = {}
        tupl[0] = category
        tupl[1] = corpus
        tupl[2] = test
        print "extracted sets from catkey " + category
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

def bigTestSet():
    sizeLimit = 10
    setSize = 1
    cat = categories()
    tuples = []
    for item1 in cat.items():
        if (len(item1[1]) < sizeLimit):
            continue
        category = item1[0]    
        corpus = getSet(item1[1][0:setSize])
        #test = getSet(item1[1][50:100])
        testList = [getSet(item1[1][i:i+setSize]) for i in range(setSize, sizeLimit-setSize, setSize)]
        tupl = {}
        tupl[0] = category
        tupl[1] = corpus
        tupl[2] = testList
        print "extracted sets from catkey " + category
        tuples.append(tupl)

    for testLists in tuples:
        for test in testLists[2]:
            maxSim= -1
            category = "xxx"
            for corpus in tuples:
                sim = jac(test, corpus[1])
                if sim > maxSim:
                    maxSim = sim
                    category = corpus[0]
            print "most similar category for %s is %s with similarity %f" % (testLists[0], category, maxSim)

def getTestSources():
    sources = ["s-immobilien_de", "rosenheimjobs_de", "my-next-home_de"]
    return sources

#returns dictionary where keys are tested sources and values are dictionaries with <key, value> = <categorykey, files with ads of given categorykey>
def readAllTestInput(mypath):
    output = {}
    onlyfiles = [ f for f in listdir(mypath) ]
    for f in onlyfiles:
        output[splitext(f)[0]] = mypath + "/" + f
    for item in output.items():
        outputList = {}
        directory = item[1]
        categoryFiles = [f for f in listdir(directory)]
        for cat in categoryFiles:
            category = splitext(cat)[0]
            outputList[category] = directory + "/" + cat
        output[item[0]] = outputList
    return output

def processTestInput(categoriesTrain):
    testData = readAllTestInput(TEST_DIR)
    for item in testData.items():
        print "processing input " + item[0]
        categoriesTest = {}
        for cat in item[1].items():
            categoriesTest[cat[0]] = readJson(cat[1])
        #for cat in categoriesTest.items():
        #    print cat[0], len(cat[1])
        crossCategoryTest(categoriesTest, categoriesTrain)

def categories(inputDir):
    inp = readAllInput(inputDir)
    categories = {}
    for item in inp.items():
        print "processing category " + item[0]
        categories[item[0]] = readJson(item[1])
    return categories

def doTest(testLists):
    testSet = testLists[1]
    testCategory = testLists[0]
    maxOverallSim = -1
    bestCategory = "xxx"
    for trainCategories in testLists[2].items():
        trainCategory = trainCategories[0]
        trainSets = trainCategories[1]
        maxSim= -1

        #category = "yyy"
        #for trainSet in trainSets:
        #    sim = jac(testSet, trainSet)
        #    if sim > maxSim:
        #        maxSim = sim
        #        category = trainCategory

        sum = 0
        for trainSet in trainSets:
            sim = jac(testSet, trainSet)
            sum += sim
        maxSim = sum/len(trainSets)

        if maxSim > maxOverallSim:
            print "    better category is " + trainCategory + " with sim " + "%f" % (maxSim)
            maxOverallSim = maxSim
            bestCategory = trainCategory
        if (testCategory == trainCategory):
            print "    score for this category is " + "%f" % (maxSim)
        #print "sim for category %s is %s with similarity %f" % (testLists[0], trainCategory, maxSim)
    resultString = "most similar category for category %s is %s with similarity %f" % (testCategory, bestCategory, maxOverallSim)
    prefix = ""
    if (testCategory == bestCategory):
        prefix = "EQUAL "
    print prefix + resultString

def crossCategoryTest(categoriesTest, categoriesTrain):
    sizeLimit = 2
    maxTrainSets = 10
    maxTestSetSize = 200
    tuples = []
    for item1 in categoriesTest.items():
        if (len(item1[1]) < sizeLimit):
            continue
        testCategory = item1[0]
        ads = item1[1]
        if (len(ads) > maxTestSetSize):
            ads = item1[1][0:maxTestSetSize]
        testSetAdsCount = len(ads)
        testSet = getSet(ads)

        trainCategories = {}
        for categories in categoriesTrain.items():
            trainList = [getSet(categoriesTrain[categories[0]][i:i+testSetAdsCount]) for i in range(0, maxTrainSets * testSetAdsCount, testSetAdsCount)]
            trainCategories[categories[0]] = trainList
        tupl = {}
        tupl[0] = testCategory
        tupl[1] = testSet
        tupl[2] = trainCategories
        doTest(tupl)

processTestInput(categories(TRAIN_DIR))
#processInput("de")
#createTrainTest(getTestSources())

#bigTestSet()
#computation()
#processInput()
