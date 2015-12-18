import json
import os
from pprint import pprint
from collections import defaultdict
from os import listdir
from os.path import isfile,join,basename,splitext
import shutil
from operator import itemgetter, attrgetter, methodcaller


#with open('input_short.json') as data_file:
#    data = json.load(data_file)
#pprint(data)

#with open('input_short.json') as data_file:
#    data = json.loads(data_file.read())

directory = "data"
TRAIN_DIR = "train"
TEST_DIR = "test"
BATCH_SIZE = 10000
MAX_JSON_LINES = 5000
CATKEY = "categorykey"
EXPECTED_CATKEY = "expected_categorykey"
EQUAL = "equal"
MAINCAT_ORIG = "maincategory_original"
CAT_ORIG = "category_original"
SUBCAT_ORIG = "subcategory_original"
SOURCENAME = "sourcename"
DEFAULT_MISSING = "### MISSING ###"
IDS_FILENAME = "ids.txt"
RESULTS_FILENAME = "results.txt"
AMONG_TOP = "among_top"
ID = "id"
DATA = "data"
TOP_SCORE = "top_score"
TOP_RESULTS = "top_results"
PROCESSED_SUFFIX = "_processed"
TRAINTEST_SUFFIX = "_traintest"
MAX_TRAIN_SET = 5
SHINGLE_SIZE = 7
#THRESHOLD_LIST = [10, 50]
THRESHOLD_USED = "threshold_used"
#THRESHOLD_LIST = [1, 5, 10, 50, 100, 500]
#THRESHOLD_LIST = [3, 30, 300]
THRESHOLD_LIST = [2, 5, 10, 20, 50,100, 200, 500, 1000]

# Returns processed folder
def getProcessedFolder(inputFolder):
    return inputFolder + PROCESSED_SUFFIX

# Returns traintest folder
def getTrainTestFolder(inputFolder):
    return inputFolder + TRAINTEST_SUFFIX

def getIdsFile(inputFolder):
    return "%s/%s" % (getProcessedFolder(inputFolder), IDS_FILENAME)

# Split input folder according identifiers (soucename, maincategory_original, scategory_original, subcategory_original): 
def splitInput(inputFolder):
    d = {}
    inputFolderProcessed = getProcessedFolder(inputFolder)
    if os.path.exists(inputFolderProcessed):
        shutil.rmtree(inputFolderProcessed)
    onlyfiles = [ filename for filename in listdir(inputFolder) if isfile(join(inputFolder,filename)) ]
    dct_id = {}
    for filename in onlyfiles:
        fullpath = inputFolder + "/" + filename
        print "processing file " + fullpath
        with open(fullpath) as f:
            buff = []
            for line in f:
                if len(buff) < BATCH_SIZE:
                    buff.append(line)
                else:
                    processBatch(buff, inputFolderProcessed, dct_id)
                    buff = []
            processBatch(buff, inputFolderProcessed, dct_id)
    print "number of category tuples is %s" % len(dct_id.keys())
    idsFile = getIdsFile(inputFolder)
    with open(idsFile, 'w') as f:
        f.write("%s" % json.dumps(dct_id))

# Get hash of ad identifier
def getUid(sourcename, maincategory, category, subcategory):
    return hash(sourcename + maincategory + category + subcategory) & 0xffffffff

# Returns dictionary with ads identifiers
def getDict(uid, sourcename, maincategory, category, subcategory, categorykey):
    dct = {}
    data = {}
    dct[ID] = uid
    data[ID] = uid
    data[SOURCENAME] = sourcename
    data[MAINCAT_ORIG] = maincategory
    data[CAT_ORIG] = category
    data[SUBCAT_ORIG] = subcategory
    data[CATKEY] = categorykey
    dct[DATA] = data
    return dct

# Process serialized json lines
def processBatch(lines, folder, dct_id):
    dct_data = {}
    for line in lines:
        json_line = json.loads(line)
        catkey = json_line.get(CATKEY, DEFAULT_MISSING)
        maincategory = json_line.get(MAINCAT_ORIG, DEFAULT_MISSING)
        category = json_line.get(CAT_ORIG, DEFAULT_MISSING)
        subcategory = json_line.get(SUBCAT_ORIG, DEFAULT_MISSING)
        sourcename = json_line.get(SOURCENAME, DEFAULT_MISSING)

        uid = getUid(sourcename, maincategory, category, subcategory)
        dct = getDict(uid, sourcename, maincategory, category, subcategory, catkey)
        dct_id[dct[ID]] = dct[DATA]
        if not dct_data.has_key(dct[ID]):
            dct_data[dct[ID]] = []
        dct_data[dct[ID]].append(line)
    for item in dct_data.items():
        uid = item[0]
        dataList = item[1]
        dirname = folder
        filename = str(uid) + ".txt"
        saveSerializedJson(dirname, filename, dataList)

# Saves serialized json lines
def saveSerializedJson(dirname, filename, list):
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    path = "%s/%s" % (dirname, filename)
    with open(path, 'a') as f:
        for item in list:
            f.write("%s" % item)

# Returns dictionary with category tuples
def getIdsDict(inputFolder):
    idsFile = getIdsFile(inputFolder)
    with open(idsFile, 'r') as f:
        return json.loads(f.read())

# Split processed dataset to train and test set
# testSourcenames contains list of sources, which data are used for training
def splitToTrainTest(inputFolder, trainSourcenames):
    traintestFolder = getTrainTestFolder(inputFolder)
    trainFolder = traintestFolder + "/" + TRAIN_DIR
    testFolder = traintestFolder + "/" + TEST_DIR
    processedFolder = getProcessedFolder(inputFolder)
    if os.path.exists(traintestFolder):
        shutil.rmtree(traintestFolder)
    os.makedirs(traintestFolder)
    os.makedirs(trainFolder)
    os.makedirs(testFolder)
    dct_id = getIdsDict(inputFolder)
    for item in dct_id.items():
        #print processedFolder + "/" + item[0] + ".txt"
        filename = item[0] + ".txt"
        filepath = processedFolder + "/" + filename
        if item[1][SOURCENAME] in trainSourcenames:
            processTrainFile(inputFolder, item[0], dct_id)
        else:
            dstFilepath = testFolder + "/" + filename
            shutil.copyfile(filepath, dstFilepath)

# Create train set with files named by catkeys
def processTrainFile(inputFolder, uid, dct_id):
    trainFolder = getTrainTestFolder(inputFolder) + "/" + TRAIN_DIR
    filepath = getProcessedFolder(inputFolder) + "/" + uid + ".txt"
    catkey = dct_id[uid][CATKEY]
    dstFilepath = trainFolder + "/" + catkey + ".txt"
    with open(filepath, 'r') as f:
        with open(dstFilepath, 'a') as g:
            g.write("%s" % f.read())

# Returns list of train categories <category, filepath>
def getCategoriesDict(inputFolder, trainSourcenames):
    dct_id = getIdsDict(inputFolder)
    categories = {}
    trainFolder = getTrainTestFolder(inputFolder) + "/" + TRAIN_DIR
    for item in dct_id.items():
        catkey = item[1][CATKEY]
        sourcename = item[1][SOURCENAME]
        if sourcename in trainSourcenames:
            categories[catkey] = trainFolder + "/" + catkey + ".txt"
    return categories

# Returs dictionary with test files <uid, filepath>
def getTestFiles(inputFolder, trainSourcenames):
    dct_id = getIdsDict(inputFolder)
    testFiles = {}
    testFolder = getTrainTestFolder(inputFolder) + "/" + TEST_DIR
    for item in dct_id.items():
        uid = item[0]
        sourcename = item[1][SOURCENAME]
        if not sourcename in trainSourcenames:
            testFiles[uid] = testFolder + "/" + uid + ".txt"
    return testFiles

# Reads json file to list
def readJson(jsonFile):
    d = []
    maxLines = MAX_JSON_LINES
    with open(jsonFile) as f:
        for i,line in enumerate(f):
            if (i < maxLines):
                d.append(json.loads(line))
#        for line in f:
#            d.append(json.loads(line))
    return d;

# Extracts sets from trainSet
def extractTrainSet(inputFolder, thresholdsList, trainSourcenames):
    categories = getCategoriesDict(inputFolder, trainSourcenames)
    testFiles = getTestFiles(inputFolder, trainSourcenames)
    trainData = {}
    for item in categories.items():
        print "extraction from " + item[0]
        thresholds = {}
        json_list = readJson(item[1])
        for t in thresholdsList:
            t_int = int(t)
            trainList = [i for i in range(0, min(t_int*(MAX_TRAIN_SET), len(json_list)) - t_int, t_int)]
            if len(trainList) >= 1:
                sets = []
                for index in trainList:
                    sets.append(getSet(json_list[i:i+t_int]))
                thresholds[t] = sets
        trainData[item[0]] = thresholds
    return trainData

# get biggest item which is in both lists
def chooseMaxThresholdSize(list1, list2):
    maxItem = 0
    for item in list1:
        if (item in list2) & (item > maxItem):
            maxItem = item
    return maxItem

# Returns shingles of desired size
def shingling(text, shingleLength):
    tokens_desc = [text[i:i+shingleLength] for i in range(len(text) - shingleLength + 1) if len(text[i]) < shingleLength + 1]
    return tokens_desc

# Get set of shingles coded to integers
# Removes blank characters
def getSet(d):
    hash_set = set()
    shingleLength = SHINGLE_SIZE
    for i in range(0,len(d)):
        tokens = []
        if "ad__headline" in d[i].keys():
            tokens = shingling(d[i]["ad__headline"].strip(), shingleLength)
        if "ad__description" in d[i].keys():
            tokens += shingling(d[i]["ad__description"].strip(), shingleLength)
        hashes = [hash(token) & 0xffffffff for token in tokens]
        hash_set.update(hashes)
    return hash_set

# Returns jaccard similarity between 2 sets
def jac(set1, set2):
    unionLength = len(set1 | set2)
    if unionLength == 0:
        return 0
    return float(len(set1 & set2)) / unionLength

# returns score which is average of scores of multiple subsets
def getScore(listSet1, listSet2):
    count = 0
    simSum = 0
    for itemSet1 in listSet1:
        for itemSet2 in listSet2:
            #MAX
            #score = jac(itemSet1, itemSet2)
            #count = 1
            #if (score > simSum):
            #    simSum = score
            #AVG
            simSum += jac(itemSet1, itemSet2)
            count += 1
    return float(simSum / count)


def getTestTrainResults(inputFolder, thresholdsList, trainSourcenames):
    trainData = extractTrainSet(inputFolder, thresholdsList, trainSourcenames)
    testFiles = getTestFiles(inputFolder, trainSourcenames)
    trainTestFolder = getTrainTestFolder(inputFolder)
    dct_id = getIdsDict(inputFolder)
    
    resultsFilepath = trainTestFolder + "/" + RESULTS_FILENAME
    if os.path.exists(resultsFilepath):
        os.remove(resultsFilepath)

    result_dict = {}
    for testItem in testFiles.items():
        thresholds = {}
        json_list = readJson(testItem[1])
        for t in thresholdsList:
            t_int = int(t)
            testList = [i for i in range(0, min(t_int*(MAX_TRAIN_SET), len(json_list)) - t_int, t_int)]
            if len(testList) >= 1:
                sets = []
                for index in testList:
                    sets.append(getSet(json_list[i:i+t_int]))
                thresholds[t] = sets
        scores = []
        for trainItem in trainData.items():
            maxThreshold = chooseMaxThresholdSize(thresholds.keys(), trainItem[1].keys())
            if (maxThreshold == 0):
                # score 0
                #tup = (trainItem[0], 0);
                #scores.append(tup)
                continue
            #trainSets = trainItem[1][maxThreshold]
            #testSets = thresholds[maxThreshold]
            maxThreshold = min(max(trainItem[1].keys()), max(thresholds.keys()))
            trainSets = trainItem[1][max(trainItem[1].keys())]
            testSets = thresholds[max(thresholds.keys())]
            score = getScore(trainSets, testSets)
            tup = (trainItem[0], score, maxThreshold);
            scores.append(tup)
        MAX_RESULTS = 5
        topResults = sorted(scores, key=itemgetter(1), reverse=True)[0:MAX_RESULTS]

        results = {}
        results[ID] = testItem[0]
        results[EXPECTED_CATKEY] = dct_id[testItem[0]][CATKEY]
        if len(topResults) == 0:
            results[CATKEY] = "N/A"
            results[THRESHOLD_USED] = -1
            results[TOP_SCORE] = -1
            results[AMONG_TOP] = False
        else:
            results[CATKEY] = topResults[0][0]
            results[TOP_SCORE] = topResults[0][1]
            results[THRESHOLD_USED] = topResults[0][2]
            results[TOP_RESULTS] = topResults
            top_categories = [x[0] for x in topResults]
            results[AMONG_TOP] = results[EXPECTED_CATKEY] in top_categories
        results[EQUAL] = results[CATKEY] == results[EXPECTED_CATKEY]
        result_dict[testItem[0]] = results
        with open(resultsFilepath, 'a') as f:
            f.write("%s\n" % json.dumps(results))
        if results[CATKEY] == "N/A":
            continue
        else:
            tup = (results[EQUAL], results[AMONG_TOP], results[THRESHOLD_USED], results[ID], results[CATKEY], results[EXPECTED_CATKEY])
            print tup
    return result_dict

def saveToFile(name, list):
    if not os.path.exists(directory):
        os.makedirs(directory)
    path = "%s/%s.txt" % (directory, name)
    with open(path, 'w') as f:
        for item in list:
            f.write("%s" % item)

















# Split data from input folder to subfolders based on categorykeys
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
    summaries = {}
    for item in testData.items():
        print "processing input " + item[0]
        categoriesTest = {}
        for cat in item[1].items():
            categoriesTest[cat[0]] = readJson(cat[1])
        #for cat in categoriesTest.items():
        #    print cat[0], len(cat[1])
        summary = crossCategoryTest(categoriesTest, categoriesTrain)
        summaries[item[0]] = summary
    for key in sorted(summaries.iterkeys()):
        print ("%30s %s" % (key, summaries[key]))

def categories(inputDir):
    inp = readAllInput(inputDir)
    categories = {}
    for item in inp.items():
        print "processing category " + item[0]
        categories[item[0]] = readJson(item[1])
    return categories

def getSomeSources():
    sources = []
    d = readJson("data/autos.txt")
    s = set()
    for item in d:
        s.add(item["sourcename"])
    sources = [source for source in s]
    return sources[0:30]


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
            # print "    better category is " + trainCategory + " with sim " + "%f" % (maxSim)
            maxOverallSim = maxSim
            bestCategory = trainCategory
        #if (testCategory == trainCategory):
        #   print "    score for this category is " + "%f" % (maxSim)
        #print "sim for category %s is %s with similarity %f" % (testLists[0], trainCategory, maxSim)
    resultString = "most similar category for category %s is %s with similarity %f" % (testCategory, bestCategory, maxOverallSim)
    prefix = ""

    returnValue = 0
    if (testCategory == bestCategory):
        prefix = "EQUAL "
        returnValue = 1
    print prefix + resultString
    return returnValue

def crossCategoryTest(categoriesTest, categoriesTrain):
    sizeLimit = 5
    maxTrainSets = 10
    maxTestSetSize = 200
    tuples = []

    counterOfAll = 0
    counterOfEqual = 0
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
        areEqual = doTest(tupl)
        counterOfAll += 1
        counterOfEqual += areEqual
    summary = "equal count %3d out of %3d test set size %3d" % (counterOfEqual, counterOfAll, testSetAdsCount)
    return summary



##############################################
#processInput("vn")
#createTrainTest(["nhaban_vn","nhadat24h_net","chosaigon_com"])
#processTestInput(categories(TRAIN_DIR))

##############################################
#processInput("australia1000")
#createTrainTest(["australia_global-free-classified-ads_com","quicksales_com_au","cracker_com_au","localclassifieds_com_au","truebuy_com_au","newsclassifieds_com_au"])
#processTestInput(categories(TRAIN_DIR))

splitInput("australia1000")
trainSourcenames = ["australia_global-free-classified-ads_com","quicksales_com_au","cracker_com_au","localclassifieds_com_au","truebuy_com_au","newsclassifieds_com_au"]
splitToTrainTest("australia1000", trainSourcenames)
getTestTrainResults("australia1000", THRESHOLD_LIST, trainSourcenames)


#bigTestSet()
#computation()
#processInput()
