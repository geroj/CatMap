import json
import os
from pprint import pprint
from collections import defaultdict
from os import listdir
from os.path import isfile,join,basename,splitext
import shutil
from operator import itemgetter, attrgetter, methodcaller

EQUAL = "equal"
AMONG_TOP = "among_top"
TOP_RESULTS = "top_results"
N_A = "N/A"
CATKEY = "categorykey"
EXPECTED_CATKEY = "expected_categorykey"
TRAINTEST_SUFFIX = "_traintest"
RESULTS_FILENAME = "results.txt"
MAX_JSON_LINES = 50000

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

# Returns relative path to file with results for given datasetId
def getResultFile(datasetId):
	return datasetId + TRAINTEST_SUFFIX + "/" + RESULTS_FILENAME

# Returns number of mappings used for test
def getNumberOfMappings(results):
	return len(results)

# Returns list with mappings with enough ads
def getOnlyExistingMappings(results):
	d = []
	for result in results:
		if not result[CATKEY] == N_A:
			d.append(result)
	return d

# Returns list with correct mappings
def getOnlyTrueMappings(results):
	d = []
	for result in results:
		if result[EQUAL]:
			d.append(result)
	return d

def getTopResultsMapping(results):
	d = []
	for result in results:
		if result[AMONG_TOP]:
			d.append(result)
	return d

def getAverageRank(topResultsAdsMaps):
	total_rank = 0
	total_count = getNumberOfMappings(topResultsAdsMaps)
	for m in topResultsAdsMaps:
		expected_cat = m[EXPECTED_CATKEY]
		top_list = m[TOP_RESULTS]
		counter = 1
		rank = counter
		for tup in top_list:
			if tup[0] == expected_cat:
				rank = counter
			else:
				counter += 1
		total_rank += rank
	return total_rank * (1.0)/total_count

# Nice formatting of percentage
def perc(value, base):
	return_value = "(%.2f%%)" % ((value * (1.0)/base) * 100)
	return return_value

# Nice formatting of output of 2 numbers
def nice_output(value, base):
	return_value = "%d out of %d %s" % (value, base, perc(value, base))
	return return_value



# Runs to get stats for datasetId
def getStats(datasetId):
	print "STATS for " + datasetId + ":"
	allMaps = readJson(getResultFile(datasetId))
	allMaps_count = getNumberOfMappings(allMaps)
	enoughAdsMaps = getOnlyExistingMappings(allMaps)
	enoughAdsMaps_count = getNumberOfMappings(enoughAdsMaps)
	trueAdsMaps = getOnlyTrueMappings(enoughAdsMaps)
	trueAdsMaps_count = getNumberOfMappings(trueAdsMaps)
	topResultsAdsMaps = getTopResultsMapping(enoughAdsMaps)
	topResultsAdsMaps_count = getNumberOfMappings(topResultsAdsMaps)
	print "# mappings: %d" % getNumberOfMappings(allMaps)
	print "# mappings with enough ads (>=10): %s" % (nice_output(enoughAdsMaps_count, allMaps_count))
	print "# correct mappings:                %s" % (nice_output(trueAdsMaps_count, enoughAdsMaps_count))
	print "# topResults mappings:             %s" % (nice_output(topResultsAdsMaps_count, enoughAdsMaps_count))
	print "# average rank among top results:  %.3f (out of 5)" % (getAverageRank(topResultsAdsMaps))
	print

getStats("australia1000")
getStats("australia1000_ver2")
getStats("australia1000_ver3")
getStats("australia1000_ver4")
getStats("germany1000")
getStats("germany1000_ver2")
getStats("germany1000_ver3")
