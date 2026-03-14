import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
from elasticsearch import Elasticsearch

es = Elasticsearch(os.environ['ELASTICSEARCH_HOST'])
indicesList = ["daily_btc", "daily_eth", "daily_bnb"]
coinsList = ["BTC", "ETH", "BNB"]
colorsList = ["blue", "yellow", "red"]
# (23*22)/2*3 plots
researchableParamsList = [
    "volume",
    "avgPriceComparedToYesterday",
    "avgPriceMonth",
    "avgPriceWeek",
    "avgPrice",
    "priceVariation",
    "priceVariationAbs",
    "priceVariationWeek",
    "priceVariationMonth",
    "priceVariationComparedToWeek",
    "priceVariationComparedToMonth",
    "increaseDay",
    "LgAvgPrice",
    "LogAvgPrice",
    "LogAvgPriceWeek",
    "LogAvgPriceMonth",
    "deviationVariationWeek",
    "deviationVariationMonth",
    "deviationAvgPriceWeek",
    "deviationAvgPriceMonth",
    "LogVolume",
    "increasePercentWeek",
    "increasePercentWeightedMonth"
]
analyzedField = ""
def getAnalyzedField(i):
    global analyzedField
    #print(i)
    if analyzedField == "deviationVariationWeek":
        return i["_source"]["deviationVariationWeek"] if "deviationVariationWeek" in i["_source"] else 0
    elif analyzedField == "increaseDay":
        return 1 if i["_source"]["increaseDay"] else 0
    elif analyzedField == "increasePercentWeek":
        return i["_source"]["increasePercentWeek"] if "increasePercentWeek" in i["_source"] else 0.5
    elif analyzedField == "increasePercentWeightedMonth":
        return i["_source"]["increasePercentWeightedMonth"] if "increasePercentWeek" in i["_source"] else 0.5
    else:
        return i["_source"][analyzedField]
def getVector(values, f):
    vectorizeNpFunction = np.vectorize(f)
    return vectorizeNpFunction(values)

for i in range(len(indicesList)):
    data = es.search(index=indicesList[i], query={"match_all": {}}, size=10000, sort='timeOpen')
    data = data['hits']['hits'][7:]

    directory = "./sandbox/tmp/corr/" + coinsList[i] + "/"
    if not os.path.exists(directory):
        os.makedirs(directory)

    for j in range(len(researchableParamsList)):
        for k in range(len(researchableParamsList)):
            if j >= k:
                continue

            analyzedField = researchableParamsList[j]
            vData1 = getVector(data, getAnalyzedField)

            analyzedField = researchableParamsList[k]
            vData2 = getVector(data, getAnalyzedField)

            pd.DataFrame(np.array([vData1, vData2]).T).plot.scatter(0, 1, s=12, grid=True)
            plt.xlabel(researchableParamsList[j])
            plt.ylabel(researchableParamsList[k])
            plt.savefig(directory + researchableParamsList[j] + "_" + researchableParamsList[k] + ".png")
            plt.close()

