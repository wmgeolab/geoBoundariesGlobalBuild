#Download the datasets to be used in the construction of all products
#in this project.  These will include:
#(1) The Department of State Large-Scale International Boundary (LISB) Dataset
#(2) The specified geoBoundaries release geoJsons

import json
import requests
import os
import hashlib
import pandas as pd

with open('buildConfig.json') as json_file:
  config = json.load(json_file)

print(config)


globalHashId = hashlib.md5(str(config["globalBoundaryURL"]).encode()).hexdigest()
print(globalHashId)

if(os.path.exists('./rawData/' +  globalHashId + ".geojson")):
  print("Boundary data already retrieved.")
else:
  r = requests.get(config["globalBoundaryURL"])
  open('./rawData/' + globalHashId + ".geojson", 'wb').write(r.content)
  

if(os.path.exists('./rawData/ISO_3166_1_Alpha_3.csv')):
  print("ISO data already retrieved")
else:
  r = requests.get('https://raw.githubusercontent.com/wmgeolab/gbRelease/master/ISO_3166_1_Alpha_3.csv')
  open('./rawData/ISO_3166_1_Alpha_3.csv', 'wb').write(r.content)

#Pull the relevant geoBoundaries data
base_url = "https://www.geoboundaries.org/data/geoBoundaries-" + config["geoBoundariesVersion"] + "/"

#Download the metadata CSV
meta = pd.read_csv(base_url + "geoBoundaries-" + config["geoBoundariesVersion"] + ".csv")

#Subset it to only include the boundaries requested
meta = meta[meta["boundaryType"] == config["admLevel"]]


for index, row in meta.iterrows():
  url = row["downloadURL"].replace("-all.zip", ".geojson")
  r = requests.get(url)
  geoJsonPath = ("./rawData/geoBoundaries/" + config["geoBoundariesVersion"] + 
       "/" + config["admLevel"] + "/" 
       + row["boundaryISO"] + ".geojson")
  if (not os.path.exists(geoJsonPath)):
    try:
      os.makedirs(os.path.dirname(geoJsonPath))
    except:
      pass
    open(geoJsonPath, 'wb').write(r.content)