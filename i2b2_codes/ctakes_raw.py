import os
import multiprocessing as mp
import logging
import re
from tqdm import tqdm
from glob import glob
import sys
import json
from base64 import b64decode
import requests
from uuid import uuid4
import pandas as pd
from datetime import datetime


def process_sentence(sent):
    url = 'http://localhost:8080/ctakes-web-rest/service/analyze'
    r = requests.post(url, data=sent)
    return r.json()

def process_csv_file(inpath, outpath):

    outPatientPath = "/Users/binmao/i2b2DB/bch_mapping/patient_mapping.json"
    outEncounterPath = "/Users/binmao/i2b2DB/bch_mapping/encounter_mapping.json"

    global patiMap, enctMap

    # read in mapping for creating fhir resources
    with open(outPatientPath, "r") as json_file:
        patiMap = json.load(json_file)
    
    with open(outEncounterPath, "r") as json_file:
        enctMap = json.load(json_file)


    notes_csv = pd.read_csv(inpath,dtype=str)
    print("Processing notes_csv. Size: "+ str(notes_csv.shape))
    for index, row in notes_csv.iterrows():

        try:
            row["PATIENT_NUM"] = patiMap[row["PATIENT_NUM"]]
            row["ENCOUNTER_NUM"] = enctMap[row["ENCOUNTER_NUM"]]
            if len(str(row["OBSERVATION_BLOB"])) > 0:
                ctakes = process_sentence(str(row["OBSERVATION_BLOB"]))
                row["OBSERVATION_BLOB"] = ctakes
            else:
                row["OBSERVATION_BLOB"] = {}
            with open(outpath + "ctakes_raw.ndjson", "a+") as f:
                f.write(json.dumps(row.to_dict()) + "\n")
        except:
            print("Error at index " + str(index))
        if index % 10000 == 0:
            print(str(index) + " completed", datetime.now())


def main(args):
    if len(args) < 2:
        sys.stderr.write('2 required arguments: <input ndjson file> <output dir> [enable progress bar=true]\n')
        sys.exit(-1)

    if len(args) == 3:
        disable_progress = not (args[2].lower() == 'true')

    inpath = args[0]
    if os.path.isdir(inpath):
        inpath = re.sub(r'\/$', '', inpath) +"/"
    
    global outpath 
    outpath = args[1]
    outpath = re.sub(r'\/$', '', outpath) +"/"
    disable_progress = True

    
    if os.path.isfile(inpath):
        print("processing file: " + inpath)
        process_csv_file(inpath, outpath)
        



if __name__ == "__main__":
   main(sys.argv[1:])


