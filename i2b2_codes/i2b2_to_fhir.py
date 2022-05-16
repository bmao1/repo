#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  8 16:16:33 2022

@author: binmao
"""

from uuid import uuid4
import pandas as pd
from fhir.resources import construct_fhir_element
import json
import base64
import glob
import os
from datetime import datetime
import logging
import requests
################## Mapping ends 


def newUUID (usedList = set()):
    uuid = str(uuid4())
    while uuid in usedList:
        uuid = str(uuid4())
    usedList.add(uuid)
    return uuid


def cohortUUID(inPath, outPatientPath, outEncounterPath, existList = set()):
    # create uuid for patient and encounter from notes cohort
    note_cohort = pd.read_csv(inPath,dtype=str)
    """
    patient = note_cohort[["PATIENT_NUM"]].drop_duplicates()
    patient["UUID"] = [ str(uuid4())  for p in patient.PATIENT_NUM] 
    patient.to_csv(outPatientPath, index=False)
    
    encounter = note_cohort[["ENCOUNTER_NUM"]].drop_duplicates()
    encounter["UUID"] = [ str(uuid4())  for p in encounter.ENCOUNTER_NUM] 
    encounter.to_csv(outEncounterPath, index=False)
    """
    # load uuid mapping to dict
    patiMap = {}
    enctMap = {}
    uuidList = existList
    #uuidList = set()
    
    #note_cohort.reset_index()
    
    for index, row in note_cohort.iterrows():
        if row['PATIENT_NUM'] not in patiMap:
            patiMap[row['PATIENT_NUM']] = newUUID(uuidList)
        if row['ENCOUNTER_NUM'] not in enctMap:
            enctMap[row['ENCOUNTER_NUM']] = newUUID(uuidList) 
        
    with open(outPatientPath, "w") as outp:
        json.dump(patiMap, outp)
        
    with open(outEncounterPath, "w") as oute:
        json.dump(enctMap, oute)




                    
                    
#  PCR Labs: Observation resource   
                    
Observation_template = {
   "resourceType":"Observation",
   "id":"0",
   "category":[
      {
         "coding":[
            {
               "code":"laboratory",
               "display":"laboratory",
               "system":"http://terminology.hl7.org/CodeSystem/observation-category"
            }
         ]
      }
   ],
   "code":{
      "coding":[
         {
            "code":"0",
            "system":"http://loinc.org"
         }
      ]
   },
   "effectiveDateTime":"1800",
   "encounter":{
      "reference":"0"
   },
   "status":"final",
   "subject":{
      "reference":"0"
   },
   "valueCodeableConcept":{
      "coding":[
         {
            "code":"0",
            "display":"0",
            "system":"http://snomed.info/sct"
         }
      ]
   }
}

def labMapping (code):
    # code example: LAB:1043473617
    hashmap = { "LAB:1043473617" : "94500-6",
                "LAB:1044804335" : "94500-6",
                "LAB:1044704735" : "94500-6",
                "LAB:1134792565" : "95406-5",
                "LAB:1148157467" : "95406-5",
                "LAB:467288722" : "85477-8",
                "LAB:152831642" : "85476-0",
                "LAB:467288694" : "85478-6",
                "LAB:467288700" : "85479-4",
                "LAB:13815125" : "62462-7"}
    if not code:
        return 
    if code in hashmap:
        return hashmap[code]
    return

def snomedValueMapping(code):
    
    hashmap = {"positive": "10828004",
               "negative": "260385009"}
    if not code:
        return 
    code = str(code).lower()
    if code in hashmap:
        return hashmap[code]
    return "272519000"
    
def buildObservation(i2b2): 
    #lab_cohort to observation mapping
    res = construct_fhir_element("Observation",Observation_template)
    res.id = newUUID()
    res.code.coding[0].code = labMapping(i2b2.CONCEPT_CD)  # no code in i2b2, maybe use CONCEPT_CD
    #res.code.coding[0].display = i2b2.CONCEPT_CD # no diaplay, maybe use CONCEPT_CD
    res.effectiveDateTime = i2b2.START_DATE[:10]
    res.encounter.reference = 'Encounter/'+ enctMap[i2b2.ENCOUNTER_NUM] # uuid from cookbook
    res.subject.reference = 'Patient/' + patiMap[i2b2.PATIENT_NUM] # uuid from cookbook
    res.valueCodeableConcept.coding[0].display = i2b2.TVAL_CHAR
    res.valueCodeableConcept.coding[0].code = snomedValueMapping(i2b2.TVAL_CHAR)
    return res




# Diagnoses: Condition resource
# only keep ICD10:U07.1

Condition_template = {
   "resourceType":"Condition",
   "id":"0",
   "meta":{
      "profile":[
         "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition"
      ]
   },
   "clinicalStatus":{
      "coding":[
         {
            "system":"http://terminology.hl7.org/CodeSystem/condition-clinical",
            "code":"active"
         }
      ]
   },
   "verificationStatus":{
      "coding":[
         {
            "system":"http://terminology.hl7.org/CodeSystem/condition-ver-status",
            "code":"unconfirmed"
         }
      ]
   },
   "category":[
      {
         "coding":[
            {
               "system":"http://terminology.hl7.org/CodeSystem/condition-category",
               "code":"encounter-diagnosis",
               "display":"Encounter Diagnosis"
            }
         ]
      }
   ],
   "code":{
      "coding":[
         {
            "system":"0",
            "code":"0"
         }
      ]
   },
   "subject":{
      "reference":"0"
   },
   "encounter":{
      "reference":"0"
   },
   "onsetDateTime":"1800"
}

def diagMapping(code):
    # code example: ICD10:E66.9
    system = code.split(":")[0]
    systemMap = {"ICD9": "ICD-9", "ICD10": "ICD-10"}
    
    if system in systemMap :
        return systemMap[system] 
    return "Not Found"

def buildCondition(i2b2):
    # i2b2 diagnostic to fhir condition
    res = construct_fhir_element("Condition",Condition_template)
    res.id = newUUID()
    # res.code.coding[0].system = i2b2.CONCEPT_CD.split(":")[0]
    res.code.coding[0].system = diagMapping(i2b2.CONCEPT_CD)
    res.code.coding[0].code = i2b2.CONCEPT_CD.split(":")[1]
    res.subject.reference = 'Patient/' + patiMap[i2b2.PATIENT_NUM] # uuid from cookbook
    res.encounter.reference = 'Encounter/' + enctMap[i2b2.ENCOUNTER_NUM] # uuid from cookbook
    res.onsetDateTime = i2b2.START_DATE[:10]
    return res



# patient dimension: Patient resource

Patient_template= {
   "resourceType":"Patient",
   "id":"0",
   "meta":{
      "profile":[
         "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient"
      ]
   },
   "extension":[
      {
         "url":"http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
         "extension":[
            {
               "url":"ombCategory",
               "valueCoding":{
                  "system":"urn:oid:2.16.840.1.113883.6.238",
                  "code":"0",
                  "display":"0"
               }
            }
         ]
      }
   ],
   "identifier":[
      {
         "system":"BCH i2b2 patient number",
         "value":"0"
      }
   ],
   "gender":"U",
   "birthDate":"1800",
   "address":[
      {
         "postalCode" : "00000",
         "country" : "US"
      }
   ]
}
def raceMapping(code):
    
    hashmap = {"White":"2106-3",
               "Black or African American": "2054-5",
               "American Indian or Alaska Native": "1002-5",
               "Asian": "2028-9",
               "Native Hawaiian or Other Pacific Islander": "2076-8",
               "Hispanic or Latino": "2135-2",
               "Not Hispanic or Latino": "2186-5"
               }
    if not code:
        return
    if code in hashmap:
        return hashmap[code]
    return
    

def genderMapping(code):
    hashmap = {"F": "female",
               "M": "male"}
    if not code:
        return 
    if code in hashmap:
        return hashmap[code]
    return "unknown"
    
    
def buildPatient(i2b2):
    # i2b2 patient dimension to fhir Patient
    res = construct_fhir_element("Patient",Patient_template)
    res.id = patiMap[i2b2.PATIENT_NUM]
    res.extension[0].extension[0].valueCoding.code = raceMapping(i2b2.RACE_CD)
    res.extension[0].extension[0].valueCoding.display = i2b2.RACE_CD
    res.identifier[0].value = i2b2.PATIENT_NUM
    res.gender = genderMapping(i2b2.SEX_CD)
    res.birthDate = i2b2.BIRTH_DATE[:4]
    res.address[0].postalCode = i2b2.ZIP_CD
    return res


# notes (ED, Admission, DIscharge, Radiology) -> DocRef

DocRef_template = {
   "resourceType":"DocumentReference",
   "id":"0",
   "status":"superseded",
   "category":[
      {
         "coding":[
            {
               "system":"http://hl7.org/fhir/us/core/CodeSystem/us-core-documentreference-category",
               "code":"clinical-note",
               "display":"Clinical Note"
            }
         ]
      }
   ],
   "subject":{
      "reference":"0"
   },
   "type":{
      "text":"0"
   },
   "date":"1800-01-01T00:00:00Z",
   "content":[
      {
         "attachment":{
            "contentType":"text/plain",
            "data":"0"
         }
      }
   ],
   "context":{
      "encounter":[
         {
            "reference":"0"
         }
      ]
   }
}

def buildDocumentReference(i2b2):
    # i2b2 notes to fhir DocumentReference
    res = construct_fhir_element("DocumentReference",DocRef_template)
    res.id = newUUID()
    res.subject.reference = "Patient/" + patiMap[i2b2.PATIENT_NUM] # uuid from cookbook
    res.type.text = i2b2.CONCEPT_CD
    res.date = i2b2.START_DATE[:10] + "T00:00:00Z"
    res.content[0].attachment.data = base64.b64encode(str(i2b2.OBSERVATION_BLOB).encode()) # base64 encoding
    res.context.encounter[0].reference = 'Encounter/'+ enctMap[i2b2.ENCOUNTER_NUM] # context?  uuid from cookbook
    return res


Encounter_template = {
   "resourceType":"Encounter",
   "id":"0",
   "meta":{
      "profile":[
         "http://hl7.org/fhir/us/core/StructureDefinition/us-core-encounter"
      ]
   },
   "identifier":[
      {
         "use":"official",
         "system":"BCH i2b2 visit dimension",
         "value":"0"
      }
   ],
   "status":"finished",
   "class":{
      "system":"http://terminology.hl7.org/CodeSystem/v3-ActCode",
      "code":"0"
   },
   "type":[
      {
         "coding":[
            {
               "system":"http://snomed.info/sct",
               "code":"308335008",
               "display":"Patient encounter procedure"
            }
         ]
      }
   ],
   "subject":{
      "reference":"0"
   },

   "period":{
      "start":"1800-01-01T00:00:00Z",
      "end":"1800-01-01T00:00:00Z"
   }
}
    
def buildEncounter(i2b2):
    # i2b2 notes to fhir DocumentReference
    res = construct_fhir_element("Encounter", Encounter_template)
    res.id = newUUID()
    res.identifier[0].value = i2b2.ENCOUNTER_NUM
    res.class_fhir.code = "IMP" if i2b2.INOUT_CD == "Inpatient" else "EMER"
    res.subject.reference = "Patient/" + patiMap[i2b2.PATIENT_NUM] # uuid from cookbook
    res.period.start = i2b2.START_DATE
    res.period.end = i2b2.END_DATE
    return res

Classifier_template = {
   "resourceType":"Observation",
   "id":"0",
   "text": {
       "status": "generated",
       "div": "NLP Classifier"
       },
   "category":[
      {
         "coding":[
            {
               "code":"laboratory",
               "display":"laboratory",
               "system":"NLP Covid classifier"
            }
         ]
      }
   ],
   "code":{
      "coding":[
         {
            "code":"94500-6",
            "system":"http://loinc.org"
         }
      ]
   },
   "effectiveDateTime":"1800",
   "encounter":{
      "reference":"0"
   },
   "status":"unknown",
   "subject":{
      "reference":"0"
   },
   "valueCodeableConcept":{
      "coding":[
         {
            "code":"0",
            "display":"0",
            "system":"http://snomed.info/sct"
         }
      ]
   },
   "modifierExtension":[{"url":"http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-clissifier",
                     "valueString":"Covid classifier v1"}]
}
def buildClassifier(i2b2):
    url = 'http://tmill-desktop:8000/covid-19/is_covid_positive'
    classifier = "Not classified"
    try:
        r = requests.post(url, json= {'enc_id':int(i2b2["ENCOUNTER_NUM"])} )
        contentParse = r.json()
        classifier = contentParse["covid_pos"]
    except:
        logging.warning("Unable to find classifier for : " + i2b2["ENCOUNTER_NUM"])
    
    if classifier == "Not classified":
        return 
    
    res = construct_fhir_element("Observation",Classifier_template)
    res.id = newUUID()
    res.effectiveDateTime = i2b2.START_DATE[:10]
    res.encounter.reference = 'Encounter/'+ enctMap[i2b2.ENCOUNTER_NUM] # uuid from cookbook
    res.subject.reference = 'Patient/' + patiMap[i2b2.PATIENT_NUM] # uuid from cookbook
    res.valueCodeableConcept.coding[0].display = classifier
    res.valueCodeableConcept.coding[0].code = "10828004" if classifier == "Positive" else "260385009"
    return res

Classifier_template_202204 = {
   "resourceType":"Observation",
   "id":"0",
   "text": {
       "status": "generated",
       "div": "NLP Classifier"
       },
   "category":[
      {
         "coding":[
            {
               "code":"laboratory",
               "display":"laboratory",
               "system":"NLP Covid classifier"
            }
         ]
      }
   ],
   "code":{
      "coding":[
         {
            "code":"94500-6",
            "system":"http://loinc.org"
         }
      ]
   },
   "effectiveDateTime":"1800",
   "encounter":{
      "reference":"0"
   },
   "status":"unknown",
   "subject":{
      "reference":"0"
   },
   "valueCodeableConcept":{
      "coding":[
         {
            "code":"0",
            "display":"0",
            "system":"http://snomed.info/sct"
         }
      ]
   },
   "modifierExtension":[{"url":"http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-clissifier",
                     "valueString":"Covid classifier 202204"}]
}

def import_classifier_202204(classifier_loc):
    global classifier_result
    classifier_result = {}
    classifier_tb = pd.read_csv(classifier_loc,dtype=str,header=0)
    for index, row in classifier_tb.iterrows():
        if row["ENCOUNTER_NUM"] in classifier_result:
            continue
        else :
            classifier_result[row["ENCOUNTER_NUM"]] = row["PREDICTION"]
    
import_classifier_202204(classifier_loc)

def buildClassifier_202204(i2b2):
    classifier = "Not classified"
    if i2b2["ENCOUNTER_NUM"] not in classifier_result:
        return
    classifier = classifier_result[i2b2["ENCOUNTER_NUM"]]
    
    res = construct_fhir_element("Observation",Classifier_template_202204)
    res.id = newUUID()
    res.effectiveDateTime = i2b2.START_DATE[:10]
    res.encounter.reference = 'Encounter/'+ enctMap[i2b2.ENCOUNTER_NUM] # uuid from cookbook
    res.subject.reference = 'Patient/' + patiMap[i2b2.PATIENT_NUM] # uuid from cookbook
    res.valueCodeableConcept.coding[0].display = classifier
    res.valueCodeableConcept.coding[0].code = "10828004" if classifier.lower() == "positive" else "260385009"
    return res


def main(maxN = 0):
        
    inPath = "/Users/binmao/i2b2DB/data0228/NOTE_COHORT_202202062242.csv"
    outPatientPath = "/Users/binmao/i2b2DB/bch_mapping/patient_mapping.json"
    outEncounterPath = "/Users/binmao/i2b2DB/bch_mapping/encounter_mapping.json"
    
    patient_file = "/Users/binmao/i2b2DB/data0228/PATIENT_DIMENSION_202202280957.csv"
    patient_out = "/Users/binmao/i2b2DB/bch_mapping/Patient.ndjson"
    
    diag_file = "/Users/binmao/i2b2DB/data0228/*Diagnosis.csv"
    diag_out = "/Users/binmao/i2b2DB/bch_mapping/Condition.ndjson"
    
    lab_file = "/Users/binmao/i2b2DB/data0228/LAB_COHORT_202202062349.csv"
    lab_out = "/Users/binmao/i2b2DB/bch_mapping/Observation.ndjson"
    
    docref_file = inPath
    docref_out = "/Users/binmao/i2b2DB/bch_mapping/DocumentReference.ndjson"
    classifier_out = "/Users/binmao/i2b2DB/bch_mapping/Observation_classifier.ndjson"
    
    encounter_file = "/Users/binmao/i2b2DB/data0228/visit_dim_20220302.csv"
    encounter_out = "/Users/binmao/i2b2DB/bch_mapping/Encounter.ndjson"
    
    classifier_loc = "/Users/binmao/i2b2DB/data0228/prediction_results_with_enco-apr12-2022.txt"
    
    # create patient/encounter to uuid mapping - cookbook
    from os.path import exists

    if not os.path.exists(outPatientPath):
        cohortUUID(inPath = inPath
                   , outPatientPath = outPatientPath
                   , outEncounterPath = outEncounterPath)

    global patiMap, enctMap

    # read in mapping for creating fhir resources
    with open(outPatientPath, "r") as json_file:
        patiMap = json.load(json_file)
    
    with open(outEncounterPath, "r") as json_file:
        enctMap = json.load(json_file)

 
    
    n = 0
    # patient dimension to Patient
    patient_tb = pd.read_csv(patient_file,dtype=str)
    print("Processing patient dimension -> Patient. Size: "+ str(patient_tb.shape))
    for index, row in patient_tb.iterrows():
        if row["PATIENT_NUM"] in patiMap:
            res = buildPatient(row)
            with open(patient_out, "a+") as f:
                f.write(res.json() + "\n")
            n += 1
        if n == maxN and maxN >0:
            break
        if index % 10000 == 0 and index >0 :
            print(str(index) + " completed", datetime.now())
            
            
    n = 0
    # diagnostic to Condition, map all
    # keep one diag with per encounter per concept code
    diag_tb = pd.concat(map(lambda file: pd.read_csv(file, dtype=str), glob.glob(os.path.join('', diag_file))))
    print("Processing diagnostic -> Condition. Size: "+ str(diag_tb.shape))
    #diag_tb = pd.read_csv(diag_file,dtype=str)
    hashmap = {}
    for index, row in diag_tb.iterrows():
        if row["PATIENT_NUM"] in patiMap and row["ENCOUNTER_NUM"] in enctMap: # and row["CONCEPT_CD"] in ["ICD10:U07.1"]:
            # check repeatd encounter/ICD code combo, skip if already exist
            if row["ENCOUNTER_NUM"] in hashmap:
                if row["CONCEPT_CD"] in hashmap[row["ENCOUNTER_NUM"]]:
                    continue
                else:
                    hashmap[row["ENCOUNTER_NUM"]].append(row["CONCEPT_CD"])
            else:
                hashmap[row["ENCOUNTER_NUM"]] = [row["CONCEPT_CD"]]
                
            res = buildCondition(row)
            with open(diag_out, "a+") as f:
                f.write(res.json() + "\n")
            n += 1
        if n == maxN and maxN >0:
            break
        if index % 10000 == 0 and index >0 :
            print(str(index) + " completed", datetime.now())
                      
    
    n = 0
    # lab to observation
    labCodes = ("LAB:1043473617", "LAB:1044804335", "LAB:1044704735", "LAB:1134792565", "LAB:1148157467", "LAB:467288722", "LAB:152831642", "LAB:467288694", "LAB:467288700", "LAB:13815125")
    lab_tb = pd.read_csv(lab_file,dtype=str)
    print("Processing lab -> Observation. Size: "+ str(lab_tb.shape))
    for index, row in lab_tb.iterrows():
        if row["PATIENT_NUM"] in patiMap and row["ENCOUNTER_NUM"] in enctMap and row["CONCEPT_CD"] in labCodes:
            res = buildObservation(row)
            if res:
                with open(lab_out, "a+") as f:
                    f.write(res.json() + "\n")
                n += 1
        if n == maxN and maxN >0:
            break
        if index % 10000 == 0  and index >0:
            print(str(index) + " completed", datetime.now())


    n = 0
    # Notes to DocumentReference
    docref_tb = pd.read_csv(docref_file,dtype=str)
    print("Processing notes -> DocumentReference. Size: "+ str(docref_tb.shape))
    print("Processing classifier")
    for index, row in docref_tb.iterrows():
       
        #build document reference
        res = buildDocumentReference(row)
        with open(docref_out, "a+") as f:
            f.write(res.json() + "\n")
        n += 1
        if n == maxN and maxN >0:
            break
        if index % 10000 == 0 and index >0:
            print(str(index) + " completed", datetime.now())
    
    n = 0
    # classifier
    classifierEnct = []
    docref_tb = pd.read_csv(docref_file,dtype=str)
    print("Processing classifier Size: "+ str(docref_tb.shape))
    try:
        import_classifier_202204(classifier_loc)
    except:
        print("Raw classifier file is not loaded")
    for index, row in docref_tb.iterrows():
        if row["ENCOUNTER_NUM"] in classifierEnct:
            pass
        else:
            classifierEnct.append(row["ENCOUNTER_NUM"])
            #build classifier, output to observation
            #res = buildClassifier(row)
            res = buildClassifier_202204(row)
            if res:
                with open(classifier_out, "a+") as f:
                    f.write(res.json() + "\n")

        n += 1
        if n == maxN and maxN >0:
            break
        if index % 10000 == 0 and index >0:
            print(str(index) + " completed", datetime.now())
            
            
            
            
    n = 0
    # visit dim to encounter
    encounter_tb = pd.read_csv(encounter_file,dtype=str)
    print("Processing visit dim -> Encounter. Size: "+ str(encounter_tb.shape))
    for index, row in encounter_tb.iterrows():
        if row["PATIENT_NUM"] in patiMap :
            res = buildEncounter(row)
            if res:
                with open(encounter_out, "a+") as f:
                    f.write(res.json() + "\n")
                n += 1
        if n == maxN and maxN >0:
            break
        if index % 10000 == 0 and index >0:
            print(str(index) + " completed", datetime.now())
    
    
    
if __name__ == "__main__":
    main(maxN = 0)


"""
maxN = 0

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

docref_tb.columns
docref_tb.head(2)
docref_tb.groupby("TVAL_CHAR").size()


# testing 

diag_tb = pd.read_csv("/Users/binmao/i2b2DB/data0228/1_Diagnosis.csv",dtype=str)

diag_tb[["codesystem","billCode"]] = diag_tb["CONCEPT_CD"].str.split(":",n=1, expand=True)

diag_tb.groupby("billCode").size()

out = diag_tb["CONCEPT_CD"].str.split(":")[0]







# class IMP EMER
n =0
for index, row in visit_tb.iterrows():
    if row["PATIENT_NUM"] in patiMap and row["ENCOUNTER_NUM"] in enctMap:
        res = buildEncounter(row)
        print(res)
        n +=1
    if n >3:
        break

def buildEncounter(i2b2):
    # i2b2 notes to fhir DocumentReference
    res = construct_fhir_element("Encounter", Encounter_template)
    res.id = enctMap[i2b2.ENCOUNTER_NUM]
    res.identifier[0].value = i2b2.ENCOUNTER_NUM
    res.class_fhir.code = "IMP" if i2b2.INOUT_CD == "Inpatient" else "EMER"
    res.subject.reference = "Patient/" + patiMap[i2b2.PATIENT_NUM] # uuid from cookbook
    res.period.start = i2b2.START_DATE
    res.period.end = i2b2.END_DATE
    return res

"""