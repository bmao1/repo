# i2b2 to fhir mapping 
These codes are being used to convert csv files from i2b2 to FHIR v4 ndjson files.
CHIP team doesn't have the actual code to extract the csv from BCH i2b2. That process was supported by BCH Research Computing.

## custom_ext_config.csv
This is the configuration file for De-id module to support use cases in CDC demo. It need to be used by FHIR_ETL tool (https://github.com/bmao1/FHIR_ETL/blob/main/custom_config.py) to update the config
Last few lines containing "nodesByType" is the most impactful since it preserve lots of raw data. It give the accurate data for the demo but may expose PHI unintentionally. Further investigation may be needed.
Run `python3 custom_config.py `

## i2b2_to_fhir.py
`python3 i2b2_to_fhir.py` to kick off the process. (Please note the file path are all hard-coded in this file.

1. General mapping procedure
	- Construct and validate FHIR resource `from fhir.resources import construct_fhir_element`
	- Build patient MRN to uuid mapping (call `cohortUUID` if mapping not exist)
	- Setup a blank template for each FHIR resource with correct structure in json ```Observation_template = {
	   "resourceType":"Observation",
	   "id":"0",
	   "category":[......```
	- Use pandas to read csv files and iterate the df by rows, filling the template with data, then append it to ndjson file 

2. For NLP classifier:
	- read "ENCOUNTER_NUM" from cohort.csv and pass it into classifier server (BCH VPN required)
	- Get result from the server and update the classifier template
	
## ctakes_v2temp.py
This is an edited version of NLP_to_FHIR (https://github.com/bmao1/NLP_to_FHIR) to run against the clinical notes resides in cohort.csv from BCH i2b2
To reduce the data size for CDC demo while preserve the patient symptoms, only the "SignSymptomMention" is captured in this process.
(Re-running the code requires cTAKES containerized server https://github.com/Machine-Learning-for-Medical-Language/ctakes-covid-container)

## ctakes_raw.py
This code has been used to support the team to investigate the raw output from cTAKES for BCH clinical notes. It helps to explore the possible use cases for cTAKES with real data feed, but it's not being used in the CDC demo.
- Notes are extracted from cohort.csv and passed to cTAKES. The raw output is located in s3://s3-for-athena-bintest2/data/nlp/bch_ctakes
- A glue crawler is then used to create external table in Athena with inferred schema. https://us-east-2.console.aws.amazon.com/glue/home?region=us-east-2#crawler:name=bch_ctakes_raw

## cpt4.csv (not in use)
Exploratory: occurrence of procedure code in i2b2

# Output Data
The data is located in `s3://s3-for-athena-bintest2/transition/i2b2_to_fhir/data`
- "Condition.ndjson.gz": From BCH i2b2 Diagnosis
- "DocumentReference.ndjson.gz": DELETED due to PHI in notes. Find it on my laptop /Users/binmao/i2b2DB/bch_mapping/backup_0314/DocumentReference.ndjson.gz
- "Encounter.ndjson.gz": From i2b2 visit dimension. Not used in CDC demo
- "Observation_classifier_202204.ndjson.gz": The latest version of covid classifier
- "Observation_ctakes_all.ndjson.gz": Clinical notes to FHIR by cTAKES
- "Observation_i2b2.ndjson.gz": From BCH i2b2 PCR lab test
- "Patient.ndjson.gz": From BCH i2b2 patient
- "ctakes_raw_v2.ndjson.gz": All ctakes raw output with negation for exploratory analysis
- "encounter_mapping.json.gz": BCH ENCOUNTER_NUM to uuid mapping, need to re-produce the FHIR data in this folder
- "patient_mapping.json.gz": BCH PATIENT_NUM to uuid mapping, need to re-produce the FHIR data in this folder




