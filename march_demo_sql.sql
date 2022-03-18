-- Query 1 covid count

with classifier as (
SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id 
    , effectiveDatetime as enct_date
    , case when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '260385009' then 'Negative'
           when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '10828004' then 'Positive'
           else null end as covid_classifier
FROM "delta"."observation", unnest(modifierExtension) t(modext), unnest(valueCodeableConcept.coding) t(valuecode)
where modext.url = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-clissifier' and modext.valueInteger = 1
),

lab_test as (SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id
    , effectiveDatetime as enct_date
    , case when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '260385009' then 'Negative'
           when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '10828004' then 'Positive'
           else 'Unknown' end as covid_lab_test
FROM "delta"."observation"
    , unnest(valueCodeableConcept.coding) t(valuecode)
    , unnest(code.coding) t(codecoding)
    , unnest(category) t(categ)
    , unnest(categ.coding) t(categcode)
where modifierExtension is null 
    and codecoding.code in ('94500-6','95406-5')
    and categcode.code = 'laboratory'
),

patient_demo as (SELECT DISTINCT
case when gender is null then 'Unknown' else gender end as gender
, "date"("concat"(birthdate, '-01-01')) dob
, ("year"("now"()) - CAST(birthdate AS int)) age
, (CASE WHEN (ext.valuecoding.code IN ('1002-5', '2028-9', '2054-5', '2076-8', '2106-3')) THEN ext.valuecoding.display_aa ELSE null END) race
, addr.postalcode postalcode
, id subject_id
FROM
  patient
, UNNEST(extension) t (ext)
, UNNEST(address) t (addr)
WHERE ((birthdate IS NOT NULL) AND (gender IS NOT NULL))
),

dx as (SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id 
    , onsetdatetime as dx_date
    , codecoding.code as dx_code
FROM "delta"."condition" , unnest(code.coding) t(codecoding)
where codecoding.code = 'U07.1'
), 

join1 as (
select 
    coalesce(l.encounter_id, c.encounter_id) as encounter_id
    , coalesce(l.subject_id, c.subject_id) as subject_id
    , substr(coalesce(l.enct_date, c.enct_date),1,7) as enct_date
    , l.covid_lab_test
    , c.covid_classifier
from 
    lab_test l
    full join 
    classifier c
    on l.encounter_id = c.encounter_id and l.subject_id = c.subject_id
), 

join2 as (select 
    coalesce(j.encounter_id, c.encounter_id) as encounter_id
    , coalesce(j.subject_id, c.subject_id) as subject_id
    , substr(coalesce(j.enct_date, c.dx_date),1,7) as enct_date
    , covid_lab_test
    , covid_classifier
    , dx_code
from 
    join1 j
    full join 
    dx c
    on j.encounter_id = c.encounter_id and j.subject_id = c.subject_id
),



combine as (
    select distinct
     j.encounter_id
    , enct_date
    , covid_lab_test
    , covid_classifier
    , dx_code
    , p.gender 
    , (CASE WHEN (p.age < 19) THEN '0-18' 
            WHEN (p.age BETWEEN 19 AND 44) THEN '19-44' 
            WHEN (p.age BETWEEN 45 AND 64) THEN '45-64' 
            WHEN (p.age BETWEEN 65 AND 84) THEN '65-84' 
            WHEN (p.age BETWEEN 85 AND 300) THEN '85+' ELSE '?' END) as age_group
    , p.race
from 
    join2 j
    left join 
    patient_demo p
    on j.subject_id = p.subject_id
)

select enct_date
    , covid_lab_test
    , covid_classifier
    , dx_code
    , gender
    , age_group
    , race
    
    , count(distinct encounter_id) as cnt
from combine
where enct_date like '2021%'
group by cube (enct_date, covid_lab_test, covid_classifier, dx_code, gender, age_group, race)














----------------------------------------------------------------------------------------------


-- Query 2 symptom for covid patient long vs. long format

with symptom as (SELECT subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id
    , codecoding.code as code
    , codecoding.system as code_system
    , codecoding.display_aa as display
    , modext.valuedate as clin_date
FROM "delta"."observation"
    , unnest(modifierExtension) t(modext)
    , unnest(code.coding) t(codecoding)
where modext.url = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-algorithm/dateofauthorship'
    and codecoding.system = 'http://snomed.info/sct'
    and codecoding.code in ('68235000','64531003','49727002','11833005','28743005','62315008','84229001','367391008','43724002','103001002','426000000','25064002','21522001','29857009','57676002','68962001','422587007','422400008','44169009','36955009','267036007','162397003')
-- limit 5
),

-- positive covid lab test
lab_test as (SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id
    , effectiveDatetime as enct_date
FROM "delta"."observation"
    , unnest(valueCodeableConcept.coding) t(valuecode)
    , unnest(code.coding) t(codecoding)
    , unnest(category) t(categ)
    , unnest(categ.coding) t(categcode)
where modifierExtension is null 
    and codecoding.code in ('94500-6','95406-5')
    and categcode.code = 'laboratory'
    and valuecode.system = 'http://snomed.info/sct' and valuecode.code = '10828004'
),

combine as (
select a.encounter_id
    , a.display
    , a.code
    , substr(coalesce(a.clin_date, b.enct_date),1,7) as enct_date
from symptom a
    join lab_test b
    on a.encounter_id = b.encounter_id
        and a.subject_id = b.subject_id
)

/*
select distinct display
    , enct_date
    , count(distinct encounter_id) as cnt
from combine
group by cube (enct_date, display)
*/



select enct_date
    , "LOSS OF TASTE"
    , "DIFFICULTY BREATHING"
    , "JOINT PAIN"
    , "SORE THROAT"
    , "FATIGUED"
    , count(distinct encounter_id) as cnt
from (
    select enct_date
    , encounter_id
    , case when display = 'LOSS OF TASTE' then 'Y' else 'N' end as "LOSS OF TASTE"
    , case when display = 'DIFFICULTY BREATHING' then 'Y' else 'N' end as "DIFFICULTY BREATHING"
    , case when display = 'JOINT PAIN' then 'Y' else 'N' end as "JOINT PAIN"
    , case when display = 'SORE THROAT' then 'Y' else 'N' end as "SORE THROAT"
    , case when display = 'FATIGUED' then 'Y' else 'N' end as "FATIGUED"
    from combine
    )
group by cube (enct_date, "LOSS OF TASTE", "DIFFICULTY BREATHING", "JOINT PAIN", "SORE THROAT", "FATIGUED")








