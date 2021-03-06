with classifier as (
SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id 
    , effectiveDatetime as enct_date
    , case when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '260385009' then 'Negative'
           when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '10828004' then 'Positive'
           else null end as covid_classifier
FROM "delta"."observation", unnest(modifierExtension) t(modext), unnest(valueCodeableConcept.coding) t(valuecode)
where modext.url = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-classifier' and modext.valueInteger = 1
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
    , onsetdatetime as enct_date
    , 'dx U07.1' as test_method
    , 'Unavailable' as covid_result
FROM "delta"."condition" , unnest(code.coding) t(codecoding)
where codecoding.code = 'U07.1'
), 

join1 as (
select 
    coalesce(l.encounter_id, c.encounter_id) as encounter_id
    , coalesce(l.subject_id, c.subject_id) as subject_id
    , substr(coalesce(l.enct_date, c.enct_date),1,10) as enct_date
    , case when covid_lab_test in ('Positive', 'Negative') then 'Lab_test'
           when covid_lab_test not in ('Positive', 'Negative')  and covid_classifier in ('Positive', 'Negative') then 'covid_classifier' 
           when covid_lab_test = 'Unknown' and covid_classifier not in ('Positive', 'Negative') then 'Lab_test'
           else 'Unknown' end as test_method 
    , case when covid_lab_test in ('Positive', 'Negative') then covid_lab_test
           when covid_lab_test not in ('Positive', 'Negative')  and covid_classifier in ('Positive', 'Negative') then covid_classifier 
           else 'Unavailable' end as covid_result
from 
    lab_test l
    full join 
    classifier c
    on l.encounter_id = c.encounter_id and l.subject_id = c.subject_id
    where covid_lab_test is not null 
        or covid_classifier is not null
),



combine as (
    select distinct
     j.encounter_id
    , date(date_trunc('week', date_parse(enct_date,'%Y-%m-%d')))  as enct_date
    , test_method
    , covid_result
    , case when p.gender is not null then gender else 'Unavailable' end as gender
    , (CASE WHEN (p.age < 19) THEN '0-18' 
            WHEN (p.age BETWEEN 19 AND 44) THEN '19-44' 
            WHEN (p.age BETWEEN 45 AND 64) THEN '45-64' 
            WHEN (p.age BETWEEN 65 AND 84) THEN '65-84' 
            WHEN (p.age BETWEEN 85 AND 300) THEN '85+' ELSE '?' END) as age_group
    , case when p.race is not null then race else 'Unavailable' end as race
from 
    (select * from join1 union all select * from dx ) j
    left join 
    patient_demo p
    on j.subject_id = p.subject_id
)

select enct_date
    , test_method
    , covid_result
    , gender
    , age_group
    , race
    , count(distinct encounter_id) as cnt
from combine
where year(enct_date) = 2021
group by cube (enct_date, test_method, covid_result, gender, age_group, race)






















----------------------------------------------------------------------------------------------

/*

---------  symptoms v6


with encounter as (
    select distinct enct.reference_id_aa as encounter_id
        , subject.reference_id_aa as subject_id
        , min(substr("date", 1, 10)) as enct_date
    from documentreference, unnest(context.encounter) t(enct)
    group by enct.reference_id_aa, subject.reference_id_aa
),


symptom as (SELECT subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id
    , codecoding.code as symptom_code
    , codecoding.system as symptom_code_system
    , codecoding.display_aa as display
    , modext.valuedate as clin_date
FROM "delta"."observation"
    , unnest(modifierExtension) t(modext)
    , unnest(code.coding) t(codecoding)
where modext.url = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity' and modext.valueinteger = 0
    and codecoding.system = 'http://snomed.info/sct'
    and codecoding.code in ('68235000','64531003','49727002','11833005','28743005','62315008','84229001','367391008','43724002','103001002','426000000','25064002','21522001','29857009','57676002','68962001','422587007','422400008','44169009','36955009','267036007','162397003')
-- limit 5
),

fhir_patient as (SELECT DISTINCT
    gender
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

dx as (SELECT distinct encounter.reference_id_aa as encounter_id 
    , subject.reference_id_aa as subject_id
    , onsetdatetime as enct_date
    , 'dx U07.1' as test_method
    , 'presumably positive' as covid_result
FROM "delta"."condition" , unnest(code.coding) t(codecoding)
where codecoding.code = 'U07.1'
), 

combine as (
select distinct e.encounter_id
    , e.subject_id
    , date(date_trunc('month', date_parse(e.enct_date,'%Y-%m-%d')))  as enct_date 
    , case symptom_code when '68235000' then 'Congestion or runny nose'
                        when '64531003' then 'Congestion or runny nose'
                        when '49727002' then 'Cough'
                        when '11833005' then 'Cough'
                        when '28743005' then 'Cough'
                        when '62315008' then 'Diarrhea'
                        when '84229001' then 'Fatigue'
                        when '367391008' then 'Fatigue'
                        when '43724002' then 'Fever or chills'
                        when '103001002' then 'Fever or chills'
                        when '426000000' then 'Fever or chills'
                        when '25064002' then 'Headache'
                        when '21522001' then 'Muscle or body aches'
                        when '29857009' then 'Muscle or body aches'
                        when '57676002' then 'Muscle or body aches'
                        when '68962001' then 'Muscle or body aches'
                        when '422587007' then 'Nausea or vomiting'
                        when '422400008' then 'Nausea or vomiting'
                        when '44169009' then 'New loss of taste or smell'
                        when '36955009' then 'New loss of taste or smell'
                        when '267036007' then 'Shortness of breath or difficulty breathing'
                        when '162397003' then 'Sore throat'
                        else 'Other Symptoms' end as symptoms
    , case when p.gender in ('male', 'female') then gender else 'Missing' end as gender
    , (CASE WHEN (p.age < 19) THEN '0-18' 
            WHEN (p.age BETWEEN 19 AND 44) THEN '19-44' 
            WHEN (p.age BETWEEN 45 AND 64) THEN '45-64' 
            WHEN (p.age BETWEEN 65 AND 84) THEN '65-84' 
            WHEN (p.age BETWEEN 85 AND 300) THEN '85+' ELSE 'Missing' END) as age_group
    , case when p.race is not null then p.race else 'Missing' end as race
    , case when p.postalcode is not null then p.postalcode else 'Missing' end as postalcode3dig
    , case when dx.covid_result is not null then dx.covid_result else 'Negative' end as covid_diagnosis
from encounter e
    left join 
    symptom s
    on e.encounter_id = s.encounter_id
    left join 
    fhir_patient p
    on e.subject_id = p.subject_id
    left join
    dx
    on e.encounter_id = dx.encounter_id
    
)

select enct_date
    , symptoms
    , gender
    , age_group
    , race
    -- , postalcode3dig
    , covid_diagnosis
    , count(distinct encounter_id) as cnt
from combine
where year(enct_date) = 2021
group by cube(enct_date, symptoms, gender, age_group, race, covid_diagnosis)






---------  symptoms v7



with encounter as (
    select distinct enct.reference_id_aa as encounter_id
        , subject.reference_id_aa as subject_id
        , min(substr("date", 1, 10)) as enct_date
    from documentreference, unnest(context.encounter) t(enct)
    group by enct.reference_id_aa, subject.reference_id_aa
),


symptom as (SELECT subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id
    , codecoding.code as symptom_code
    , codecoding.system as symptom_code_system
    , codecoding.display_aa as display
    , modext.valuedate as clin_date
FROM "delta"."observation"
    , unnest(modifierExtension) t(modext)
    , unnest(code.coding) t(codecoding)
where modext.url = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity' and modext.valueinteger = 0
    and codecoding.system = 'http://snomed.info/sct'
    and codecoding.code in ('68235000', '64531003', '49727002', '11833005', '28743005', '62315008', '84229001', '367391008', '43724002', '103001002', '426000000', '386661006', '25064002', '68962001', '422587007', '422400008', '44169009', '36955009', '267036007', '162397003')
-- limit 5
),

fhir_patient as (SELECT DISTINCT
    gender
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

dx as (SELECT distinct encounter.reference_id_aa as encounter_id 
    , subject.reference_id_aa as subject_id
    , onsetdatetime as enct_date
    , 'dx U07.1' as test_method
    , 'Positive' as covid_result
FROM "delta"."condition" , unnest(code.coding) t(codecoding)
where codecoding.code = 'U07.1'
), 

lab_test as (SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id
    , effectiveDatetime as enct_date
    , case when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '260385009' then 'Negative'
           when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '10828004' then 'Positive'
           end as covid_lab_test
FROM "delta"."observation"
    , unnest(valueCodeableConcept.coding) t(valuecode)
    , unnest(code.coding) t(codecoding)
    , unnest(category) t(categ)
    , unnest(categ.coding) t(categcode)
where modifierExtension is null 
    and codecoding.code in ('94500-6','95406-5')
    and categcode.code = 'laboratory'
),

classifier as (
SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id 
    , effectiveDatetime as enct_date
    , case when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '260385009' then 'Negative'
           when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '10828004' then 'Positive'
           end as covid_classifier
FROM "delta"."observation", unnest(modifierExtension) t(modext), unnest(valueCodeableConcept.coding) t(valuecode)
where modext.url = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-classifier' and modext.valueInteger = 1
),

combine as (
select distinct e.encounter_id
    , e.subject_id
    , date(date_trunc('month', date_parse(e.enct_date,'%Y-%m-%d')))  as enct_date 
    , case symptom_code when '68235000' then 'Congestion or runny nose'
                        when '64531003' then 'Congestion or runny nose'
                        when '49727002' then 'Cough'
                        when '11833005' then 'Cough'
                        when '28743005' then 'Cough'
                        when '62315008' then 'Diarrhea'
                        when '84229001' then 'Fatigue'
                        when '367391008' then 'Fatigue'
                        when '43724002' then 'Fever or chills'
                        when '103001002' then 'Fever or chills'
                        when '426000000' then 'Fever or chills'
                        when '25064002' then 'Headache'
                        when '21522001' then 'Muscle or body aches'
                        when '29857009' then 'Muscle or body aches'
                        when '57676002' then 'Muscle or body aches'
                        when '68962001' then 'Muscle or body aches'
                        when '422587007' then 'Nausea or vomiting'
                        when '422400008' then 'Nausea or vomiting'
                        when '44169009' then 'New loss of taste or smell'
                        when '36955009' then 'New loss of taste or smell'
                        when '267036007' then 'Shortness of breath or difficulty breathing'
                        when '162397003' then 'Sore throat'
                        else 'Other Symptoms' end as symptoms
    , case when p.gender in ('male', 'female') then gender else 'Missing' end as gender
    , (CASE WHEN (p.age < 19) THEN '0-18' 
            WHEN (p.age BETWEEN 19 AND 44) THEN '19-44' 
            WHEN (p.age BETWEEN 45 AND 64) THEN '45-64' 
            WHEN (p.age BETWEEN 65 AND 84) THEN '65-84' 
            WHEN (p.age BETWEEN 85 AND 300) THEN '85+' ELSE 'Missing' END) as age_group
    , case when p.race is not null then p.race else 'Missing' end as race
    , case when p.postalcode is not null then p.postalcode else 'Missing' end as postalcode3dig
    , case when dx.covid_result is not null then dx.covid_result else 'No covid diagnosis' end as covid_diagnosis
    , case when l.covid_lab_test is not null then l.covid_lab_test else 'No lab test' end as covid_lab_test
    , case when c.covid_classifier is not null then c.covid_classifier else 'No classifier' end as covid_classifier
    
from encounter e
    left join 
    symptom s
    on e.encounter_id = s.encounter_id
    left join 
    fhir_patient p
    on e.subject_id = p.subject_id
    left join
    dx
    on e.encounter_id = dx.encounter_id
    left join 
    lab_test l
    on e.encounter_id = l.encounter_id
    left join 
    classifier c
    on e.encounter_id = c.encounter_id
    
)

-- change in v7: count patients, not encounters
select enct_date
    , symptoms
    , gender
    , age_group
    , race
    -- , postalcode3dig
    , covid_diagnosis
    , covid_lab_test
    , covid_classifier
    , count(distinct subject_id) as cnt
from combine
where year(enct_date) between 2016 and 2022
group by cube(enct_date, symptoms, gender, age_group, race, covid_diagnosis, covid_lab_test, covid_classifier)

*/




-- symptom 7.2



with encounter as (
    select distinct enct.reference_id_aa as encounter_id
        , subject.reference_id_aa as subject_id
        , min(substr("date", 1, 10)) as enct_date
    from documentreference, unnest(context.encounter) t(enct)
    group by enct.reference_id_aa, subject.reference_id_aa
),


symptom as (SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id
    , codecoding.code as symptom_code
    , codecoding.system as symptom_code_system
    , codecoding.display_aa as display
FROM "delta"."observation"
    , unnest(modifierExtension) t(modext)
    , unnest(code.coding) t(codecoding)
where modext.url = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity' and modext.valueinteger = 0
    and codecoding.system = 'http://snomed.info/sct'
    and codecoding.code in ('68235000', '64531003', '49727002', '11833005', '28743005', '62315008', '84229001', '367391008', '43724002', '103001002', '426000000', '386661006', '25064002', '68962001', '422587007', '422400008', '44169009', '36955009', '267036007', '162397003')
-- limit 5
),

fhir_patient as (SELECT DISTINCT
    gender
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

dx as (SELECT distinct encounter.reference_id_aa as encounter_id 
    , subject.reference_id_aa as subject_id
    , onsetdatetime as enct_date
    , 'dx U07.1' as test_method
    , 'DX Positive' as covid_result
FROM "delta"."condition" , unnest(code.coding) t(codecoding)
where codecoding.code = 'U07.1'
), 

lab_test as (SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id
    , effectiveDatetime as enct_date
    , case when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '260385009' then 'PCR Negative'
           when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '10828004' then 'PCR Positive'
           end as covid_lab_test
FROM "delta"."observation"
    , unnest(valueCodeableConcept.coding) t(valuecode)
    , unnest(code.coding) t(codecoding)
    , unnest(category) t(categ)
    , unnest(categ.coding) t(categcode)
where modifierExtension is null 
    and codecoding.code in ('94500-6','95406-5')
    and categcode.code = 'laboratory'
),

classifier as (
SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id 
    , effectiveDatetime as enct_date
    , case when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '260385009' then 'NLP Negative'
           when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '10828004' then 'NLP Positive'
           end as covid_classifier
FROM "delta"."observation", unnest(modifierExtension) t(modext), unnest(valueCodeableConcept.coding) t(valuecode)
where modext.url = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-classifier' and modext.valueInteger = 1
),

combine as (
select distinct e.encounter_id
    , e.subject_id
    , date(date_trunc('month', date_parse(e.enct_date,'%Y-%m-%d')))  as enct_date 
    , case symptom_code when '68235000' then 'Congestion or runny nose'
                        when '64531003' then 'Congestion or runny nose'
                        when '49727002' then 'Cough'
                        when '11833005' then 'Cough'
                        when '28743005' then 'Cough'
                        when '62315008' then 'Diarrhea'
                        when '84229001' then 'Fatigue'
                        when '367391008' then 'Fatigue'
                        when '43724002' then 'Fever or chills'
                        when '103001002' then 'Fever or chills'
                        when '426000000' then 'Fever or chills'
                        when '386661006' then 'Fever or chills'
                        when '25064002' then 'Headache'
                        when '68962001' then 'Muscle or body aches'
                        when '422587007' then 'Nausea or vomiting'
                        when '422400008' then 'Nausea or vomiting'
                        when '44169009' then 'New loss of taste or smell'
                        when '36955009' then 'New loss of taste or smell'
                        when '267036007' then 'Shortness of breath or difficulty breathing'
                        when '162397003' then 'Sore throat'
                        else 'Other Symptoms' end as symptoms
    , case when p.gender in ('male', 'female') then gender else 'Missing' end as gender
    , (CASE WHEN (p.age < 19) THEN '0-18' 
            WHEN (p.age BETWEEN 19 AND 44) THEN '19-44' 
            WHEN (p.age BETWEEN 45 AND 64) THEN '45-64' 
            WHEN (p.age BETWEEN 65 AND 84) THEN '65-84' 
            WHEN (p.age BETWEEN 85 AND 300) THEN '85+' ELSE 'Missing' END) as age_group
    , case when p.race is not null then p.race else 'Missing' end as race
    , case when p.postalcode is not null then p.postalcode else 'Missing' end as postalcode3dig
    , case when dx.covid_result is not null then dx.covid_result else 'No covid diagnosis' end as covid_diagnosis
    , case when l.covid_lab_test is not null then l.covid_lab_test else 'No lab test' end as covid_lab_test
    , case when c.covid_classifier is not null then c.covid_classifier else 'No classifier' end as covid_classifier
    
from encounter e
    left join 
    symptom s
    on e.encounter_id = s.encounter_id
    left join 
    fhir_patient p
    on e.subject_id = p.subject_id
    left join
    dx
    on e.encounter_id = dx.encounter_id
    left join 
    lab_test l
    on e.encounter_id = l.encounter_id
    left join 
    classifier c
    on e.encounter_id = c.encounter_id
    
)

-- change in v7: count patients, not encounters
select enct_date
    , symptoms
    , gender
    , age_group
    , race
    , covid_diagnosis
    , covid_lab_test
    , covid_classifier
    , count(distinct subject_id) as cnt
from combine
where year(enct_date) >= 2016 and enct_date <= now()
group by cube(enct_date, symptoms, gender, age_group, race, covid_diagnosis, covid_lab_test, covid_classifier)






--symptom v8 ???negated less often than non-negated??? 
--symptom v8.1 use new classifier


with encounter as (
    select distinct enct.reference_id_aa as encounter_id
        , subject.reference_id_aa as subject_id
        , min(substr("date", 1, 10)) as enct_date
    from documentreference, unnest(context.encounter) t(enct)
    group by enct.reference_id_aa, subject.reference_id_aa
),


symptom as (select 
        encounter_id
        , symptoms
    from (SELECT  
            encounter.reference_id_aa as encounter_id
            , case codecoding.code when '68235000' then 'Congestion or runny nose'
                                when '64531003' then 'Congestion or runny nose'
                                when '49727002' then 'Cough'
                                when '11833005' then 'Cough'
                                when '28743005' then 'Cough'
                                when '62315008' then 'Diarrhea'
                                when '84229001' then 'Fatigue'
                                when '367391008' then 'Fatigue'
                                when '43724002' then 'Fever or chills'
                                when '103001002' then 'Fever or chills'
                                when '426000000' then 'Fever or chills'
                                when '386661006' then 'Fever or chills'
                                when '25064002' then 'Headache'
                                when '68962001' then 'Muscle or body aches'
                                when '422587007' then 'Nausea or vomiting'
                                when '422400008' then 'Nausea or vomiting'
                                when '44169009' then 'New loss of taste or smell'
                                when '36955009' then 'New loss of taste or smell'
                                when '267036007' then 'Shortness of breath or difficulty breathing'
                                when '162397003' then 'Sore throat'
                                else 'Other Symptoms' end as symptoms
            , case when modext.valueinteger = 0 then 1 
                    when modext.valueinteger = -1 then -1
                    end as negation
        FROM "delta"."observation"
            , unnest(modifierExtension) t(modext)
            , unnest(code.coding) t(codecoding)
        where modext.url = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity'
            and codecoding.system = 'http://snomed.info/sct'
            and codecoding.code in ('68235000', '64531003', '49727002', '11833005', '28743005', '62315008', '84229001', '367391008', '43724002', '103001002', '426000000', '386661006', '25064002', '68962001', '422587007', '422400008', '44169009', '36955009', '267036007', '162397003')
        )
    group by encounter_id, symptoms
    having sum(negation) >0
),

/*
-- symptom from cui code, same result as using snomed
symptom as (select 
        encounter_id
        , symptoms
    from (SELECT encounter.reference_id_aa as encounter_id
        , case codecoding.code when 'C0027424' then 'Congestion or runny nose'
                        when 'C1260880' then 'Congestion or runny nose'
                        when 'C0010200' then 'Cough'
                        when 'C0850149' then 'Cough'
                        when 'C0239134' then 'Cough'
                        when 'C0011991' then 'Diarrhea'
                        when 'C0015672' then 'Fatigue'
                        when 'C0231218' then 'Fatigue'
                        when 'C0085593' then 'Fever or chills'
                        when 'C0687681' then 'Fever or chills'
                        when 'C1959900' then 'Fever or chills'
                        when 'C0015967' then 'Fever or chills'
                        when 'C0018681' then 'Headache'
                        when 'C0231528' then 'Muscle or body aches'
                        when 'C0027497' then 'Nausea or vomiting'
                        when 'C0042963' then 'Nausea or vomiting'
                        when 'C0003126' then 'New loss of taste or smell'
                        when 'C2364111' then 'New loss of taste or smell'
                        when 'C0013404' then 'Shortness of breath or difficulty breathing'
                        when 'C0242429' then 'Sore throat'
                        end as symptoms
    , case when modext.url = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity' and modext.valueinteger = 0 then 1 
            when  modext.url = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity' and modext.valueinteger = -1 then -1
            end as negation
    , case when modext.valuedate is not null then modext.valuedate end as symp_date
FROM "delta"."observation"
    , unnest(modifierExtension) t(modext)
    , unnest(code.coding) t(codecoding)
where codecoding.code in ('C0027424','C1260880','C0010200','C0850149','C0239134','C0011991','C0015672','C0231218','C0085593','C0687681','C1959900','C0015967','C0018681','C0231528','C0027497','C0042963','C0003126','C2364111','C0013404','C0242429')
        )
group by encounter_id, symptoms
having sum(negation) >0
),
*/


fhir_patient as (SELECT DISTINCT
    gender
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

dx as (SELECT distinct encounter.reference_id_aa as encounter_id 
    , subject.reference_id_aa as subject_id
    , onsetdatetime as enct_date
    , 'dx U07.1' as test_method
    , 'DX Positive' as covid_result
FROM "delta"."condition" , unnest(code.coding) t(codecoding)
where codecoding.code = 'U07.1'
), 

lab_test as (SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id
    , effectiveDatetime as enct_date
    , case when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '260385009' then 'PCR Negative'
           when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '10828004' then 'PCR Positive'
           end as covid_lab_test
FROM "delta"."observation"
    , unnest(valueCodeableConcept.coding) t(valuecode)
    , unnest(code.coding) t(codecoding)
    , unnest(category) t(categ)
    , unnest(categ.coding) t(categcode)
where modifierExtension is null 
    and codecoding.code in ('94500-6','95406-5')
    and categcode.code = 'laboratory'
),

classifier as (
SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id 
    , effectiveDatetime as enct_date
    , case when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '260385009' then 'Negative'
           when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '10828004' then 'Positive'
           else null end as covid_classifier
FROM "delta"."observation", unnest(modifierExtension) t(modext), unnest(valueCodeableConcept.coding) t(valuecode)
where modext.url = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-classifier' and modext.valueString = 'Covid classifier 202204'
),

combine as (
select distinct e.encounter_id
    , e.subject_id
    , date(date_trunc('month', date_parse(e.enct_date,'%Y-%m-%d')))  as enct_date 
    , case when s.symptoms is null then 'No Covid Symptoms' else s.symptoms end as symptoms
    , case when p.gender in ('male', 'female') then gender else 'Missing' end as gender
    , (CASE WHEN (p.age < 19) THEN '0-18' 
            WHEN (p.age BETWEEN 19 AND 44) THEN '19-44' 
            WHEN (p.age BETWEEN 45 AND 64) THEN '45-64' 
            WHEN (p.age BETWEEN 65 AND 84) THEN '65-84' 
            WHEN (p.age BETWEEN 85 AND 300) THEN '85+' ELSE 'Missing' END) as age_group
    , case when p.race is not null then p.race else 'Missing' end as race
    , case when p.postalcode is not null then p.postalcode else 'Missing' end as postalcode3dig
    , case when dx.covid_result is not null then dx.covid_result else 'No covid diagnosis' end as covid_diagnosis
    , case when l.covid_lab_test is not null then l.covid_lab_test else 'No lab test' end as covid_lab_test
    , case when c.covid_classifier is not null then c.covid_classifier else 'No classifier' end as covid_classifier
    
from encounter e
    left join 
    symptom s
    on e.encounter_id = s.encounter_id
    left join 
    fhir_patient p
    on e.subject_id = p.subject_id
    left join
    dx
    on e.encounter_id = dx.encounter_id
    left join 
    lab_test l
    on e.encounter_id = l.encounter_id
    left join 
    classifier c
    on e.encounter_id = c.encounter_id
    
)

select enct_date
    , symptoms
    , gender
    , age_group
    , race
    , covid_diagnosis
    , covid_lab_test
    , covid_classifier
    , count(distinct subject_id) as cnt
from combine
where year(enct_date) >= 2016 and enct_date <= now()
group by cube(enct_date, symptoms, gender, age_group, race, covid_diagnosis, covid_lab_test, covid_classifier)







-- temporal events

with encounter as (
    select distinct enct.reference_id_aa as encounter_id
        , subject.reference_id_aa as subject_id
        , min(substr("date", 1, 10)) as enct_date
    from documentreference, unnest(context.encounter) t(enct)
    group by enct.reference_id_aa, subject.reference_id_aa
),


symptom as (SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id
    , codecoding.code as symptom_code
    , codecoding.system as symptom_code_system
    , codecoding.display_aa as display
FROM "delta"."observation"
    , unnest(modifierExtension) t(modext)
    , unnest(code.coding) t(codecoding)
where modext.url = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity' and modext.valueinteger = 0
    and codecoding.system = 'http://snomed.info/sct'
    and codecoding.code in ('68235000','64531003','49727002','11833005','28743005','62315008','84229001','367391008','43724002','103001002','426000000','25064002','21522001','29857009','57676002','68962001','422587007','422400008','44169009','36955009','267036007','162397003')
-- limit 5
),

fhir_patient as (SELECT DISTINCT
    gender
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

dx as (SELECT distinct encounter.reference_id_aa as encounter_id 
    , subject.reference_id_aa as subject_id
    , onsetdatetime as enct_date
    , 'dx U07.1' as test_method
    , 'DX Positive' as covid_result
FROM "delta"."condition" , unnest(code.coding) t(codecoding)
where codecoding.code = 'U07.1'
), 

lab_test as (SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id
    , effectiveDatetime as enct_date
    , case when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '260385009' then 'PCR Negative'
           when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '10828004' then 'PCR Positive'
           end as covid_lab_test
FROM "delta"."observation"
    , unnest(valueCodeableConcept.coding) t(valuecode)
    , unnest(code.coding) t(codecoding)
    , unnest(category) t(categ)
    , unnest(categ.coding) t(categcode)
where modifierExtension is null 
    and codecoding.code in ('94500-6','95406-5')
    and categcode.code = 'laboratory'
),

classifier as (
SELECT distinct subject.reference_id_aa as subject_id
    , encounter.reference_id_aa as encounter_id 
    , effectiveDatetime as enct_date
    , case when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '260385009' then 'NLP Negative'
           when valuecode.system = 'http://snomed.info/sct' and valuecode.code = '10828004' then 'NLP Positive'
           end as covid_classifier
FROM "delta"."observation", unnest(modifierExtension) t(modext), unnest(valueCodeableConcept.coding) t(valuecode)
where modext.url = 'http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-classifier' and modext.valueInteger = 1
),

combine as (
select distinct e.encounter_id
    , date_trunc('day', date_parse(e.enct_date,'%Y-%m-%d'))  as enct_date 
    , date_trunc('day', date_parse(substr(dx.enct_date,1,10),'%Y-%m-%d')) dx_date
    , date_trunc('day', date_parse(substr(l.enct_date,1,10),'%Y-%m-%d')) lab_date
  
from encounter e
    left join 
    symptom s
    on e.encounter_id = s.encounter_id
    left join 
    fhir_patient p
    on e.subject_id = p.subject_id
    left join
    dx
    on e.encounter_id = dx.encounter_id
    left join 
    lab_test l
    on e.encounter_id = l.encounter_id
    left join 
    classifier c
    on e.encounter_id = c.encounter_id
),

temp as (
    select encounter_id
        , case when dx_date is not null then date_diff('day', enct_date, dx_date) end as dx_delay
        , case when lab_date is not null then date_diff('day', enct_date, lab_date) end as lab_delay
    from combine
    where dx_date is not null or lab_date is not null
),

temp2 as (
select encounter_id
    , min(dx_delay) as min_dx_delay
    , max(dx_delay) as max_dx_delay
    , max(dx_delay) - min(dx_delay) as multi_dx_gap
    , min(lab_delay) as min_lab_delay
    , max(lab_delay) as max_lab_delay
    , max(lab_delay) - min(lab_delay) as multi_lab_gap
from temp
group by encounter_id
)

select min_lab_delay 
    , count(distinct encounter_id) as cnt
from temp2
group by min_lab_delay
order by min_lab_delay









