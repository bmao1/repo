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
    , substr(coalesce(a.clin_date, b.enct_date),1,10) as enct_date
from symptom a
    join lab_test b
    on a.encounter_id = b.encounter_id
        and a.subject_id = b.subject_id
)

select enct_date
    , symptoms
    , count(distinct encounter_id) as cnt
from
    (select date(date_trunc('week', date_parse(enct_date,'%Y-%m-%d')))  as enct_date
        , encounter_id
        , case code when '28743005' then 'Cough'
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
                    end as symptoms
        from combine
    )
where symptoms is not null
group by cube (enct_date, symptoms)



/*
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
*/







---------  symptoms v4


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

combine as (
select distinct e.encounter_id
    , e.subject_id
    , date(date_trunc('week', date_parse(e.enct_date,'%Y-%m-%d')))  as enct_date 
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
                        else 'Asymptomatic' end as symptoms
from encounter e
    left join 
    symptom s
    on e.encounter_id = s.encounter_id
)

select enct_date
    , symptoms
    , count(distinct encounter_id) as cnt
from combine
where year(enct_date) = 2021
group by cube(enct_date, symptoms)





---------  symptoms v5



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

combine as (
select distinct e.encounter_id
    , e.subject_id
    , date(date_trunc('week', date_parse(e.enct_date,'%Y-%m-%d')))  as enct_date 
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
                        else 'Asymptomatic' end as symptoms
    , case when p.gender in ('male', 'female') then gender else 'Missing' end as gender
    , (CASE WHEN (p.age < 19) THEN '0-18' 
            WHEN (p.age BETWEEN 19 AND 44) THEN '19-44' 
            WHEN (p.age BETWEEN 45 AND 64) THEN '45-64' 
            WHEN (p.age BETWEEN 65 AND 84) THEN '65-84' 
            WHEN (p.age BETWEEN 85 AND 300) THEN '85+' ELSE 'Missing' END) as age_group
    , case when p.race is not null then p.race else 'Missing' end as race
    , case when p.postalcode is not null then p.postalcode else 'Missing' end as postalcode3dig
from encounter e
    left join 
    symptom s
    on e.encounter_id = s.encounter_id
    left join 
    fhir_patient p
    on e.subject_id = p.subject_id
    
)

select enct_date
    , symptoms
    , gender
    , age_group
    , race
    , postalcode3dig
    , count(distinct encounter_id) as cnt
from combine
where year(enct_date) = 2021
group by cube(enct_date, symptoms, gender, age_group, race, postalcode3dig)




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
                        else 'Asymptomatic' end as symptoms
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







































