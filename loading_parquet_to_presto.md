# loading delta/parquet to Presto demo

make sure data in "delta" sub-directory
start presto container
```
docker run -d \
  --shm-size 1G \
  -p 19999:19999 \
  -p 8080:8080 \
  -v `pwd`/delta:/usr/delta \
  --name alluxio-presto-sandbox \
  alluxio/alluxio-presto-sandbox
  ```

get into the container
`docker exec -it alluxio-presto-sandbox bash`
under FSStorage is a mounted local path for this deployment, copy data into it
`cp -R /usr/delta /opt/alluxio/underFSStorage`

start presto cli
`presto --catalog hive --debug`

create table 
`create table hive.alluxio.tablename (schema_here) with (format = 'parquet', external_location = 'alluxio:///delta/patient')`

test query
`select oneaddr.state from hive.alluxio.patient, UNNEST(address) as oneaddr`


 ** schema for FHIR should come from presto_schema_gen.py or https://github.com/bmao1/FHIR_ETL/blob/main/presto_schema_gen.py **


