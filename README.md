This fork includes minor improvements from the code of the original [repo](https://github.com/mmr12/MIMIC-III-sepsis-3-labels).

# MIMIC-III Sepsis-3 Labels
An SQL and Python based implementation of the extraction of sepsis III labels in the MIMIC-III dataset.

## Reproduce the labels
This code assumes you have access to the MIMIC-III dataset in Postgres format and all tables are located at `mimiciii` schema.

If you don't have yet built MIMIC-III dataset in a postgres database, you can just follow the [MIT Laboratory tutorial](https://github.com/MIT-LCP/mimic-code/tree/main/mimic-iii/buildmimic/postgres).

> [!IMPORTANT]
> We're using Postgres 16 in this project.

First make sure all the requirements in ```requirements.txt``` are met:
```pip install -r requirements.txt```

Then run:
`python make_labels.py -u SQLUSER -pw SQLPASS -host HOST -db DBNAME`

By default, `make_labels.py` uses the following parameters:

* Database name: `mimic`
* User name: `postgres`
* Password: `postgres`
* Schema: `mimiciii`
* Host: `localhost`
* Port: `5432`
                      
## Aknowledgements
If you find this code useful we would appreciate if you could cite our work.

