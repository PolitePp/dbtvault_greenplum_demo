# 1. Create Greenplum cluster on Yandex.Cloud

**[Data Warehouse Analyst – Analytics Engineer @ OTUS.ru](https://otus.ru/lessons/dwh/)**

Request access to Manged Greenplum Service (beta), than create a cluster.

Configuration I have used:

![](https://habrastorage.org/webt/hu/tf/6s/hutf6sstdjvtxgujcmhchocjpws.png)


# 2. Generate data with DBGen

Spin up a virtual machine, install libs, generate data.

Or just get what I have prepared for you from `s3://otus-dwh/tpch-dbgen/` (Yandex Object Storage)

```bash

ssh -i ~/.ssh/key dbgen@{ip}  # ssh to VM

sudo apt-get install -y gcc git awscli postgresql # install libs

git clone https://github.com/electrum/tpch-dbgen.git # TPCH generator
make makefile.suite

./dbgen -v -h -s 10 # generate data

for i in `ls *.tbl`; do sed 's/|$//' $i > ${i/tbl/csv}; echo $i; done; # convert to a CSV format compatible with PostgreSQL

aws configure # to sync with S3

aws --endpoint-url=https://storage.yandexcloud.net s3 sync . s3://otus-dwh/tpch-dbgen/ \
	--exclude=* \
	--include=*.csv \
	--acl=public-read \
	--dryrun

```

Read more at:
- https://github.com/RunningJon/TPC-H
- https://github.com/wangguoke/blog/blob/master/How%20to%20use%20the%20pg_tpch.md


# 3. COPY to database

First create table definitions.

Then load data into it.


```sql
-- DDL scripts to create table

CREATE TABLE customer
(C_CUSTKEY INT, 
C_NAME VARCHAR(25),
C_ADDRESS VARCHAR(40),
C_NATIONKEY INTEGER,
C_PHONE CHAR(15),
C_ACCTBAL DECIMAL(15,2),
C_MKTSEGMENT CHAR(10),
C_COMMENT VARCHAR(117))
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (C_CUSTKEY);

CREATE TABLE lineitem
(L_ORDERKEY BIGINT,
L_PARTKEY INT,
L_SUPPKEY INT,
L_LINENUMBER INTEGER,
L_QUANTITY DECIMAL(15,2),
L_EXTENDEDPRICE DECIMAL(15,2),
L_DISCOUNT DECIMAL(15,2),
L_TAX DECIMAL(15,2),
L_RETURNFLAG CHAR(1),
L_LINESTATUS CHAR(1),
L_SHIPDATE DATE,
L_COMMITDATE DATE,
L_RECEIPTDATE DATE,
L_SHIPINSTRUCT CHAR(25),
L_SHIPMODE CHAR(10),
L_COMMENT VARCHAR(44))
WITH (appendonly=true, orientation=column, compresstype=ZSTD)
DISTRIBUTED BY (L_ORDERKEY,L_LINENUMBER)
PARTITION BY RANGE (L_SHIPDATE)
(start('1992-01-01') INCLUSIVE end ('1998-12-31') INCLUSIVE every (30),
default partition others);

CREATE TABLE nation
(N_NATIONKEY INTEGER, 
N_NAME CHAR(25), 
N_REGIONKEY INTEGER, 
N_COMMENT VARCHAR(152))
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (N_NATIONKEY);

CREATE TABLE orders
(O_ORDERKEY BIGINT,
O_CUSTKEY INT,
O_ORDERSTATUS CHAR(1),
O_TOTALPRICE DECIMAL(15,2),
O_ORDERDATE DATE,
O_ORDERPRIORITY CHAR(15), 
O_CLERK  CHAR(15), 
O_SHIPPRIORITY INTEGER,
O_COMMENT VARCHAR(79))
WITH (appendonly=true, orientation=column, compresstype=ZSTD)
DISTRIBUTED BY (O_ORDERKEY)
PARTITION BY RANGE (O_ORDERDATE)
(start('1992-01-01') INCLUSIVE end ('1998-12-31') INCLUSIVE every (30),
default partition others);

CREATE TABLE part
(P_PARTKEY INT,
P_NAME VARCHAR(55),
P_MFGR CHAR(25),
P_BRAND CHAR(10),
P_TYPE VARCHAR(25),
P_SIZE INTEGER,
P_CONTAINER CHAR(10),
P_RETAILPRICE DECIMAL(15,2),
P_COMMENT VARCHAR(23))
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (P_PARTKEY);

CREATE TABLE partsupp
(PS_PARTKEY INT,
PS_SUPPKEY INT,
PS_AVAILQTY INTEGER,
PS_SUPPLYCOST DECIMAL(15,2),
PS_COMMENT VARCHAR(199))
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (PS_PARTKEY,PS_SUPPKEY);

CREATE TABLE region
(R_REGIONKEY INTEGER, 
R_NAME CHAR(25),
R_COMMENT VARCHAR(152))
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (R_REGIONKEY);

CREATE TABLE supplier 
(S_SUPPKEY INT,
S_NAME CHAR(25),
S_ADDRESS VARCHAR(40),
S_NATIONKEY INTEGER,
S_PHONE CHAR(15),
S_ACCTBAL DECIMAL(15,2),
S_COMMENT VARCHAR(101))
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (S_SUPPKEY);
```

On a VM with installed psql execute COPY pointing to local csv files:

```bash
export GREENPLUM_URI="postgres://greenplum:<pass>@<host>:5432/postgres"
psql $GREENPLUM_URI

\copy customer from  '/home/dbgen/tpch-dbgen/data/customer.csv' WITH (FORMAT csv, DELIMITER '|');
\copy lineitem from  '/home/dbgen/tpch-dbgen/data/lineitem.csv' WITH (FORMAT csv, DELIMITER '|');
\copy nation from  '/home/dbgen/tpch-dbgen/data/nation.csv' WITH (FORMAT csv, DELIMITER '|');
\copy orders from  '/home/dbgen/tpch-dbgen/data/orders.csv' WITH (FORMAT csv, DELIMITER '|');
\copy part from  '/home/dbgen/tpch-dbgen/data/part.csv' WITH (FORMAT csv, DELIMITER '|');
\copy partsupp from  '/home/dbgen/tpch-dbgen/data/partsupp.csv' WITH (FORMAT csv, DELIMITER '|');
\copy region from  '/home/dbgen/tpch-dbgen/data/region.csv' WITH (FORMAT csv, DELIMITER '|');
\copy supplier from  '/home/dbgen/tpch-dbgen/data/supplier.csv' WITH (FORMAT csv, DELIMITER '|');
```

# 4. Run dbtVault + Greenplum demo 

**1. First read the official guide:**

[dbtVault worked example](https://dbtvault.readthedocs.io/en/latest/worked_example/we_worked_example/)

**2. Clone repo with dbt project**

Clone demo repo: https://github.com/kzzzr/dbtvault_greenplum_demo

```bash
git clone https://github.com/kzzzr/dbtvault_greenplum_demo.git
```

**3. Configure database connection**

Example `profiles.yml`

```yaml
config:
  send_anonymous_usage_stats: False
  use_colors: True
  partial_parse: True

dbtvault_greenplum_demo:
  outputs:
    dev:
      type: postgres
      threads: 2
      host: {yc-greenplum-host}
      port: 5432
      user: greenplum
      pass: {yc-greenplum-pass}
      dbname: postgres
      schema: public
  target: dev

```

**4. Make sure you run on `dbt==0.19.0`**

You may use repo's Pipfile with pipenv or install dbt yourself

```bash
pipenv install
pipenv shell

dbt debug # check if OK
```

**5. Install dependencies**

Initial repo is intended to run on Snowflake only.

I have forked it and adapted to run on Greenplum/PostgreSQL.
Check out what has been changed: [47e0261cea67c3284ea409c86dacdc31b1175a39](https://github.com/kzzzr/dbtvault/tree/47e0261cea67c3284ea409c86dacdc31b1175a39)

`packages.yml`:

```yaml
packages:
  # - package: Datavault-UK/dbtvault
  #   version: 0.7.3
  - git: "https://github.com/kzzzr/dbtvault.git"
    revision: master
    warn-unpinned: false
```

Install package:

```bash
dbt deps

```

**6. Adapt models to Greenplum/PostgreSQL**

Check out the [commit history](https://github.com/kzzzr/dbtvault_greenplum_demo/commits/master).

* [a97a224](https://github.com/kzzzr/dbtvault_greenplum_demo/commit/a97a22431a182e59c9cb8be807200f0292672b0f) - adapt prepared staging layer for greenplum - Artemiy Kozyr (HEAD -> master, kzzzr/master)
* [dfc5866](https://github.com/kzzzr/dbtvault_greenplum_demo/commit/dfc5866a63e81393f5bfc0b163cc84b56efc6354) - configure raw layer for greenplum - Artemiy Kozyr
* [bba7437](https://github.com/kzzzr/dbtvault_greenplum_demo/commit/bba7437a7d29fd5dd9c383bff49c4604fc84d2ab) - configure data sources for greenplum - Artemiy Kozyr
* [aa25600](https://github.com/kzzzr/dbtvault_greenplum_demo/commit/aa2560071b27b2e7f6de924222b7d465e28d8af2) - configure package (adapted dbt_vault) for greenplum - Artemiy Kozyr
* [eafed95](https://github.com/kzzzr/dbtvault_greenplum_demo/commit/eafed95ad5b912daf9339d877dfa0ee246bd089f) - configure dbt_project.yml for greenplum - Artemiy Kozyr


**7. Run models step-by-step**

Load one day to Data Vault structures:

```bash
dbt run -m tag:raw
dbt run -m tag:stage
dbt run -m tag:hub
dbt run -m tag:link
dbt run -m tag:satellite
dbt run -m tag:t_link
```

**8. Load next day**

Simulate next day load by incrementing `load_date` varible:

```yaml dbt_profiles.yml
# dbt_profiles.yml

vars:
  load_date: '1992-01-08' # increment by one day '1992-01-09'

```

# HOMEWORK

## 1. Prepare CLI command to create Greenplum cluster

Сделал через веб интерфейс

## 2. Load 4-5 days to Data Vault.

Загрузил с 1992-01-08 по 1992-01-11
Для stage сделал table материализацию, т.к. объекты переиспользуются при создании дата волта

## 3. Prepare Point-in-Time & Bridge Tables

Create models that combine Hubs + Satellites, Hubs + Links.
Choose how to materialize it.

Как я должен быть сделать pit и bridge, если они появились только в 0.7.6 версии? 
Прикрепил модели, но они не работают. Попробовал реализовать pit и bridge макросы, что, конечно же, не получилось. 

**(?) Now run a couple of queries on top of your models:**

```sql
-- Q1
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
```

| l\_returnflag | l\_linestatus | sum\_qty | sum\_base\_price | sum\_disc\_price | sum\_charge | avg\_qty | avg\_price | avg\_disc | count\_order |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| A | F | 377518399 | 566065727797.25 | 537759104278.0656 | 559276670892.116819 | 25.5009751030070973 | 38237.151008958546 | 0.05000657454024320463 | 14804077 |
| N | F | 9851614 | 14767438399.17 | 14028805792.2114 | 14590490998.366737 | 25.5224483028409474 | 38257.81066008114 | 0.0499733677376566718 | 385998 |
| N | O | 743124873 | 1114302286901.88 | 1058580922144.9638 | 1100937000170.591854 | 25.4980758706893147 | 38233.90292348181 | 0.05000081182113130603 | 29144351 |
| R | F | 377732830 | 566431054976 | 538110922664.7677 | 559634780885.086257 | 25.5083847896801383 | 38251.219273559761 | 0.04999679231408742045 | 14808183 |


```sql
-- Q2
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < date '1995-03-15'
    AND l_shipdate > date '1995-03-15'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue desc,
    o_orderdate
LIMIT 20;
```

| l\_orderkey | revenue | o\_orderdate | o\_shippriority |
| :--- | :--- | :--- | :--- |
| 4791171 | 440715.2185 | 1995-02-23 | 0 |
| 46678469 | 439855.325 | 1995-01-27 | 0 |
| 23906758 | 432728.5737 | 1995-03-14 | 0 |
| 23861382 | 428739.1368 | 1995-03-09 | 0 |
| 59393639 | 426036.0662 | 1995-02-12 | 0 |
| 3355202 | 425100.6657 | 1995-03-04 | 0 |
| 9806272 | 425088.0568 | 1995-03-13 | 0 |
| 22810436 | 423231.969 | 1995-01-02 | 0 |
| 16384100 | 421478.7294 | 1995-03-02 | 0 |
| 52974151 | 415367.1195 | 1995-02-05 | 0 |
| 3778628 | 411836.2827 | 1995-02-25 | 0 |
| 21353479 | 410325.6287 | 1995-03-04 | 0 |
| 20524164 | 409472.0867 | 1995-03-04 | 0 |
| 33059171 | 409156.4696 | 1995-02-16 | 0 |
| 8207586 | 407736.967 | 1995-03-04 | 0 |
| 9365575 | 406258.5739 | 1995-03-12 | 0 |
| 9874305 | 404121.8245 | 1995-01-19 | 0 |
| 4860004 | 401782.3025 | 1995-02-22 | 0 |
| 45512673 | 400541.9454 | 1995-02-08 | 0 |
| 53144198 | 399667.5504 | 1995-02-17 | 0 |


```sql
-- Q3

SELECT
    100.00 * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= date '1995-09-01'
    AND l_shipdate < date '1995-09-01' + interval '1' month;
```

| promo\_revenue |
| :--- |
| 16.6475949416150953 |
