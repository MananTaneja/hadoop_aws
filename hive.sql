-- Create table and load data

create table food(
    adm0_id INT ,
    adm0_name VARCHAR(100),
    adm1_id INT,
    adm1_name VARCHAR(100),
    mkt_id INT,
    mkt_name VARCHAR(100),
    cm_id INT,
    cm_name VARCHAR(100),
    cur_id INT,
    cur_name VARCHAR(100),
    pt_id INT,
    pt_name VARCHAR(100),
    noofunits INT,
    um_name VARCHAR(100),
    mp_month INT,
    mp_year INT,
    mp_price INT,
    mp_commoditysource VARCHAR(100)
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/hduser/hadoop_aws/food.csv' INTO TABLE food;


-- Select query
select * from food;

-- Aggregatio Query
select count(adm0_id) from food where cm_name="Bread";
select count(adm0_id) from food where cm_name="Wheat";

-- Group by
select cm_name, count(adm0_id) from food group by cm_name;

-- subquery

select max(mp_price) from food where mp_year in(select max(mp_year) from food);