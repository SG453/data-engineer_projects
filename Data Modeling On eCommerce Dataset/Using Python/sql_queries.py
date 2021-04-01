import configparser
# Importing AWS details
config = configparser.ConfigParser()
config.read('dwh.cfg')

DAILY_DATA = config.get('S3','MULTI_DATA')
ELECTRONICS_DATA = config.get('S3','ELECTRONICS_DATA')
JEWELRY_DATA = config.get('S3','JEWELRY_DATA')
KEY = config.get('AWS','KEY')
SECRET = config.get('AWS','SECRET')

# Staging tables create scripts

create_staging_multi = ("""
create table if not exists staging_shopping_activity_multi
(event_time varchar(100) not null,
event_type varchar(50),
product_id numeric(25,0) not null,
category_id numeric(25,0),
category_code varchar(1000),
brand varchar(100),
price numeric(10,2),
user_id numeric(25,0),
user_session varchar(5000))
""")

create_staging_electronics=("""
create table if not exists staging_shopping_activity_electronics
(event_time varchar(100) not null,
order_id numeric(25,0),
product_id numeric(25,0) not null,
category_id numeric(25,0),
category_code varchar(1000),
brand varchar(100),
price numeric(10,2),
user_id numeric(25,0))
""")

create_staging_jewelry=("""
create table if not exists staging_shopping_activity_jewelry
(event_time varchar(100) not null,
order_id numeric(25,0),
product_id numeric(25,0) not null,
quantity smallint,
category_id numeric(25,0),
category_code varchar(1000),
brand varchar(100),
price numeric(10,2),
user_id numeric(25,0),
gender char(1),
color varchar(30),
metal varchar(100),
gem varchar(100))
""")

# Dimension tables create scripts

create_dimProduct=("""
create table if not exists dim_product
(product_id int identity(1,1) sortkey distkey ,
category_code varchar(1000),
category varchar(100),
sub_cat1 varchar(100),
sub_cat2 varchar(100))
""")

create_dimBrand = ("""
create table if not exists dim_brand
(brand_id int identity(1,1),
brand_name varchar(100))diststyle all;
""")


create_dimTime = ("""
create table if not exists dim_time
(ts1 numeric(14,0) not null sortkey, 
ts timestamp not null,
minutes smallint, 
hour smallint, 
day smallint,
week smallint, 
isWeekDay boolean, 
month smallint,
quarter smallint,
year smallint distkey)
""")

create_factShoppingActivity = ("""
create table if not exists fact_shoppingActivity(
f_id int identity(1,1) not null sortkey,
ts numeric(14,0) not null, 
event_type varchar(30) default('purchased') distkey,
product_id varchar(30) not null,
brand_id varchar(10) not null,
price numeric(10,2) not null,
user_id varchar(30) not null, 
user_session varchar(100))
""")

# Copy scripts to load data from S3 to redshift
load_staging_data_multi = ("""
copy staging_shopping_activity_multi 
from {}
access_key_id '{}'
secret_access_key '{}'
CSV
DELIMITER ','
FILLRECORD
IGNOREHEADER 1
""").format(DAILY_DATA,KEY,SECRET)

load_staging_data_electronics = ("""
copy staging_shopping_activity_electronics 
from {}
access_key_id '{}'
secret_access_key '{}'
CSV
DELIMITER ','
FILLRECORD
IGNOREHEADER 1
""").format(ELECTRONICS_DATA,KEY,SECRET)

load_staging_data_jewelry = ("""
copy staging_shopping_activity_jewelry 
from {}
access_key_id '{}'
secret_access_key '{}'
CSV
DELIMITER ','
FILLRECORD
IGNOREHEADER 1
""").format(JEWELRY_DATA,KEY,SECRET)


# Insert dummy records into dimension tables
insert_dummy_dimProduct = """
insert into dim_Product (category_code,category,sub_cat1,sub_cat2)
select t1.*
from (
select 'Unknown' as Category_code,'Unknown' as Category,'NA' as sub_cat1,'NA' as sub_cat2 ) as t1
left join dim_Product t2
on t1.category_code = t2.category_code and t1.category = t2.category
where t2.category_code is null
"""

insert_dummy_brand = """
insert into dim_Brand(brand_name)
select t1.*
from (
select 'Unknown' as brand_name) as t1
left join dim_brand t2 
on t1.brand_name = t2.brand_name
where t2.brand_name is null
"""

# Insert scripts to load data into dimension tables

load_dimProduct = """
insert into dim_Product(category_code,category,sub_cat1,sub_cat2)
select t1.*
from (
      
      select case when category_code='' then 'Unknown' else category_code end as category_code
        , case when category_code = '' then 'Unknown' else split_part(category_code,'.',1) end as category
        , case when category_code != '' then split_part(category_code,'.',2) else 'NA' end as sub1
        , case when category_code != '' then split_part(category_code,'.',3) else 'NA' end as sub2
        from staging_shopping_activity_multi
      union      
      select case when category_code='' then 'Unknown' else category_code end as category_code
      , case when category_code = '' then 'Unknown' else split_part(category_code,'.',1) end as category
      , case when category_code != '' then split_part(category_code,'.',2) else 'NA' end as sub1
      , case when category_code != '' then split_part(category_code,'.',3) else 'NA' end as sub2
      from staging_shopping_activity_electronics
      union 
      select case when category_code='' then 'Unknown' else category_code end as category_code
      , case when category_code = '' then 'Unknown' else split_part(category_code,'.',1) end as category
      , case when category_code != '' then split_part(category_code,'.',2) else 'NA' end as sub1
      , case when category_code != '' then split_part(category_code,'.',3) else 'NA' end as sub2
      from staging_shopping_activity_jewelry
  ) as t1
left join dim_product t2 
on t1.category_code = t2.category_code
where t2.category_code isnull
"""

load_dimBrand = """
insert into dim_brand(brand_name)
select t1.brand
from (select case when brand = '' then 'Unknown' else brand end as brand
      from staging_shopping_activity_multi
      group by brand
	  
	  union 
	  select case when brand = '' then 'Unknown' else brand end as brand
      from staging_shopping_activity_electronics
      group by brand
      
      union 
      select case when brand = '' then 'Unknown' else brand end as brand
      from staging_shopping_activity_jewelry
      group by brand
      
      ) as t1
left join dim_brand t2 
on t1.brand = t2.brand_name
where t2.brand_name isnull
"""

load_dimTime = """
insert into dim_time(ts1,ts,minutes,hour,day,week,isweekday,month,quarter,year)
select B.ts1
, B.ts
, extract(minute from B.ts) as minute
, extract(hour from B.ts) as hour
, extract(day from B.ts) as day
, extract(week from B.ts) as week
, case when extract(dow from B.ts) in (6,7) then False else True end as isWeekday
, extract(month from B.ts) as month
, extract(quarter from B.ts) as quarter
, extract(year from B.ts) as year
from (
        select replace(replace(replace(replace(event_time,' UTC','')::TIMESTAMP(0),'-',''),':',''),' ','') as ts1,
        (replace(event_time,' UTC','')::TIMESTAMP(0)) as ts
        from (
                select event_time from staging_shopping_activity_multi
				union 
				select event_time from staging_shopping_activity_electronics
                union
                select event_time from staging_shopping_activity_jewelry                
        ) as A
) as B

left join dim_time t
on B.ts1::numeric(14,0) = t.ts1
where t.ts1 isnull
"""


load_factData = """
insert into fact_shoppingactivity(ts,event_type,product_id,brand_id,price,user_id,user_session)
select
t1.ts1
, t1.event_type
, t2.product_id
, t3.brand_id
, t1.price
, t1.user_id
, t1.user_session
from (
        select replace(replace(replace(replace(event_time,' UTC','')::TIMESTAMP(0),'-',''),':',''),' ','')::numeric(14,0) as ts1
        , event_type
        , case when category_code = '' then 'Unknown' else category_code end as category_code
        , case when brand='' then 'Unknown' else brand end as brand
        , price
        , user_id
        , user_session
        from staging_shopping_activity_multi
		
		union 
		
		select replace(replace(replace(replace(event_time,' UTC','')::TIMESTAMP(0),'-',''),':',''),' ','')::numeric(14,0) as ts1
        , 'NA' as event_type
        , case when category_code = '' then 'Unknown' else category_code end as category_code
        , case when brand='' then 'Unknown' else brand end as brand
        , price
        , user_id
        , 'None' as user_session
        from staging_shopping_activity_electronics
    
        union
    
        select replace(replace(replace(replace(event_time,' UTC','')::TIMESTAMP(0),'-',''),':',''),' ','')::numeric(14,0) as ts1
        , 'NA' as event_type
        , case when category_code = '' then 'Unknown' else category_code end as category_code
        , case when brand='' then 'Unknown' else brand end as brand
        , price
        , user_id
        , 'None' as user_session
        from staging_shopping_activity_jewelry
) as t1
join dim_product t2 
on t1.category_code = t2.category_code
join dim_brand t3
on t1.brand = t3.brand_name
"""

dq_check1 = ("""
select count(1) from staging_shopping_activity_multi
""")

dq_check2 = ("""
select count(1) from staging_shopping_activity_electronics
""")

dq_check3 = ("""
select count(1) from staging_shopping_activity_jewelry
""")

dq_check4 = ("""
select count(cnt) as cnt from (
select count(1) as cnt from staging_shopping_activity_electronics
group by event_time,category_code,brand,price,user_id
union all
select count(1) as cnt from staging_shopping_activity_jewelry
group by event_time,category_code,brand,price,user_id
union all
select count(1) as cnt from staging_shopping_activity_multi
group by event_time,event_type,category_code,brand,price,user_id,user_session
) as A
""")




dq_check5 =("""
select
        count(1) as cnt
        from (
                select replace(replace(replace(replace(event_time,' UTC','')::TIMESTAMP(0),'-',''),':',''),' ','')::numeric(14,0) as ts1
                , event_type
                , case when category_code = '' then 'Unknown' else category_code end as category_code
                , case when brand='' then 'Unknown' else brand end as brand
                , price
                , user_id
                , user_session
                from staging_shopping_activity_multi

                union 

                select replace(replace(replace(replace(event_time,' UTC','')::TIMESTAMP(0),'-',''),':',''),' ','')::numeric(14,0) as ts1
                , 'NA' as event_type
                , case when category_code = '' then 'Unknown' else category_code end as category_code
                , case when brand='' then 'Unknown' else brand end as brand
                , price
                , user_id
                , 'None' as user_session
                from staging_shopping_activity_electronics

                union

                select replace(replace(replace(replace(event_time,' UTC','')::TIMESTAMP(0),'-',''),':',''),' ','')::numeric(14,0) as ts1
                , 'NA' as event_type
                , case when category_code = '' then 'Unknown' else category_code end as category_code
                , case when brand='' then 'Unknown' else brand end as brand
                , price
                , user_id
                , 'None' as user_session
                from staging_shopping_activity_jewelry
        ) as t1
        join dim_product t2 
        on t1.category_code = t2.category_code
        join dim_brand t3
        on t1.brand = t3.brand_name
        """)
delete_from_staging_multi="""
delete from staging_shopping_activity_multi
where user_id is null or price is null
"""

delete_from_staging_electronics="""
delete from staging_shopping_activity_electronics
where user_id is null or price is null
"""

delete_from_staging_jewelry="""
delete from staging_shopping_activity_jewelry
where user_id is null or price is null
"""

# Query lists
create_table_queries = [create_staging_multi,create_staging_electronics,create_staging_jewelry,create_dimProduct,create_dimBrand,create_dimTime,create_factShoppingActivity]
load_staging_queries = [load_staging_data_multi,load_staging_data_electronics,load_staging_data_jewelry]
load_all_queries = [insert_dummy_dimProduct,insert_dummy_brand,load_dimProduct,load_dimBrand,load_dimTime,load_factData]
dq_check_queries = [dq_check1,dq_check2,dq_check3,dq_check4, dq_check5]
data_cleanup_queries = [delete_from_staging_multi,delete_from_staging_electronics,delete_from_staging_jewelry]

