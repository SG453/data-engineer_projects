
create table if not exists staging_shopping_activity_multi
(event_time varchar(100) not null,
event_type varchar(50),
product_id numeric(25,0) not null,
category_id numeric(25,0),
category_code varchar(1000),
brand varchar(100),
price numeric(10,2),
user_id numeric(25,0),
user_session varchar(5000));

create table if not exists staging_shopping_activity_electronics
(event_time varchar(100) not null,
order_id numeric(25,0),
product_id numeric(25,0) not null,
category_id numeric(25,0),
category_code varchar(1000),
brand varchar(100),
price numeric(10,2),
user_id numeric(25,0));

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
gem varchar(100));


create table if not exists dim_product
(product_id int identity(1,1) sortkey distkey ,
category_code varchar(1000),
category varchar(100),
sub_cat1 varchar(100),
sub_cat2 varchar(100));


create table if not exists dim_brand
(brand_id int identity(1,1),
brand_name varchar(100))diststyle all;

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
year smallint distkey);

create table if not exists fact_shoppingActivity(
f_id int identity(1,1) not null sortkey,
ts numeric(14,0) not null, 
event_type varchar(30) default('purchased') distkey,
product_id varchar(30) not null,
brand_id varchar(10) not null,
price numeric(10,2) not null,
user_id varchar(30) not null, 
user_session varchar(100));
