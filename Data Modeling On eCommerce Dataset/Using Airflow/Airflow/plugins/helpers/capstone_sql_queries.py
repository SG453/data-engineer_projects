class CapStoneSqlQueries:  
    load_dimProduct = ("""
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
                        """)
    
    load_dimBrand = ("""
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
                    """)
    
    load_dimTime = ("""
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
                    """)
    
    load_factData = ("""
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
                        """)
    dq_query1 = ("""
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
    dq_query2 =("""
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
    
    
    
    
    
    
    
    