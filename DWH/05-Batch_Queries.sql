-- Query #1

with all_data as (
    select product_key , units from branch_sales_fact 
    union all 
    select product_key , units from online_sales_fact)
select name , sum(units) as total from product_dim pd inner join all_data ad 
on pd.product_key = ad.product_key group by name order by total desc

-- Query #2

with all_data as (
    select discount from branch_sales_fact 
    union all
    select discount from online_sales_fact)
select discount , count(*) as total from all_data group by discount order by total desc

-- Query #3

with all_data as (
    select product_key , discount from branch_sales_fact 
    union all
    select product_key , discount from online_sales_fact)
select name , discount , count(*) as total from product_dim pd inner join all_data ad 
on pd.product_key = ad.product_key group by name ,discount order by name ,total desc

-- Query #4

select city , sum(total_price) as total from online_sales_fact group by city order by total asc