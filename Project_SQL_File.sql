use project;

# Problem Statement:
# Assuming you are a data analyst/ scientist at Target, you have been assigned the task of analyzing the given dataset to extract 
# valuable insights and provide actionable recommendations.

# 1.	Import the dataset and do usual exploratory analysis steps like checking the structure & characteristics of the dataset:
# i).	Data type of all columns in the "customers" table.
desc customers;

# 2.	Get the time range between which the orders were placed.
select min(time(order_purchase_timestamp)) ,max(time(order_purchase_timestamp)) 
from orders;

# 3.	Count the Cities & States of customers who ordered during the given period.
select customer_city, count(customer_city) over(partition by customer_city), customer_state,
count(customer_state) over(partition by customer_state)
from customers c join orders o
on c.customer_id=o.customer_id;

select customer_city, customer_state, count(customer_city), count(customer_state)
from customers c join orders o
on c.customer_id=o.customer_id
group by 1,2;

# 2.	In-depth	Exploration:

# i).	Is there a growing trend in the no. of orders placed over the past years?

select year(order_purchase_timestamp) as orders_placed ,count(order_id)
from orders 
group by 1
order by orders_placed;

# Yes, there's a growing trend in the number of orders places over the past years.

# ii).	Can we see some kind of monthly seasonality in terms of the no. of orders being placed?

select year(order_purchase_timestamp), month(order_purchase_timestamp), count(order_id)
from orders
group by 1,2
order by year(order_purchase_timestamp), month(order_purchase_timestamp);

# iii).	During what time of the day, do the Brazilian customers mostly place their orders? (Dawn, Morning, Afternoon or Night)
# 	0-6 hrs : Dawn
# 	7-12 hrs : Mornings
# 	13-18 hrs : Afternoon
# 	19-23 hrs : Night

select timings,count(order_id)
from
(select *, 
case when hour(order_purchase_timestamp) between 0 and 6 then 'Dawn'
when hour(order_purchase_timestamp) between 7 and 12 then 'Mornings'
when hour(order_purchase_timestamp) between 13 and 18 then 'Afternoon'
else 'Night' end as Timings
from orders) t1
group by 1
order by count(order_id) desc;

# seems, brazalian customers mostly place their orders at Afternoon

# 3.	Evolution of E-commerce orders in the Brazil region:
# i).	Get the month-on-month no. of orders placed in each state.
select customer_state, month(order_purchase_timestamp), count(order_id)
from customers c join orders o
on c.customer_id=o.customer_id
group by 1,2
order by customer_state desc;

# ii).	How are the customers distributed across all the states?
select customer_state, count(customer_state) as counts
from customers
group by 1
order by counts desc; 

# 4.	Impact on Economy: Analyze the money movement by e-commerce by looking at order prices, freight and others.
# i).	Get the % increase in the cost of orders from year 2017 to 2018 (include months between Jan to Aug only).
#       You can use the "payment_value" column in the payments table to get the cost of orders.

SELECT 
    ((SUM(payment_value_2018) - SUM(payment_value_2017)) / SUM(payment_value_2017)) * 100 AS percentage_increase
FROM (
    SELECT 
        EXTRACT(YEAR FROM order_purchase_timestamp) AS year,
        SUM(CASE WHEN EXTRACT(YEAR FROM order_purchase_timestamp) = 2017 THEN payment_value ELSE 0 END) AS payment_value_2017,
        SUM(CASE WHEN EXTRACT(YEAR FROM order_purchase_timestamp) = 2018 THEN payment_value ELSE 0 END) AS payment_value_2018
    FROM 
        orders o join payments p on o.order_id=p.order_id
    WHERE 
        EXTRACT(YEAR FROM order_purchase_timestamp) IN (2017, 2018)
        AND EXTRACT(MONTH FROM order_purchase_timestamp) BETWEEN 1 AND 8
    GROUP BY 
        EXTRACT(YEAR FROM order_purchase_timestamp)
) AS year_payments;


# ii).	Calculate the Total & Average value of order price for each state.
select c.customer_state,sum(payment_value) over(partition by c.customer_state) as Total, Avg(payment_value) over(partition by c.customer_state) as Average
from payments p join orders o on p.order_id=o.order_id join customers c on c.customer_id=o.customer_id;

select c.customer_state, sum(payment_value) as Total, Avg(payment_value) as Average
from payments p join orders o on p.order_id=o.order_id join customers c on c.customer_id=o.customer_id
group by 1;

# iii).	Calculate the Total & Average value of order freight for each state.
select c.customer_state, sum(freight_value) as Total, Avg(freight_value) as Average
from order_items oi join orders o on oi.order_id=o.order_id join customers c on c.customer_id=o.customer_id
group by 1;

# 5. Analysis based on sales, freight and delivery time.
# i). Find the no. of days taken to deliver each order from the order’s purchase date as delivery time.
#     Also, calculate the difference (in days) between the estimated & actual delivery date of an order.
#     Do this in a single query.
#     You can calculate the delivery time and the difference between the estimated & actual delivery date using the given formula:
#     	time_to_deliver = order_delivered_customer_date - order_purchase_timestamp
#     	diff_estimated_delivery =	order_estimated_delivery_date	- order_delivered_customer_date

select order_id, datediff(order_delivered_customer_date,order_purchase_timestamp) as time_to_deliver, 
datediff(order_estimated_delivery_date,order_delivered_customer_date) as diff_estimated_delivery
from orders;

# ii).	Find out the top 5 states with the highest & lowest average freight value.

SELECT 
    highest_state,
    highest_avg_freight,
    lowest_state,
    lowest_avg_freight
FROM (
    SELECT 
        customer_state AS highest_state,
        AVG(freight_value) AS highest_avg_freight,
        ROW_NUMBER() OVER (ORDER BY AVG(freight_value) DESC) AS highest_rank
    FROM 
        customers c join orders o on c.customer_id=o.customer_id join order_items oi on oi.order_id=o.order_id
    GROUP BY 
        customer_state
    ORDER BY 
        AVG(freight_value) DESC
    LIMIT 5
) AS highest
JOIN (
    SELECT 
        customer_state AS lowest_state,
        AVG(freight_value) AS lowest_avg_freight,
        ROW_NUMBER() OVER (ORDER BY AVG(freight_value) ASC) AS lowest_rank
    FROM 
        customers c join orders o on c.customer_id=o.customer_id join order_items oi on oi.order_id=o.order_id
    GROUP BY 
        customer_state
    ORDER BY 
        AVG(freight_value) ASC
    LIMIT 5
) AS lowest
ON highest.highest_rank = lowest.lowest_rank;

(select c.customer_state,avg(oi.freight_value) as Highest
from customers c join orders o on c.customer_id=o.customer_id join order_items oi on oi.order_id=o.order_id
group by 1
order by highest desc
limit 5)
union all
(select c.customer_state,avg(oi.freight_value) as Lowest
from customers c join orders o on c.customer_id=o.customer_id join order_items oi on oi.order_id=o.order_id
group by 1
order by Lowest asc
limit 5);


# iii).	Find out the top 5 states with the highest & lowest average delivery time.

SELECT 
    highest_state,
    highest_time_to_deliver,
    lowest_state,
    lowest_time_to_deliver
FROM (
    SELECT 
        customer_state AS highest_state,
        Avg(datediff(order_delivered_customer_date,order_purchase_timestamp)) AS highest_time_to_deliver,
        ROW_NUMBER() OVER (ORDER BY avg(datediff(order_delivered_customer_date,order_purchase_timestamp)) DESC) AS highest_rank
    FROM 
        orders o join customers c on o.customer_id=c.customer_id
    GROUP BY 
        customer_state
    ORDER BY 
        avg(datediff(order_delivered_customer_date,order_purchase_timestamp)) DESC
    LIMIT 5
) AS highest
JOIN (
    SELECT 
        customer_state AS lowest_state,
        avg(datediff(order_delivered_customer_date,order_purchase_timestamp)) AS lowest_time_to_deliver,
        ROW_NUMBER() OVER (ORDER BY avg(datediff(order_delivered_customer_date,order_purchase_timestamp)) ASC) AS lowest_rank
    FROM 
        orders o join customers c on o.customer_id=c.customer_id
    GROUP BY 
        customer_state
    ORDER BY 
        avg(datediff(order_delivered_customer_date,order_purchase_timestamp)) ASC
    LIMIT 5
) AS lowest
ON highest.highest_rank = lowest.lowest_rank;


# iv).	Find out the top 5 states where the order delivery is really fast as compared to the estimated date	of	delivery. 
# You can use the difference between the averages of actual & estimated delivery date to figure out how fast the delivery was for 
# each state.

select customer_state, 
avg(datediff(order_delivered_customer_date,order_purchase_timestamp)) as actual_delivery_days,
avg(datediff(order_estimated_delivery_date,order_delivered_customer_date)) as estimated_delivery_days
from customers c join orders o on c.customer_id=o.customer_id
group by 1
having avg(datediff(order_delivered_customer_date,order_purchase_timestamp)) < 
avg(datediff(order_estimated_delivery_date,order_delivered_customer_date));

# 6.	Analysis based on the payments:
# i).	Find the month-on-month no. of orders placed using different payment types.

select year(order_purchase_timestamp) as year, month(order_purchase_timestamp) as month, payment_type,count(*) as total_orders
from orders o join payments p on o.order_id=p.order_id
group by 1,2,3
order by year(order_purchase_timestamp) asc, month(order_purchase_timestamp) asc;

# ii).	Find the no. of orders placed on the basis of the payment installments that have been paid.

select payment_type, payment_installments, count(*) as total_orders
from payments p join orders o on p.order_id=o.order_id
group by 1,2
order by payment_installments asc;






