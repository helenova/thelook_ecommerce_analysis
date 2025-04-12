#1.Визначаємо який гендер частіше повертає товар та співвідношення між повернутими та всією кількістю замовлень
SELECT
  gender,
  COUNT(1) AS numb_orders,
  COUNT(CASE
      WHEN status='Returned' THEN order_id
  END
    ) AS returned_orders,
  COUNT(CASE
      WHEN status='Returned' THEN order_id
  END
    )/COUNT(1) AS share_returned
FROM
  `bigquery-public-data.thelook_ecommerce.orders`
GROUP BY 1
ORDER BY 2 DESC;

#2.Визначаємо як змінювався відсоток повернутих замовлень по місяцях
SELECT
  DATE_TRUNC(created_at,month) AS order_month,
  COUNT(oi.id) AS numb_orders,
  COUNT(CASE
      WHEN status='Returned' THEN order_id
  END
    ) AS returned_orders,
  COUNT(CASE
      WHEN status='Returned' THEN order_id
  END
    )/COUNT(1) AS share_returned
FROM
  `bigquery-public-data.thelook_ecommerce.order_items` oi
GROUP BY 1
ORDER BY 1 DESC;

#3.Визначаємо яку категорію товарів найбільше повертали
SELECT
  p.category,
  COUNT(oi.id) AS numb_orders,
  COUNT(CASE
      WHEN status='Returned' THEN order_id
  END
    ) AS returned_orders,
  COUNT(CASE
      WHEN status='Returned' THEN order_id
  END
    )/COUNT(1) AS share_returned
FROM
  `bigquery-public-data.thelook_ecommerce.order_items` oi
LEFT JOIN
  `bigquery-public-data.thelook_ecommerce.products` p
ON
  oi.product_id=p.id
GROUP BY 1
ORDER BY 4 DESC;

#4.Визначаємо кількість повернень згідно вікової групи
SELECT
  u.gender,
  CASE
    WHEN age<21 THEN '<21'
    WHEN age<35 THEN '21-35'
    WHEN age<50 THEN '35-50'
    ELSE '+50'
  END AS age_group,
  COUNT(CASE
      WHEN status='Returned' THEN order_id
  END
    )/COUNT(1) AS share_returned
FROM
  `bigquery-public-data.thelook_ecommerce.order_items` oi
LEFT JOIN
  `bigquery-public-data.thelook_ecommerce.users` u
ON
  oi.user_id=u.id
GROUP BY 1,2
ORDER BY 3 DESC; 

#5.Визначаємо кількість повернень згідно вікової групи з умовою більше 15 тим замовлень
SELECT
  u.gender,
  COUNT(oi.id) AS numb_orders,
  CASE
    WHEN age<21 THEN '<21'
    WHEN age<35 THEN '21-35'
    WHEN age<50 THEN '35-50'
    ELSE '+50'
  END
  AS age_group,
  COUNT(CASE
      WHEN status='Returned' THEN order_id
  END
    )/COUNT(1) AS share_returned
FROM
  `bigquery-public-data.thelook_ecommerce.order_items` oi
LEFT JOIN
  `bigquery-public-data.thelook_ecommerce.users` u
ON
  oi.user_id=u.id
GROUP BY 1, 3
HAVING
  numb_orders >15000
ORDER BY 4 DESC; 

#6.Визначаємо профіт від користувачів які входять в топ10
WITH
  top_ten AS (
  SELECT
    user_id,
    SUM(sale_price)
  FROM
    `bigquery-public-data.thelook_ecommerce.order_items` oi
  WHERE
    status='Complete'
  GROUP BY
    1
  ORDER BY
    2 DESC
  LIMIT
    10)
SELECT
  oi.user_id,
  SUM(oi.sale_price)-SUM(ii.cost) AS profit_from_top
FROM
  `bigquery-public-data.thelook_ecommerce.order_items` oi
LEFT JOIN
  `bigquery-public-data.thelook_ecommerce.inventory_items` ii
USING
  (product_id)
WHERE
  oi.status='Complete'
  AND user_id IN (
  SELECT
    user_id
  FROM
    top_ten)
GROUP BY
  oi.user_id
ORDER BY
  2 DESC
LIMIT
  10;
  

#7.Визначаємо чи давати знижку клієнту чи ні (з додаванням фулнейму юзерів)
SELECT
  oi.user_id,
  us.first_name,
  us.last_name,
  SUM(oi.sale_price) AS total_sales,
  AVG(oi.sale_price) OVER() AS avg_total_sales,
  CASE
    WHEN SUM(oi.sale_price)>AVG(oi.sale_price) OVER() THEN TRUE
    ELSE FALSE
END
FROM
  `bigquery-public-data.thelook_ecommerce.order_items` oi
LEFT JOIN
  `bigquery-public-data.thelook_ecommerce.users` us
ON
  oi.user_id=us.id
GROUP BY
  oi.user_id,
  oi.sale_price,
  us.first_name,
  us.last_name,
  oi.status
HAVING
  oi.status ='Complete'
ORDER BY
  total_sales DESC;

 #8.Визначаємо чи підлягає покупець для надання знижки чи ні (умова що його вартість замовлення вище середньогосередньої) 
SELECT
  order_id,
  user_id,
  sale_price,
  AVG(sale_price) OVER() AS avg_total_sales,
  CASE
    WHEN sale_price>AVG(sale_price) OVER() THEN TRUE
    ELSE FALSE 
  END
FROM
  `bigquery-public-data.thelook_ecommerce.order_items` oi
GROUP BY
  1,2,3;

#9.Визначаємо 5% відсотків найкращих покупців яким ми можемо запропонувати знижку
WITH orders AS (
  SELECT
  user_id,
  SUM(sale_price) as sum_order
FROM
  `bigquery-public-data.thelook_ecommerce.order_items`
GROUP BY 1),
ranks AS (
  SELECT
  *,
  PERCENT_RANK() OVER(ORDER BY orders.sum_order) as persent_rank
FROM orders
)
SELECT
*
FROM ranks
WHERE persent_rank>=0.95

#Порівнюємо суму кожного замовлення з сумою першого замовлення
WITH user_orders AS (
  SELECT 
    user_id,
    order_id,
    created_at,
    SUM(sale_price) AS order_amount
  FROM `bigquery-public-data.thelook_ecommerce.order_items`
  WHERE status = 'Complete'
  GROUP BY user_id, order_id, created_at
),
ranked_orders AS (
  SELECT
    user_id,
    order_id,
    created_at,
    order_amount,
    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY created_at) AS rn,
    FIRST_VALUE(order_amount) OVER(PARTITION BY user_id ORDER BY created_at) AS first_order_amount
  FROM user_orders
)
SELECT
  user_id,
  ROUND(AVG(order_amount - first_order_amount),2) AS avg_diff,
  ROUND(AVG((order_amount - first_order_amount) / first_order_amount),2) AS avg_prc_diff
FROM ranked_orders
WHERE rn > 1 -- щоб не враховувати перше замовлення
GROUP BY 1
ORDER BY 3 DESC
LIMIT 100
