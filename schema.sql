-- Databases, schema, external stage, file format --

CREATE DATABASE markazapp;
CREATE SCHEMA stage;

-- Transactions --

CREATE OR REPLACE TABLE markazapp.stage.transactions
(
    customer_id int,
    product_id int,
    purchase_date date,
    quantity int,
    product_price decimal(38,2),
    total_price decimal(38,2),
    rating decimal(38,2),
    review varchar,
    product_category varchar(255),
    updated_at timestamp
);

-- Sales_Per_Product

CREATE OR REPLACE TABLE markazapp.stage.sales_per_product
(
    product_category varchar(255),
    purchase_date date,
    total_sales decimal(38,2)
);

-- Review_Ratings

CREATE OR REPLACE TABLE markazapp.stage.review_ratings
(
    product_category varchar(255),
    total_orders int,
    total_reviews int,
    total_positive_reviews int,
    total_negative_reviews int
);

CREATE OR REPLACE TABLE markazapp.stage.rolling_avg_rating_per_product
(
    product_category varchar(255),
    purchase_date date,
    rating decimal(38,2),
    rolling_avg_rating decimal(38,2)
);

-- Satisfactory_Score View

CREATE OR REPLACE VIEW markazapp.stage.satisfactory_score as
(    
    select product_category, 
            total_orders, 
            total_reviews, 
            cast(total_positive_reviews / total_orders * 100 as int) as satisfactory_score
    from markazapp.stage.review_ratings
    where total_orders > 3
    order by satisfactory_score desc, total_orders desc
    limit 3
);

-- Most_Popular_Categories View

CREATE OR REPLACE VIEW most_popular_categories AS
(    
    select product_category, 
            count(*) as total_orders
    from transactions
    group by 1
    order by count(*) desc
    limit 3
);

-- Most_Profitable_Week View

CREATE OR REPLACE VIEW most_profitable_week AS
(
    select  week(purchase_date) week_number, 
            count(*) as total_purchases,
            sum(total_price) as total_price
    from transactions
    group by 1
    order by total_price desc
    limit 1
);