-- 							WINDOWS FUNCTIONS

-- THIS DATASET WAS GOTTEN FROM KAGGLE
-- https://www.kaggle.com/datasets/dgomonov/new-york-city-airbnb-open-data 
-- In the lines of codes that follows, I will perform some windows functions on this dataset.

# -- COLUMNS that I will be working with:
# 1. id
# 2. name
# 3. neighbourhood_group
# 4. neighbourhood 
# 5. price
# 6. last_review

-- Selecting the database to use
USE new_york_airbnb;

-- Viewing the dataset
SELECT * FROM bookings;

-- Calculating the average, minimum and maximum prices of all the New York airbnb houses. And comparing them to the listed house price.
SELECT id,
	   name,
       neighbourhood_group,
       price,
       AVG(price) OVER() AS avg_price,
       MIN(price) OVER() AS min_price,
       MAX(price) OVER() AS max_price
FROM bookings;

-- Difference from avg_price with OVER()
-- That is, calculating the price difference between listed house price and the average price of all the NewYork airbnb houses.
SELECT id,
	   name,
       neighbourhood_group,
       price,
       ROUND(AVG(price) OVER(), 2) AS avg_price,
       ROUND(price - AVG(price) OVER(), 2) AS price_diff_from_avg_price
FROM bookings
ORDER BY price_diff_from_avg_price DESC;
-- The average price is $139.79.

-- PARTITIONING BY neighbourhood group
SELECT id,
	   name,
       neighbourhood_group,
       neighbourhood,
       price,
       ROUND(AVG(price) OVER(PARTITION BY neighbourhood_group), 2) AS avg_price_by_neighbourhood_group
FROM bookings
ORDER BY avg_price_by_neighbourhood_group;
-- This code calculates the average price for each neighbourhood group.

-- PARTITION BY neighbourhood_group and neighbourhood
SELECT id,
	   name,
       neighbourhood_group,
       neighbourhood,
       price,
       ROUND(AVG(price) OVER(PARTITION BY neighbourhood_group), 2) AS avg_price_by_neighbourhood_group,
       ROUND(AVG(price) OVER(PARTITION BY neighbourhood_group, neighbourhood), 2) AS avg_price_by_neighbourhood_group_and_neigh
FROM bookings
ORDER BY 6,7;
-- This code calculates the average price for each neighbourhood_group together with its neighbourhood.

-- Neighbourhood price rank
SELECT id,
	   name,
       neighbourhood_group,
       neighbourhood,
       price,
       ROUND(AVG(price) OVER(PARTITION BY neighbourhood_group), 2) AS avg_price_by_neighbourhood_group,
       ROUND(AVG(price) OVER(PARTITION BY neighbourhood_group, neighbourhood), 2) AS avg_price_by_neighbourhood_group_and_neigh,
       ROUND(price - AVG(price) OVER(PARTITION BY neighbourhood_group), 2) AS neigh_group_delta,
	   ROUND(price - AVG(price) OVER(PARTITION BY neighbourhood_group, neighbourhood), 2) AS neigh_group_and_neigh_delta
FROM bookings
ORDER BY 6,7,5;
-- This calcualtes the price difference between the listed price and the average neighbourhood_group price.
-- Also the price difference between the listed price and the average price of neighbourhood_group together with the neighbourhood.

-- ROW_NUMBER
SELECT id,
	   name,
       neighbourhood_group,
       neighbourhood,
       price,
       ROW_NUMBER() OVER (ORDER BY price DESC) AS overall_price_rank
FROM bookings;
-- This code ranks the different houses based on their prices.
-- Though, the ROW_NUMBER function gives different ranks to houses of the same price.

SELECT id,
	   name,
       neighbourhood_group,
       neighbourhood,
       price,
       ROW_NUMBER() OVER (ORDER BY price DESC) AS overall_price_rank,
	   ROW_NUMBER() OVER (PARTITION BY neighbourhood_group ORDER BY price DESC) AS neigh_group_price_rank
FROM bookings;
-- Here, we are ranking the different houses based on(group by) their neighbourhood_group.

-- Using RANK
SELECT id,
	   name,
       neighbourhood_group,
       neighbourhood,
       price,
       RANK() OVER (ORDER BY price DESC) AS overall_price_rank
FROM bookings;
-- This code ranks the different houses based on their prices.
-- The RANK function assigns the same rank to houses of the same price. But skips the succeeding ranks. 

SELECT id,
	   name,
       neighbourhood_group,
       neighbourhood,
       price,
       RANK() OVER (ORDER BY price DESC) AS overall_price_rank,
	   RANK() OVER (PARTITION BY neighbourhood_group ORDER BY price DESC) AS neigh_group_price_rank
FROM bookings;
-- This code ranks the different houses based on their neighbourhood_group.

-- Using DENSE_RANK
SELECT id,
	   name,
       neighbourhood_group,
       neighbourhood,
       price,
       DENSE_RANK() OVER (ORDER BY price DESC) AS overall_price_rank,
	   DENSE_RANK() OVER (PARTITION BY neighbourhood_group ORDER BY price DESC) AS neigh_group_price_rank
FROM bookings;
-- The DENSE_RANK function assigns the same rank to houses of the same price. But does not skips the succeeding ranks. 

-- Using LAG
SELECT id,
	   name,
       neighbourhood_group,
       neighbourhood,
       price,
       last_review,
	   LAG(price) OVER(PARTITION BY neighbourhood_group ORDER BY last_review DESC) AS neigh_group_price_rank
FROM bookings;
-- lag prints the preceding(above) price. 

-- LAGGING by 2 Periods
SELECT id,
	   name,
       neighbourhood_group,
       neighbourhood,
       price,
       last_review,
	   LAG(price, 2) OVER(PARTITION BY neighbourhood_group ORDER BY last_review DESC) AS neigh_group_price_rank
FROM bookings;
-- this prints the preceding antepenultimate price.

-- Using LEAD
SELECT id,
	   name,
       neighbourhood_group,
       neighbourhood,
       price,
       last_review,
	   LEAD(price) OVER(PARTITION BY neighbourhood_group ORDER BY last_review DESC) AS neigh_group_price_rank
FROM bookings;
-- LEAD prints the succeeding price

-- You can also LEAD by 2 Periods
SELECT id,
	   name,
       neighbourhood_group,
       neighbourhood,
       price,
       last_review,
	   LEAD(price, 2) OVER(PARTITION BY neighbourhood_group ORDER BY last_review DESC) AS neigh_group_price_rank
FROM bookings;

-- MISCELLANEOUS
SELECT *
FROM (SELECT id,
	   name,
       neighbourhood_group,
       neighbourhood,
       price,
       ROW_NUMBER() OVER (ORDER BY price DESC) AS overall_price_rank,
	   ROW_NUMBER() OVER (PARTITION BY neighbourhood_group ORDER BY price DESC) AS neigh_group_price_rank,
       CASE
			WHEN ROW_NUMBER() OVER (PARTITION BY neighbourhood_group ORDER BY price DESC) <= 3 THEN 'YES'
            ELSE 'NO'
		END AS top3
FROM bookings) A
WHERE top3 = 'YES';
-- This code prints the TOP 3 houses in each neighbourhood_group.