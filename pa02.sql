create database pa02;
use pa02;

-- Table creation:
create table movies(
	id int,
    title varchar(500),
    release_date date,
    runtime int,
    original_language varchar(30)
);

create table statistics(
	id int,
    budget int,
    revenue int
);

create table ratings(
	id int,
    avg_vote float,
    vote_count int,
    popularity float
);

-- Importing data into three tables table using "Table Data Import Wizard"
-- Sample of tables data:
select * from movies limit 5;
/*
2','Ariel','1988-10-21','73','fi' 
'3','Shadows in Paradise','1986-10-17','74','fi' 
'5','Four Rooms','1995-12-09','98','en' 
'6','Judgment Night','1993-10-15','109','en' 
'8','Life in Loops (A Megacities RMX)','2006-01-01','80','en' 
*/
select * from statistics limit 5;
/*
'2','0','0' 
'3','0','0' 
'5','4000000','4257354' 
'6','21000000','12136938' 
'8','42000','0' 
*/
select * from ratings limit 5;
/*
'2','7.1','262','8.155' 
'3','7.199','281','5.946' 
'5','5.784','2436','15.295' 
'6','6.533','302','13.564' 
'8','7.7','25','1.587' 
*/


/*
1. Used ChatGPT to generate unoptimized query based on the data in my tables.
Link to the chat : https://chatgpt.com/share/68f4fa38-70bc-800b-b925-9e46e6b4a2db

The generated query return the basic data about the film (id, title, release_date, budget, etc). 
It selects films with original languages used by movies with runtime bigger than average and have revenue
higher than average revenue of movies with budget > 1000000. 
The result is ordered by movie popularity in descending order.
*/

-- Sample result: time taken - 2.834 sec
SELECT 
    m.title,
    m.release_date,
    m.runtime,
    s.budget,
    s.revenue,
    r.avg_vote,
    r.vote_count,
    r.popularity
FROM movies m
JOIN statistics s ON m.id = s.id
JOIN ratings r ON m.id = r.id
WHERE m.original_language IN (
    SELECT original_language 
    FROM movies 
    WHERE runtime > (SELECT AVG(runtime) FROM movies)
)
AND s.revenue > (
    SELECT AVG(revenue) 
    FROM statistics 
    WHERE budget > 1000000
)
ORDER BY r.popularity DESC
limit 10;
/*
'Blue Beetle','2023-08-16','128','120000000','124818235','7.139','1023','2994.36'
'Gran Turismo','2023-08-09','135','60000000','114800000','8.068','702','2680.59'
'The Nun II','2023-09-06','110','38500000','231200000','6.545','365','1692.78'
'Meg 2: The Trench','2023-08-02','116','129000000','384056482','6.912','2034','1567.27'
'Talk to Me','2023-07-26','95','4500000','72600000','7.214','973','1458.51'
'Fast X','2023-05-17','142','340000000','704709660','7.265','3881','1175.27'
'Sound of Freedom','2023-07-03','131','15000000','212587173','7.973','503','1111.04'
'Barbie','2023-07-19','114','145000000','1428545028','7.279','5074','1069.34'
'Elemental','2023-06-14','102','200000000','486797988','7.757','2467','1008.94'
'Transformers: Rise of the Beasts','2023-06-06','127','195000000','429800000','7.492','3270','616.74'
*/

-- USING EXPLAIN ANALYZE 
EXPLAIN ANALYZE
SELECT 
    m.title,
    m.release_date,
    m.runtime,
    s.budget,
    s.revenue,
    r.avg_vote,
    r.vote_count,
    r.popularity
FROM movies m
JOIN statistics s ON m.id = s.id
JOIN ratings r ON m.id = r.id
WHERE m.original_language IN (
    SELECT original_language 
    FROM movies 
    WHERE runtime > (SELECT AVG(runtime) FROM movies)
)
AND s.revenue > (
    SELECT AVG(revenue) 
    FROM statistics 
    WHERE budget > 1000000
)
ORDER BY r.popularity DESC;
/*
EXPLAIN ANALYZE RESULT: 
-> Sort: r.popularity DESC  (actual time=3036..3036 rows=2924 loops=1)
    -> Stream results  (cost=164e+18 rows=164e+18) (actual time=2631..3035 rows=2924 loops=1)
        -> Inner hash join (r.id = m.id)  (cost=164e+18 rows=164e+18) (actual time=2631..3035 rows=2924 loops=1)
            -> Table scan on r  (cost=15 rows=1.3e+6) (actual time=0.0224..332 rows=1.3e+6 loops=1)
            -> Hash
                -> Filter: (s.revenue > (select #4))  (cost=3.79e+15 rows=1.26e+15) (actual time=1605..2630 rows=2924 loops=1)
                    -> Inner hash join (s.id = m.id)  (cost=3.79e+15 rows=1.26e+15) (actual time=1263..2241 rows=934533 loops=1)
                        -> Table scan on s  (cost=12.1 rows=1.3e+6) (actual time=0.0282..326 rows=1.3e+6 loops=1)
                        -> Hash
                            -> Inner hash join (m.original_language = `<subquery2>`.original_language)  (cost=29.1e+9 rows=29.1e+9) (actual time=671..1107 rows=934539 loops=1)
                                -> Table scan on m  (cost=4.69e+6 rows=934745) (actual time=0.0154..311 rows=934565 loops=1)
                                -> Hash
                                    -> Table scan on <subquery2>  (cost=64154..68051 rows=311551) (actual time=671..671 rows=157 loops=1)
                                        -> Materialize with deduplication  (cost=64154..64154 rows=311551) (actual time=671..671 rows=157 loops=1)
                                            -> Filter: (movies.original_language is not null)  (cost=32999 rows=311551) (actual time=280..608 rows=445545 loops=1)
                                                -> Filter: (movies.runtime > (select #3))  (cost=32999 rows=311551) (actual time=280..583 rows=445545 loops=1)
                                                    -> Table scan on movies  (cost=32999 rows=934745) (actual time=0.0445..234 rows=934565 loops=1)
                                                    -> Select #3 (subquery in condition; run only once)
                                                        -> Aggregate: avg(movies.runtime)  (cost=188793 rows=1) (actual time=280..280 rows=1 loops=1)
                                                            -> Table scan on movies  (cost=95318 rows=934745) (actual time=0.00683..232 rows=934565 loops=1)
                    -> Select #4 (subquery in condition; run only once)
                        -> Aggregate: avg(statistics.revenue)  (cost=174953 rows=1) (actual time=342..342 rows=1 loops=1)
                            -> Filter: (statistics.budget > 1000000)  (cost=131568 rows=433847) (actual time=0.0126..341 rows=13856 loops=1)
                                -> Table scan on statistics  (cost=131568 rows=1.3e+6) (actual time=0.00979..297 rows=1.3e+6 loops=1)
*/

/*
Parts to improve:
1. Create primary keys for id in all three tables.
2. Create index for original_language column for faster search (WHERE m.original_language IN ... )
3. Create index for numerical values that are frequently used in where parts and orderings(runtime, budget, popularity, revenue)
4. Rewrite subqueries with CTEs to avoid recalculations
*/

alter table movies
  add primary key (id);
alter table statistics
  add primary key (id);
alter table ratings
  add primary key (id);
create index idx_movies_original_language ON movies(original_language);
create index idx_movies_runtime ON movies(runtime);
create index idx_ratings_popularity ON ratings(popularity);
create index idx_statistics_budget ON statistics(budget);
create index idx_statistics_revenue ON statistics(revenue);

-- Rewriten query:
with average_runtime as(
select avg(runtime) as avg_runtime from movies -- avg will be computed only once, not each time for each row
), 
languages as(
select distinct original_language from movies m -- without distinct 445545 are returned, with distinct - 157
cross join average_runtime ar
where m.runtime > ar.avg_runtime
),
average_revenue as ( 
select avg(revenue) as avg_revenue from statistics -- counting average revenue only once
where budget > 1000000
)
select 
    m.title,
    m.release_date,
    m.runtime,
    s.budget,
    s.revenue,
    r.avg_vote,
    r.vote_count,
    r.popularity
from movies m
join statistics s on m.id = s.id
join ratings r on m.id = r.id
where m.original_language in (select original_language from languages)
and s.revenue > (select avg_revenue from average_revenue)
order by r.popularity desc
limit 10; -- to compare results
/*
SAMPLE RESULT:
'Blue Beetle','2023-08-16','128','120000000','124818235','7.139','1023','2994.36'
'Gran Turismo','2023-08-09','135','60000000','114800000','8.068','702','2680.59'
'The Nun II','2023-09-06','110','38500000','231200000','6.545','365','1692.78'
'Meg 2: The Trench','2023-08-02','116','129000000','384056482','6.912','2034','1567.27'
'Talk to Me','2023-07-26','95','4500000','72600000','7.214','973','1458.51'
'Fast X','2023-05-17','142','340000000','704709660','7.265','3881','1175.27'
'Sound of Freedom','2023-07-03','131','15000000','212587173','7.973','503','1111.04'
'Barbie','2023-07-19','114','145000000','1428545028','7.279','5074','1069.34'
'Elemental','2023-06-14','102','200000000','486797988','7.757','2467','1008.94'
'Transformers: Rise of the Beasts','2023-06-06','127','195000000','429800000','7.492','3270','616.74'
*/
EXPLAIN ANALYZE
with average_runtime as(
select avg(runtime) as avg_runtime from movies 
), 
languages as(
select distinct original_language from movies m 
cross join average_runtime ar
where m.runtime > ar.avg_runtime
),
average_revenue as ( 
select avg(revenue) as avg_revenue from statistics
where budget > 1000000
)
select 
    m.title,
    m.release_date,
    m.runtime,
    s.budget,
    s.revenue,
    r.avg_vote,
    r.vote_count,
    r.popularity
from movies m
join statistics s on m.id = s.id
join ratings r on m.id = r.id
where m.original_language in (select original_language from languages)
and s.revenue > (select avg_revenue from average_revenue)
order by r.popularity desc;
/*
-> Sort: r.popularity DESC  (actual time=1051..1052 rows=2924 loops=1)
    -> Stream results  (cost=67210 rows=490197) (actual time=1031..1051 rows=2924 loops=1)
        -> Nested loop semijoin  (cost=67210 rows=490197) (actual time=1031..1050 rows=2924 loops=1)
            -> Nested loop inner join  (cost=10483 rows=3083) (actual time=0.334..17.4 rows=2924 loops=1)
                -> Nested loop inner join  (cost=7091 rows=3083) (actual time=0.327..11.7 rows=2924 loops=1)
                    -> Filter: (s.revenue > (select #5))  (cost=3701 rows=3083) (actual time=0.317..5.13 rows=3083 loops=1)
                        -> Index range scan on s using idx_statistics_revenue over (51463814 <= revenue)  (cost=3701 rows=3083) (actual time=0.316..4.9 rows=3083 loops=1)
                        -> Select #5 (subquery in condition; run only once)
                            -> Rows fetched before execution  (cost=0..0 rows=1) (actual time=83e-6..125e-6 rows=1 loops=1)
                    -> Filter: (m.original_language is not null)  (cost=1 rows=1) (actual time=0.0019..0.00198 rows=0.948 loops=3083)
                        -> Single-row index lookup on m using PRIMARY (id=s.id)  (cost=1 rows=1) (actual time=0.00178..0.00181 rows=0.948 loops=3083)
                -> Single-row index lookup on r using PRIMARY (id=s.id)  (cost=1 rows=1) (actual time=0.0018..0.00183 rows=1 loops=2924)
            -> Covering index lookup on languages using <auto_key0> (original_language=m.original_language)  (cost=144010..144032 rows=10) (actual time=0.353..0.353 rows=1 loops=2924)
                -> Materialize CTE languages  (cost=144007..144007 rows=159) (actual time=1031..1031 rows=157 loops=1)
                    -> Group (no aggregates)  (cost=143991 rows=159) (actual time=0.186..1031 rows=157 loops=1)
                        -> Filter: (m.runtime > '54.8624')  (cost=97265 rows=467264) (actual time=0.173..1009 rows=445545 loops=1)
                            -> Index scan on m using idx_movies_original_language  (cost=97265 rows=934529) (actual time=0.172..942 rows=934565 loops=1)
*/

-- BEFORE/AFTER EXPLAIN ANALYZE COMPARISON:
/*
1. Total execution cost dropped from cost=164e+18 to cost=67210. The number of proceded rows also gradually decreased.
2. Thanks to indexes the join uses Single-row index lookup (instead of Inner hash join).
3. Table scans are changed for index scans, covering index lookup, index range scan.
4. Sorting time is now three time less.
5. The aggregated values are counted only one time in CTEs (subquery in condition; run only once) 
   and results are reused (e.g Materialize CTE languages).
*/

EXPLAIN
with average_runtime as(
select avg(runtime) as avg_runtime from movies
), 
languages as(
select distinct original_language from movies m
cross join average_runtime ar
where m.runtime > ar.avg_runtime
),
average_revenue as ( 
select avg(revenue) as avg_revenue from statistics
where budget > 1000000
)
select 
    m.title,
    m.release_date,
    m.runtime,
    s.budget,
    s.revenue,
    r.avg_vote,
    r.vote_count,
    r.popularity
from movies m
join statistics s on m.id = s.id
join ratings r on m.id = r.id
where m.original_language in (select original_language from languages)
and s.revenue > (select avg_revenue from average_revenue)
order by r.popularity desc;
/*
'1','PRIMARY','s',NULL,'range','PRIMARY,idx_statistics_revenue','idx_statistics_revenue','5',NULL,'3083','100.00','Using where; Using temporary; Using filesort'
'1','PRIMARY','m',NULL,'eq_ref','PRIMARY,idx_movies_original_language','PRIMARY','4','pa02.s.id','1','100.00','Using where'
'1','PRIMARY','r',NULL,'eq_ref','PRIMARY','PRIMARY','4','pa02.s.id','1','100.00',NULL
'1','PRIMARY','<derived3>',NULL,'ref','<auto_key0>','<auto_key0>','123','pa02.m.original_language','10','100.00','Using index; FirstMatch(r)'
'5','SUBQUERY','<derived6>',NULL,'system',NULL,NULL,NULL,NULL,'1','100.00',NULL
'6','DERIVED','statistics',NULL,'range','idx_statistics_budget','idx_statistics_budget','5',NULL,'25552','100.00','Using index condition'
'3','DERIVED','<derived4>',NULL,'system',NULL,NULL,NULL,NULL,'1','100.00',NULL
'3','DERIVED','m',NULL,'index','idx_movies_original_language,idx_movies_runtime','idx_movies_original_language','123',NULL,'934529','50.00','Using where'
'4','DERIVED','movies',NULL,'index',NULL,'idx_movies_runtime','5',NULL,'934529','100.00','Using index'
*/

-- We can see that index idx_ratings_popularity was not used in ordering part.
-- Lets use optimizer hint and check execution plan
EXPLAIN ANALYZE
with average_runtime as(
select avg(runtime) as avg_runtime from movies
), 
languages as(
select distinct original_language from movies m
cross join average_runtime ar
where m.runtime > ar.avg_runtime
),
average_revenue as ( 
select avg(revenue) as avg_revenue from statistics
where budget > 1000000
)
select 
    m.title,
    m.release_date,
    m.runtime,
    s.budget,
    s.revenue,
    r.avg_vote,
    r.vote_count,
    r.popularity
from movies m
join statistics s on m.id = s.id
join ratings r FORCE INDEX (idx_ratings_popularity) on m.id = r.id -- forcing to use the index
where m.original_language in (select original_language from languages)
and s.revenue > (select avg_revenue from average_revenue)
order by r.popularity desc;
/*
-> Nested loop semijoin  (cost=4.13e+6 rows=10.4e+6) (actual time=1109..4756 rows=2924 loops=1)
    -> Nested loop inner join  (cost=2.94e+6 rows=65096) (actual time=21.9..3667 rows=2924 loops=1)
        -> Nested loop inner join  (cost=2.86e+6 rows=65096) (actual time=21.9..3659 rows=3083 loops=1)
            -> Index scan on r using idx_ratings_popularity (reverse)  (cost=1.43e+6 rows=1.3e+6) (actual time=21.9..1745 rows=1.3e+6 loops=1)
            -> Filter: (s.revenue > (select #5))  (cost=1 rows=0.05) (actual time=0.00138..0.00138 rows=0.00236 loops=1.3e+6)
                -> Single-row index lookup on s using PRIMARY (id=r.id)  (cost=1 rows=1) (actual time=0.00119..0.00122 rows=1 loops=1.3e+6)
                -> Select #5 (subquery in condition; run only once)
                    -> Rows fetched before execution  (cost=0..0 rows=1) (actual time=83e-6..125e-6 rows=1 loops=1)
        -> Filter: (m.original_language is not null)  (cost=1 rows=1) (actual time=0.00228..0.00236 rows=0.948 loops=3083)
            -> Single-row index lookup on m using PRIMARY (id=r.id)  (cost=1 rows=1) (actual time=0.00214..0.00217 rows=0.948 loops=3083)
    -> Covering index lookup on languages using <auto_key0> (original_language=m.original_language)  (cost=144010..144032 rows=10) (actual time=0.372..0.372 rows=1 loops=2924)
        -> Materialize CTE languages  (cost=144007..144007 rows=159) (actual time=1087..1087 rows=157 loops=1)
            -> Group (no aggregates)  (cost=143991 rows=159) (actual time=0.613..1087 rows=157 loops=1)
                -> Filter: (m.runtime > '54.8624')  (cost=97265 rows=467264) (actual time=0.597..1065 rows=445545 loops=1)
                    -> Index scan on m using idx_movies_original_language  (cost=97265 rows=934529) (actual time=0.594..998 rows=934565 loops=1)
*/

/* 
Main insight here is that the forced index usage has negative influence on the execution. 
The cost and the number of proceeded rows gradually increased.
For this particular situation it may be better to drop the existing index and create new one for column (popularity desc)
*/ 

drop index idx_ratings_popularity ON ratings;
create index idx_ratings_popularity ON ratings(popularity desc);

EXPLAIN
with average_runtime as(
select avg(runtime) as avg_runtime from movies 
), 
languages as(
select distinct original_language from movies m 
cross join average_runtime ar
where m.runtime > ar.avg_runtime
),
average_revenue as ( 
select avg(revenue) as avg_revenue from statistics
where budget > 1000000
)
select 
    m.title,
    m.release_date,
    m.runtime,
    s.budget,
    s.revenue,
    r.avg_vote,
    r.vote_count,
    r.popularity
from movies m
join statistics s on m.id = s.id
join ratings r on m.id = r.id
where m.original_language in (select original_language from languages)
and s.revenue > (select avg_revenue from average_revenue)
order by r.popularity desc;
/*
'1','PRIMARY','s',NULL,'range','PRIMARY,idx_statistics_revenue','idx_statistics_revenue','5',NULL,'3083','100.00','Using where; Using temporary; Using filesort'
'1','PRIMARY','m',NULL,'eq_ref','PRIMARY,idx_movies_original_language','PRIMARY','4','pa02.s.id','1','100.00','Using where'
'1','PRIMARY','r',NULL,'eq_ref','PRIMARY','PRIMARY','4','pa02.s.id','1','100.00',NULL
'1','PRIMARY','<derived3>',NULL,'ref','<auto_key0>','<auto_key0>','123','pa02.m.original_language','10','100.00','Using index; FirstMatch(r)'
'5','SUBQUERY','<derived6>',NULL,'system',NULL,NULL,NULL,NULL,'1','100.00',NULL
'6','DERIVED','statistics',NULL,'range','idx_statistics_budget','idx_statistics_budget','5',NULL,'25552','100.00','Using index condition'
'3','DERIVED','<derived4>',NULL,'system',NULL,NULL,NULL,NULL,'1','100.00',NULL
'3','DERIVED','m',NULL,'index','idx_movies_original_language,idx_movies_runtime','idx_movies_original_language','123',NULL,'934529','50.00','Using where'
'4','DERIVED','movies',NULL,'index',NULL,'idx_movies_runtime','5',NULL,'934529','100.00','Using index'
*/

-- This index is still not used, because it is more efficient to sort approximately 3000 rows than to apply index that covers ~1,3M rows.
drop index idx_ratings_popularity ON ratings;

/*
Some additional checkings:
SELECT count(original_language)
    FROM movies 
    WHERE runtime > (SELECT AVG(runtime) FROM movies);
    
SELECT count(distinct original_language)
    FROM movies 
    WHERE runtime > (SELECT AVG(runtime) FROM movies);
*/