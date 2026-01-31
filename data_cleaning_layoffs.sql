create database layoff;

use layoff;

select * from world_layoffs;

-- 1.Remove duplicates
-- 2. Standardrize the data
-- 3. Null values or blank values
-- 4. remove any columns

create table layoffs_staging
like world_layoffs;

select * from layoffs_staging;

INSERT layoffs_staging
SELECT * 
FROM world_layoffs;

SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, 'date' , stage, country, funds_raised_millions) AS row_num
from layoffs_staging;

with duplicate_cte as 
(SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, 'date' , stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
select * 
from duplicate_cte
where row_num >1;

select * from
layoffs_staging
where company = "Casper";

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * from layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, 'date' , stage, country, funds_raised_millions) AS row_num
from layoffs_staging;


set sql_safe_updates = 0;

DELETE
FROM layoffs_staging2 
where row_num > 1;

SELECT * FROM 
layoffs_staging2 
where row_num >1;


-- Standardrizing data

SELECT company , TRIM(company)
from layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

select * from layoffs_staging2;

SELECT DISTINCT industry
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like "Crypto%";

Update layoffs_staging2
set industry = "Crypto"
where industry LIKE "Crypto%";

select distinct location
from layoffs_staging2
order by 1;


Select * from layoffs_staging2;

select distinct country
from layoffs_staging2
order by 1;

UPDATE layoffs_staging2
set country = trim(trailing "." from country)
where country like "United States%" ;

select `date` 
from 
layoffs_staging2
order by 1;

SELECT `date`
, STR_TO_DATE(`date`, "%m/%d/%Y")
from layoffs_staging2;

UPDATE layoffs_staging2
SET `date` =
CASE
WHEN `date` like "%/%"
THEN STR_TO_DATE (`date`, "%m/%d/%Y")
WHEN `date` like "%-%"
THEN STR_TO_DATE (`date`, "%m-%d-%Y")
END
where `date` is not null;


ALTER table layoffs_staging2
MODIFY COLUMN `date` DATE;


-- NULLS

SELECT  * from layoffs_staging2;


select industry
from layoffs_staging2
order by 1;

select * 
from layoffs_staging2 
where industry is null 
or industry = "";

select *
from layoffs_staging2
where company like "Bally's%";


select * 
from layoffs_staging2
where company = "Airbnb";

use layoff;
select * from layoffs_staging2;

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

 set sql_safe_updates= 0;

update layoffs_staging2
set industry = null
where industry = '';

UPDATE layoffs_staging2 t1hge
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;


select *
from layoffs_staging2
where total_laid_off is null
and
percentage_laid_off is null;

DELETE
from layoffs_staging2
where total_laid_off is null
and
percentage_laid_off is null;

SELECT * from layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- EDA


select * from layoffs_staging2;

-- KPIs

-- TOTAL LAID OFF 
SELECT year(`date`) as `year`, SUM(Total_laid_off) as Total_Laid_Off
from layoffs_staging2
where  year(`date`) is not null
group by 1
order by `year`;

-- AVERAGE LAY OFF
 SELECT year(`date`) as `year`, round(AVG(percentage_laid_off),2) as average_percentage_laid_off
from layoffs_staging2
where  year(`date`) is not null
group by 1
order by `year`;

-- Companies affected
select year(`date`) as year , count(distinct company) as companies_affected
from layoffs_staging2
where year(`date`) is not null
group by year(`date`);


-- country wise job  loss

select * from layoffs_staging2;

-- WHICH INDUSTRY HAD THE MOST LAYOFFS

SELECT year(`DATE`) as `year`, INDUSTRY, 
ROUND(avg(PERCENTAGE_LAID_OFF),2) as average_percentage_laidOff
FROM LAYOFFS_STAGING2
where year(`date`) is not null and industry is not null
GROUP BY YEAR(`date`), industry
order by industry,year(`date`);

select * from layoffs_staging2;

-- which country had the most lay_offs

with k as (SELECT year(`DATE`) as `year`, country, 
sum(total_laid_off) as total_laid_off,
round(avg(percentage_laid_off),2) as average_percent_laid_off
FROM LAYOFFS_STAGING2
where total_laid_off is not null and  year(`DATE`) is not null
GROUP BY YEAR(`date`), country
order by country,year(`date`))
select * from k
where average_percent_laid_off is not null;



-- TOTAL IMPACT BY YEAR AND STAGE

select year(`date`) as `year` , stage,
sum(total_laid_off) as total_affected,
round(avg(percentage_laid_off),2) as avg_pct_cut
from layoffs_staging2
where year(`date`) is not null
group by 1 , 2
order by 1 desc, 3 desc;

-- THE HEAVY HITTERS (TOP 5 COMPANIES BY COUNTRY)

SELECT * FROM LAYOFFS_STAGING2;

WITH RabkedLayoff as (SELECT   INDUSTRY, COMPANY,PERCENTAGE_LAID_OFF,
dense_rank() OVER (PARTITION BY INDUSTRY ORDER BY PERCENTAGE_LAID_OFF DESC) AS RANKING
FROM layoffs_staging2
where PERCENTAGE_LAID_OFF is not null and industry is not null
)
select * from RabkedLayoff
where ranking<=5;

-- Industry vulnerability score

select year(`date`) as `year` , industry,
round(avg(percentage_laid_off),2) as vulnerability_score,
count(company) as number_of_events
from layoffs_staging2
where percentage_laid_off is not null
group by 1,2
having number_of_events>5
order by vulnerability_score desc;

-- funding vs layoffs 
select year(`date`) as years ,sum(funds_raised_millions) as total_funds_raised, 
round(avg(percentage_laid_off),2) average_percent_laid_off
from layoffs_staging2
where  year(`date`)  is not null
group by 1
order by sum(funds_raised_millions) desc;

SELECT * FROM LAYOFFS_STAGING2;

