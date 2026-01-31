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
order by `YEAR`,vulnerability_score desc
;

SELECT YEAR(`DATE`) YEARS, INDUSTRY, ROUND(AVG(PERCENTAGE_LAID_OFF),2) AS AVERAGE_PERCENT_LAID_OFF, 
COUNT(COMPANY) AS NUMBER_OF_COMPANIES
FROM LAYOFFS_STAGING2
WHERE YEAR(`DATE`) IS NOT NULL AND INDUSTRY IS NOT NULL
GROUP BY 1,2
ORDER BY YEARS;



with company_year (company , years, total_laid_off) as 
(select company , year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company , year(`date`)),
company_year_rank as
(select *, dense_rank() over (partition by years
order by total_laid_off desc) as ranking
from company_year
where years is not null
order by ranking asc)
select * from company_year_rank
where ranking<=5
order by years
;






-- funding vs layoffs 
select year(`date`) as years ,industry, sum(funds_raised_millions) as total_funds_raised, 
round(avg(percentage_laid_off),2) average_percent_laid_off
from layoffs_staging2
where  year(`date`)  is not null
group by 1,2
order by sum(funds_raised_millions) desc;

SELECT * FROM LAYOFFS_STAGING2;
use layoff;


select year(`date`), industry, sum(funds_raised_millions) as total_funds
, sum(total_laid_off) as total_layoffs,
sum(total_laid_off)/sum(funds_raised_millions) as layoffs_per_million_raised
from layoffs_staging2
where year(`date`) is not null 
group by 1,2
having sum(total_laid_off) is not null
order by layoffs_per_million_raised desc;


select * from layoffs_staging2;

select 









