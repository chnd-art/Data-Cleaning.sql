-- Data cleaning 

select * 
from layoffs ;

-- 1.Removing Duplicates: 
-- 2.Standardizing Data: 
-- 3.Null/Blank Values: 
-- 4.Remove Unnecessary Columns/Rows: 

create table layoffs_staging
like layoffs ;

select *
from layoffs_staging ;

insert layoffs_staging 
select *
from layoffs ;

select *
from layoffs_staging ;

select * ,
ROW_NUMBER() OVER (
PARTITION BY company,location,industry, percentage_laid_off ,total_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging ;

with duplicate_cte as 
(
select * ,
ROW_NUMBER() OVER (
PARTITION BY company,location,industry, percentage_laid_off ,total_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging 
)
select *
from duplicate_cte
where row_num > 1 ;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL ,
  `row_num` int  
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
 from layoffs_staging2;

insert into layoffs_staging2
select * ,
ROW_NUMBER() OVER (
PARTITION BY company,location,industry, percentage_laid_off ,total_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging ;
 
delete  
from layoffs_staging2 
where row_num > 1 ;

select *
from layoffs_staging2 ;

-- Standardizing data 

select company , trim(company )
from layoffs_staging2 ;

update layoffs_staging2 
set company = trim(company) ;

select distinct industry
from layoffs_staging2 
order by 1 ;

select distinct industry
from layoffs_staging2 ;

update layoffs_staging2 
set industry = 'Crypto'
where industry like 'Crypto%' ;  

select *
from layoffs_staging2;

select distinct country 
from layoffs_staging2
order by 1 ;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
where country like 'United States%' ;


select `date` 
-- STR_TO_DATE(`date`, '%Y-%m-%d')
from layoffs_staging2 ;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%Y-%m-%d');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- 3. Look at Null Values

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';


select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
     on t1.company = t2.company
where (t1.industry is null or t1.industry =' ' )
and t2.industry is not null ;

update layoffs_staging2 t1
join layoffs_staging2 t2
     on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null ;

select *
from layoffs_staging2
where company = 'Airbnb';

-- 4.Remove Unnecessary Columns/Rows: 

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM world_layoffs.layoffs_staging2;








