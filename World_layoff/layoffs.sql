### DATA CLEANING
#1) Remove duplicates
#2)standardize the data
#3) remove null values or blank values
#4)remove any columns or row which is not needed.

select * from layoffs;
create table layoffs_staging 
like layoffs;

select * from layoffs_staging;

insert  layoffs_staging select * from layoffs;

select * , 
row_number() over(
partition by company  , location , industry , total_laid_off , percentage_laid_off , 'date' , stage , country , funds_raised_millions) As row_no
from layoffs_staging;

with duplicate_cte  As (
select * , 
row_number() over(
partition by company  , location , industry , total_laid_off , percentage_laid_off , 'date' , stage , country , funds_raised_millions) As row_no
from layoffs_staging
)
 select *
 from duplicate_cte
 where row_no >1;
 
 select  * from layoffs_staging
 where company = 'Casper';
 
 
 CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int default null,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_no` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

insert into layoffs_staging2
select * , 
row_number() over(
partition by company  , location , industry , total_laid_off , percentage_laid_off , 'date' , stage , country , funds_raised_millions) As row_no
from layoffs_staging;

select * from layoffs_staging2
where row_no > 1;

SET SQL_SAFE_UPDATES = 0;

### trim removes extra white space in cell

update layoffs_staging2
set company = trim(company);

select distinct industry 
from layoffs_staging2 
order by 1;

###  by the above statement we can see there is crypto , cryptocurrency and crypto currency all are same thing so we will change it to crypto

update layoffs_staging2 
set industry = 'crypto'
where industry like 'crypto%';


### we can check for all columns
select distinct country
from layoffs_staging2
order by 1;

### we can check for all columns

### there is an full stop issue in United states country so we will fix it 

update layoffs_staging2 
set country = 'United states'
where country like 'united states%';

### there is another way to fixx above issue 

select distinct country , trim(trailing '.' from country)
from layoffs_staging2
order by 1;

## above query is to remove full stop .

## changing the format of date

select `date` ,
str_to_date(`date`, '%m/%d/%Y') 
from layoffs_staging2;

### updating date format

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y') 

select * from layoffs_staging2;



### now we can update `date` datatype

alter table layoffs_staging2
modify column `date` Date;

### now we will try to remove null values

## 1) look for the null values

select * from layoffs_staging2
where industry is null
OR industry = '';

###2) we find null and empty values in industry , we will try to check for the same company in other row if industry
## is mentioned example

select * from layoffs_staging2
where company = 'Airbnb';

## we have find out there is industry mentioned in other rows for same comapny so we can join them by self join
## we will first set empty spaces to null for no issue


select * from layoffs_staging2 t1
join layoffs_staging2 t2
    ON t1.company = t2.company
    and t1.industry = t2.industry
where (t1.industry is null or t1.industry ='')
And t2.industry is not null

## we will now update all blank spaces to null so for no issue 
update layoffs_staging2  
set industry = null
where industry = '';

#### now we will update the industry 
update  layoffs_staging2 t1
join layoffs_staging2 t2
    ON t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
And t2.industry is not null;

## we may be not required below values as total_laid_off and percentage_laid_off both are null means there is no use for us 
## so maybe we can delete

select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

## for deleting
delete  from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

 ###we will now drop row_no which we created at begining which may be not usefull now so
 
 alter table layoffs_staging2
 drop column row_no;

select * from layoffs_staging;

### now we have done some data cleaning

