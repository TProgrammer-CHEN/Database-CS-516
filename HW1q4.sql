with 
jy as  
(select 
case when year<=1959 and year>=1950 then '1950-1959'
when year<=1969 and year>=1960 then '1960-1969'
when year<=1979 and year>=1970 then '1970-1979'
when year<=1989 and year>=1980 then '1980-1989'
when year<=1999 and year>=1990 then '1990-1999'
when year<=2009 and year>=2000 then '2000-2009'
when year>=2010 then '2010-'
end as decade from article),
cy as  
(select 
case when year<=1959 and year>=1950 then '1950-1959'
when year<=1969 and year>=1960 then '1960-1969'
when year<=1979 and year>=1970 then '1970-1979'
when year<=1989 and year>=1980 then '1980-1989'
when year<=1999 and year>=1990 then '1990-1999'
when year<=2009 and year>=2000 then '2000-2009'
when year>=2010 then '2010-'
end as decade from inproceedings),
jyn as(select decade, count(*) as nofj from jy group by decade),
cyn as (select decade, count(*) as nofc from cy group by decade)
select jyn.decade,jyn.nofj,cyn.nofc from jyn inner join cyn on jyn.decade=cyn.decade order by decade


with ia as (select i.pubkey as key,a.author as author,i.area as area,
case when i.year<=1959 and i.year>=1950 then '1950-1959'
when i.year<=1969 and i.year>=1960 then '1960-1969'
when i.year<=1979 and i.year>=1970 then '1970-1979'
when i.year<=1989 and i.year>=1980 then '1980-1989'
when i.year<=1999 and i.year>=1990 then '1990-1999'
when i.year<=2009 and i.year>=2000 then '2000-2009'
when i.year>=2010 then '2010-'
end as decade from inproceedings i inner join authorship a on a.pubkey=i.pubkey where i.area!='UNKNOWN'),
ac as (select ia.key as key,ia.author as author,a.author as colab, ia.area as area,ia.decade as decade 
from ia inner join authorship a on a.pubkey=ia.key where a.author!=ia.author),
aatn as (select author,area,decade,count(distinct colab) as cln from ac group by area,author,decade)
select decade,area, avg(cln) from aatn group by area,decade order by decade, area

with ia as (select i.pubkey as key,i.area as area, a.author as author,
case when i.year<=1959 and i.year>=1950 then '1950-1959'
when i.year<=1969 and i.year>=1960 then '1960-1969'
when i.year<=1979 and i.year>=1970 then '1970-1979'
when i.year<=1989 and i.year>=1980 then '1980-1989'
when i.year<=1999 and i.year>=1990 then '1990-1999'
when i.year<=2009 and i.year>=2000 then '2000-2009'
when i.year>=2010 then '2010-'
end as decade 
from inproceedings i inner join authorship a on i.pubkey=a.pubkey where i.area!='UNKNOWN'),
x as (select key,area,decade, count(*) as n from ia group by key,area,decade)
select decade,area,avg(n) as mean from x group by decade,area order by decade,area

with q4c as (
    with ia as (select i.pubkey as key,i.area as area, a.author as author,
case when i.year<=1959 and i.year>=1950 then '1950-1959'
when i.year<=1969 and i.year>=1960 then '1960-1969'
when i.year<=1979 and i.year>=1970 then '1970-1979'
when i.year<=1989 and i.year>=1980 then '1980-1989'
when i.year<=1999 and i.year>=1990 then '1990-1999'
when i.year<=2009 and i.year>=2000 then '2000-2009'
when i.year>=2010 then '2010-'
end as decade 
from inproceedings i inner join authorship a on i.pubkey=a.pubkey where i.area!='UNKNOWN'),
x as (select key,area,decade, count(*) as n from ia group by key,area,decade)
select area,decade,avg(n) as mean from x group by decade,area)

,xy as (select area,
case when decade='1950-1959' then 1950
when decade='1960-1969' then 1960
when decade='1970-1979' then 1970
when decade='1980-1989' then 1980
when decade='1990-1999' then 1990
when decade='2000-2009' then 2000
when decade='2010-' then 2010
end as decade,mean from q4c)
select area,(count(*)*sum(decade*mean)-sum(decade)*sum(mean))/(count(*)*sum(decade^2)-sum(decade)^2) as coef 
from xy group by area