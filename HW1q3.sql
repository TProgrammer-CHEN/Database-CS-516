select Area, count(*) from inproceedings group by Area

select author, count(*) as n
from inproceedings i inner join authorship a on a.pubkey=i.pubkey 
where Area='Database' group by author order by n desc limit 20

with an as (select a.author as author, count(distinct i.area) as nofarea 
from inproceedings i inner join authorship a on a.pubkey=i.pubkey 
where i.area!='UNKNOWN' group by a.author)
select count(author) from an where nofarea=2

with 
anj as (select a2.author as author,count(*) as nofj 
from article a1 inner join authorship a2 on a1.pubkey=a2.pubkey group by a2.author),
anc as (select a2.author as author,count(*) as nofc 
from inproceedings i inner join authorship a2 on i.pubkey=a2.pubkey group by a2.author)
select anj.author
from anj left join anc on anj.author=anc.author where anj.nofj>anc.nofc or anc.nofc is null

with 
namelist as 
(select distinct a.author 
from authorship a inner join inproceedings i on a.pubkey=i.pubkey where i.area='Database'),
keylist as 
(select pubkey from authorship where author IN (select author from namelist)),
modernpp as 
(select pubkey from article where pubkey in (select pubkey from keylist) and year>=2000
union
select pubkey from inproceedings where pubkey in (select pubkey from keylist) and year>=2000)
select author
from modernpp m inner join authorship a on m.pubkey=a.pubkey where author in(select author from namelist) group by author order by count(*) desc limit 5
