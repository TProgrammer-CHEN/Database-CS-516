ALTER TABLE inproceedings ADD Area varchar;

update inproceedings
SET area = case when booktitle in ('SIGMOD Conference','VLDB','ICDE','PODS') then 'Database'
				when booktitle in ('STOC','FOCS','SODA','ICALP') then 'Theory'
				when booktitle in ('SIGCOMM','ISCA','HPCA','PLDI') then 'System'
				when booktitle in ('ICML','NIPS','AAAI','IJCAI') then 'ML-AI'
				else 'UNKNOWN' end

    