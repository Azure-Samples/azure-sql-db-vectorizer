/*
	Create table
*/
drop table if exists [dbo].[wikipedia_articles];
create table [dbo].[wikipedia_articles]
(
	[id] [int] not null,
	[url] [varchar](1000) not null,
	[title] [varchar](1000) not null,
	[text] [varchar](max) not null,
	[title_vector] [varchar](max) not null,
	[content_vector] [varchar](max) not null,
	[vector_id] [int] not null
)
go

/*
	Import data
*/
bulk insert dbo.[wikipedia_articles]
from 'wikipedia/vector_database_wikipedia_articles_embedded.csv'
with (
	data_source = 'openai_playground',
    format = 'csv',
    firstrow = 2,
    codepage = '65001',
	fieldterminator = ',',
	rowterminator = '0x0a',
    fieldquote = '"',
    batchsize = 1000,
    tablock
)
go

/*
	Remove unneded columns
*/
alter table [dbo].[wikipedia_articles]
drop column [title_vector]
go
alter table [dbo].[wikipedia_articles]
drop column [content_vector]
go
alter table [dbo].[wikipedia_articles]
drop column [vector_id]
go

/*
	Add primary key
*/
alter table [dbo].[wikipedia_articles]
add constraint pk__wikipedia_articles primary key clustered (id)
go

/*
	Add index on title
*/
create index [ix_title] on [dbo].[wikipedia_articles](title)
go

/*
	Verify data
*/
select top (10) * from [dbo].[wikipedia_articles]
go

select * from [dbo].[wikipedia_articles] where title = 'Alan Turing'
go



