drop table if exists [dbo].[wikipedia_articles_embeddings_title_embeddings]
create table [dbo].[wikipedia_articles_embeddings_title_embeddings]
(
    id int identity(1,1) primary key nonclustered,
    parent_id int not null,
    embedding varbinary(8000) not null
)
go
create clustered index [ixc] on [dbo].[wikipedia_articles_embeddings_title_embeddings](parent_id)
go

select 
    count(*)
from
    [dbo].[wikipedia_articles_embeddings] s 
left join
    [dbo].[wikipedia_articles_embeddings_title_embeddings] e on s.id = e.parent_id
where
    e.parent_id is null


select top(100)
    s.id,
    s.title
from
    [dbo].[wikipedia_articles_embeddings] s 
left join
    [dbo].[wikipedia_articles_embeddings_title_embeddings] e on s.id = e.parent_id
where
    e.parent_id is null
