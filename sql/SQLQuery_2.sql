update wikipedia_articles_embeddings set title_vector_text3 = null
go

create index ixe on [dbo].[wikipedia_articles_embeddings] (id, content_vector_text3)
where (content_vector_text3 is null)
go

create index ixe on [dbo].[wikipedia_articles_embeddings] (id, content_vector_text3)
where (content_vector_text3 is null)
go

update [dbo].[wikipedia_articles_embeddings]
set content_vector_text3 = null
go

select count(*) from [dbo].[wikipedia_articles_embeddings]
where content_vector_text3 is null

select count(*) from [dbo].[wikipedia_articles_embeddings]
where title_vector_text3 is null