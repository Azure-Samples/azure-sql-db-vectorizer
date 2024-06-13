alter table wikipedia_articles_embeddings
add title_vector_text3 varbinary(8000);

alter table wikipedia_articles_embeddings
add content_vector_text3 varbinary(8000);

create index ixe1 on [dbo].[wikipedia_articles_embeddings] (id, content_vector_text3)
where (content_vector_text3 is null)
go

create index ixe2 on [dbo].[wikipedia_articles_embeddings] (id, title_vector_text3)
where (title_vector_text3 is null)
go

select count(*) from wikipedia_articles_embeddings where title_vector_text3 is null;

