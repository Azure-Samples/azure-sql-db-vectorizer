select 
    count(*)
from
    [dbo].[wikipedia_articles_embeddings] s 
left join
    [dbo].[wikipedia_articles_embeddings_title_embeddings] e on s.id = e.parent_id
where
    e.parent_id is null

select top(10)
    s.id,
    s.title
from
    [dbo].[wikipedia_articles_embeddings] s 
left join
    [dbo].[wikipedia_articles_embeddings_title_embeddings] e on s.id = e.parent_id
where
    e.parent_id is null
