-- how many rows were processed?
select count(distinct parent_id) from 
    [dbo].[wikipedia_articles] s 
left join
    [dbo].[wikipedia_articles_embeddings] e on s.id = e.parent_id
go

-- how many embeddings have been generated?
select count(*) from 
    [dbo].[wikipedia_articles] s 
left join
    [dbo].[wikipedia_articles_embeddings] e on s.id = e.parent_id
go

-- find the most similar articles to a given reference value
declare @e vector(1536);
select top(1)
    @e = e.title_vector_text3
from
    [dbo].[wikipedia_articles] s 
left join
    [dbo].[wikipedia_articles_embeddings] e on s.id = e.parent_id
where
    s.title = 'Alan Turing'; -- reference value

select top(10)
	s.id,
	title,
	vector_distance('cosine', @e, e.title_vector_text3) as distance
from 
	[dbo].[wikipedia_articles] s 
left join
    [dbo].[wikipedia_articles_embeddings] e on s.id = e.parent_id
where
	e.[title_vector_text3] is not null
order by
	distance 
	