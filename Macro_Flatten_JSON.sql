/* Creating dynamic pivot macro with input values */
{% macro get_flatten_json (data_model, json_column, primary_key) -%}


with low_level_flatten as (
	select f.key as json_key, f.path as json_path, 
	f.value as json_value, {{json_column}}:id::string AS id
	from {{data_model}}, 
	lateral flatten(input => {{json_column}}, recursive => true ) f
)

,get_json_path as (
	select distinct id, json_path, 
    trim((replace((regexp_replace(json_path,'\\(|\\)|\\$|\\@|\\.|\\[|]|-|"|"','_')),'''','')),'_') as json_path,json_key,
    to_varchar(json_value::string) as json_value
	from low_level_flatten
	where not contains(json_value, '{')
  )
  
select * from get_json_path

{%- endmacro %}
