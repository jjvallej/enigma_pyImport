
    
    

with dbt_test__target as (

  select cod_mpio as unique_field
  from `datagov-473122`.`silver_dpt_planeacion_municipal_dev`.`ipm_transformed_data`
  where cod_mpio is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


