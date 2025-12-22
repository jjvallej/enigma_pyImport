
    
    

with all_values as (

    select
        anio as value_field,
        count(*) as n_records

    from `datagov-473122`.`silver_dpt_planeacion_municipal_dev`.`idi_transformed_data_consolidated`
    group by anio

)

select *
from all_values
where value_field not in (
    '2023','2024'
)


