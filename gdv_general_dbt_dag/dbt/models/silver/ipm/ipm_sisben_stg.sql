{{ config(
    materialized='view',
    schema=var('silver_dataset'),
    alias='ipm_sisben_transformed_data',
    description='Modelo de staging que agrega columnas de descripción (DESCRIPCION) para códigos numéricos, utilizando la nomenclatura prefijada de la tabla BigQuery.'
) }}

WITH source_data AS (
    SELECT *
    FROM {{ source('bronze_ipm_sisben', 'ipm_sisben_raw') }}
)

SELECT
    -- 1. Columnas originales (Nombres prefijados)
    t1._2_num_ficha,
    t1._3_cod_departamento,
    t1.departamento,
    t1._4_cod_municipio,
    t1.municipio,

    -- 2. Transformaciones con descripciones (cod_clase)
    t1._6_cod_clase,
    CASE
        WHEN t1._6_cod_clase = '1' THEN 'CABECERA'
        WHEN t1._6_cod_clase = '2' THEN 'CENTRO POBLADO'
        WHEN t1._6_cod_clase = '3' THEN 'RURAL DISPERSO'
        ELSE 'OTRO/NO APLICA'
    END AS _6_cod_clase_DESCRIPCION,

    -- 3. tip_vivienda
    t1._25_tipo_vivienda,
    CASE
        WHEN t1._25_tipo_vivienda = '1' THEN 'CASA'
        WHEN t1._25_tipo_vivienda = '2' THEN 'APARTAMENTO'
        WHEN t1._25_tipo_vivienda = '3' THEN 'CUARTO'
        WHEN t1._25_tipo_vivienda = '4' THEN 'OTRO TIPO DE VIVIENDA'
        WHEN t1._25_tipo_vivienda = '5' THEN 'VIVIENDA INDÍGENA'
        ELSE 'OTRO/NO APLICA'
    END AS _25_tipo_vivienda_DESCRIPCION,

    -- 4. tip_mat_paredes
    t1._26_tipo_material_paredes,
    CASE
        WHEN t1._26_tipo_material_paredes = '1' THEN 'BLOQUE, LADRILLO, PIEDRA, MADERA PULIDA'
        WHEN t1._26_tipo_material_paredes = '2' THEN 'TAPIA PISADA, ADOBE'
        WHEN t1._26_tipo_material_paredes = '3' THEN 'BAHAREQUE'
        WHEN t1._26_tipo_material_paredes = '4' THEN 'MATERIAL PREFABRICADO'
        WHEN t1._26_tipo_material_paredes = '5' THEN 'MADERA BURDA, TABLA, TABLÓN'
        WHEN t1._26_tipo_material_paredes = '6' THEN 'GUADUA, CASA, ESTERILLA, OTRO VEGETAL'
        WHEN t1._26_tipo_material_paredes = '7' THEN 'ZINC, TELA, LONA, CARTÓN, LATAS, DESECHOS, PLÁSTICO'
        WHEN t1._26_tipo_material_paredes = '0' THEN 'SIN PAREDES'
        ELSE 'OTRO/NO APLICA'
    END AS _26_tipo_material_paredes_DESCRIPCION,

    -- 5. ind_tiene_energia
    t1._28_energia,
    CASE WHEN t1._28_energia = '1' THEN 'SI' WHEN t1._28_energia = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _28_energia_DESCRIPCION,
    
    -- 6. tip_estrato_energia
    t1._29_estrato_energia,
    CASE
        WHEN t1._29_estrato_energia = '0' THEN 'NO TIENE'
        WHEN t1._29_estrato_energia BETWEEN '1' AND '6' THEN t1._29_estrato_energia -- El valor es la misma cadena '1'...'6'
        WHEN t1._29_estrato_energia = '9' THEN 'NO SABE'
        WHEN t1._29_estrato_energia = '99' THEN 'NO APLICA POR FLUJO'
        ELSE 'OTRO/NO APLICA'
    END AS _29_estrato_energia_DESCRIPCION,

    -- 7. ind_tiene_alcantarillado
    t1._30_alcantarillado,
    CASE WHEN t1._30_alcantarillado = '1' THEN 'SI' WHEN t1._30_alcantarillado = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _30_alcantarillado_DESCRIPCION,

    -- 8. ind_tiene_gas
    t1._31_gas,
    CASE WHEN t1._31_gas = '1' THEN 'SI' WHEN t1._31_gas = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _31_gas_DESCRIPCION,

    -- 9. ind_tiene_recoleccion
    t1._32_recoleccion_basura,
    CASE WHEN t1._32_recoleccion_basura = '1' THEN 'SI' WHEN t1._32_recoleccion_basura = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _32_recoleccion_basura_DESCRIPCION,

    -- 10. ind_tiene_acueducto
    t1._33_acueducto,
    CASE WHEN t1._33_acueducto = '1' THEN 'SI' WHEN t1._33_acueducto = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _33_acueducto_DESCRIPCION,

    -- 11. tip_estrato_acueducto (usando _34_estrato_energia)
    t1._34_estrato_energia, -- Usando el nombre de columna de la lista
    CASE
        WHEN t1._34_estrato_energia = '0' THEN 'NO TIENE'
        WHEN t1._34_estrato_energia BETWEEN '1' AND '6' THEN t1._34_estrato_energia -- El valor es la misma cadena '1'...'6'
        WHEN t1._34_estrato_energia = '9' THEN 'NO SABE'
        WHEN t1._34_estrato_energia = '99' THEN 'NO APLICA POR FLUJO'
        ELSE 'OTRO/NO APLICA'
    END AS _34_estrato_energia_ACUEDUCTO_DESCRIPCION, -- Renombrado para evitar confusión con el campo 29
    
    -- 12. tip_ocupa_vivienda
    t1._38_tip_ocupacion,
    CASE
        WHEN t1._38_tip_ocupacion = '1' THEN 'EN ARRIENDO O SUBARRIENDO'
        WHEN t1._38_tip_ocupacion = '2' THEN 'PROPIA, LA ESTÁN PAGANDO'
        WHEN t1._38_tip_ocupacion = '3' THEN 'PROPIA, TOTALMENTE PAGADA'
        WHEN t1._38_tip_ocupacion = '4' THEN 'CON PERMISO DEL PROPIETARIO'
        WHEN t1._38_tip_ocupacion = '5' THEN 'POSESIÓN SIN TÍTULO, OCUPANTE DE HECHO'
        ELSE 'OTRO/NO APLICA'
    END AS _38_tip_ocupacion_DESCRIPCION,

    -- 13. tip_origen_agua
    t1._45_orig_agua,
    CASE
        WHEN t1._45_orig_agua = '1' THEN 'ACUEDUCTO'
        WHEN t1._45_orig_agua = '2' THEN 'POZO CON BOMBA'
        WHEN t1._45_orig_agua = '3' THEN 'POZO SIN BOMBA, JAGÜEY'
        WHEN t1._45_orig_agua = '4' THEN 'AGUA LLUVIA'
        WHEN t1._45_orig_agua = '5' THEN 'RÍO, QUEBRADA, MANANTIAL O NACIMIENTO'
        WHEN t1._45_orig_agua = '6' THEN 'PILA PÚBLICA'
        WHEN t1._45_orig_agua = '7' THEN 'CARRO TANQUE'
        WHEN t1._45_orig_agua = '8' THEN 'AGUATERO'
        WHEN t1._45_orig_agua = '9' THEN 'AGUA EMBOTELLADA O EN BOLSA'
        ELSE 'OTRO/NO APLICA'
    END AS _45_orig_agua_DESCRIPCION,

    -- 14. ind_agua_llega_7dias
    t1._46_agua_7_dias,
    CASE WHEN t1._46_agua_7_dias = '1' THEN 'SI' WHEN t1._46_agua_7_dias = '2' THEN 'NO' WHEN t1._46_agua_7_dias = '9' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _46_agua_7_dias_DESCRIPCION,

    -- 15. tip_uso_agua_beber
    t1._50_agua_beber,
    CASE
        WHEN t1._50_agua_beber = '1' THEN 'LA USAN TAL COMO LA OBTIENEN'
        WHEN t1._50_agua_beber = '2' THEN 'LA HIERVEN'
        WHEN t1._50_agua_beber = '3' THEN 'LE ECHAN CLORO'
        WHEN t1._50_agua_beber = '4' THEN 'UTILIZAN FILTROS'
        WHEN t1._50_agua_beber = '5' THEN 'LA DECANTAN O USAN FILTROS NATURALES'
        WHEN t1._50_agua_beber = '6' THEN 'COMPRAN AGUA EMBOTELLADA O EN BOLSA'
        ELSE 'OTRO/NO APLICA'
    END AS _50_agua_beber_DESCRIPCION,

    -- 16. tip_elimina_basura
    t1._51_elimina_basura,
    CASE
        WHEN t1._51_elimina_basura = '1' THEN 'LA RECOGEN LOS SERVICIOS DEL ASEO'
        WHEN t1._51_elimina_basura = '2' THEN 'LA ENTIERRAN'
        WHEN t1._51_elimina_basura = '3' THEN 'LA QUEMAN'
        WHEN t1._51_elimina_basura = '4' THEN 'LA TIRAN A UN PATIO, LOTE, ZANJA O BALDÍO'
        WHEN t1._51_elimina_basura = '5' THEN 'LA TIRAN A UN RÍO, QUEBRADA, CAÑO O LAGUNA'
        WHEN t1._51_elimina_basura = '6' THEN 'LA RECOGE UN SERVICIO INFORMAL (ZORRA, CARRETA)'
        WHEN t1._51_elimina_basura = '7' THEN 'LA ELIMINAN DE OTRA FORMA'
        ELSE 'OTRO/NO APLICA'
    END AS _51_elimina_basura_DESCRIPCION,

    -- 17. num_habita_vivienda
    t1._80_num_hab,
    CASE
        WHEN t1._80_num_hab = '1' THEN 'MENOS DE UN AÑO'
        WHEN t1._80_num_hab = '2' THEN 'ENTRE 1 Y 5 AÑOS'
        WHEN t1._80_num_hab = '3' THEN 'ENTRE 5 Y 10 AÑOS'
        WHEN t1._80_num_hab = '4' THEN 'MÁS DE 10 AÑOS'
        ELSE 'OTRO/NO APLICA'
    END AS _80_num_hab_DESCRIPCION,

    -- 18. sexo_persona
    t1._101_sexo,
    CASE WHEN t1._101_sexo = '1' THEN 'HOMBRE' WHEN t1._101_sexo = '2' THEN 'MUJER' ELSE 'OTRO/NO APLICA' END AS _101_sexo_DESCRIPCION,

    -- 19. tip_documento
    t1._102_tip_doc,
    CASE
        WHEN t1._102_tip_doc = '1' THEN 'REGISTRO CIVIL'
        WHEN t1._102_tip_doc = '2' THEN 'TARJETA DE IDENTIDAD'
        WHEN t1._102_tip_doc = '3' THEN 'CÉDULA DE CIUDADANÍA'
        WHEN t1._102_tip_doc = '4' THEN 'CÉDULA DE EXTRANJERÍA'
        WHEN t1._102_tip_doc = '5' THEN 'DNI (PAÍS DE ORIGEN)'
        WHEN t1._102_tip_doc = '6' THEN 'PASAPORTE'
        WHEN t1._102_tip_doc = '7' THEN 'SALVOCONDUCTO PARA REFUGIADO'
        WHEN t1._102_tip_doc = '8' THEN 'PERMISO ESPECIAL DE PERMANENCIA (PEP)'
        ELSE 'OTRO/NO APLICA'
    END AS _102_tip_doc_DESCRIPCION,

    -- 20. ind_discap_ver
    t1._117_disca_ver,
    CASE WHEN t1._117_disca_ver = '1' THEN 'SI' WHEN t1._117_disca_ver = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _117_disca_ver_DESCRIPCION,
    
    -- 21. ind_discap_oir
    t1._118_disca_oir,
    CASE WHEN t1._118_disca_oir = '1' THEN 'SI' WHEN t1._118_disca_oir = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _118_disca_oir_DESCRIPCION,
    
    -- 22. ind_discap_hablar
    t1._119_disca_hablar,
    CASE WHEN t1._119_disca_hablar = '1' THEN 'SI' WHEN t1._119_disca_hablar = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _119_disca_hablar_DESCRIPCION,
    
    -- 23. ind_discap_moverse
    t1._120_disca_moverse,
    CASE WHEN t1._120_disca_moverse = '1' THEN 'SI' WHEN t1._120_disca_moverse = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _120_disca_moverse_DESCRIPCION,

    -- 24. ind_discap_bañarse
    t1._121_disca_banarse,
    CASE WHEN t1._121_disca_banarse = '1' THEN 'SI' WHEN t1._121_disca_banarse = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _121_disca_banarse_DESCRIPCION,

    -- 25. ind_discap_salir
    t1._122_disca_salir,
    CASE WHEN t1._122_disca_salir = '1' THEN 'SI' WHEN t1._122_disca_salir = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _122_disca_salir_DESCRIPCION,

    -- 26. ind_discap_entender
    t1._123_disca_entender,
    CASE WHEN t1._123_disca_entender = '1' THEN 'SI' WHEN t1._123_disca_entender = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _123_disca_entender_DESCRIPCION,

    -- 27. ind_discap_ninguna
    t1._124_disca_ninguna,
    CASE WHEN t1._124_disca_ninguna = '1' THEN 'SI' WHEN t1._124_disca_ninguna = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _124_disca_ninguna_DESCRIPCION,

    -- 28. seg_social
    t1._125_seg_social,
    CASE
        WHEN t1._125_seg_social = '1' THEN 'CONTRIBUTIVO'
        WHEN t1._125_seg_social = '2' THEN 'ESPECIAL (FUERZAS ARMADAS, ECOPETROL, UNIVERSIDADES PÚBLICAS, MAGISTERIO)'
        WHEN t1._125_seg_social = '3' THEN 'SUBSIDIADO (EPS-S)'
        WHEN t1._125_seg_social = '0' THEN 'NINGUNA'
        WHEN t1._125_seg_social = '9' THEN 'NO SABE'
        ELSE 'OTRO/NO APLICA'
    END AS _125_seg_social_DESCRIPCION,

    -- 29. ind_enfermo_30
    t1._126_enfermo,
    CASE WHEN t1._126_enfermo = '1' THEN 'SI' WHEN t1._126_enfermo = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _126_enfermo_DESCRIPCION,

    -- 30. ind_acudio_salud
    t1._127_acudio_salud,
    CASE WHEN t1._127_acudio_salud = '1' THEN 'SI' WHEN t1._127_acudio_salud = '2' THEN 'NO' WHEN t1._127_acudio_salud = '9' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _127_acudio_salud_DESCRIPCION,

    -- 31. ind_fue_atendido_salud
    t1._128_fue_atendido,
    CASE WHEN t1._128_fue_atendido = '1' THEN 'SI' WHEN t1._128_fue_atendido = '2' THEN 'NO' WHEN t1._128_fue_atendido = '9' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _128_fue_atendido_DESCRIPCION,

    -- 32. ind_esta_embarazada
    t1._129_estaba_embarazada,
    CASE WHEN t1._129_estaba_embarazada = '1' THEN 'SI' WHEN t1._129_estaba_embarazada = '2' THEN 'NO' WHEN t1._129_estaba_embarazada = '9' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _129_estaba_embarazada_DESCRIPCION,

    -- 33. ind_tuvo_hijos
    t1._130_tuvo_hijos,
    CASE WHEN t1._130_tuvo_hijos = '1' THEN 'SI' WHEN t1._130_tuvo_hijos = '2' THEN 'NO' WHEN t1._130_tuvo_hijos = '9' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _130_tuvo_hijos_DESCRIPCION,

    -- 34. tip_cuidado_niños
    t1._131_cuidado_hijos,
    CASE
        WHEN t1._131_cuidado_hijos = '1' THEN 'ASISTE A UN LUGAR COMUNITARIO, JARDÍN O CENTRO DE DESARROLLO INFANTIL O COLEGIO'
        WHEN t1._131_cuidado_hijos = '2' THEN 'CON SU PADRE O MADRE EN LA CASA'
        WHEN t1._131_cuidado_hijos = '3' THEN 'CON SU PADRE O MADRE EN EL TRABAJO'
        WHEN t1._131_cuidado_hijos = '4' THEN 'CON EMPLEADA O NIÑERA EN LA CASA'
        WHEN t1._131_cuidado_hijos = '5' THEN 'AL CUIDADO DE UN PARIENTE DE 18 AÑOS O MÁS'
        WHEN t1._131_cuidado_hijos = '6' THEN 'AL CUIDADO DE UN PARIENTE MENOR DE 18 AÑOS'
        WHEN t1._131_cuidado_hijos = '7' THEN 'EN CASA SOLO'
        WHEN t1._131_cuidado_hijos = '9' THEN 'NO APLICA POR FLUJO'
        ELSE 'OTRO/NO APLICA'
    END AS _131_cuidado_hijos_DESCRIPCION,

    -- 35. ind_recibe_comida
    t1._132_recibe_comida,
    CASE WHEN t1._132_recibe_comida = '1' THEN 'SI' WHEN t1._132_recibe_comida = '2' THEN 'NO' ELSE 'OTRO/NO APLICA' END AS _132_recibe_comida_DESCRIPCION,

    -- 36. ind_leer_escribir
    t1._133_sabe_leer,
    CASE WHEN t1._133_sabe_leer = '1' THEN 'SI' WHEN t1._133_sabe_leer = '2' THEN 'NO' WHEN t1._133_sabe_leer = '9' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _133_sabe_leer_DESCRIPCION,

    -- 37. ind_estudia
    t1._134_estudia,
    CASE WHEN t1._134_estudia = '1' THEN 'SI' WHEN t1._134_estudia = '2' THEN 'NO' WHEN t1._134_estudia = '9' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _134_estudia_DESCRIPCION,

    -- 38. niv_educativo
    t1._135_nivel_educativo,
    CASE
        WHEN t1._135_nivel_educativo = '0' THEN 'NINGUNO'
        WHEN t1._135_nivel_educativo = '1' THEN 'PREESCOLAR'
        WHEN t1._135_nivel_educativo = '2' THEN 'BÁSICA PRIMARIA (1O. - 5O)'
        WHEN t1._135_nivel_educativo = '3' THEN 'BÁSICA SECUNDARIA (6O. - 9O.)'
        WHEN t1._135_nivel_educativo = '4' THEN 'MEDIA (10O. 13O.)'
        WHEN t1._135_nivel_educativo = '5' THEN 'TÉCNICO O TECNOLÓGICO'
        WHEN t1._135_nivel_educativo = '6' THEN 'UNIVERSITARIO'
        WHEN t1._135_nivel_educativo = '7' THEN 'POSTGRADO'
        WHEN t1._135_nivel_educativo = '9' THEN 'NO APLICA POR FLUJO'
        ELSE 'OTRO/NO APLICA'
    END AS _135_nivel_educativo_DESCRIPCION,

    -- 39. ind_fondo_pensiones
    t1._137_fondo_pensiones,
    CASE WHEN t1._137_fondo_pensiones = '1' THEN 'SI' WHEN t1._137_fondo_pensiones = '2' THEN 'NO' WHEN t1._137_fondo_pensiones = '3' THEN 'PENSIONADO' WHEN t1._137_fondo_pensiones = '9' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _137_fondo_pensiones_DESCRIPCION,

    -- 40. tip_actividad_mes
    t1._138_actividad_mes,
    CASE
        WHEN t1._138_actividad_mes = '1' THEN 'TRABAJANDO'
        WHEN t1._138_actividad_mes = '2' THEN 'BUSCANDO TRABAJO'
        WHEN t1._138_actividad_mes = '3' THEN 'ESTUDIANDO'
        WHEN t1._138_actividad_mes = '4' THEN 'OFICIOS DEL HOGAR'
        WHEN t1._138_actividad_mes = '5' THEN 'RENTISTA'
        WHEN t1._138_actividad_mes = '6' THEN 'JUBILADO O PENSIONADO'
        WHEN t1._138_actividad_mes = '7' THEN 'INCAPACITADO PERMANENTEMENTE'
        WHEN t1._138_actividad_mes = '0' THEN 'SIN ACTIVIDAD'
        WHEN t1._138_actividad_mes = '9' THEN 'NO APLICA POR FLUJO'
        ELSE 'OTRO/NO APLICA'
    END AS _138_actividad_mes_DESCRIPCION,

    -- 41. tip_empleado
    t1._140_empleado,
    CASE
        WHEN t1._140_empleado = '1' THEN 'EMPLEADO DE EMPRESA PARTICULAR'
        WHEN t1._140_empleado = '2' THEN 'EMPLEADO DEL GOBIERNO'
        WHEN t1._140_empleado = '3' THEN 'EMPLEADO DOMÉSTICO'
        WHEN t1._140_empleado = '4' THEN 'PROFESIONAL INDEPENDIENTE'
        WHEN t1._140_empleado = '5' THEN 'TRABAJADOR INDEPENDIENTE O POR CUENTA PROPIA'
        WHEN t1._140_empleado = '6' THEN 'PATRÓN O EMPLEADOR'
        WHEN t1._140_empleado = '7' THEN 'TRABAJADOR DE FINCA, TIERRA O PARCELA PROPIA, EN ARRIENDO, APARCERÍA O USUFRUCTO'
        WHEN t1._140_empleado = '8' THEN 'TRABAJADOR SIN REMUNERACIÓN'
        WHEN t1._140_empleado = '9' THEN 'AYUDANTE SIN REMUNERACIÓN'
        WHEN t1._140_empleado = '10' THEN 'JORNALERO O PEÓN'
        WHEN t1._140_empleado = '99' THEN 'NO APLICA POR FLUJO'
        ELSE 'OTRO/NO APLICA'
    END AS _140_empleado_DESCRIPCION,

    -- 42. Indicador Ingreso Salario
    t1._141_ingreso,
    CASE WHEN t1._141_ingreso = '1' THEN 'RECIBE' WHEN t1._141_ingreso = '2' THEN 'NO RECIBE' WHEN t1._141_ingreso = '9' THEN 'RECIBE, PERO NO SABE' WHEN t1._141_ingreso = '99' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _141_ingreso_SALARIO_DESCRIPCION,

    -- 43. Indicador Honorarios
    t1._143_ind_honorarios_mes,
    CASE WHEN t1._143_ind_honorarios_mes = '1' THEN 'RECIBE' WHEN t1._143_ind_honorarios_mes = '2' THEN 'NO RECIBE' WHEN t1._143_ind_honorarios_mes = '9' THEN 'RECIBE, PERO NO SABE' WHEN t1._143_ind_honorarios_mes = '99' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _143_ind_honorarios_mes_DESCRIPCION,

    -- 44. Indicador Cosecha
    t1._145_ind_ingre_cosecha,
    CASE WHEN t1._145_ind_ingre_cosecha = '1' THEN 'RECIBE' WHEN t1._145_ind_ingre_cosecha = '2' THEN 'NO RECIBE' WHEN t1._145_ind_ingre_cosecha = '9' THEN 'RECIBE, PERO NO SABE' WHEN t1._145_ind_ingre_cosecha = '99' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _145_ind_ingre_cosecha_DESCRIPCION,

    -- 45. Indicador Pensión
    t1._148_ind_pension,
    CASE WHEN t1._148_ind_pension = '1' THEN 'RECIBE' WHEN t1._148_ind_pension = '2' THEN 'NO RECIBE' WHEN t1._148_ind_pension = '9' THEN 'RECIBE, PERO NO SABE' WHEN t1._148_ind_pension = '99' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _148_ind_pension_DESCRIPCION,

    -- 46. Indicador Remesa País
    t1._150_ind_ingreso_remesa,
    CASE WHEN t1._150_ind_ingreso_remesa = '1' THEN 'RECIBE' WHEN t1._150_ind_ingreso_remesa = '2' THEN 'NO RECIBE' WHEN t1._150_ind_ingreso_remesa = '9' THEN 'RECIBE, PERO NO SABE' WHEN t1._150_ind_ingreso_remesa = '99' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _150_ind_ingreso_remesa_PAIS_DESCRIPCION,

    -- 47. Indicador Remesa Exterior
    t1._152_ind_ingreso_remesa_ext,
    CASE WHEN t1._152_ind_ingreso_remesa_ext = '1' THEN 'RECIBE' WHEN t1._152_ind_ingreso_remesa_ext = '2' THEN 'NO RECIBE' WHEN t1._152_ind_ingreso_remesa_ext = '9' THEN 'RECIBE, PERO NO SABE' WHEN t1._152_ind_ingreso_remesa_ext = '99' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _152_ind_ingreso_remesa_ext_DESCRIPCION,

    -- 48. Indicador Arriendos
    t1._154_ind_ingreso_arrendo,
    CASE WHEN t1._154_ind_ingreso_arrendo = '1' THEN 'RECIBE' WHEN t1._154_ind_ingreso_arrendo = '2' THEN 'NO RECIBE' WHEN t1._154_ind_ingreso_arrendo = '9' THEN 'RECIBE, PERO NO SABE' WHEN t1._154_ind_ingreso_arrendo = '99' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _154_ind_ingreso_arrendo_DESCRIPCION,

    -- 49. Indicador Otros Ingresos
    t1._156_ind_otros_ingresos,
    CASE WHEN t1._156_ind_otros_ingresos = '1' THEN 'RECIBE' WHEN t1._156_ind_otros_ingresos = '2' THEN 'NO RECIBE' WHEN t1._156_ind_otros_ingresos = '9' THEN 'RECIBE, PERO NO SABE' WHEN t1._156_ind_otros_ingresos = '99' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _156_ind_otros_ingresos_DESCRIPCION,

    -- 50. Indicador Subsidios
    t1._158_ind_subsidios,
    CASE WHEN t1._158_ind_subsidios = '1' THEN 'RECIBE' WHEN t1._158_ind_subsidios = '2' THEN 'NO RECIBE' WHEN t1._158_ind_subsidios = '9' THEN 'RECIBE, PERO NO SABE' WHEN t1._158_ind_subsidios = '99' THEN 'NO APLICA POR FLUJO' ELSE 'OTRO/NO APLICA' END AS _158_ind_subsidios_DESCRIPCION,

    -- 51. IPM (Indicador Pobreza Multidimensional)
    t1._175 AS IPM_H5,
    CASE WHEN t1._175 = '0' THEN 'NO' WHEN t1._175 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _175_IPM_H5_DESCRIPCION,

    -- 52. IPM I_1 (Bajo logro educativo)
    t1._176 AS IMP_I1,
    CASE WHEN t1._176 = '0' THEN 'NO' WHEN t1._176 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _176_IMP_I1_DESCRIPCION,
    
    -- 53. IPM I_2 (Analfabetismo)
    t1._177 AS IMP_I2,
    CASE WHEN t1._177 = '0' THEN 'NO' WHEN t1._177 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _177_IMP_I2_DESCRIPCION,

    -- 54. IPM I_3 (Inasistencia escolar)
    t1._178 AS IMP_I3,
    CASE WHEN t1._178 = '0' THEN 'NO' WHEN t1._178 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _178_IMP_I3_DESCRIPCION,

    -- 55. IPM I_4 (Rezago escolar)
    t1._179 AS IMP_I4,
    CASE WHEN t1._179 = '0' THEN 'NO' WHEN t1._179 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _179_IMP_I4_DESCRIPCION,

    -- 56. IPM I_5 (Barreras a servicios p. infancia)
    t1._180 AS IMP_I5,
    CASE WHEN t1._180 = '0' THEN 'NO' WHEN t1._180 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _180_IMP_I5_DESCRIPCION,

    -- 57. IPM I_6 (Trabajo infantil)
    t1._181 AS IMP_I6,
    CASE WHEN t1._181 = '0' THEN 'NO' WHEN t1._181 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _181_IMP_I6_DESCRIPCION,

    -- 58. IPM I_7 (Desempleo de larga duración)
    t1._182 AS IMP_I7,
    CASE WHEN t1._182 = '0' THEN 'NO' WHEN t1._182 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _182_IMP_I7_DESCRIPCION,

    -- 59. IPM I_8 (Trabajo informal)
    t1._183 AS IMP_I8,
    CASE WHEN t1._183 = '0' THEN 'NO' WHEN t1._183 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _183_IMP_I8_DESCRIPCION,

    -- 60. IPM I_9 (Sin aseguramiento en salud)
    t1._184 AS IMP_I9,
    CASE WHEN t1._184 = '0' THEN 'NO' WHEN t1._184 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _184_IMP_I9_DESCRIPCION,

    -- 61. IPM I_10 (Barreras de acceso a servicios de salud)
    t1._185 AS IMP_I10,
    CASE WHEN t1._185 = '0' THEN 'NO' WHEN t1._185 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _185_IMP_I10_DESCRIPCION,

    -- 62. IPM I_11 (Sin acceso a fuentes de agua mejorada)
    t1._186 AS IMP_I11,
    CASE WHEN t1._186 = '0' THEN 'NO' WHEN t1._186 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _186_IMP_I11_DESCRIPCION,

    -- 63. IPM I_12 (Inadecuada eliminación de excretas)
    t1._187 AS IMP_I12,
    CASE WHEN t1._187 = '0' THEN 'NO' WHEN t1._187 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _187_IMP_I12_DESCRIPCION,

    -- 64. IPM I_13 (Material inadecuado de pisos)
    t1._188 AS IMP_I13,
    CASE WHEN t1._188 = '0' THEN 'NO' WHEN t1._188 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _188_IMP_I13_DESCRIPCION,

    -- 65. IPM I_14 (Material inadecuado de paredes exteriores)
    t1._189 AS IMP_I14,
    CASE WHEN t1._189 = '0' THEN 'NO' WHEN t1._189 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _189_IMP_I14_DESCRIPCION,

    -- 66. IPM I_15 (Hacinamiento crítico)
    t1._190 AS IMP_I15,
    CASE WHEN t1._190 = '0' THEN 'NO' WHEN t1._190 = '1' THEN 'SI' ELSE 'OTRO/NO APLICA' END AS _190_IMP_I15_DESCRIPCION,
    
    -- 67. Columnas Sisbén IV
    t1._192_grupo,
    t1._193_nivel,

    -- 68. Resto de columnas (Seleccionadas directamente)
    t1._35_cuartos,
    t1._36_hogares,
    t1._37_ide_hogar,
    t1._39_cuartos_excl,
    t1._40_cuartos_dorm,
    t1._41_cuartos_dorm_uni,
    t1._42_sanitario,
    t1._43_tip_sanitario,
    t1._44_uso_sanitario,
    t1._47_num_dias_agua,
    t1._48_agua_llega,
    t1._49_horas_agua,
    t1._52_tiene_cocina,
    t1._53_prepara_alimentos,
    t1._54_tipo_cocina,
    t1._55_tip_ener_coc,
    t1._56_nevera,
    t1._57_lavadora,
    t1._58_pc,
    t1._59_internet,
    t1._60_moto,
    t1._61_tractor,
    t1._62_carro,
    t1._63_bien_raiz,
    t1._64_tiene_gasto_ali,
    t1._65_gasto, -- Gasto Alimentación
    t1._66_tiene_gasto_trans,
    t1._67_gasto_transporte,
    t1._68_tiene_gasto_educ,
    t1._69_gasto_educ,
    t1._70_tiene_gasto_salud,
    t1._71_gasto_salud,
    t1._72_tiene_gasto_servpub,
    t1._73_gasto_servpub,
    t1._74_tiene_gasto_cel,
    t1._75_gasto_cel,
    t1._76_tiene_gasto_arren,
    t1._77_gasto_arrendo,
    t1._78_tiene_gastos_otros,
    t1._79_gastos_otros,
    t1._79_gastos_otros_1,
    t1._81_inundacion,
    t1._82_num_inundacion,
    t1._83_avalancha,
    t1._84_num_avalancha,
    t1._85_terremoto,
    t1._86_num_terremoto,
    t1._87_incendio,
    t1._88_num_incendio,
    t1._89_vendaval,
    t1._90_num_vendaval,
    t1._91_hundimiento,
    t1._92_num_hundimiento,
    t1._93_num_pers_hogar,
    t1._95_ord_persona,
    t1.xxx, -- Columna intermedia sin mapeo
    t1._105_edad,
    t1._109_tip_parentesci,
    t1._110_estado_civil,
    t1._111_conyuge_vive_hogar,
    t1._112_ide_conyuge,
    t1._113_padre_vive_hogar,
    t1._114_ide_padre,
    t1._115_pariente,
    t1._116_serv_domestico,
    t1._136_grado,
    t1._139_sem_buscando,
    t1._142_salario,
    t1._144_honorarios_mes,
    t1._146_ingre_cosecha,
    t1._147_valor_cosecha,
    t1._149_ingre_pension,
    t1._151_ingreso_remesa,
    t1._153_ingreso_remesa_ext,
    t1._155_ingreso_arrendo,
    t1._157_otros_ingresos,
    t1._159_fam_accion,
    t1._160_col_mayor,
    t1._161_otro_subsidio
    
FROM
    source_data t1

