

/*

13/03/2025 DBF:

Van a querer seguramente histórico por lo que abrá que realizar automatización de información
por periodos.
Para realizar el modelo previamente vamos a sacar el dato de un periodo concreto y tras revisión 
abriremos el modelo a los demás periodos y automatizaremos todo.
*/

#####################################################################################################
--1 IDENTIFICAMOS LA CARTERA CON TECHNOLOGY "HFC"
#####################################################################################################

--Clientes HFC (TODOS, NO SEGMENTA ENTRE CON BA Y SIN BA) --> 150731 registros (26/02)
CREATE OR REPLACE TABLE `mo-migrations-reporting.HFC.cartera_hfcdevel_ser_krt` AS   
SELECT
  customer_id,
  brand_ds,
  SEGMENT_DS,
  PHONE_NM,
  SERVICE_ID,
  TECHNOLOGY_DS,
  TECHNOLOGY_DS_POWERFI,
  ZIP_CODE,
  SW_SURF,
  GESCAL17
FROM
  `mm-corporate-reporting.FINANCE_BI.CUSTOMER_BASE_20250201` 
WHERE 
  daily = true
  AND BILLING_TYPE = 'POSTPAID'
  AND SERVICE_TYPE = 'BROADBAND'
  AND TECHNOLOGY_DS_POWERFI LIKE '%HFC%'
;

#####################################################################################################
--2 IDENTIFICAMOS LOS SERVICIOS CON HFC SIN BA DE LAS TABLAS QUE NOS PROPORCIONAN
#####################################################################################################
  
--Tabla para identificar las lineas HFC sin BA
--pdte meter cobertura FTTH que sale de esta misma query (correo de Noela)
CREATE OR REPLACE TABLE `mo-migrations-reporting.HFC.hfc_wout_ba_ser_krt` AS   
SELECT
  a.*
FROM
  `mm-datamart-kd.ZZ_SERVICES.service_base_net_krt_hfc` a --tabla prporcionada por el equipo de Finance para recoger las HFC sin BA
INNER JOIN
  `mm-datamart-kd.SERVICES.service_base_telco` b --Service_Base_Telco para cruzar las coincidencias de los servicios anteriores.
ON
  a.service_id = b.service_id
  AND a.data_source_ds = b.data_source_ds
  AND a.brand_ds = b.brand_ds
  AND a.service_type = b.service_type
  AND b.service_type IN (
    'FIX',
    'TV',
    'TELEVISION')
WHERE
  a.activation_date <= CURRENT_DATE()
  AND COALESCE(a.deactivation_date, '2030-12-31') > CURRENT_DATE()
;


#####################################################################################################
--3 IDENTIFICAMOS LOS SERVICIOS CON HFC Y CREAMOS FLAG PARA SEGMENTAR SIN BA Y CON BA
#####################################################################################################

--Generamos tabla con Flag para identificar dentro de la cartera HFC de Finance las que son Sin BA ya que no vienen identificadas de forma natural.
--Tabla que sería la referencia o base donde obtenemos todas los servicios HFC con BA y Sin BA Segmentados por un flag generado. (BA_TYPE)
CREATE OR REPLACE TABLE `mo-migrations-reporting.HFC.cartera_hfc_ser_krt`  AS   
SELECT 
  A.*,
  CASE WHEN B.PHONE_NM IS NOT NULL AND B.CUSTOMER_ID IS NOT NULL AND B.BRAND_DS IS NOT NULL THEN 'WOUT_BA'
  ELSE 'BA'
  END AS BA_TYPE
FROM `mo-migrations-reporting.HFC.cartera_hfcdevel_ser_krt` A
LEFT JOIN `mo-migrations-reporting.HFC.hfc_wout_ba_ser_krt` B
ON CAST(A.PHONE_NM AS STRING) = CAST(B.phone_nm AS STRING)
AND A.BRAND_DS = B.BRAND_DS
AND A.customer_id = B.customer_id
AND A.SERVICE_ID = B.service_id
;


#####################################################################################################
--4 DEBEMOS ENRIQUECER LA TABLA ANTERIOR CON LA INFORMACIÓN QUE SE NOS DEMANDA
#####################################################################################################
/*

1- Areas influencia --> GoogleSheet, esta se vuelca en la tabla bi-data-science-pilots.SCORING_MODELOS.AreasInfluencia

2- Huecos Bidasoa –> tiro de esta consulta de Javier Gijon:

select 
  distinct substr(gescal37, 1, 17) G17, 
  footprint_detail 
from `mm-datamart-kd.ACCESS_PROVISION.footprint_ftth_mm` 
where footprint_detail like '%BID%'
;

3- Nodos 2025 --> es el listado de nodos del fichero que adjunto.(Correo de Noela a DBF)

4- Cobertura FTTH --> Esta Info ahora está en las tablas que nos pasó David Arroyo la semana pasada (adjunta correo a DBF). 
Noela los quita directamente de los CRMs, tiene pendiente cambiar este punto a esta tabla de David Arroyo. (Nosotros ya estamos aplicando este paso con las tablas de David Arroyo. (paso n2 Query de Finance y David Arroyo y paso n3 donde aplicamos la identificación de las lineas)

5- Nodo HFC --> es el campo node asociado al hueco (campo geocode_plus), de las tablas:
mm-datamart-kd.SERVICES.gaps_coverage_r
mm-datamart-kd.SERVICES.gaps_coverage_tcnet
mm-datamart-kd.SERVICES.gaps_coverage_ektnet

6- Categorización --> es un campo calculado por Noela, para identificar el punto del proceso en el que se encuentran los clientes, se calcula teniendo en cuenta todas las variables que influyen en el proceso swap. Esta calculado en BBOO, y es este código:

=Si([cto_saturada]="CTO saturada";"CTO bloqueada";Si([campaña_swap_pendiente]<>"////";"ClienteConBuzonSwapPendiente";Si([cliente contactado]="no";Si([hueco_migrable]="si";Si([coberturaFTTH]="ConFTTH" ;Si([campaña_swap_pendiente]<>"////";"ClienteConBuzonSwapPendiente";Si([cliente moroso en campaña fide]="si";"ClienteMoroso";Si([sin BA]="sinBA";"Disponible_ClienteSinBA";Si([Variables].[Nodos2025]="Nodos 2025";"Disponible_ClienteNodos2025";Si(EsNulo([clienteBidasoa]);"Disponible_clientesResto";"Disponible_ClientesBidasoa")))));"ClienteSinFTTH");"hueco no migrable");Si([Cliente Rechazado/ilocalizado]="si";Si(EsNulo([clientes contactados 2 meses]);"Disponible_Ref.Cliente Rechaza/ilocalizado de hace mas de dos meses";"Cliente Rechaza/ilocalizado de hace menos de dos meses");Si(EsNulo([proceso swap en curso]);"Cliente Informado sin proceso swap en curso";"Cliente Informado con proceso swap en curso")))))
*/
 
--
#####################################################################################################
--4.1 AREA DE INFLUENCIA
#####################################################################################################

SELECT EIC_CP,ZDC , COUNT(*)AS REG
FROM
(
SELECT 
  A.*,
  B.EIC_CP,
  B.ZDC
FROM `mo-migrations-reporting.HFC.cartera_hfc_ser_krt` AS A  
LEFT JOIN  (SELECT DISTINCT CP,EIC_CP,ZDC FROM `bi-data-science-pilots.SCORING_MODELOS.AreasInfluencia`) AS B
  ON A.ZIP_CODE = B.CP
)
GROUP BY ALL
ORDER BY REG DESC
;

--DE LOS QUE NO CRUZA VER EL CP Y COMPROBAR QUE EXISTE EN LA TABLA AREA DE INFLUENCIA.



#####################################################################################################
--4.2 HUECOS BIDASOA
#####################################################################################################

-- Tiran de esta query de Javier Gijón. Cruzamos con cartera HFC y generamos un flag para identificar los Huecos Bidasoa
select 
  distinct substr(gescal37, 1, 17) G17, 
  footprint_detail 
from `mm-datamart-kd.ACCESS_PROVISION.footprint_ftth_mm` 
where footprint_detail like '%BID%'
;


SELECT 
  A.*,
  CASE WHEN B.G17 IS NOT NULL 
    THEN 1
    ELSE 0
  END AS HUECO_BIDASOA
FROM `mo-migrations-reporting.HFC.cartera_hfc_ser_krt` AS A    
LEFT JOIN (select 
  distinct substr(gescal37, 1, 17) G17, 
  footprint_detail 
from `mm-datamart-kd.ACCESS_PROVISION.footprint_ftth_mm`   
where footprint_detail like '%BID%') AS B   
ON A.GESCAL17 = B.G17
;





--Nodo HFC --> es el campo node asociado al hueco (campo geocode_plus), de las tablas:
-- mm-datamart-kd.SERVICES.gaps_coverage_r
-- mm-datamart-kd.SERVICES.gaps_coverage_tcnet
-- mm-datamart-kd.SERVICES.gaps_coverage_ektnet

--817.398 reg
--Solo contiene R
--Actualizada diariamente
SELECT DISTINCT BRAND_DS
FROM `mm-datamart-kd.SERVICES.gaps_coverage_r`
;

--156.226 reg
--Solo contiene Telecable
--Actualizada diariamente

SELECT DISTINCT BRAND_DS
FROM `mm-datamart-kd.SERVICES.gaps_coverage_tcnet`
;

--1.792.987 reg
--Contiene Euskaltel, Telecable,Racc,Virgin y Viva Mobile
--Actualizada diariamente
SELECT DISTINCT BRAND_DS
FROM `mm-datamart-kd.SERVICES.gaps_coverage_ektnet`
;


--UNION EKTNET CON R
-- ASEGURAR DE NUTRIR LOS REGISTROS DE TELECABLE QUE ESTÁN EN TELECABLE y NO EN EKTNET









-- Buscando algunos campos en Semantic para insertar en Cartera HFC --157568 registros (26/02)
-- SELECT  *
  -- customer_id,
  -- brand_ds,
  -- SEGMENT_DS,
  -- PHONE_NM,
  -- TECHNOLOGY_DS,
  --CIUDAD-- --PROVINCIA--ZIP_CODE_DS
-- FROM `mm-datamart-kd.SERVICES.semantic_kpi_services`
-- WHERE 
--   kpi = 'eop'
--   -- AND period >= '2024-01-01'
--   AND period = '2025-02-01'
--   AND service_type = 'FIX'
--   AND technology_ds = 'HFC'
--   AND billing_type_ds = 'POSTPAID'
--   ;

