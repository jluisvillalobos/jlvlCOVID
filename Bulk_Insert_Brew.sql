USE Coronadata
Go
--*******************************************************************
--Crear tablas de dias zero para cada pais/Region
--*******************************************************************
drop table if exists  wd_PaisDiaZero_Cases
drop table if exists  wd_PaisDiaZero_Deaths
drop table if exists  wd_DistrictDiaZero_Cases
drop table if exists  wd_DistrictDiaZero_Deaths
drop table if exists  NumerosParaDiaZero
CREATE TABLE [dbo].[NumerosParaDiaZero](
	[id] [int] not null,
	[CasesForDiaZero] [int] NULL,
	[DeathsForDiaZero] [int] NULL
) ON [PRIMARY]
GO
INSERT INTO [dbo].[NumerosParaDiaZero] ([id],[CasesForDiaZero],[DeathsForDiaZero])
     VALUES (1,150,20) --Se considera DiaZero cuando se pasa de 150 casoss o 20 muertes

Declare @CasesforDiaZero  integer
Declare @DeathsForDiaZero  integer
Select @CasesforDiaZero=CasesForDiaZero from NumerosParaDiaZero where NumerosParaDiaZero.id=1 
Select @DeathsForDiaZero=DeathsForDiaZero from NumerosParaDiaZero where NumerosParaDiaZero.id=1 






--*******************************************************************
--TABLA world_data
--Datos globales agregados
--*******************************************************************
drop table if exists  world_data
go

CREATE TABLE [dbo].[world_data](
	[country] [nvarchar](50) NOT NULL,
	[date] [datetime2](7) NOT NULL,
	[cases] [nvarchar](50)  NOT NULL,
	[deaths] [nvarchar](50)  NOT NULL,
	[uci] [nvarchar](50)  NOT NULL,
	[hospitaliz] [nvarchar](50) NOT NULL,
	[cases_non_cum] [nvarchar](50) NOT NULL,
	[deaths_non_cum] [nvarchar](50) NOT NULL,
	[uci_non_cum] [nvarchar](50) NOT NULL,
	[iso] [nvarchar](50) NOT NULL
) ON [PRIMARY]
GO
BULK INSERT world_data
    FROM 'C:\Users\U1\Documents\ProyectosVB\DatosBrew\data-raw\isglobal\world_data.csv'
    WITH    (FIRSTROW=2, FORMAT='CSV')
GO
--Añadir columna de dia zero
ALTER TABLE [dbo].[world_data]
ADD DiaFromZero_Cases int
GO
ALTER TABLE [dbo].[world_data]
ADD DiaFromZero_Deaths int
GO
ALTER TABLE [dbo].[world_data]
ADD CasesXMillon int
GO
ALTER TABLE [dbo].[world_data]
ADD DeathsXMillon int
GO
ALTER TABLE [dbo].[world_data]
ADD Population int
GO
ALTER TABLE [dbo].[world_data]
ADD delta_cases_day int default 0
go
ALTER TABLE [dbo].[world_data]
ADD delta_deaths_day int default 0
go

--Asignación de dias zero para cada pais
Declare @CasesforDiaZero  integer
Declare @DeathsForDiaZero  integer
Select @CasesforDiaZero=CasesForDiaZero from NumerosParaDiaZero where NumerosParaDiaZero.id=1 
Select @DeathsForDiaZero=DeathsForDiaZero from NumerosParaDiaZero where NumerosParaDiaZero.id=1 
Select country, min(date) as diazero_cases 
INTO wd_PaisDiaZero_Cases
from world_data where cases >@CasesforDiaZero group by country order by country

Select country, min(date) as diazero_deaths 
INTO wd_PaisDiaZero_Deaths
from world_data where Deaths >@CasesforDiaZero group by country order by country

-- Actualizar los dias desde dia zero para los paises que hayan pasado el umbral 
update world_data
SET world_data.DiaFromZero_Cases= DATEDIFF(dd,wd_PaisDiaZero_Cases.diazero_cases,world_data.date)
				FROM        world_data INNER JOIN
                  wd_PaisDiaZero_Cases ON world_data.country = wd_PaisDiaZero_Cases.country
update world_data
SET world_data.DiaFromZero_Deaths= DATEDIFF(dd,wd_PaisDiaZero_Deaths.diazero_Deaths,world_data.date)
				FROM        world_data INNER JOIN
                  wd_PaisDiaZero_Deaths ON world_data.country = wd_PaisDiaZero_Deaths.country
go
--***********************************************************************

--*******************************************************************
--TABLA world_region_data
--Datos globales agregados
--*******************************************************************
drop table if exists  world_region_data
go
CREATE TABLE [dbo].[world_region_data](
	
	[country] [nvarchar](50) NOT NULL,
	[district] [nvarchar](50) NOT NULL,
	[date] [datetime2](7) NOT NULL,
	[cases] [nvarchar](50) NOT NULL,
	[deaths] [nvarchar](50) NOT NULL,
	[uci] [nvarchar](50) NOT NULL,
	[hospital] [nvarchar](50) NOT NULL,
	[cases_non_cum] [nvarchar](50) NOT NULL,
	[deaths_non_cum] [nvarchar](50) NOT NULL,
	[uci_non_cum] [nvarchar](50) NOT NULL,
	[iso] [nvarchar](50) NOT NULL
) ON [PRIMARY]
BULK INSERT world_region_data
    FROM 'C:\Users\U1\Documents\ProyectosVB\DatosBrew\data-raw\isglobal\world_region_data.csv'
    WITH    (FIRSTROW=2, FORMAT='CSV', CODEPAGE=65001)
go
--Añadir columna de dia zero
ALTER TABLE [dbo].[world_region_data]
ADD DiaFromZero_Cases integer
go
ALTER TABLE [dbo].[world_region_data]
ADD DiaFromZero_Death integer
GO
ALTER TABLE [dbo].[world_region_data]
ADD CasesXMillon int
GO
ALTER TABLE [dbo].[world_region_data]
ADD DeathsXMillon int
GO
ALTER TABLE [dbo].[world_region_data]
ADD Population int
GO

ALTER TABLE [dbo].[world_region_data]
ADD delta_cases_day int default 0
go
ALTER TABLE [dbo].[world_region_data]
ADD delta_deaths_day int default 0
go

--Asignacion de dias zero para cada 'district' o región para los paises que la especifiquen
Declare @CasesforDiaZero  integer
Declare @DeathsForDiaZero  integer
Select @CasesforDiaZero=CasesForDiaZero from NumerosParaDiaZero where NumerosParaDiaZero.id=1 
Select @DeathsForDiaZero=DeathsForDiaZero from NumerosParaDiaZero where NumerosParaDiaZero.id=1 

Select district, min(date) as diazero_cases 
	INTO wd_DistrictDiaZero_Cases
	from world_region_data where cases >@CasesforDiaZero and district <> 'NA' 
	group by district order by district

Select district, min(date) as diazero_deaths 
	INTO wd_DistrictDiaZero_Deaths
	from world_region_data where Deaths >@CasesforDiaZero and district <> 'NA' 
	group by district order by district
go

-- Actualizar los dias desde dia zero para los paises que hayan pasado el umbral 
update world_region_data
SET		world_region_data.DiaFromZero_Cases= DATEDIFF(dd,wd_PaisDiaZero_Cases.diazero_cases,world_region_data.date)
				FROM        world_region_data INNER JOIN
                  wd_PaisDiaZero_Cases ON world_region_data.country = wd_PaisDiaZero_Cases.country
update world_region_data
SET		world_region_data.DiaFromZero_Death= DATEDIFF(dd, wd_PaisDiaZero_Deaths.diazero_deaths,world_region_data.date)
				FROM        world_region_data INNER JOIN
                  wd_PaisDiaZero_Deaths ON world_region_data.country = wd_PaisDiaZero_Deaths.country
go
-- Correccion de los dias zero regionales para los paises que tengan distritos 
update world_region_data
SET		world_region_data.DiaFromZero_Cases= DATEDIFF(dd,wd_DistrictDiaZero_Cases.diazero_cases,world_region_data.date)
				FROM        world_region_data INNER JOIN
                  wd_DistrictDiaZero_Cases ON world_region_data.district = wd_DistrictDiaZero_Cases.district
update world_region_data
SET 	world_region_data.DiaFromZero_Death= DATEDIFF(dd,wd_DistrictDiaZero_Deaths.diazero_deaths,world_region_data.date)
				FROM        world_region_data INNER JOIN
                  wd_DistrictDiaZero_Deaths ON world_region_data.district = wd_DistrictDiaZero_Deaths.district
go

--***********************************************************************
--*******************************************************************
--TABLA world_pop
--Datos globales agregados
--*******************************************************************
drop table if exists  depurable_world_pop
go
drop table if exists  world_pop
go

CREATE TABLE [dbo].[depurable_world_pop](
	[country] [nvarchar](100) NOT NULL,
	[iso] [nvarchar](50) NOT NULL,
	[pop] [nvarchar] (50) NULL,
	[region] [nvarchar](50) NOT NULL,
	[sub_region] [nvarchar](50) NOT NULL
) ON [PRIMARY]
GO

BULK INSERT depurable_world_pop
    FROM 'C:\Users\U1\Documents\ProyectosVB\DatosBrew\data-raw\isglobal\world_pop.csv'
    WITH    (FIRSTROW=2, FORMAT='CSV')
GO
--depuración de filas que no se desean
delete depurable_world_pop where pop='NA'
go
delete depurable_world_pop where region='NA'
go
select country, iso, cast(pop as int) as pop, region, sub_region
into world_pop
from depurable_world_pop
--*******************************************************************
--TABLA esp_pop
--Poblacion de CCAA españolas
--*******************************************************************
drop table if exists  esp_pop
go
CREATE TABLE [dbo].[esp_pop](
	[ccaa] [nvarchar](50) NOT NULL,
	[pop] [int] NOT NULL
) ON [PRIMARY]
GO

BULK INSERT esp_pop
    FROM 'C:\Users\U1\Documents\ProyectosVB\DatosBrew\data-raw\isglobal\esp_pop.csv'
    WITH    (FIRSTROW=2, FORMAT='CSV', CODEPAGE=65001)
GO
--*******************************************************************
--TABLA esp_pop
--Poblacion regiones italianas
--*******************************************************************
drop table if exists  ita_pop
go
CREATE TABLE [dbo].[ita_pop](
	[region] [nvarchar](50) NOT NULL,
	[pop] [int] NOT NULL
) ON [PRIMARY]
GO
BULK INSERT ita_pop
    FROM 'C:\Users\U1\Documents\ProyectosVB\DatosBrew\data-raw\isglobal\ita_pop.csv'
    WITH    (FIRSTROW=2, FORMAT='CSV', CODEPAGE=65001)
GO
--************************************************************************
--Actualizacion de la población de cada pais en WORLD_DATA y WORLD_REGION_ DATA
--**************************************************************************
update world_data
SET		world_data.Population= world_pop.pop
				FROM        world_data INNER JOIN
                  world_pop ON world_data.iso = world_pop.iso
--Actualizacion de la población en WORLD_REGION_DATA solo en filas de pais SIN distrito
update world_region_data
SET		world_region_data.Population= world_pop.pop
				FROM        world_region_data INNER JOIN
                  world_pop ON world_region_data.iso = world_pop.iso
				  --WHERE world_region_data.district='NA'
--***************************************************************************************
--AJUSTE de la población en WORLD_REGION_DATA para filas que tienen distrito en ESPAÑA (ccaa) o en ITALIA (regiones)
-- IMPORTANTE: La población de otros distritos, por ejemplo los de USA o CHINA no será correcta ya que
--             tendrá la población total del pais
--***************************************************************************************

update world_region_data
SET		world_region_data.Population= esp_pop.pop
				FROM        world_region_data INNER JOIN
                  esp_pop ON world_region_data.district = esp_pop.ccaa
				  WHERE world_region_data.country='Spain'
update world_region_data
SET		world_region_data.Population= ita_pop.pop
				FROM        world_region_data INNER JOIN
                  ita_pop ON world_region_data.district = ita_pop.region
				  WHERE world_region_data.country='Italy'
go
--***********************************************************************
--Acondicionamiento final de columnas
--Eliminacion de campos numéricos con NA
--Ajuste de cifras x millon
	--***********************************************************************
	--      WORLD_REGION_DATA
	--************************************************************************
update world_region_data
set uci='0'
WHERE uci='NA'
go
update world_region_data
set uci_non_cum='0'
WHERE uci_non_cum='NA'
go
update world_region_data
set hospital='0'
WHERE hospital='NA'
go
ALTER TABLE world_region_data ALTER COLUMN cases int NOT NULL
ALTER TABLE world_region_data ALTER COLUMN deaths int not null
ALTER TABLE world_region_data ALTER COLUMN uci int not null
ALTER TABLE world_region_data ALTER COLUMN hospital int not null
ALTER TABLE world_region_data ALTER COLUMN cases_non_cum int not null
ALTER TABLE world_region_data ALTER COLUMN deaths_non_cum int not null
ALTER TABLE world_region_data ALTER COLUMN uci_non_cum int not null

declare @millon bigint
select @millon=1000000
update world_region_data
set CasesXMillon=Cases*@millon/Population,
    DeathsXMillon=Deaths*@millon/Population
go
	--***********************************************************************
	--      WORLD_REGION_DATA
	--************************************************************************
ALTER TABLE world_data ALTER COLUMN cases int NOT NULL
ALTER TABLE world_data ALTER COLUMN deaths int not null
ALTER TABLE world_data ALTER COLUMN uci int not null
ALTER TABLE world_data ALTER COLUMN cases_non_cum int not null
ALTER TABLE world_data ALTER COLUMN deaths_non_cum int not null
ALTER TABLE world_data ALTER COLUMN uci_non_cum int not null
declare @millon bigint
select @millon=1000000
update world_data
set CasesXMillon=Cases*@millon/Population,
    DeathsXMillon=Deaths*@millon/Population
go
--***********************************************************************
--***********************************************************************
--CALCULO DE deltas (incrementos) dia a dia para las dos tablas: WORLD_DATA y WORLD_REGION_DATA
--***********************************************************************
--***********************************************************************
drop table if exists #tmpdeltas1
go
select HOY.country, HOY.date fechahoy, 
hoy.cases_non_cum casosHoy, ayer.cases_non_cum casosAyer, 
(hoy.cases_non_cum-ayer.cases_non_cum) as delta_cases, 
hoy.deaths_non_cum deathsHoy, ayer.deaths_non_cum as deathsAyer,
(hoy.deaths_non_cum-ayer.deaths_non_cum) as delta_deaths,
ayer.date fechaayer
into #tmpdeltas1
from world_data HOY, world_data AYER
where HOY.Country=AYER.country and HOY.Date=dateadd(dd,1,AYER.date)

UPDATE
    world_data
SET
    world_data.delta_cases_day = #tmpDeltas1.delta_cases,
    world_data.delta_deaths_day =  #tmpDeltas1.delta_deaths
FROM
    world_data 
    INNER JOIN #tmpDeltas1
        ON world_data.country = #tmpDeltas1.country and 
		   world_data.date=#tmpdeltas1.fechahoy
drop table #tmpdeltas1
go
select HOY.country, HOY.district district, HOY.date fechahoy, 
hoy.cases_non_cum casosHoy, ayer.cases_non_cum casosAyer, 
(hoy.cases_non_cum-ayer.cases_non_cum) as delta_cases, 
hoy.deaths_non_cum deathsHoy, ayer.deaths_non_cum as deathsAyer,
(hoy.deaths_non_cum-ayer.deaths_non_cum) as delta_deaths,
ayer.date fechaayer
into #tmpdeltas1
from world_region_data HOY, world_region_data AYER
where (HOY.Country=AYER.country and HOY.district=AYER.district) and HOY.Date=dateadd(dd,1,AYER.date)

UPDATE
    world_region_data
SET
    world_region_data.delta_cases_day = #tmpDeltas1.delta_cases,
    world_region_data.delta_deaths_day =  #tmpDeltas1.delta_deaths
FROM
    world_region_data 
    INNER JOIN #tmpDeltas1
        ON world_region_data.country = #tmpDeltas1.country and 
			world_region_data.district= #tmpDeltas1.district and
		   world_region_data.date=#tmpdeltas1.fechahoy
--Eliminar los deltas nulos en las primeras filas de cada pais
UPDATE World_data
	set delta_cases_day=cases_non_cum where delta_cases_day is null
UPDATE World_data
	set delta_deaths_day=deaths_non_cum where delta_deaths_day is null
UPDATE World_region_data
	set delta_cases_day=cases_non_cum where delta_cases_day is null
UPDATE World_region_data
	set delta_deaths_day=deaths_non_cum where delta_deaths_day is null
-- fin acondicionamiento
--***********************************************************************
--***********************************************************************
-- ELABORACION DE TABLAS PARA GRAFICOS
--***********************************************************************
--***********************************************************************

-- eliminacion de tablas para gráficos
drop table if exists  t01_globalXDia
go
drop table if exists  t02_globalXPais
go
drop table if exists  t03_globalXDiaSoloChina
go
drop table if exists  t04_globalXDiaSinChina
go
drop table if exists  t05_Global10DayZero_cases
go
drop table if exists  t06_Global10DayZero_deaths
go
drop table if exists  t07_esp_CCAA_Hoy
go
drop table if exists  t08_esp_diarioVariasCCAA
go
drop table if exists  t09_esp_diarioCatalunya
go
drop table if exists  t10_esp_diarioMadrid
go
drop table if exists  t11_esp_diarioEuskadi
go
--***********************************************************************
-- Generacion de tablas para gráficos
--***********************************************************************
-- Evolucion diaria GLOBAL
--***********************************************************************
SELECT [date]
      ,sum([cases]) as cases
      ,sum([deaths]) as deaths
      ,sum([uci]) as uci
into t01_globalXDia 
FROM [CoronaData].[dbo].[world_region_data]
group by date
order by date
--***********************************************************************
-- Evolucion diaria SOLO CHINA
--***********************************************************************
SELECT [date]
      ,sum([cases]) as cases
      ,sum([deaths]) as deaths
      ,sum([uci]) as uci
into t03_globalXDiaSoloChina 
FROM [CoronaData].[dbo].[world_region_data]
where country='China'
group by date
order by date
--***********************************************************************
-- Evolucion diaria GLOBAL SIN CHINA
--***********************************************************************
SELECT [date]
      ,sum([cases]) as cases
      ,sum([deaths]) as deaths
      ,sum([uci]) as uci
into t04_globalXDiaSinChina 
FROM [CoronaData].[dbo].[world_region_data]
where country<>'China'
group by date
order by date
--***********************************************************************
-- -- Totales por pais de la fecha máxima disponible
--***********************************************************************
declare @fechamax datetime2(7)
declare @fechamaxSpain datetime2(7)

select @fechamax= max(date) from world_data
--a veces los datos de Spain solo están disponibles para el dia anterior
select @fechamaxSpain= max(date) from world_data where country='Spain'

SELECT [country]
      ,[date]
      ,sum([cases]) as cases
      ,sum([deaths]) as deaths
      ,sum([uci]) as uci
	  ,cast(sum([deaths])*100/sum([cases]) as decimal(6,2)) as CaseFatality
	  ,min(CasesxMillon) as CasesXMillon
	  ,min(DeathsxMillon) as DeathsXMillon
INTO t02_globalXPais 
FROM [CoronaData].[dbo].[world_data]
where world_data.date =@fechamax or (world_data.date=@fechamaxSpain and country='Spain')
Group by country, date order by DeathsXMillon desc

--***********************************************************************
-- -- SERIE por pais con los datos desde su dia ZERO (de casos)
--***********************************************************************
SELECT     *
INTO t05_Global10DayZero_cases
FROM        world_data
where world_data.DiaFromZero_cases >= 0 and 
	country in (Select top 10 country  from t02_globalXPais order by t02_globalXPais.cases desc)
go
--***********************************************************************
-- -- SERIE por pais con los datos desde su dia ZERO (de muertes)
--***********************************************************************
SELECT     *
INTO t06_Global10DayZero_deaths
FROM        world_data
where world_data.DiaFromZero_deaths >= 0 and country in (Select top 10 country  from t02_globalXPais order by t02_globalXPais.deaths desc)
go

--***********************************************************************
-- -- --Datos España x CCAA Acumulados a la última fecha
--***********************************************************************
declare @fechamax datetime2(7)
select @fechamax= max(date) from world_region_data Where Country='Spain'

SELECT * 
into t07_esp_CCAA_HOY
From world_region_data Where Country='Spain' and date=@fechamax
go

--***********************************************************************
-- -- SERIE diaria para las principales CCAA con los datos desde su dia ZERO (de muertes)
--***********************************************************************
SELECT *
into t08_esp_diarioVariasCCAA
  FROM [CoronaData].[dbo].[world_region_data]
where country='Spain' and district in ('Andalucía','Cataluña','Madrid' ,'País Vasco')
go



SELECT * 
into t09_esp_diarioCatalunya
  FROM [CoronaData].[dbo].[world_region_data]
where country='Spain' and district='Cataluña'


SELECT *
into t10_esp_diarioMadrid
  FROM [CoronaData].[dbo].[world_region_data]
where country='Spain' and district='Cataluña'

SELECT *
into t11_esp_diarioEuskadi
  FROM [CoronaData].[dbo].[world_region_data]
where country='Spain' and district='País Vasco'
