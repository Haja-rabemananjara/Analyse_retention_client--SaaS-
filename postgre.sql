-- Vérification de toutes les données existantes
select * 
from public.saas_sales ss 
limit 50;

-- Analyse des type de données selon les colonnes
select
	column_name,
	data_type
from information_schema.columns
where table_name = 'saas_sales';


-- Analyse des types de données de la colonne "Order Date" (échantillon aléatoire)
select distinct ss."Order Date" 
from public.saas_sales ss
limit 50

-- Analyse des types de données de la colonne "Order Date" suivant les premières lignes
select distinct ss."Order Date"
from public.saas_sales ss 
order by ss."Order Date" 
limit 400;

-- Test de conversion des données de "Order Date"
select
	ss."Order Date" ,
	to_date(ss."Order Date" , 'MM/DD/YYYY') as converted_date
from public.saas_sales ss 
limit 50;

-- Création d'une nouvelle colonne "Order Date Clean" dans la table
alter table public.saas_sales 
add column "Order Date Clean" date;

-- Mise à jour de la colonne "ORder Date Clean" avec les données de "Order Date" converties en format Date
update public.saas_sales ss 
set "Order Date Clean" = to_date(ss."Order Date", 'MM/DD/YYYY');

-- Visualisation des données de la colonne "ORder Date" et "Order Date Clean"
select
	"Order Date", "Order Date Clean"
from saas_sales ss 
limit 50;

-- Comptage des données "Null" dans la colone "Order Date Clean"
select count(*)
from saas_sales ss 
where "Order Date Clean" is null

-- Test de vérification de création de la colonne "Order Month" avec des données de date mensuelle (début de mois)
select
	"Customer ID",
	"Order Date Clean",
	date_trunc('month',  "Order Date Clean")::date as "Order Month"
from public.saas_sales ss 
limit 50;

-- Ajout de la colonne "Order Month"
alter table public.saas_sales
add column "Order Month" date;

-- Mise à jour de la colonne "Order Month" avec des dates trunc
update saas_sales ss
set "Order Month" = date_trunc('month', "Order Date Clean")::date;

-- Vérification des colonnes "Order Date Clean" et "Order Month"
select
	"Order Date Clean",
	"Order Month"
from saas_sales ss
limit 50;

--  Test de mise en place de l'analyse cohorte
select
	"Customer ID",
	"Order Date Clean",
	"Order Month",
	min("Order Date Clean") over (partition by "Customer ID") as "Acquisition Date",
	date_trunc('month', min("Order Date Clean") over (partition by "Customer ID"))::date as "Acquisition Month",
	row_number() over (partition by "Customer ID" order by ("Order Date Clean")) as "Order Rank"
from public.saas_sales ss
limit 200;

-- Vue SQL
create view saas_sales_enriched as
select
	"Customer ID",
	"Order Date Clean",
	date_trunc('month', "Order Date Clean")::date as "Order Month",
	date_trunc('month', min("Order Date Clean") over (partition by "Customer ID"))::date as "Acquisition Month",
	row_number() over (partition by "Customer ID" order by ("Order Date Clean")) as "Order Rank",
from public.saas_sales;


select  *
from saas_sales_enriched
limit 50;

-- Calcul CHURN
-- Identifier le mois précédent pour chaque client
select
	"Customer ID",
	"Order Month",
	lag("Order Month") over (partition by "Customer ID" order by "Order Month") as "Previous Month"
from saas_sales_enriched
order by "Customer ID", "Order Month"
limit 200;

-- Identifier les clients actifs par mois
select
	"Customer ID",
	"Order Month",
	("Order Month" + interval '1 month')::date as "Next Month"
from saas_sales_enriched
order by "Customer ID", "Order Month"
limit 200;

-- Vérifier si le client a acheté le month suivant
select
	s1."Customer ID",
	s1."Order Month" as "Current Month",
	(s1."Order Month" + interval '1 month')::date as "Next Month",
	case 
		when exists (
				select 1
				from saas_sales_enriched s2
				where s2."Customer ID" = s1."Customer ID"
					and s2."Order Month" = (s1."Order Month" + interval '1 month')::date
		)
		then 1
		else 0
	end as "Returned Next_Month"
from saas_sales_enriched s1
order by s1."Customer ID", s1."Order Month"
limit 200;

-- Regrouper par mois pour obtenir le churn
select
	"Order Month",
	count(distinct "Customer ID") as "Active clients",
	sum("Returned Next_Month") as "Clients Returned Next_Month"
from (
	select
		s1."Customer ID",
		s1."Order Month",
		case 
			when exists (
					select 1
					from saas_sales_enriched s2
					where s2."Customer ID" = s1."Customer ID"
					and s2."Order Month" = (s1."Order Month" + interval '1 month')::date
			)
			then 1
			else 0
		end as "Returned Next_Month"
	from saas_sales_enriched s1
) t
group by "Order Month"
order by "Order Month";

-- Création de table propre pour un client = une ligne par mois
WITH Monthly_clients AS 
	( SELECT DISTINCT "Customer ID", "Order Month" 
	FROM saas_sales_enriched ) 
SELECT * 
FROM Monthly_clients 
ORDER BY "Customer ID", "Order Month"
LIMIT 50;

-- Calcul churn
with Monthly_clients as (
	select distinct 
		"Customer ID",
		"Order Month"
	from saas_sales_enriched
),
returns as (
	select
		mc."Customer ID",
		mc."Order Month",
		case
			when exists (
				select 1
				from Monthly_clients mc2
				where mc2."Customer ID" = mc."Customer ID"
					and mc2."Order Month" = (mc."Order Month" + interval '1 month')::date
			)
			then 1
			else 0
		end as Returned_Next_Month
	from Monthly_clients mc
)
select
	"Order Month",
	count(*) as Active_clients,
	sum(Returned_Next_Month) as Clients_Returned_Next_Month,
	count(*) - sum(Returned_Next_Month) as Churn_Clients,
	round(
		(count(*) - sum(Returned_Next_Month))::numeric / count(*)*100, 2
	) as Churn_Rate
from returns
group by "Order Month"
order by "Order Month";

-- 
select column_name
from information_schema.columns
where table_name = 'saas_sales'

-- Recréer (updating) la vue enrichie avec la colonne sales
create or replace view saas_sales_enriched as
select
	"Customer ID",
	"Order Date Clean",
	date_trunc('month', "Order Date Clean")::date as "Order Month",
	date_trunc('month', min("Order Date Clean") over (partition by "Customer ID"))::date as "Acquisition Month",
	row_number() over (partition by "Customer ID" order by "Order Date Clean") as "Order Rank",
	"Sales"
from public.saas_sales;

select *
from saas_sales_enriched
limit 50;

-- Calcul Life Time Value simple
select 
	"Customer ID",
	sum("Sales") as LTV
from saas_sales_enriched
group by "Customer ID"
order by LTV desc;

-- CONTRUCTION DE LA LTV par cohorte
-- Niveau 1: LTV par client + cohorte
select 
	"Customer ID",
	"Acquisition Month",
	sum("Sales") as LTV
from saas_sales_enriched
group by "Customer ID", "Acquisition Month"
order by "Acquisition Month", LTV desc;

-- Niveau 2: LTV moyenne par cohorte
select 
	"Acquisition Month",
	avg(LTV) as Avg_ltv_per_cohort,
	sum(LTV) as total_ltv_per_cohort,
	count(*) as client_in_cohort
from(
	select
		"Customer ID",
		"Acquisition Month",
		sum("Sales") as LTV
	from saas_sales_enriched
	group by "Customer ID", "Acquisition Month"
) t
group by "Acquisition Month"
order by "Acquisition Month";

-- CONSTRUCTION DE LA LTV CUMULEE
-- Etape 1 : Agréger les ventes
select
	"Customer ID",
	"Order Month",
	sum("Sales") as Monthly_sales
from saas_sales_enriched
group by "Customer ID", "Order Month"
order by "Customer ID", "Order Month";

-- Etape 2: Calcul de la LTV cumulée
with Monthly_sales as (
	select
		"Customer ID",
		"Order Month",
		sum("Sales") as Monthly_sales
	from saas_sales_enriched
	group by "Customer ID", "Order Month"
)
select
	"Customer ID",
	"Order Month",
	sum(Monthly_sales) over (
		partition by "Customer ID"
		order by "Order Month"
		rows between unbounded preceding and current row
	) as Cumulative_ltv
from Monthly_sales 
order by "Customer ID", "Order Month";

-- Etape 3: LTV cumulée par cohorte
with Monthly_sales as (
	select
		"Customer ID",
		"Acquisition Month",
		"Order Month",
		sum("Sales") as Monthly_sales
	from saas_sales_enriched
	group by "Customer ID", "Acquisition Month", "Order Month"
),
cumulative as (
	select
		"Customer ID",
		"Acquisition Month",
		"Order Month",
		sum(Monthly_sales) over (
			partition by "Customer ID"
			order by "Order Month"
			rows between unbounded preceding and current row
		) as Cumulative_ltv
	from Monthly_sales
),
relative as(
	select "Customer ID",
	"Acquisition Month",
	"Order Month",
	Cumulative_ltv,
	extract(year from age("Order Month", "Acquisition Month")) * 12 +
	extract(month from age("Order Month", "Acquisition Month")) as Month_index from cumulative
)
select
	"Acquisition Month",
	Month_index,
	sum(Cumulative_ltv) as Avg_Cumulative_ltv
from relative
group by "Acquisition Month", Month_index 
order by "Acquisition Month", Month_index;

-- Etape 4: LTV cumulée totale par cohorte
with Monthly_sales as (
	select
		"Customer ID",
		"Acquisition Month",
		"Order Month",
		sum("Sales") as Monthly_sales
	from saas_sales_enriched
	group by "Customer ID", "Acquisition Month", "Order Month"
),
cumulative as (
	select
		"Customer ID",
		"Acquisition Month",
		"Order Month",
		sum(Monthly_sales) over (
			partition by "Customer ID"
			order by "Order Month"
			rows between unbounded preceding and current row
		) as Cumulative_ltv
	from Monthly_sales
),
relative as(
	select
		"Customer ID",
		"Acquisition Month",
		extract(year from age("Order Month", "Acquisition Month")) * 12 +
		extract(month from age("Order Month", "Acquisition Month")) as Month_index, Cumulative_ltv 
	from cumulative
)
select
	"Acquisition Month",
	Month_index,
	sum(Cumulative_ltv) as Cohort_cumulative_ltv
from relative
group by "Acquisition Month", Month_index 
order by "Acquisition Month", Month_index;

-- Version tujours croissante:
WITH Cohort_monthly AS (
    SELECT
        "Acquisition Month",
        "Order Month",
        SUM("Sales") AS Cohort_monthly_sales
    FROM saas_sales_enriched
    GROUP BY "Acquisition Month", "Order Month"
),
relative AS (
    SELECT
        "Acquisition Month",
        "Order Month",
        EXTRACT(YEAR FROM AGE("Order Month", "Acquisition Month")) * 12 +
        EXTRACT(MONTH FROM AGE("Order Month", "Acquisition Month")) AS Month_index,
        Cohort_monthly_sales
    FROM Cohort_monthly
),
cumulative AS (
    SELECT
        "Acquisition Month",
        Month_index,
        SUM(Cohort_monthly_sales) OVER (
            PARTITION BY "Acquisition Month"
            ORDER BY month_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS Cohort_cumulative_ltv
    FROM relative
)
SELECT *
FROM cumulative
ORDER BY "Acquisition Month", Month_index;


-- TABLE FINALE: Cohorte x Month_index x Order Month
-- Etape 1: Revenus Mensuels cohorte
with Cohort_monthly as (
	select
		"Acquisition Month",
		"Order Month",
		sum("Sales") as Cohort_monthly_sales
	from saas_sales_enriched
	group by "Acquisition Month", "Order Month" 
),
relative as (
	select
		"Acquisition Month",
		"Order Month",
		extract(year from age("Order Month", "Acquisition Month")) * 12 +
		extract(month from age("Order Month", "Acquisition Month")) as Month_index,
		Cohort_monthly_sales
	from Cohort_monthly 
)
select 
	"Acquisition Month",
	"Order Month",
	Month_index,
	Cohort_monthly_sales
from relative 
order by "Acquisition Month", Month_index;  

-- Etape 2: ajout de clients actifs par cohorte x mois
with Active_clients as (
	select
		"Acquisition Month",
		"Order Month",
		count(distinct "Customer ID") as Active_clients
	from saas_sales_enriched
	group by "Acquisition Month", "Order Month"
)
select *
from Active_clients
order by "Acquisition Month", "Order Month";

-- Etape 3 : fusion
with Cohort_monthly as (
	select
		"Acquisition Month",
		"Order Month",
		sum("Sales") as Cohort_monthly_sales
	from saas_sales_enriched
	group by "Acquisition Month", "Order Month" 
),
relative as (
	select
		"Acquisition Month",
		"Order Month",
		extract(year from age("Order Month", "Acquisition Month")) * 12 +
		extract(month from age("Order Month", "Acquisition Month")) as Month_index,
		Cohort_monthly_sales
	from Cohort_monthly 
),
Active_clients as (
	select
		"Acquisition Month",
		"Order Month",
		count(distinct "Customer ID") as Active_clients
	from saas_sales_enriched
	group by "Acquisition Month", "Order Month"
)
select
	r."Acquisition Month",
	r."Order Month",
	r.Month_index,
	r.Cohort_monthly_sales,
	ac.Active_clients
from relative r
left join Active_clients ac
	on r."Acquisition Month" = ac."Acquisition Month" 
	and r."Order Month" = ac."Order Month" 
order by r."Acquisition Month", r.Month_index;


-- Etape 4 : Préparer la table churn (Active_clients shifted)
with Active_clients as (
	select
		"Acquisition Month",
		"Order Month",
		count(distinct "Customer ID") as Active_clients
	from saas_sales_enriched
	group by "Acquisition Month", "Order Month"
),
relative as (
	select
		"Acquisition Month",
		"Order Month",
		extract(year from age("Order Month", "Acquisition Month")) *12 +
		extract(month from age("Order Month", "Acquisition Month")) as Month_index, Active_clients
	from Active_clients
),
shifted as (
	select
		"Acquisition Month",
		Month_index,
		Active_clients,
		lead(Active_clients) over (
			partition by "Acquisition Month"
			order by Month_index
		) as Next_Month_Active_clients
	from relative
)
select *
from shifted 
order by "Acquisition Month", Month_index; 

-- Version complète avec churn, churn_rate et retention_rate
with Active_clients as (
	select
		"Acquisition Month",
		"Order Month",
		count(distinct "Customer ID") as Active_clients
	from saas_sales_enriched
	group by "Acquisition Month", "Order Month"
),
relative as (
	select
		"Acquisition Month",
		"Order Month",
		extract(year from age("Order Month", "Acquisition Month")) * 12 +
		extract(month from age("Order Month", "Acquisition Month")) as Month_index, Active_clients
	from Active_clients
),
shifted as (
	select
		"Acquisition Month",
		Month_index,
		Active_clients,
		Lead(Active_clients) over (
			partition by "Acquisition Month"
			order by Month_index
		) as Next_Month_Active_clients
	from relative
)
select
	"Acquisition Month",
	Month_index,
	Active_clients,
	Next_Month_Active_clients,
	(Active_clients - Next_Month_Active_clients) as Churn,
	case 
		when Active_clients = 0 then null
		else (Active_clients - Next_Month_Active_clients)::float / Active_clients
	end as Churn_rate,
	case 
		when Active_clients = 0 then null
		else Next_Month_Active_clients::float / Active_clients
	end as Retention_rate
from shifted 
order by "Acquisition Month", Month_index;

-- TABLE FINALE
with base as (
	select
		"Customer ID",
		"Acquisition Month",
		"Order Month",
		"Sales"
	from saas_sales_enriched
),
-- 1. Revenus mensuels cohorte
Cohort_monthly as (
	select
		"Acquisition Month",
		"Order Month",
		sum("Sales") as Cohort_Monthly_Sales
	from base
	group by "Acquisition Month", "Order Month"
),
-- 2. Month_index
relative as (
	select
		cm."Acquisition Month",
		cm."Order Month",
		extract(year from age(cm."Order Month", cm."Acquisition Month")) * 12 +
		extract(month from age(cm."Order Month", cm."Acquisition Month")) as Month_index,
		cm.Cohort_Monthly_sales 
	from Cohort_monthly cm
),
-- 3. Revenus cumulés cohorte (LTV cohorte)
Cohorte_cumulative as (
	select
		"Acquisition Month",
		Month_index,
		Cohort_Monthly_Sales,
		sum(Cohort_Monthly_Sales) over (
			partition by "Acquisition Month"
			order by Month_index
		) as Cohorte_cumulative_ltv
	from relative 
),
-- 4.LTV réelle cumulée par client
Monthly_sales as (
	select
		"Customer ID",
		"Acquisition Month",
		"Order Month",
		sum("Sales") as Monthly_sales
	from base 
	group by "Customer ID", "Acquisition Month", "Order Month"
),
LTV_customer as (
	select
		"Customer ID",
		"Acquisition Month",
		"Order Month",
		sum(Monthly_sales) over (
			partition by "Customer ID"
			order by "Order Month"
		) as Cumulative_ltv
	from Monthly_sales 
),
LTV_relative as (
	select
		"Acquisition Month",
		extract(year from age("Order Month", "Acquisition Month")) * 12 +
		extract(month from age("Order Month", "Acquisition Month")) as Month_index,
		Cumulative_ltv
	from LTV_customer 
),
LTV_avg as (
	select
		"Acquisition Month",
		Month_index,
		AVG(Cumulative_ltv) as AVG_ltv_real
	from LTV_relative 
	group by "Acquisition Month", Month_index
),
-- 5. Clients actifs
Active_clients as (
	select
		"Acquisition Month",
		"Order Month",
		count(distinct "Customer ID") as Active_clients
	from base
	group by "Acquisition Month", "Order Month"
),
Active_relative as (
	select
		"Acquisition Month",
		"Order Month",
		extract(year from age("Order Month", "Acquisition Month")) * 12 +
		extract(month from age("Order Month", "Acquisition Month")) as Month_index,
		Active_clients
	from Active_clients
),
Active_shifted as (
	select
		"Acquisition Month",
		Month_index,
		Active_clients,
		Lead(Active_clients) over (
			partition by "Acquisition Month"
			order by Month_index
		) as Next_Month_Active_clients
	from Active_relative
),
-- 6. Final merge
final as (
	select
		c."Acquisition Month",
		c.Month_index,
		c.Cohort_Monthly_Sales,
		c.Cohorte_Cumulative_ltv,
		l.AVG_ltv_real,
		a.Active_clients,
		a.Next_Month_Active_clients,
		(a.Active_clients - a.Next_Month_Active_clients) as Churn,
		case 
			when a.Active_clients = 0 then null
			else (a.Active_clients - a.Next_Month_Active_clients)::float / a.Active_clients
		end as Churn_rate,
		case
			when a.Active_clients = 0 then null
			else a.Next_Month_Active_clients::float / a.Active_clients
		end as Retention_rate
	from Cohorte_cumulative c
	left join LTV_avg l
		on c."Acquisition Month" = l."Acquisition Month"
		and c.Month_index = l.Month_index
	left join Active_shifted a
		on c."Acquisition Month" = a."Acquisition Month"
		and c.Month_index = a.Month_index
)
select *
from Final
order by "Acquisition Month", Month_index;