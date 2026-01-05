# **ELT proces datasetu 'TV_METADATA_FOR_TVCM_ADVERTISING_IN_JAPAN_KANTO'**

Tento repozitár predstavuje ukážkovú implementáciu ELT procesu v Snowflake a vytvorenie dátového skladu so schémou Star Schema. Projekt pracuje s **TV_METADATA_FOR_TVCM_ADVERTISING_IN_JAPAN_KANTO** datasetom. Pre účely analýzy televízneho reklamného vysielania bol navrhnutý dimenzionálny model vo forme hviezdicovej schémy (star schema).

---
## **1. Úvod a popis zdrojových dát**
V tomto príklade analyzujeme dáta o televíznych reklamách, ich vysielaní a sledovanosti / ratingoch. Cieľom je porozumieť:
-časovým vzorom vysielania reklám
-výkonu reklám naprieč televíznymi stanicami a kategóriami
-vzťahu medzi typom reklamy a jej dosahom

Zdrojové dáta pochádzajú z M Data datasetu dostupného [tu](https://app.snowflake.com/marketplace/listing/GZT2Z5FY7J/m-data-co-ltd-tv-metadata-for-tv-cm-advertising-in-japan-kanto). Dataset obsahuje jednu hlavnu tabulky:
- `CM_SAMPLE` - To je mala tabulka pre testing

Účelom ELT procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/17gon/DatabaseProject/blob/master/img/ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma</em>
</p>

---
## **2 Dimenzionálny model**

V ukážke bola navrhnutá **schéma hviezdy (star schema)** podľa Kimballovej metodológie, ktorá obsahuje 1 tabuľku faktov **`fact_ratings`**, ktorá je prepojená s nasledujúcimi 6 dimenziami:
- **`dim_commercial`**: Obsahuje podrobné informácie o reklame, ako sú názov produktu, značka, spoločnosť, klasifikácia vysielania, meno účinkujúceho, hudobný podklad a vyhľadávacie kľúčové slová.
- **`dim_station`**: Uchováva údaje o televíznych staniciach, vrátane identifikátora stanice, názvu stanice a oblasti vysielania.
- **`dim_category`**: Obsahuje hierarchickú klasifikáciu reklám podľa kategórie, podkategórie a subpodkategórie.
- **`dim_time`**: Slúži na časovú analýzu vysielania a obsahuje dátumové a časové atribúty, ako sú dátum, deň, mesiac, štvrťrok, rok, hodina a minúta.
- **`dim_description`**: Zahrňuje textové popisy reklám, ako sú rozprávanie (narration), situácia, poznámky a dodatočné informácie.
- **`dim_audit`**: Uchováva auditné informácie o záznamoch, najmä dátum vytvorenia a poslednej aktualizácie záznamu.

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/17gon/DatabaseProject/blob/master/img/Star_Schema.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy</em>
</p>

---
## **3. ELT proces v Snowflake**
ETL proces pozostáva z troch hlavných fáz: `extrahovanie` (Extract), `načítanie` (Load) a `transformácia` (Transform). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu  boli najprv nahraté do Snowflake prostredníctvom marketplace. 

---
### **3.2 Load (Načítanie dát)**

Do data_staging bolo následne nahraté súbory obsahujúce údaje z dataset-y. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

```sql
TRUNCATE TABLE table_name;
INSERT INTO table_name (column_name)
SELECT DISTINCT
    column_name_from_dataset
FROM data_staging;
```

---
### **3.3 Transfor (Transformácia dát)**

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Dimenzia `dim_commercial`.

#### Príklad kódu:
```sql
CREATE OR REPLACE TABLE dim_commercial AS
SELECT DISTINCT
    c.cm_base_id,
    p.product_name,
    b.brand_name,
    co.company_name,
    c.classification,
    c.performer_name,
    c.background_music,
    c.search_keyword
FROM staging_commercial c
INNER JOIN staging_product p ON p.product_name = c.product_name
INNER JOIN staging_brand b ON b.brand_name = p.brand_name
INNER JOIN staging_company co ON co.company_name = b.company_name;
```
Dimenzia `dim_station`.

#### Príklad kódu:
```sql
CREATE OR REPLACE TABLE dim_station AS
SELECT DISTINCT
    s.station_id AS broadcast_station_id,
    s.station_name AS broadcast_station_name,
    s.area AS area
FROM staging_station s;
```

Faktová tabuľka `fact_broadcast` obsahuje záznamy o vysielaní televíznych reklám a prepojenia na všetky dimenzie. Obsahuje kľúčové metriky, je dĺžka reklamy (duration) a stav vysielania (enabled/disabled).

#### Príklad kódu:
```sql
CREATE OR REPLACE TABLE fact_broadcast AS
SELECT DISTINCT
    bl.cm_log_id AS CM_LOG_ID,
    bl.enabled_or_disabled AS ENABLED_OR_DISABLED,
    c.duration AS DURATION,
    s.station_id AS STATION_ID,
    dt_start.key AS start_time_key,
    dt_end.key AS end_time_key,
    dt_first.key AS first_broadcast_time_key,
    dca.category AS category_key,
    dc.cm_base_id AS dim_commersial_key,
    la.cm_log_id AS dim_audit_key,
    dd.cm_base_id AS dim_description_key
FROM staging_commercial c
INNER JOIN staging_broadcast_logs bl ON bl.cm_base_id = c.cm_base_id 
INNER JOIN staging_station s ON s.station_id = bl.station_id
INNER JOIN dim_category dca ON dca.category = c.category
INNER JOIN dim_commercial dc ON c.cm_base_id = dc.cm_base_id
INNER JOIN dim_description dd ON c.narration = dd.narration AND c.situation = dd.situation AND c.memo = dd.memo AND c.note = dd.note
INNER JOIN dim_time dt_start ON bl.start_datetime = dt_start.full_datetime
INNER JOIN dim_time dt_end ON bl.end_datetime = dt_end.full_datetime
INNER JOIN dim_time dt_first ON bl.first_broadcast_start = dt_first.full_datetime
LEFT JOIN staging_log_audit la ON bl.cm_log_id = la.cm_log_id;
```

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:

#### Príklad kódu:
```sql
DROP TABLE IF EXISTS staging_commercial;
DROP TABLE IF EXISTS staging_brand;
DROP TABLE IF EXISTS staging_broadcast_logs;
DROP TABLE IF EXISTS staging_category;
DROP TABLE IF EXISTS staging_company;
DROP TABLE IF EXISTS staging_log_audit;
DROP TABLE IF EXISTS staging_product;
DROP TABLE IF EXISTS staging_station;
```
---
## **4 Vizualizácia dát**

Dashboard obsahuje `5 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa televíznych reklám, televíznych staníc a časového vysielania. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie reklamného vysielania v regióne Kanto.

<p align="center">
  <img src="https://github.com/17gon/DatabaseProject/blob/master/img/dashboard.png" alt="ERD Schema">
  <br>
  <em>Obrázok 3 Dashboard datasetu</em>
</p>

---

---

**Autors:** Yaroslav Besschotnov, Danylo Vinnytskyi

---
