/* Author: Mladen Vidic
   E-mail: mladen.vidic@gmail.com
   Location: Belgrade, Serbia || Doboj, Bosnia and Herzegovina
   Date Signed: 14.5.2021.
   
   Scope: Task 3.
   
   LICENCE: The licensing rules from the 'license.txt' or '..\license.txt' file apply to this file, solution and its parts.
*/

/*
Task 3. Bill Calculation

Task description
There is table of facts of some services usage by clients:

Picture1.png

USAGES table is stored in schema USAGE_MGMT. It is filled by external application once a month.
It’s necessary to implement Bill Calculation solution for preparation of Bills for clients, 
by reading of USAGES table and filling the following tables in BILL_CALC schema, to have 
calculation results detailed in three levels hierarchy:

Picture2.png

Solution shall be implemented in DB stored package(s). It shall allow launching of Bill 
Calculation process for particular month and year, for single or all client(s). 
Solution shall be applicable for mass calculation process, assuming USAGES table can contain 
billions of records for different clients and usage dates. Process shall not stop if PRICE 
column is empty for some usage record, but error shall be logged and calculation process shall 
be continued for next client.
Assuming USAGE_MGMT and BILL_CALC schemas are in different Oracle DB instances and sys admin 
rights are granted for both, provide DDL scripts for necessary DB entities creation, including 
(but not limited to) DB link, Grants, Packages specifications and bodies, Table Indexes etc.

*/

/* SOLUTION DEMO 3: 
Author: Mladen Vidic
Location: Doboj,BiH
Date: 6.5.2021. "DD.MM.YYYY"
Scope: Demonstration that shows the way how can be solved above task 3.
*/

-------------------- A) USAGE MANAGEMENT SCHEMA in THE 1. DB INSTANCE -----------------------------------------------------------------
conn &&usage_mgmt/&&usgmgmt_pass@&&dbalias_um
-- Not needed DB link to 2. instance

-- Table USAGES in 1. db instance 
CREATE TABLE &&usage_mgmt..USAGES(
  ID NUMBER 			NOT NULL, -- constraint USAGES_PK PRIMARY KEY,
  CLIENT_ID NUMBER 		NOT NULL,
  TYPE_ID NUMBER 		NOT NULL,
  U_DATE DATE			NOT NULL,
  QUANTITY NUMBER 		NOT NULL,
  UNIT_OF_MEASURE NUMBER 		NOT NULL,
  PRICE NUMBER
)
PCTFREE 80 PCTUSED 10 
STORAGE (INITIAL 100M NEXT 100M MINEXTENTS 20 PCTINCREASE 10)
tablespace &&UM_FAST_READING_TBS
/

ALTER TABLE &&usage_mgmt..USAGES ADD CONSTRAINT USAGES_PK PRIMARY KEY (ID)
USING INDEX tablespace &&UM_FAST_READING_TBS_I
/

-- It is assumed that exists user &&bill_calc in both instances.
grant select, index on &&usage_mgmt..USAGES to &&bill_calc
/

-------------------- B) BILL CALCULATION SCHEMA in THE 2. DB INSTANCE----------------------------------------------------------------
conn &&somedba1/&&somedba_pass1@&&dbalias_bc
grant create database link to &&bill_calc;

conn &&somedba2/&&somedba_pass2@&&dbalias_um
grant create any context to &&usage_mgmt;

----------------------------------------------------------------------------------------------------------------
conn &&bill_calc/&&billcalc_pass@&&dbalias_bc

create database link dblink_to_um using '&&serverside_dbalias_to_um'
/

--  Not preferable to use this link because sending clear password of the user.
create database link dblink_to_um2 connect to current_user using '&&serverside_dbalias_to_um'
/

-- Table BILL_TOTAL in the 2. db instance 
CREATE TABLE &&bill_calc..BILL_TOTAL(
  ID NUMBER 			NOT NULL, -- constraint BILL_TOTAL_PK PRIMARY KEY,
  CLIENT_ID NUMBER 		NOT NULL,
  BT_MONTH NUMBER 		NOT NULL,
  BT_YEAR NUMBER 		NOT NULL,
  FIRST_USAGE_DATE DATE		NOT NULL,
  LAST_USAGE_DATE DATE		NOT NULL,
  COST NUMBER 				NOT NULL
)
PCTFREE 80 PCTUSED 10
tablespace &&BC_FAST_READWRITE_TBS
/

ALTER TABLE &&bill_calc..BILL_TOTAL ADD CONSTRAINT BILL_TOTAL_PK PRIMARY KEY (ID)
USING INDEX tablespace &&BC_FAST_READWRITE_TBS_I
/

create sequence &&bill_calc..BILL_TOTAL_seq start with 1 increment by 1 minvalue 1 nocycle nocache noorder;


-- Table BILL_SUBTOTAL in the 2. db instance
CREATE TABLE &&bill_calc..BILL_SUBTOTAL(
  ID NUMBER 			NOT NULL, -- constraint BILL_SUBTOTAL_PK PRIMARY KEY,
  BILL_TOTAL_ID NUMBER 		NOT NULL,
  UNIT_OF_MEASURE NUMBER 	NOT NULL,
  QUANTITY NUMBER 			NOT NULL,
  COST NUMBER 				NOT NULL
)
PCTFREE 80 PCTUSED 10
tablespace &&BC_FAST_READWRITE_TBS
/

ALTER TABLE &&bill_calc..BILL_SUBTOTAL ADD CONSTRAINT BILL_SUBTOTAL_PK PRIMARY KEY (ID)
USING INDEX tablespace &&BC_FAST_READWRITE_TBS_I
/

create sequence &&bill_calc..BILL_SUBTOTAL_seq start with 1 increment by 1 minvalue 1 nocycle nocache noorder;

ALTER TABLE &&bill_calc..BILL_SUBTOTAL ADD CONSTRAINT BILL_SUBTOTAL_FK 
	FOREIGN KEY (BILL_TOTAL_ID) REFERENCES &&bill_calc..BILL_TOTAL (ID)
/

CREATE INDEX &&bill_calc..BILL_SUBTOTAL_TOTAL_IX ON &&bill_calc..BILL_SUBTOTAL (BILL_TOTAL_ID)
TABLESPACE &&BC_FAST_READWRITE_TBS_I
/


-- Table BILL_ITEM in the 2. db instance
CREATE TABLE &&bill_calc..BILL_ITEM(
  ID NUMBER 				NOT NULL, -- constraint BILL_ITEM_PK PRIMARY KEY,
  BILL_SUBTOTAL_ID NUMBER 	NOT NULL,
  USAGE_TYPE_ID NUMBER 		NOT NULL,
  FIRST_USAGE_DATE DATE		NOT NULL,
  LAST_USAGE_DATE DATE		NOT NULL,
  QUANTITY NUMBER 			NOT NULL,
  UNIT_OF_MEASURE NUMBER 	NOT NULL,
  PRICE NUMBER				NOT NULL,
  COST NUMBER 				NOT NULL
)
PCTFREE 80 PCTUSED 10
tablespace &&BC_FAST_READWRITE_TBS
/

ALTER TABLE &&bill_calc..BILL_ITEM ADD CONSTRAINT BILL_ITEM_PK PRIMARY KEY (ID)
USING INDEX tablespace &&BC_FAST_READWRITE_TBS_I
/

create sequence &&bill_calc..BILL_ITEM_seq start with 1 increment by 1 minvalue 1 nocycle nocache noorder;

ALTER TABLE &&bill_calc..BILL_ITEM ADD CONSTRAINT BILL_ITEM_FK 
	FOREIGN KEY (BILL_SUBTOTAL_ID) REFERENCES &&bill_calc..BILL_SUBTOTAL (ID)
/

CREATE INDEX &&bill_calc..BILL_ITEM_SUBTOTAL_IX ON &&bill_calc..BILL_ITEM (BILL_SUBTOTAL_ID)
TABLESPACE &&BC_FAST_READWRITE_TBS_I
/

-------------------------------------C) LOG FOR MISSING PRICES in THE 2. DB INSTANCE -----------------------------------
CREATE TABLE &&bill_calc..LOG_MISSING_PRICES(
	u_year 		integer NOT NULL, 
	u_month  	integer NOT NULL, 
	client_id 	number NOT NULL, 
	type_id 	number NOT NULL, 
	first_ud 	date NOT NULL, 
	last_ud 	date NOT NULL, 
	quantity 	number NOT NULL,
	unit_of_measure number NOT NULL, 
	c_times 	integer NOT NULL,
	um_schema   varchar2(128) NOT NULL,
	db_link		varchar2(100)
)
PCTFREE 80 PCTUSED 10
tablespace &&BC_FAST_READWRITE_TBS
/

ALTER TABLE &&bill_calc..LOG_MISSING_PRICES ADD CONSTRAINT LOG_MISSING_PRICES_PK 
PRIMARY KEY (u_year, u_month, client_id)
USING INDEX tablespace &&BC_FAST_READWRITE_TBS_I
/

alter table &&bill_calc..LOG_MISSING_PRICES
add um_schema   varchar2(128) NOT NULL
/

alter table &&bill_calc..LOG_MISSING_PRICES
add db_link		varchar2(100)
/

-------------------- D) USAGE MANAGEMENT SCHEMA VIEWS FOR FASTER REMOTE QUERIES in THE 1. DB INSTANCE -----------------------------------------------------------------
conn &&usage_mgmt/&&usgmgmt_pass@&&dbalias_um

-- User must have CREATE VIEW privilege

---------------------------- VIEW for Bill Total -------------------------------------
-- NOTE: FOR DFINITIONS WITH VIRTUAL COLUMNS FOR extract expresion, got on option F further in the script.

-- by grouping for latter filter through va_year, va_month. 
CREATE OR REPLACE VIEW &&usage_mgmt..V_USAGES_BT
AS
(select /* +ALL_ROWS */ extract (year from u_date) va_year, extract (month from u_date) va_month, 
		client_id, min(u_date) first_bt_ud, max(u_date) last_bt_ud, sum(quantity*nvl(price,0)) bt_cost
from &&usage_mgmt..USAGES
group by extract (year from u_date), extract (month from u_date), client_id 
)
/
/* This kind of view are correct, but is not good for performance if you look for data of the single month 
because it allow users to query them without inner filters that are applied before grouping results. They can be filtered just after grouping
was applied on the whole set of data in USAGES table. It slows down performance of the 
dbms unnecessary if you want data just for one month. 
Parametric views are good suggestion but because views are named sql queries, than binding value to the parameters in sql 
text of the view is not good option. Because of that, neither SQL Server and Oracle and others, don't have real parametric views.
They use stored T-SQL/PLSQL table function concepts for function with parameters that can return a table as the result set.

In Oracle we have yet another option, to use SYS_CONTEXT parameters. We can pass these parameters for the query in a view 
through mechanism of global variables, that is called system application context in Oracle. It is good if we use views in a 
single-instance queries. But we must be careful in distributed queries if our dedicated view is another instance 
because SYS_CONTEXT parameters are setted through mechanism of the single session. When we call procedures in another instance to set 
parameter by database link and than make query through the view, it could be possible the view doesn't see value that was previously  setted as it is not same session 
xto another database. To avoid that situation, we must use application context that is declared for ACCESS GLOBALY
to indicate that different sessions will share same values of variables in same context.
*/

-------------------- E) APP CONTEXT in &&usage_mgmt.. schema for global variables in the 1. instance --------------------------------------
conn &&usage_mgmt/&&usgmgmt_pass@&&dbalias_um

@@Solution3_procs_bcu_api_pkg.sql

create context BILL_CALC_TO_USAGES using &&usage_mgmt..MV_bill_calc_usages_context_API 
accessed globally;

grant execute on &&usage_mgmt..MV_bill_calc_usages_context_API to &&bill_calc
/

conn &&bill_calc/&&billcalc_pass@&&dbalias_um

create synonym &&bill_calc..MV_bill_calc_usages_API for &&usage_mgmt..MV_bill_calc_usages_context_API
/

conn &&usage_mgmt/&&usgmgmt_pass@&&dbalias_um

-- With context usage to filter data for the month and year and possibility to filter view by them
-- just to prevent errors if values for sys_context variables are not passed well in the same session.
CREATE OR REPLACE VIEW &&usage_mgmt..CVE_V_USAGES_BT
AS
(select /* +ALL_ROWS */ extract (year from u_date) va_year, extract (month from u_date) va_month,
		client_id, min(u_date) first_bt_ud, max(u_date) last_bt_ud, sum(quantity*nvl(price,0)) bt_cost
from &&usage_mgmt..USAGES
where extract (year from u_date) = nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_year')),extract (year from u_date))
and extract (month from u_date) = nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_month')),extract (month from u_date))
group by extract (year from u_date), extract (month from u_date), 
		client_id 
)
/

-- Final view for Bill Total --
CREATE OR REPLACE VIEW &&usage_mgmt..CTX_V_USAGES_BT
AS
(select /* +ALL_ROWS */ client_id, min(u_date) first_bt_ud, max(u_date) last_bt_ud, sum(quantity*nvl(price,0)) bt_cost
from &&usage_mgmt..USAGES
where extract (year from u_date) = to_number(sys_context('BILL_CALC_TO_USAGES','scp_year'))
and extract (month from u_date) = to_number(sys_context('BILL_CALC_TO_USAGES','scp_month'))
group by client_id 
)
/
---------------------------- VIEW for Bill Subtotal -------------------------------------
CREATE OR REPLACE VIEW &&usage_mgmt..CVE_V_USAGES_BST
AS
(select /* +ALL_ROWS */ extract (year from u_date) va_year, extract (month from u_date) va_month, client_id,
		unit_of_measure, sum(quantity) bst_quantity, sum(quantity*nvl(price,0)) bst_cost
from &&usage_mgmt..USAGES
where extract (year from u_date) = nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_year')),extract (year from u_date))
and extract (month from u_date) = nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_month')),extract (month from u_date))
and client_id = nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_client_id')), client_id)
group by 	extract (year from u_date), extract (month from u_date), client_id, 
			unit_of_measure  
)
/

CREATE OR REPLACE VIEW &&usage_mgmt..CTX_V_USAGES_BST
AS
(select /* +ALL_ROWS */ unit_of_measure, sum(quantity) bst_quantity, sum(quantity*nvl(price,0)) bst_cost
from &&usage_mgmt..USAGES
where extract (year from u_date) = to_number(sys_context('BILL_CALC_TO_USAGES','scp_year'))
and extract (month from u_date) = to_number(sys_context('BILL_CALC_TO_USAGES','scp_month'))
and client_id = to_number(sys_context('BILL_CALC_TO_USAGES','scp_client_id'))
group by unit_of_measure 
)
/

---------------------------- VIEW for Bill Item -------------------------------------
CREATE OR REPLACE VIEW &&usage_mgmt..CVE_V_USAGES_BI
AS
(select /* +ALL_ROWS */ extract (year from u_date) va_year, extract (month from u_date) va_month, client_id, unit_of_measure,
		type_id, price, min(u_date) first_bi_ud, max(u_date) last_bi_ud,
						sum(quantity) bi_quantity, sum(quantity*nvl(price,0)) bi_cost, count(*) c_times
from &&usage_mgmt..USAGES
where extract (year from u_date)=nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_year')),extract (year from u_date))
and extract (month from u_date)=nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_month')),extract (month from u_date))
and client_id = nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_client_id')), client_id)
and unit_of_measure = nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_unit_of_measure')), unit_of_measure)
group by 	extract (year from u_date), extract (month from u_date), client_id, unit_of_measure, 
			type_id, price 
)
/

CREATE OR REPLACE VIEW &&usage_mgmt..CTX_V_USAGES_BI
AS
(select /* +ALL_ROWS */ type_id, price, min(u_date) first_bi_ud, max(u_date) last_bi_ud,
						sum(quantity) bi_quantity, sum(quantity*nvl(price,0)) bi_cost, count(*) c_times
from &&usage_mgmt..USAGES
where extract (year from u_date)=to_number(sys_context('BILL_CALC_TO_USAGES','scp_year'))
and extract (month from u_date)=to_number(sys_context('BILL_CALC_TO_USAGES','scp_month'))
and client_id = to_number(sys_context('BILL_CALC_TO_USAGES','scp_client_id'))
and unit_of_measure = to_number(sys_context('BILL_CALC_TO_USAGES','scp_unit_of_measure'))
group by type_id, price 
)
/

-------------------- F) ADD VIRTUAL COLUMNS AND CHANGE VIEW DEFINITIONS, same output interface 
conn &&usage_mgmt/&&usgmgmt_pass@&&dbalias_um

ALTER TABLE &&usage_mgmt..USAGES
add (vc_year as (extract (year from u_date)), 
	vc_month as (extract (month from u_date)));
	
---- VIEWS ARE UPDATED ----------------------
	
-- Definition of the previous vies can be shorter:
CREATE OR REPLACE VIEW &&usage_mgmt..V_USAGES_BT
AS
(select /* +ALL_ROWS */ vc_year va_year, vc_month va_month, 
		client_id, min(u_date) first_bt_ud, max(u_date) last_bt_ud, sum(quantity*nvl(price,0)) bt_cost
from &&usage_mgmt..USAGES
group by vc_year, vc_month, client_id 
)
/

CREATE OR REPLACE VIEW &&usage_mgmt..CVE_V_USAGES_BT
AS
(select /* +ALL_ROWS */ vc_year va_year, vc_month va_month,
		client_id, min(u_date) first_bt_ud, max(u_date) last_bt_ud, sum(quantity*nvl(price,0)) bt_cost
from &&usage_mgmt..USAGES
where vc_year = nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_year')), vc_year)
and vc_month = nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_month')), vc_month)
group by vc_year, vc_month, 
		client_id 
)
/

-- Final view for Bill Total --
CREATE OR REPLACE VIEW &&usage_mgmt..CTX_V_USAGES_BT
AS
(select /* +ALL_ROWS */ client_id, min(u_date) first_bt_ud, max(u_date) last_bt_ud, sum(quantity*nvl(price,0)) bt_cost
from &&usage_mgmt..USAGES
where vc_year = to_number(sys_context('BILL_CALC_TO_USAGES','scp_year'))
and vc_month = to_number(sys_context('BILL_CALC_TO_USAGES','scp_month'))
group by client_id 
)
/
---------------------------- VIEW for Bill Subtotal -------------------------------------
CREATE OR REPLACE VIEW &&usage_mgmt..CVE_V_USAGES_BST
AS
(select /* +ALL_ROWS */ vc_year va_year, vc_month va_month, client_id,
		unit_of_measure, sum(quantity) bst_quantity, sum(quantity*nvl(price,0)) bst_cost
from &&usage_mgmt..USAGES
where vc_year = nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_year')), vc_year)
and vc_month = nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_month')), vc_month)
and client_id = nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_client_id')), client_id)
group by 	vc_year, vc_month, client_id, 
			unit_of_measure  
)
/

CREATE OR REPLACE VIEW &&usage_mgmt..CTX_V_USAGES_BST
AS
(select /* +ALL_ROWS */ unit_of_measure, sum(quantity) bst_quantity, sum(quantity*nvl(price,0)) bst_cost
from &&usage_mgmt..USAGES
where vc_year = to_number(sys_context('BILL_CALC_TO_USAGES','scp_year'))
and vc_month = to_number(sys_context('BILL_CALC_TO_USAGES','scp_month'))
and client_id = to_number(sys_context('BILL_CALC_TO_USAGES','scp_client_id'))
group by unit_of_measure 
)
/
---------------------------- VIEW for Bill Item -------------------------------------
CREATE OR REPLACE VIEW &&usage_mgmt..CVE_V_USAGES_BI
AS
(select /* +ALL_ROWS */ vc_year va_year, vc_month va_month, client_id, unit_of_measure,
		type_id, price, min(u_date) first_bi_ud, max(u_date) last_bi_ud,
						sum(quantity) bi_quantity, sum(quantity*nvl(price,0)) bi_cost, count(*) c_times
from &&usage_mgmt..USAGES
where vc_year=nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_year')), vc_year)
and vc_month=nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_month')), vc_month)
and client_id = nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_client_id')), client_id)
and unit_of_measure = nvl(to_number(sys_context('BILL_CALC_TO_USAGES','scp_unit_of_measure')), unit_of_measure)
group by 	vc_year, vc_month, client_id, unit_of_measure, 
			type_id, price 
)
/

CREATE OR REPLACE VIEW &&usage_mgmt..CTX_V_USAGES_BI
AS
(select /* +ALL_ROWS */ type_id, price, min(u_date) first_bi_ud, max(u_date) last_bi_ud,
						sum(quantity) bi_quantity, sum(quantity*nvl(price,0)) bi_cost, count(*) c_times
from &&usage_mgmt..USAGES
where vc_year=to_number(sys_context('BILL_CALC_TO_USAGES','scp_year'))
and vc_month=to_number(sys_context('BILL_CALC_TO_USAGES','scp_month'))
and client_id = to_number(sys_context('BILL_CALC_TO_USAGES','scp_client_id'))
and unit_of_measure = to_number(sys_context('BILL_CALC_TO_USAGES','scp_unit_of_measure'))
group by type_id, price 
)
/

-------------------- G) SUPPORTING PLSQL API in the 2. instance FOR BILL CALCULATION ----------------------------------------- 
conn &&bill_calc/&&billcalc_pass@&&dbalias_bc

@@Solution3_procs_pkg.sql

-------------------- H) CHANGE DDL MODEL and ADD PARTITIONS FOR USAGES TABLE AND BILL CALCs TABLES for better performance ----
conn &&usage_mgmt/&&usgmgmt_pass@&&dbalias_um

ALTER TABLE &&usage_mgmt..USAGES
PCTFREE 80 PCTUSED 10;
--STORAGE(NEXT 5M MINEXTENTS 2 PCTINCREASE 10) 
-- local on laptop
--PCTFREE 80 PCTUSED 10 STORAGE(NEXT 100M MINEXTENTS 20 PCTINCREASE 10); -- demo production

conn &&bill_calc/&&billcalc_pass@&&dbalias_bc

ALTER TABLE &&bill_calc..BILL_TOTAL
PCTFREE 80 PCTUSED 10;

ALTER TABLE &&bill_calc..BILL_SUBTOTAL
PCTFREE 80 PCTUSED 10;

ALTER TABLE &&bill_calc..BILL_ITEM
PCTFREE 80 PCTUSED 10;

ALTER TABLE &&bill_calc..LOG_MISSING_PRICES
PCTFREE 80 PCTUSED 10;

-------------------- Defining partitions based on vc_year, vc_month ----------------------------------	

------------ In the 1. instance -------------
conn &&usage_mgmt/&&usgmgmt_pass@&&dbalias_um

ALTER TABLE &&usage_mgmt..USAGES
modify  
partition by range (vc_year)
	subpartition by list (vc_month)
		subpartition template 
			( subpartition p_Jan values (1),
			subpartition p_Feb values (2),
			subpartition p_Mar values (3),
			subpartition p_Apr values (4),
			subpartition p_May values (5),
			subpartition p_Jun values (6),
			subpartition p_Jul values (7),
			subpartition p_Avg values (8),
			subpartition p_Sep values (9),
			subpartition p_Oct values (10),
			subpartition p_Nov values (11),
			subpartition p_Dec values (12)
			)
(partition p2019 values less than (2020),
partition p2020 values less than (2021),
partition p2021 values less than (2022)
)
online;

ALTER TABLE &&usage_mgmt..USAGES
add partition p2022 values less than (2023);

------------ In the 2. instance -------------
conn &&bill_calc/&&billcalc_pass@&&dbalias_bc

ALTER TABLE &&bill_calc..BILL_TOTAL
modify  
partition by range (BT_YEAR)
	subpartition by list (BT_MONTH)
		subpartition template 
			( subpartition p_Jan values (1),
			subpartition p_Feb values (2),
			subpartition p_Mar values (3),
			subpartition p_Apr values (4),
			subpartition p_May values (5),
			subpartition p_Jun values (6),
			subpartition p_Jul values (7),
			subpartition p_Avg values (8),
			subpartition p_Sep values (9),
			subpartition p_Oct values (10),
			subpartition p_Nov values (11),
			subpartition p_Dec values (12)
			)
(partition p2019 values less than (2020),
partition p2020 values less than (2021),
partition p2021 values less than (2022),
partition p2022 values less than (2023)
)
online;

ALTER TABLE &&bill_calc..BILL_SUBTOTAL
modify  
partition by reference (BILL_SUBTOTAL_FK);

ALTER TABLE &&bill_calc..BILL_ITEM
modify  
partition by reference (BILL_ITEM_FK);

---------------- I) PLSQL API to support PLSQL table function -----------------------------------


