/* Author: Mladen Vidic
   E-mail: mladen.vidic@gmail.com
   Location: Belgrade, Serbia || Doboj, Bosnia and Herzegovina
   Date Signed: 12.5.2021.
   
   Scope: PLSQL Types of rows for Task 3. from Solution3.sql
   
   LICENCE: The licensing rules from the 'license.txt' or '..\license.txt' file apply to this file, solution and its parts.
*/


--- TYPES for ROW
create type t_ctx_bt_row as object 
	(client_id number, first_bt_ud date, last_bt_ud date, bt_cost number);
	/


create type t_ctx_bst_row as object 
	(unit_of_measure number, bst_quantity number, bst_cost number);
	/

create type t_ctx_bi_row as object 
	(type_id number, 
	price number, 
	first_bi_ud date, 
	last_bi_ud date,
	bi_quantity number, 
	bi_cost number,
	c_times integer);
	/
