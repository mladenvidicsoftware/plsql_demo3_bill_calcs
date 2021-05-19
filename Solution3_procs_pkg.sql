/* Author: Mladen Vidic
   E-mail: mladen.vidic@gmail.com
   Location: Belgrade, Serbia || Doboj, Bosnia and Herzegovina
   Date Signed: 12.5.2021.
   
   Scope: PLSQL APIs for Task 3. from Solution3.sql
   
   Note: Procedures and function could have shorter names. These names are used for self describing purposes for better readability 
   and easier following of code development pathway.
   
   LICENCE: The licensing rules from the 'license.txt' or '..\license.txt' file apply to this file, solution and its parts.
*/
-------------------------------- STATIC MACROS FOR DB LINK ------------------------------------
-- For local execution when &&bill_calc schema and &&usage_mgmt schema are in the same database
define UMDBLINKLOCAL=

-- For remote access to table usages over db link in different instance
define UMDBLINKREMOTE=@dblink_to_um

---------------------------- API -----------------------------
CREATE OR REPLACE Package &&bill_calc..MV_Bill_Calc_API IS	
	
	/* Slower VERSION 0A.: First extracts clients for the month and then calls for each client separately.
	*/
	procedure createAllBillsForMonthYearSlowLocally (p_month in integer, p_year in integer); -- , status out integer???
		
	-- Note: performance results could be similar if we create indexes or bitmap indexes or partitions by client and year and month.
	procedure createClientBillForMonthYearLocally (p_month in integer, p_year in integer, p_client_id in number); -- , status out integer???

	/* Fast locally VERSION 0B.:
	*/
	procedure createAllBillsForMonthYearFastLocally (p_month in integer, p_year in integer); -- , status out integer???
	
	/* Fastest remote version with static database link defined during compilation of the package body.
	   Uses declared cursors with statically defined database link.
	   This function has several versions of the cursors, 
	   VERSION 1., 2., 3A., 3B. See it in the package body.
	*/
	procedure createAllBillsForMonthYearFastestRemoteStaticDBL3 (p_month in integer, p_year in integer);
	
	/* Uses parametrized table functions, see text for VERSION 4. in the package body
	procedure createAllBillsForMonthYearFastestRemoteStaticDBL4 (p_month in integer, p_year in integer);
	Not included in the implementation of the package.
	*/
	
	/* Faster remote VERSION 5. with dynamically passed schema owner of USAGES table and 
	   database link through input parameters of the procedure.
	   Uses dynamic cursor variables, instead of static cursors, for dynamic SQL cursors with 
	   incorporated parameter for user schema and database link string.
	*/
	procedure createAllBillsForMonthYearFastRemoteDynamicDBL5 (	p_month in integer, p_year in integer, 
																p_user_schema in varchar2, p_remote_dblink in varchar2);
	
	/* Maybe, if I would have time! It would be VERSION 6.
	procedure createAllBillsForMonthYearWithoutCursorsEmbededQueries (p_month in integer, p_year in integer, remote_dblink in varchar2);
	Not included in the implementation of the package.
	*/
	
	procedure logPriceWasMissed(p_month in integer, p_year in integer, p_client_id in number, 
								p_type_id in number, p_first_ud in date, p_last_ud in date, p_quantity in number,
								p_unit_of_measure in number, p_c_times in integer, 
								p_um_schema in varchar2, p_db_link in varchar2 default 'LOCAL');
								
	/* 	NOTE FOR 'LOCALLY suffixed' procedures (Locally suffix in the name of the procedure): 
		These procedures don't use database link that operator or a person responsible for 
		creating bills don't make mistake and execute this procedure for remotely 
		USAGES table over database link.
		In these procedures is assumed that schema &&usage_mgmt of the USAGES table 
		is in the same database instance and database under &&bill_calc schema. It is because 
		of these procedures queries are defined in that way if execution optimization of the query
		is done completely in one locally database where procedure was called for execution.
		
		NOTE FOR 'REMOTE infix' procedures (Remote infix in the name of the procedure): 
		These procedures can use database link but don't need always. Static version defines database 
		link during package body compilation. Dynamic version can pass database link through parameter. 
		In these procedures is assumed that schema &&usage_mgmt of the USAGES table 
		can be in the same locally or remotely database instance and database under &&bill_calc schema.
		If database link is not defined than queries will be executed in local database. 
		These queries are defined by views under schema &&usage_mgmt so one component of the query is calculated through
		view and another part is calculated over the view mid-results. So, view part of the query can be executed on remote database.
		If no link is specified, than both parts of the queries, view part and the rest of the query are calculated in the same 
		local database. Views are good pathway to control place of execution different parts of the 
		complex and distributed queries.
		In these remotely oriented procedures with 2-phase or multi-phase queries, views and sub-queries  
		are defined that execution optimization engine splits query to remote parts and local parts and 
		delegate remote database servers to execute its part and return result to local server which 
		integrate these results for calculation of the final query result in the procedure where query 
		called for execution.	
	*/

END;
/

-- TYPES for ROWs under the 2. instance where query tables are calculated
@@Solution3_types_for_rows.sql


CREATE OR REPLACE Package Body &&bill_calc..MV_Bill_Calc_API IS
	
	-- Slower version: First extracts just clients for the month and year, then calls for each client separately 
	-- the procedure that reads again other data about that client from the same table.	
	procedure createAllBillsForMonthYearSlowLocally (p_month in integer, p_year in integer)  
	is		
		cursor curClients_MonthYear is
				select client_id							-- or change to: select DISTINCT client_id 
				from &&usage_mgmt..USAGES	--&&UMDBLINKLOCAL
				where extract (year from u_date)=p_year 
				and extract (month from u_date)=p_month
				group by client_id;							-- without: 	group by client_id 
				
	begin 
		for itemC_ClientsMY in curClients_MonthYear
		LOOP
			createClientBillForMonthYearLocally (p_month, p_year, itemC_ClientsMY.client_id);			
		END LOOP; --curClients_MonthYear
		
		/* It is better has no exception that situation can be escalated to calling procedure to handle errors
			in semantic context 
		*/		
	end; 	
		
	procedure createClientBillForMonthYearLocally (p_month in integer, p_year in integer, p_client_id in number)  
	is		
		cursor curBill_Total is
				select min(u_date) first_bt_ud, max(u_date) last_bt_ud, sum(quantity*nvl(price,0)) bt_cost
				from &&usage_mgmt..USAGES	--&&UMDBLINKLOCAL
				where extract (year from u_date)=p_year 
				and extract (month from u_date)=p_month
				and client_id=p_client_id;
				
		cursor curBill_Subtotal is
				select /* +ALL_ROWS */ unit_of_measure, sum(quantity) bst_quantity, sum(quantity*nvl(price,0)) bst_cost
				from &&usage_mgmt..USAGES	--&&UMDBLINKLOCAL
				where extract (year from u_date)=p_year 
				and extract (month from u_date)=p_month
				and client_id = p_client_id
				group by unit_of_measure;
				
		l_unit_of_measure number;

		cursor curBill_Item is
				select /* +ALL_ROWS */ type_id, price, min(u_date) first_bi_ud, max(u_date) last_bi_ud,
						sum(quantity) bi_quantity, sum(quantity*nvl(price,0)) bi_cost, count(*) c_times
				from &&usage_mgmt..USAGES	--&&UMDBLINKLOCAL
				where extract (year from u_date)=p_year 
				and extract (month from u_date)=p_month
				and client_id = p_client_id
				and unit_of_measure=l_unit_of_measure
				group by type_id, price;
				
		l_bt_id 	number;
		l_bst_id 	number;
		l_bi_id		number;
				
	begin 
		for itemC_BT in curBill_Total
		LOOP
			select BILL_TOTAL_seq.nextval into l_bt_id from dual;
			
			insert into BILL_TOTAL (ID, CLIENT_ID, BT_MONTH, BT_YEAR, FIRST_USAGE_DATE, LAST_USAGE_DATE, COST)
			values (l_bt_id, p_client_id, p_month, p_year, itemC_BT.first_bt_ud, itemC_BT.last_bt_ud, itemC_BT.bt_cost);	

			/* Maybe is better to use next code pattern for stronger controlling close of the cursor if we have to break loops earlier.
			open cursor cur;
			fetch cur into ...
			while cur%FOUND 
			loop
				
				exit...
				
				close cur; return; 
				
				fetch cur into ...
			end loop;
			close cur;		
			*/			
			
			for itemC_BST in curBill_Subtotal
			LOOP
				select BILL_SUBTOTAL_seq.nextval into l_bst_id from dual;
				
				insert into BILL_SUBTOTAL (ID, BILL_TOTAL_ID, UNIT_OF_MEASURE, QUANTITY, COST)
				values (l_bst_id, l_bt_id, itemC_BST.unit_of_measure, itemC_BST.bst_quantity, itemC_BST.bst_cost);
				
				l_unit_of_measure:=itemC_BST.unit_of_measure;	
				
				for itemC_BI in curBill_Item
				LOOP
					if itemC_BI.price is null then
						logPriceWasMissed(p_month, p_year, p_client_id, 
								itemC_BI.type_id, itemC_BI.first_bi_ud, itemC_BI.last_bi_ud, itemC_BI.bi_quantity,
								l_unit_of_measure, itemC_BI.c_times, '&&usage_mgmt');	--, '&&UMDBLINKLOCAL');
						/* 	if (needs to brake here for current client when price is unknown for the first time) then
								-- close curBill_Item;    	-- <== Is not needed if return will be used after;
								-- close curBill_Subtotal;  -- <== Is not needed if return will be used after;
								-- close curBill_Total;		-- Similar conclusion.
								return; 
							end if;
						*/ 	-- NOTE: If return is used to exit from procedure/function than locally declared cursors in 
							-- stored procedure/function will be closed by oracle automatically after returning from subroutine. 
					end if;
					
					select BILL_ITEM_seq.nextval into l_bi_id from dual;
					
					insert into BILL_ITEM (ID, BILL_SUBTOTAL_ID, USAGE_TYPE_ID, 
											FIRST_USAGE_DATE, LAST_USAGE_DATE, QUANTITY, UNIT_OF_MEASURE, PRICE, COST)
					values (l_bi_id, l_bst_id, itemC_BI.type_id, 
							itemC_BI.first_bi_ud, itemC_BI.last_bi_ud, itemC_BI.bi_quantity, l_unit_of_measure, 
							nvl(itemC_BI.price,0), itemC_BI.bi_cost);			
				END LOOP; --curBill_Item
				
			END LOOP; --curBill_Subtotal
			
		END LOOP; --curBill_Total
		
		/* It is better has no exception that situation can be escalated to calling procedure to handle errors
			in semantic context 
		*/
		
		/*exception
			when no_data_found then
				-- If error is handled here, than we must extend procedure by status to return error code.
				-- Usually, it is semantically enclosed by an application context of usage.
				null;
			when other then
				-- Similar as the previous comment.
				null;
		*/		
	end;	

	
	-- Fast locally version: In the same query extracts clients for the month and their bill total values.
	procedure createAllBillsForMonthYearFastLocally (p_month in integer, p_year in integer)  
	is		
		cursor curBills_Total is
				select /* +ALL_ROWS */ client_id, min(u_date) first_bt_ud, max(u_date) last_bt_ud, sum(quantity*nvl(price,0)) bt_cost
				from &&usage_mgmt..USAGES	--&&UMDBLINKLOCAL
				where extract (year from u_date)=p_year 
				and extract (month from u_date)=p_month
				group by client_id;
				
		l_client_id number;
				
		cursor curBill_Subtotal is
				select /* +ALL_ROWS */ unit_of_measure, sum(quantity) bst_quantity, sum(quantity*nvl(price,0)) bst_cost
				from &&usage_mgmt..USAGES	--&&UMDBLINKLOCAL
				where extract (year from u_date)=p_year 
				and extract (month from u_date)=p_month
				and client_id = l_client_id
				group by unit_of_measure;
				
		l_unit_of_measure number;

		cursor curBill_Item is
				select /* +ALL_ROWS */ type_id, price, min(u_date) first_bi_ud, max(u_date) last_bi_ud,
						sum(quantity) bi_quantity, sum(quantity*nvl(price,0)) bi_cost, count(*) c_times
				from &&usage_mgmt..USAGES	--&&UMDBLINKLOCAL
				where extract (year from u_date)=p_year 
				and extract (month from u_date)=p_month
				and client_id = l_client_id
				and unit_of_measure=l_unit_of_measure
				group by type_id, price;
				
		l_bt_id 	number;
		l_bst_id 	number;
		l_bi_id		number;
				
	begin 
		for itemC_BT in curBills_Total
		LOOP
			select BILL_TOTAL_seq.nextval into l_bt_id from dual;
			
			insert into BILL_TOTAL (ID, CLIENT_ID, BT_MONTH, BT_YEAR, FIRST_USAGE_DATE, LAST_USAGE_DATE, COST)
			values (l_bt_id, itemC_BT.client_id, p_month, p_year, itemC_BT.first_bt_ud, itemC_BT.last_bt_ud, itemC_BT.bt_cost);
			
			l_client_id:=itemC_BT.client_id;

			/* Maybe is better to use next code pattern for stronger controlling close of the cursor if we have to break loops earlier.
			open cursor cur;
			fetch cur into ...
			while cur%FOUND 
			loop
				
				exit...
				
				close cur; return; 
				
				fetch cur into ...
			end loop;
			close cur;		
			*/
			
			for itemC_BST in curBill_Subtotal
			LOOP
				select BILL_SUBTOTAL_seq.nextval into l_bst_id from dual;
				
				insert into BILL_SUBTOTAL (ID, BILL_TOTAL_ID, UNIT_OF_MEASURE, QUANTITY, COST)
				values (l_bst_id, l_bt_id, itemC_BST.unit_of_measure, itemC_BST.bst_quantity, itemC_BST.bst_cost);
				
				l_unit_of_measure:=itemC_BST.unit_of_measure;	
				
				for itemC_BI in curBill_Item
				LOOP					
					if itemC_BI.price is null then
						logPriceWasMissed(p_month, p_year, l_client_id, 
								itemC_BI.type_id, itemC_BI.first_bi_ud, itemC_BI.last_bi_ud, itemC_BI.bi_quantity,
								l_unit_of_measure, itemC_BI.c_times, '&&usage_mgmt');	--, '&&UMDBLINKLOCAL');
								
					/* 	if (needs to brake here for current client when price is unknown for the first time) then
							close curBill_Item;
							close curBill_Subtotal;
							exit; 
						end if;
					*/ -- ATTENTION: Don't use exit without closed cursors regularly if you intent to exit from both FOR loops.
					end if;
					
					select BILL_ITEM_seq.nextval into l_bi_id from dual;
					
					insert into BILL_ITEM (ID, BILL_SUBTOTAL_ID, USAGE_TYPE_ID, 
											FIRST_USAGE_DATE, LAST_USAGE_DATE, QUANTITY, UNIT_OF_MEASURE, PRICE, COST)
					values (l_bi_id, l_bst_id, itemC_BI.type_id, 
							itemC_BI.first_bi_ud, itemC_BI.last_bi_ud, itemC_BI.bi_quantity, l_unit_of_measure, 
							nvl(itemC_BI.price,0), itemC_BI.bi_cost);			
				END LOOP; --curBill_Item 
				
			END LOOP; --curBill_Subtotal
			
		END LOOP; --curBills_Total
		
		/* It is better has no exception that situation can be escalated to calling procedure to handle errors
			in semantic context 
		*/
		
		/*exception
			when no_data_found then
				-- If error is handled here, than we must extend procedure by status to return error code.
				-- Usually, it is semantically enclosed by an application context of usage.
				null;
			when other then
				-- Similar as the previous comment.
				null;
		*/		
	end; 
	
	procedure createAllBillsForMonthYearFastestRemoteStaticDBL3 (p_month in integer, p_year in integer)  
	is		
		l_client_id number;
		l_unit_of_measure number;
		
		/*	VERSION 1: Stil Slow!
			Sometimes for optimizer is not good enough just to transfer remotely oriented part 
			of the query to sub-query because binding of variable values is done in local server PGA.
			With cursor variables and cursors for dynamic sql we will change binding of variable value 
			by constant value previously calculated from the variable.
			
			cursor curBills_Total is
				select * 
				from 	
					(select client_id, min(u_date) first_bt_ud, max(u_date) last_bt_ud, sum(quantity*nvl(price,0)) bt_cost
					from &&usage_mgmt..USAGES&&UMDBLINKREMOTE
					where extract (year from u_date)=p_year 
					and extract (month from u_date)=p_month
					group by client_id);
		*/	

		/*	VERSION 2: Better but Slow!
			This cursor is faster than previous because it is calculated remotely. Its disadvantage
			is that we couldn't pass values of month and year to filer data remotely before grouping. 
			So, remote server would had been grouping data over the whole USAGES table and had had to calculates 
			bill groups for all existing years, months and clients in remote database. Latewr, remote server passed back all list
			that local server can filter it just for one year and month. It is to much sent data for just one month.
			It is because p_month and p_year are still binding in a PGA of the local server process.
			We must find the way to pass values for these parameters to remote server.
			
			cursor curBills_Total is
				select client_id, first_bt_ud, last_bt_ud, bt_cost  
				from &&usage_mgmt..V_USAGES_BT&&UMDBLINKREMOTE
				where va_year=p_year
				and va_month=p_month;
		*/		
		
		/*	VERSION 3A. Three A, faster choice uses semi-parametric views by globally accessed SYS_CONTEXT and its variables to pass parameter values 
		    to views for filtering rows before grouping, and again grouping after filter. It is much better than version 2. These parameters are passed  
			to the views that are remotely defined and which must be fully queried before passing requested results 
			back to calling locally server session and server process. Data must be be filtered on remote server for a year and month and 
			grouping will be done there. In these views with prefix CVE_V% grouping columns are returned, even was done grouping by them, and in the cursors are done
			additional filter again, even it was done if they are done correctly if global variables in context were defined, and is not needed since was done in the views. 
			It is security advantage to return correct results in final cursor.
			In version 3B we used queries on CTX_V% views with the same goal but without this additional filters in the cursors on the client. 
		*/	
			cursor curCVE_Bills_Total is
				select /* +ALL_ROWS */ client_id, first_bt_ud, last_bt_ud, bt_cost  
				from &&usage_mgmt..CVE_V_USAGES_BT&&UMDBLINKREMOTE
				where va_year=p_year
				and va_month=p_month;
			
			cursor curCVE_Bill_Subtotal is
				select /* +ALL_ROWS */ unit_of_measure, bst_quantity, bst_cost
				from &&usage_mgmt..CVE_V_USAGES_BST&&UMDBLINKREMOTE
				where va_year=p_year
				and va_month=p_month
				and client_id = l_client_id
				;	
				
			cursor curCVE_Bill_Item is
				select /* +ALL_ROWS */ type_id, price, first_bi_ud, last_bi_ud, bi_quantity, bi_cost, c_times
				from &&usage_mgmt..CVE_V_USAGES_BI&&UMDBLINKREMOTE
				where va_year=p_year
				and va_month=p_month
				and client_id = l_client_id
				and unit_of_measure=l_unit_of_measure
				;
		
		/*	VERSION 3B. Three B is even more faster choice than 3A since lesss data is transfered through the network. 
		    This option also uses semi-parametric views CTX_V%and to force passing values 
			to the view that is remotely defined and there must be fully queried before passing requested results 
			back to calling locally server. Cursors doen;t do additionl filtering but results will be empty if parameters ate not passed well through contset 
			application's parameters.
			Data are only filtered on remote server for year, month, client and unit_of_measure and 
			grouping will be done there just by client id.
		*/		
		-- These cursors are faster than case 3A but be careful with context variables if another instance is used for distributed remote query!				
		cursor curCTX_Bills_Total is
				select /* +ALL_ROWS */ client_id, first_bt_ud, last_bt_ud, bt_cost  
				from &&usage_mgmt..CTX_V_USAGES_BT&&UMDBLINKREMOTE
				;		
				
		cursor curCTX_Bill_Subtotal is
				select /* +ALL_ROWS */ unit_of_measure, bst_quantity, bst_cost
				from &&usage_mgmt..CTX_V_USAGES_BST&&UMDBLINKREMOTE
				;		

		cursor curCTX_Bill_Item is
				select /* +ALL_ROWS */ type_id, price, first_bi_ud, last_bi_ud, bi_quantity, bi_cost, c_times
				from &&usage_mgmt..CTX_V_USAGES_BI&&UMDBLINKREMOTE
				;
				
		l_bt_id 	number;
		l_bst_id 	number;
		l_bi_id		number;
				
	begin	
		--set sys_context('BILL_CALC_TO_USAGES','scp_year') na p_year u udaljenoj bazi
		MV_bill_calc_usages_API.set_parameter&&UMDBLINKREMOTE('scp_year', to_char(p_year));
		--sys_context('BILL_CALC_TO_USAGES','scp_month') na p_month u udaljenoj bazi
		MV_bill_calc_usages_API.set_parameter&&UMDBLINKREMOTE('scp_month', to_char(p_month));
		
		for itemC_BT in curCTX_Bills_Total 	/* 	Use curCVE_Bills_Total if not sure for persistence of
												remote SYS_CONTEXT values of scp_year, scp_month.
											*/											
		LOOP
			select BILL_TOTAL_seq.nextval into l_bt_id from dual;
			
			insert into BILL_TOTAL (ID, CLIENT_ID, BT_MONTH, BT_YEAR, FIRST_USAGE_DATE, LAST_USAGE_DATE, COST)
			values (l_bt_id, itemC_BT.client_id, p_month, p_year, itemC_BT.first_bt_ud, itemC_BT.last_bt_ud, itemC_BT.bt_cost);
			
			
			l_client_id:=itemC_BT.client_id;			
			--set sys_context('BILL_CALC_TO_USAGES','scp_client_id') na l_client_id u udaljenoj bazi
			MV_bill_calc_usages_API.set_parameter&&UMDBLINKREMOTE('scp_client_id', to_char(l_client_id));
			
			/* Maybe is better to use next code pattern for stronger controlling close of the cursor if we have to break loops earlier.
			open cursor cur;
			fetch cur into ...
			while cur%FOUND 
			loop
				
				exit...
				
				close cur; return; 
				
				fetch cur into ...
			end loop;
			close cur;		
			*/
			
			for itemC_BST in curCTX_Bill_Subtotal	/* 	Use curCVE_Bill_Subtotal if not sure for persistence of
													remote SYS_CONTEXT values of scp_year,scp_month, scp_client_id.
													*/				
			LOOP
				select BILL_SUBTOTAL_seq.nextval into l_bst_id from dual;
				
				insert into BILL_SUBTOTAL (ID, BILL_TOTAL_ID, UNIT_OF_MEASURE, QUANTITY, COST)
				values (l_bst_id, l_bt_id, itemC_BST.unit_of_measure, itemC_BST.bst_quantity, itemC_BST.bst_cost);
				
				l_unit_of_measure:=itemC_BST.unit_of_measure;				
				--set sys_context('BILL_CALC_TO_USAGES','scp_unit_of_measure') na l_unit_of_measure u udaljenoj bazi
				MV_bill_calc_usages_API.set_parameter&&UMDBLINKREMOTE('scp_unit_of_measure', to_char(l_unit_of_measure));
				
				for itemC_BI in curCTX_Bill_Item	/* 	Use curCVE_Bill_Item if not sure for persistence of
													remote SYS_CONTEXT values of scp_year,scp_month, scp_client_id,
													scp_unit_of_measure.
													*/
				LOOP					
					if itemC_BI.price is null then
						logPriceWasMissed(p_month, p_year, l_client_id, 
								itemC_BI.type_id, itemC_BI.first_bi_ud, itemC_BI.last_bi_ud, itemC_BI.bi_quantity,
								l_unit_of_measure, itemC_BI.c_times, '&&usage_mgmt', '&&UMDBLINKREMOTE');
								
					/* 	if (needs to brake here for current client when price is unknown for the first time) then
							close curCTX_Bill_Item;
							close curCTX_Bill_Subtotal;
							exit; 
						end if;
					*/ -- ATTENTION: Don't use exit without closed cursors regularly if you intent to exit from both FOR loops.
					end if;
					
					select BILL_ITEM_seq.nextval into l_bi_id from dual;
					
					insert into BILL_ITEM (ID, BILL_SUBTOTAL_ID, USAGE_TYPE_ID, 
											FIRST_USAGE_DATE, LAST_USAGE_DATE, QUANTITY, UNIT_OF_MEASURE, PRICE, COST)
					values (l_bi_id, l_bst_id, itemC_BI.type_id, 
							itemC_BI.first_bi_ud, itemC_BI.last_bi_ud, itemC_BI.bi_quantity, l_unit_of_measure, 
							nvl(itemC_BI.price,0), itemC_BI.bi_cost);			
				END LOOP; --curCTX_Bill_Item 
				
			END LOOP; --curCTX_Bill_Subtotal
			
		END LOOP; --curCTX_Bills_Total
		
		/* It is better has no exception that situation can be escalated to calling procedure to handle errors
			in semantic context 
		*/
		
		/*exception
			when no_data_found then
				-- If error is handled here, than we must extend procedure by status to return error code.
				-- Usually, it is semantically enclosed by an application context of usage.
				null;
			when other then
				-- Similar as the previous comment.
				null;
		*/		
	end; 
	
	/*	VERSION 4. Forth choice that uses PLSQL table functions to force passing values 
		to the query in those functions that are remotely defined and must be fully queried before passing requested results  
		back to calling local process. Data must be be filtered on remote server for a year and month, client and unit_of_measure when needed, and 
		grouping will be done there. These functions are similar to CTX_V views from solution 3B.
	*/
	
	/*	VERSION 5. Fifth choice would use cursor variable for dynamic sql cursor to insert values 
		of the variables as static text in string of the dynamic SQL before is executed and opened 
		for fetching. There is a risk that optimizer would have not delegated whole query to execute remote 
		server.
	*/
	procedure createAllBillsForMonthYearFastRemoteDynamicDBL5 (	p_month in integer, p_year in integer, 
																p_user_schema in varchar2, p_remote_dblink in varchar2)
	is
		
		l_str_bt varchar2(500);
		l_str_bst varchar2(750);
		l_str_bi varchar2(1000);
		
		l_str_plsql varchar2(500);
		
		l_user_schema_prefix varchar2(128);
		l_remote_dblink_sufix varchar2(256);
		
		l_client_id number;
		l_unit_of_measure number;
		
		cvCTX_Bills_Total SYS_REFCURSOR;  -- cursor variable
		cvCTX_Bill_Subtotal SYS_REFCURSOR;  -- cursor variable
		cvCTX_Bill_Item SYS_REFCURSOR;  -- cursor variable
		
		itemC_BT t_ctx_bt_row;		
		itemC_BST t_ctx_bst_row;		
		itemC_BI t_ctx_bi_row;		
				
		l_bt_id 	number;
		l_bst_id 	number;
		l_bi_id		number;
	
	
	begin	
		if p_user_schema is null then
			l_user_schema_prefix:='';
		else
			l_user_schema_prefix:=p_user_schema||'.';
		end if;
		
		if p_remote_dblink is null or (substr(p_remote_dblink,1,1)='@' and substr(p_remote_dblink,2)='') then
			l_remote_dblink_sufix:='';
		else
			if instr(p_remote_dblink,'@',1,1)>0 then
				l_remote_dblink_sufix:=p_remote_dblink;
			else
				l_remote_dblink_sufix:='@'||p_remote_dblink;
			end if;
		end if;
	
		--set sys_context('BILL_CALC_TO_USAGES','scp_year') na p_year u udaljenoj bazi
		l_str_plsql:='MV_bill_calc_usages_API.set_parameter'||l_remote_dblink_sufix||'(''scp_year'','''||to_char(p_year)||''')';
		EXECUTE IMMEDIATE l_str_plsql;
		--sys_context('BILL_CALC_TO_USAGES','scp_month') na p_month u udaljenoj bazi
		l_str_plsql:='MV_bill_calc_usages_API.set_parameter'||l_remote_dblink_sufix||'(''scp_month'','''||to_char(p_month)||''')';
		EXECUTE IMMEDIATE l_str_plsql;
		
		l_str_bt:='select /* +ALL_ROWS */ client_id, first_bt_ud, last_bt_ud, bt_cost from '
				||l_user_schema_prefix||'CTX_V_USAGES_BT'||l_remote_dblink_sufix;
		open cvCTX_Bills_Total for l_str_bt;		
		LOOP
		    fetch cvCTX_Bills_Total into itemC_BT;
			exit when cvCTX_Bills_Total%NOTFOUND;
			
			select BILL_TOTAL_seq.nextval into l_bt_id from dual;
			
			insert into BILL_TOTAL (ID, CLIENT_ID, BT_MONTH, BT_YEAR, FIRST_USAGE_DATE, LAST_USAGE_DATE, COST)
			values (l_bt_id, itemC_BT.client_id, p_month, p_year, itemC_BT.first_bt_ud, itemC_BT.last_bt_ud, itemC_BT.bt_cost);
			
			
			l_client_id:=itemC_BT.client_id;			
			--set sys_context('BILL_CALC_TO_USAGES','scp_client_id') na l_client_id u udaljenoj bazi
			l_str_plsql:='MV_bill_calc_usages_API.set_parameter'||l_remote_dblink_sufix||'(''scp_client_id'','''||to_char(l_client_id)||''')';
			EXECUTE IMMEDIATE l_str_plsql;
			
			l_str_bst:='select /* +ALL_ROWS */ unit_of_measure, bst_quantity, bst_cost from '
				||l_user_schema_prefix||'CTX_V_USAGES_BST'||l_remote_dblink_sufix;
			
			open cvCTX_Bill_Subtotal for l_str_bst;			
			LOOP
				fetch cvCTX_Bill_Subtotal into itemC_BST;
				exit when cvCTX_Bill_Subtotal%NOTFOUND;
				
				select BILL_SUBTOTAL_seq.nextval into l_bst_id from dual;
				
				insert into BILL_SUBTOTAL (ID, BILL_TOTAL_ID, UNIT_OF_MEASURE, QUANTITY, COST)
				values (l_bst_id, l_bt_id, itemC_BST.unit_of_measure, itemC_BST.bst_quantity, itemC_BST.bst_cost);
				
				l_unit_of_measure:=itemC_BST.unit_of_measure;				
				--set sys_context('BILL_CALC_TO_USAGES','scp_unit_of_measure') na l_unit_of_measure u udaljenoj bazi
				l_str_plsql:='MV_bill_calc_usages_API.set_parameter'||l_remote_dblink_sufix||'(''scp_unit_of_measure'','''||to_char(l_unit_of_measure)||''')';
				EXECUTE IMMEDIATE l_str_plsql;
				
				
				l_str_bi:='select /* +ALL_ROWS */ type_id, price, first_bi_ud, last_bi_ud, bi_quantity, bi_cost, c_times from '
				||l_user_schema_prefix||'CTX_V_USAGES_BI'||l_remote_dblink_sufix;
			
				open cvCTX_Bill_Item for l_str_bi;
				LOOP					
					fetch cvCTX_Bill_Item into itemC_BI;
					exit when cvCTX_Bill_Item%NOTFOUND;
					
					if itemC_BI.price is null then
						logPriceWasMissed(p_month, p_year, l_client_id, 
								itemC_BI.type_id, itemC_BI.first_bi_ud, itemC_BI.last_bi_ud, itemC_BI.bi_quantity,
								l_unit_of_measure, itemC_BI.c_times, p_user_schema, p_remote_dblink);
								
					/* 	if (needs to brake here for current client when price is unknown for the first time) then
							close cvCTX_Bill_Item;
							close cvCTX_Bill_Subtotal;
							exit; 
						end if;
					*/ -- ATTENTION: Don't use exit without closed cursors regularly if you intent to exit from both FOR loops.
					end if;
					
					select BILL_ITEM_seq.nextval into l_bi_id from dual;
					
					insert into BILL_ITEM (ID, BILL_SUBTOTAL_ID, USAGE_TYPE_ID, 
											FIRST_USAGE_DATE, LAST_USAGE_DATE, QUANTITY, UNIT_OF_MEASURE, PRICE, COST)
					values (l_bi_id, l_bst_id, itemC_BI.type_id, 
							itemC_BI.first_bi_ud, itemC_BI.last_bi_ud, itemC_BI.bi_quantity, l_unit_of_measure, 
							nvl(itemC_BI.price,0), itemC_BI.bi_cost);			
				END LOOP; --cvCTX_Bill_Item 
				close cvCTX_Bill_Item;
				
			END LOOP; --cvCTX_Bill_Subtotal
			close cvCTX_Bill_Subtotal;
			
		END LOOP; --cvCTX_Bills_Total
		close cvCTX_Bills_Total;
		
		/* It is better has no exception that situation can be escalated to calling procedure to handle errors
			in semantic context 
		*/
		
		/*exception
			when no_data_found then
				-- If error is handled here, than we must extend procedure by status to return error code.
				-- Usually, it is semantically enclosed by an application context of usage.
				null;
			when other then
				-- Similar as the previous comment.
				null;
		*/		
	end;
	
	procedure logPriceWasMissed(p_month in integer, p_year in integer, p_client_id in number, 
								p_type_id in number, p_first_ud in date, p_last_ud in date, p_quantity in number,
								p_unit_of_measure in number, p_c_times in integer, 
								p_um_schema in varchar2, p_db_link in varchar2 default 'LOCAL')
	is
	begin
		insert into LOG_MISSING_PRICES(client_id, u_year, u_month, first_ud, last_ud, type_id, 
										quantity, unit_of_measure, c_times, um_schema, db_link) 
		values (p_client_id, p_year, p_month, p_first_ud, p_last_ud, p_type_id, 
				p_quantity, p_unit_of_measure, p_c_times, p_um_schema, p_db_link);

		dbms_output.put_line('There was missing price value for client #'||p_client_id||' in the month '||p_month
							||' in the year '||p_year||' for usage type '||p_type_id||' between dates '||p_first_ud
							||' and '||p_last_ud||'. There have spent '||p_quantity||' units of measure '
							||p_unit_of_measure||' for '||p_c_times||' repeated occasions. Log saved in the log table.');	
	end;

END;
/

