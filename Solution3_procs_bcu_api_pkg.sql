/* Author: Mladen Vidic
   E-mail: mladen.vidic@gmail.com
   Location: Belgrade, Serbia || Doboj, Bosnia and Herzegovina
   Date Signed: 15.5.2021.
   
   Scope: PLSQL APIs to manage SYS CONTEXT created in Solution3.sql for Task 3.
   
   Note: Procedures and function could have shorter names. These names are used for self describing purposes for better readability 
   and easier following of code development pathway.
   
   LICENCE: The licensing rules from the 'license.txt' or '..\license.txt' file apply to this file, solution and its parts.
*/


CREATE OR REPLACE Package &&usage_mgmt..MV_bill_calc_usages_context_API IS
	procedure set_parameter(p_name in varchar2, p_value in varchar2);

END;
/

CREATE OR REPLACE Package Body &&usage_mgmt..MV_bill_calc_usages_context_API IS
	procedure set_parameter(p_name in varchar2, p_value in varchar2)
	is
	begin
		DBMS_SESSION.set_context('BILL_CALC_TO_USAGES', p_name, p_value);
	end;

END;
/