if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_locais_trab') then
	drop procedure cnv_locais_trab;
end if
;
-- bug BTHSC-7921
begin
	ooLoop: for oo as cnv_locais_trab dynamic scroll cursor for
		select 1 as w_i_entidades, cdLocal  as w_i_locais_trab,dsLocal as w_nome ,*
		from tecbth_delivery.gp001_local 
		order by 1,2 asc		
	do
		message 'Ent: '||w_i_entidades||' Loc. Tra. '||w_i_locais_trab||' Nom.: '||w_nome to client;
	
		insert into bethadba.locais_trab(i_entidades,i_locais_trab,i_cidades,i_bairros,i_ruas,nome,fone)on existing skip
		values(w_i_entidades,w_i_locais_trab,null,null,null,w_nome,null); 
	end for;
end
;


CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;