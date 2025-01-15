CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
call bethadba.pg_setoption('fire_triggers','off');
COMMIT;
if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_grupo_funcional') then
	drop procedure cnv_grupo_funcional;
end if
;

create procedure tecbth_delivery.cnv_grupo_funcional()
begin
	ooLoop: for oo as cnv_grupo_funcional dynamic scroll cursor for
		select 1 as w_i_entidades,CdFilial as w_i_grupos,upper(trim(NmFilial)) as w_nome 
		from tecbth_delivery.gp001_filial as filial
		order by 1,2 asc
	do
		message 'Ent.: '||w_i_entidades||' Gru.: '||w_i_grupos||' Nom.: '||w_nome to client;
		
		insert into bethadba.grupos(i_entidades,i_grupos,nome) 
		values(w_i_entidades,w_i_grupos,w_nome);
	end for;
end
;

call cnv_grupo_funcional()
;

commit
;

CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;