
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;
begin
	// *****  Tabela bethadba.afastamentos
	declare w_i_funcionarios integer;
	declare w_i_tipos_afast smallint;

	ooLoop: for oo as cnv_afastamentos dynamic scroll cursor for
		select 1 as w_i_entidades,cdMatricula  as w_CdMatricula,SqContrato as w_SqContrato,date(DtInicioAfastamento) as w_dt_afastamento,date(DtFimAfastamento) as w_dt_ultimo_dia
,
			   isnull(CdMotivoAfastamento, 7) as w_CdMotivoAfastamento 
from   tecbth_delivery.gp001_HISTORICOAFASTAMENTO where  DtInicioAfastamento is not null  
		order by 1,2,3,4 asc
	do
		
		// *****  Inicializa Variaveis
		set w_i_funcionarios = null;
		set w_i_tipos_afast = w_CdMotivoAfastamento;
		
		// *****  Converte tabela bethadba.afastamentos
		set w_i_funcionarios=cast(w_cdMatricula as integer);
		
		

		if exists(select 1 
					from bethadba.funcionarios 
					where i_entidades = w_i_entidades 
					and i_funcionarios = w_i_funcionarios) then
			message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Tip.: '||w_i_tipos_afast||' Dt. Ini: '||w_dt_afastamento||' Dt. Fin.: '||w_dt_ultimo_dia to client;
			
			insert into bethadba.afastamentos(i_entidades,i_funcionarios,dt_afastamento,i_tipos_afast,i_atos,dt_ultimo_dia,req_benef,comp_comunic)on existing skip
			values (w_i_entidades,w_i_funcionarios,w_dt_afastamento,w_i_tipos_afast,null,w_dt_ultimo_dia,null,null);
		end if;
		
	end for;
end;


