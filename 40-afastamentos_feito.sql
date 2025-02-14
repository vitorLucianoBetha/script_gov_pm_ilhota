CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;

begin
	// *****  Tabela bethadba.afastamentos
	declare w_i_funcionarios integer;
	declare w_i_tipos_afast smallint;

	ooLoop: for oo as cnv_afastamentos dynamic scroll cursor for
	-- BTHSC-141362 Bug em Matrículas | Afastamentos e Faltas não migraram
		select distinct 1 as w_i_entidades,
			a.cdMatricula as w_CdMatricula,
			a.SqContrato as w_SqContrato,
			date(a.DtInicioAfastamento) as w_dt_afastamento,
			date(a.DtFimAfastamento) as w_dt_ultimo_dia,
			isnull(b.i_tipos_afast, 7) as w_CdMotivoAfastamento 
		from tecbth_delivery.gp001_HISTORICOAFASTAMENTO a
		join tecbth_delivery.gp001_MOTIVOAFASTAMENTOax b on a.CdMotivoAfastamento = b.afastamento_antes 
		where a.DtInicioAfastamento is not null
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


