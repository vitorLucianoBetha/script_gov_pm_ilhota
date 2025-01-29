ROLLBACK;

CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
call bethadba.pg_setoption('fire_triggers','off');
COMMIT;
begin
	// *****  Tabela bethadba.hist_cargos
	declare w_i_funcionarios integer;
	declare w_dt_alteracoes timestamp;
	
	// *****  Variaveis auxiliares
	declare w_cont integer;
	declare w_number integer;
	declare w_i_funcionarios_aux integer;
	set w_cont=0;
	set w_number=0;
	ooLoop: for oo as cnv_hist_cargos dynamic scroll cursor for
		select 1 as w_i_entidades,Funcionario.cdMatricula as w_cdMatricula,Funcionario.SqContrato as w_SqContrato,date(dtAdmissao) as w_dtAdmissao,date(if DtHistorico is null then dtAdmissao else DtHistorico endif) as w_DtHistorico,
			cdMotivo as w_i_motivos_altcar,if HistoricoCargo.cdCargo is null then Funcionario.CdCargo else HistoricoCargo.cdCargo endif as w_i_cargos,date(DtValidade) as w_dtValidade, cast(HistoricoCargo.nrConcurso as integer) as w_i_concursos,
        date(if DtPosseCargo= 'NULL' THEN null else DtPosseCargo endif )as w_dt_posse,cdtipoferias as w_configferias from tecbth_delivery.GP001_Funcionario as Funcionario
		left outer join tecbth_delivery.GP001_HistoricoCargo AS HistoricoCargo on (Funcionario.CdMatricula = HistoricoCargo.CdMatricula and Funcionario.SqContrato = HistoricoCargo.SqContrato)
		order by 1,2,3,5,8 asc		
	do
		set w_cont=w_cont+1;
		// *****  Inicializa Variaveis
		set w_i_funcionarios=null;
		set w_dt_alteracoes=null;
		
		// *****  Converte tabela bethadba.hist_cargos
        set w_i_funcionarios = w_cdmatricula;
/*
		set w_i_funcionarios=(round(w_cdMatricula/1,0)*10)||w_SqContrato;
*/
		
		if w_i_funcionarios_aux != w_i_funcionarios then
			set w_number=0
		end if;
		
		set w_number=w_number+1;
		
		set w_dt_alteracoes=hours(date(w_DtHistorico),1);
		
		if w_number = 1 then
			set w_dt_alteracoes=hours(w_dtAdmissao,0)
		else
          if date(w_dt_alteracoes) < w_dtAdmissao then
		    set w_dt_alteracoes=hours(w_dtAdmissao,1)
          else
			set w_dt_alteracoes=hours(date(w_DtHistorico),1)
          end if;
		end if;
		
		if w_i_motivos_altcar in(0,1) then
			set w_i_motivos_altcar=null
		else 
			set w_i_motivos_altcar=1	
		end if;
		
		if w_i_concursos not in(0,9999) then			
			if not exists(select 1 from bethadba.concursos where i_entidades = w_i_entidades and i_concursos = w_i_concursos) then
				message 'Ent.: '||w_i_entidades||' Con.: '||w_i_concursos to client;
				
				insert into bethadba.concursos(i_entidades,i_concursos,descricao,dt_ini_insc,dt_fin_insc,dt_prorrog,dt_validade,dt_prorrog_validade,dt_homolog,justificativa,dt_encerra,
										       info_cand,tipo_concurso)on existing skip
				values (w_i_entidades,w_i_concursos,'Concurso '||w_i_concursos,'1900-01-01','1900-01-01',null,null,null,null,null,null,'M','C');
			end if
		else
			set w_i_concursos=null;
		end if;
		
		if w_number = 1 then
			set w_i_motivos_altcar=null
		end if;
		
		if not exists(select 1 from bethadba.cargos where i_entidades = w_i_entidades and i_cargos = w_i_cargos) then
			message 'Ent.: '||w_i_entidades||' Car.: '||w_i_cargos to client;
			
			insert into bethadba.cargos(i_entidades,i_cargos,i_cbo,i_tipos_cargos,nome)on existing skip
			values (w_i_entidades,w_i_cargos,null,99,'Não cadastrado');
						
			insert into bethadba.cargos.compl(i_entidades,i_cargos,i_config_ferias,i_config_ferias_subst,qtd_vagas,rol,grau_instrucao,codigo_tce,decimo_terc,requisitos,atividades,
											  i_licpremio_config,categoria,aposent_especial,acumula_cargos)on existing skip 
			values (w_i_entidades,w_i_cargos,1,1,99,null,1,w_i_cargos,'S',null,null,null,'M',0,'N');
			
			insert into bethadba.mov_cargos(i_entidades,i_cargos,dt_alteracoes,tipo_atualiz,num_lei,dt_lei,dt_vigorar,vagas_acresc,vagas_reduzir,i_atos)on existing skip 
			values (w_i_entidades,w_i_cargos,'1990-01-01',1,null,null,null,0,null,null);
		end if;

	    if tecbth_delivery.exists_func(w_i_entidades,w_i_funcionarios) = 1 then
			if not exists(select 1 from bethadba.hist_cargos where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios) then
				message 'Ent.: '||w_i_entidades||' Fun.:'||w_i_funcionarios||'Dt Alt.: '||w_dt_alteracoes||' Car.: '||w_i_cargos to client;
				
				insert into bethadba.hist_cargos(i_entidades,i_funcionarios,dt_alteracoes,dt_saida,i_cargos,i_motivos_altcar,i_atos,i_concursos,dt_nomeacao,dt_posse)on existing skip
				values (w_i_entidades,w_i_funcionarios,w_dt_alteracoes,null,w_i_cargos,w_i_motivos_altcar,null,w_i_concursos,null,w_dt_posse); 
			elseif exists(select 1 from bethadba.hist_cargos where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios 
						and dt_alteracoes = (select max(dt_alteracoes) 
											from bethadba.hist_cargos 
											where i_entidades = w_i_entidades 
											and i_funcionarios = w_i_funcionarios) 
						and (i_cargos != w_i_cargos or i_concursos != w_i_concursos or dt_nomeacao != null or dt_posse != w_dt_posse)) then
				if exists(select 1 from bethadba.hist_cargos where i_entidades = w_i_entidades 	and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) then
					update bethadba.hist_cargos 
					set i_cargos = w_i_cargos,
						i_motivos_altcar = w_i_motivos_altcar,
						i_atos = null,
						i_concursos = w_i_concursos,
						dt_nomeacao = null,
						dt_posse = w_dt_posse 
					where i_entidades = w_i_entidades 
					and i_funcionarios = w_i_funcionarios 
					and dt_alteracoes = w_dt_alteracoes
				else
					message 'Ent.: '||w_i_entidades||' Fun.:'||w_i_funcionarios||'Dt Alt.: '||w_dt_alteracoes||' Car.: '||w_i_cargos to client;
				
					insert into bethadba.hist_cargos(i_entidades,i_funcionarios,dt_alteracoes,dt_saida,i_cargos,i_motivos_altcar,i_atos,i_concursos,dt_nomeacao,dt_posse)on existing skip
					values (w_i_entidades,w_i_funcionarios,w_dt_alteracoes,null,w_i_cargos,w_i_motivos_altcar,null,w_i_concursos,null,w_dt_posse);
				end if
			end if
		end if;
		
		set w_i_funcionarios_aux=w_i_funcionarios;
		
	end for;
end;


-- Ajuste BTHSC-139782

UPDATE BETHADBA.hist_cargos hc
left join tecbth_delivery.gp001_conc_concursofuncionario c
on hc.i_funcionarios = c.cdMatricula
set hc.i_concursos = LEFT(cdNroAno, LENGTH(cdNroAno) - 3) 
where cdNroAno not in (0, '0');


UPDATE BETHADBA.hist_cargos hc
left join tecbth_delivery.gp001_FUNCIONARIO f 
on hc.i_funcionarios = f.cdMatricula 
set hc.dt_nomeacao = if f.dtNomeacao is null or f.dtNomeacao in ('NULL') then null else cast(f.dtNomeacao as date) endif;

UPDATE BETHADBA.hist_cargos hc
left join tecbth_delivery.gp001_FUNCIONARIO f 
on hc.i_funcionarios = f.cdMatricula 
set hc.dt_posse = if f.DtPosseCargo  is null or f.DtPosseCargo in ('NULL') then null else cast(f.DtPosseCargo as date) endif;