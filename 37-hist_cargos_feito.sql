-- UTILIZADO PARA TRATAR CARGOS QUE VIERAM DE MIGRAÇÃO DE DADOS PARA O GOV 
-- BTHSC-170309 Bug em Matrículas | Cargos errados
/*
WITH CargosSequencia AS (
    SELECT 1 as w_i_entidades,
        cdMatricula as w_cdMatricula,
		SqContrato as w_SqContrato,
		tpRegistro,
		dtAdmissao as w_dtAdmissao,        
        dtCompetencia AS data_inicial,
		1 as w_i_motivos_altcar,
		cdCargo,
		null as w_i_concursos,
        LEAD(dtCompetencia) OVER (PARTITION BY cdMatricula ORDER BY dtCompetencia) AS proxima_data,
        LEAD(cdCargo) OVER (PARTITION BY cdMatricula ORDER BY dtCompetencia) AS proximo_cargo,
		2 as w_configferias,
		(select dtRescisao from FUNCIONARIO where FUNCIONARIO.cdMatricula = FichaFinanceiraHeader.cdMatricula) as w_dtRescisao
    FROM FichaFinanceiraHeader
),
MudancasDeCargo AS (
    SELECT w_i_entidades,
        w_cdMatricula,
		w_SqContrato,
		tpRegistro,
		w_dtAdmissao,
		data_inicial,
		w_i_motivos_altcar,
        cdCargo,
		w_i_concursos,
		w_configferias,
		w_dtRescisao,
        -- Se o código do cargo muda, a data final é o dia anterior da próxima data
        CASE 
            WHEN cdCargo <> proximo_cargo THEN DATEADD(DAY, -1, proxima_data)
            ELSE NULL
        END AS data_final,
        -- Flag para marcar início de novo período
        CASE 
            WHEN cdCargo <> LAG(cdCargo) OVER (PARTITION BY w_cdMatricula, tpRegistro ORDER BY data_inicial) 
                 OR LAG(cdCargo) OVER (PARTITION BY w_cdMatricula, tpRegistro ORDER BY data_inicial) IS NULL 
            THEN 1 ELSE 0 
        END AS novo_periodo
    FROM CargosSequencia
),
Blocos AS (
    SELECT *,
        SUM(novo_periodo) OVER (PARTITION BY w_cdMatricula, tpRegistro ORDER BY data_inicial ROWS UNBOUNDED PRECEDING) AS grupo
    FROM MudancasDeCargo
),
Agrupado AS (
    SELECT w_i_entidades,
        w_cdMatricula,
		w_SqContrato,
		tpRegistro,
		w_dtAdmissao,
		w_i_motivos_altcar,
		MIN(data_inicial) AS data_inicial,
        cdCargo,        
        MAX(COALESCE(data_final, data_inicial)) AS data_final,
		w_i_concursos,
		MIN(data_inicial) AS w_dt_posse,
		w_configferias,
		w_dtRescisao
    FROM Blocos
    GROUP BY w_i_entidades, w_cdMatricula, w_SqContrato, tpRegistro, w_dtAdmissao, w_i_motivos_altcar, cdCargo, w_i_concursos, w_configferias, w_dtRescisao, grupo
)
SELECT * FROM Agrupado
ORDER BY w_cdMatricula, data_inicial;
-- OBS SÓ FUNCIONA NO SQL SERVER PARA ISSO FOI CRIADA UMA TABELA NOVA E AJUSTADO NO SCRIPT ABAIXO

CREATE TABLE tecbth_delivery.gp001_HISTORICOCARGOAux (
	w_i_entidades integer NULL,
	w_cdMatricula varchar(200) NULL,
	w_SqContrato varchar(1) NULL,
	w_dtAdmissao date NULL,
	w_i_motivos_altcar integer NULL,
	data_inicial datetime NULL,
	cdCargo integer NULL,
	data_final date NULL,
	w_i_concursos integer NULL,
	w_dt_posse date NULL,
	w_configferias integer NULL,
	w_dtRescisao date NULL
);
-- TABELA CRIADA PARA TRAZER OS DADOS E AJUSTAR NO SCRIPT ABAIXO
select 1 as w_i_entidades,
			Funcionario.cdMatricula as w_cdMatricula,
			Funcionario.SqContrato as w_SqContrato,
			date(w_dtAdmissao) as w_dtAdmissao,
            data_inicial as w_DtHistorico,
            w_i_motivos_altcar,
            if HistoricoCargo.cdCargo is null or HistoricoCargo.cdCargo = 0 then Funcionario.CdCargo else HistoricoCargo.cdCargo endif as w_i_cargos,
            HistoricoCargo.data_final w_dtValidade,
            cast(HistoricoCargo.w_i_concursos as integer) as w_i_concursos,
            w_dt_posse,
            Funcionario.CdTipoFerias,
            Funcionario.dtRescisao as w_dtRescisao
        from tecbth_delivery.GP001_Funcionario as Funcionario
        left outer join tecbth_delivery.GP001_HistoricoCargoAux AS HistoricoCargo on (Funcionario.CdMatricula = HistoricoCargo.w_cdMatricula and 
                                                                                     Funcionario.SqContrato = HistoricoCargo.w_SqContrato)	 where w_dtAdmissao is not null
		order by 1,2,3,5,8 asc
SQL utilizado no script abaixo		
*/


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
		select 1 as w_i_entidades,
			Funcionario.cdMatricula as w_cdMatricula,
			Funcionario.SqContrato as w_SqContrato,
			date(dtAdmissao) as w_dtAdmissao,
            DtHistorico as w_DtHistorico,
            cdMotivo as w_i_motivos_altcar,
            if HistoricoCargo.cdCargo is null then Funcionario.CdCargo else HistoricoCargo.cdCargo endif as w_i_cargos,
            (select date(date(min(a.DtHistorico)) - 1) from tecbth_delivery.GP001_HistoricoCargo a where a.CdMatricula = w_cdMatricula and a.DtHistorico > w_DtHistorico) as w_dtValidade,
            cast(HistoricoCargo.nrConcurso as integer) as w_i_concursos,
            date(if DtPosseCargo= 'NULL' THEN null else DtPosseCargo endif )as w_dt_posse,
            cdtipoferias as w_configferias,
            Funcionario.dtRescisao as w_dtRescisao
        from tecbth_delivery.GP001_Funcionario as Funcionario
        left outer join tecbth_delivery.GP001_HistoricoCargo AS HistoricoCargo on (Funcionario.CdMatricula = HistoricoCargo.CdMatricula and 
                                                                                     Funcionario.SqContrato = HistoricoCargo.SqContrato)                                                                         
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
		set w_number = w_number + 1;
		if w_number = 1 then
			set w_dt_alteracoes = hours(w_dtAdmissao, 0);
		else
          if date(w_dt_alteracoes) < date(w_dtAdmissao) then
            set w_dt_alteracoes = dateadd(HOUR, 1, (select max(dt_alteracoes) from bethadba.hist_cargos
                                                    where i_entidades = w_i_entidades
                                                      and i_funcionarios = w_i_funcionarios
                                                      and date(dt_alteracoes) = w_dtAdmissao));
          else
            set w_dt_alteracoes = w_DtHistorico;
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
			set w_i_motivos_altcar=null;
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
				values (w_i_entidades,w_i_funcionarios,w_dt_alteracoes,w_dtValidade,w_i_cargos,w_i_motivos_altcar,null,w_i_concursos,null,w_dt_posse); 
			else
              if (w_dtRescisao is null) or (date(w_dt_alteracoes) < date(w_dtRescisao)) then
                //set w_dt_alteracoes = dateadd(HOUR, 1, DATE(w_dt_alteracoes));
                if exists(select 1 from bethadba.hist_cargos where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) then
                   //exists(select 1 from bethadba.hist_funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) or
                   //exists(select 1 from bethadba.hist_salariais where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) then
                  set w_dt_alteracoes = dateadd(HOUR, 1, (select max(dt_alteracoes)
                                                          from (select dt_alteracoes from bethadba.hist_salariais
                                                                where i_entidades = w_i_entidades
                                                                  and i_funcionarios = w_i_funcionarios
                                                                  and date(dt_alteracoes) = date(w_dt_alteracoes)
                                                                union
                                                                select dt_alteracoes from bethadba.hist_funcionarios
                                                                where i_entidades = w_i_entidades
                                                                  and i_funcionarios = w_i_funcionarios
                                                                  and date(dt_alteracoes) = date(w_dt_alteracoes)
                                                                union
                                                                select dt_alteracoes from bethadba.hist_cargos
                                                                where i_entidades = w_i_entidades
                                                                  and i_funcionarios = w_i_funcionarios
                                                                  and date(dt_alteracoes) = date(w_dt_alteracoes)) as T));
                  message '1-Passou aqui: ' || string(w_dt_alteracoes) to client;
                end if;
              else
                set w_dt_alteracoes = dateadd(HOUR, 1, DATE(w_dtRescisao));
                if exists(select 1 from bethadba.hist_funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) or
                   exists(select 1 from bethadba.hist_cargos where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) or 
                   exists(select 1 from bethadba.hist_salariais where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) then
                   set w_dt_alteracoes = dateadd(HOUR, 1, (select max(dt_alteracoes)
                                                           from (select dt_alteracoes from bethadba.hist_salariais
                                                                 where i_entidades = w_i_entidades
                                                                   and i_funcionarios = w_i_funcionarios
                                                                   and date(dt_alteracoes) = date(w_dt_alteracoes)
                                                                 union
                                                                 select dt_alteracoes from bethadba.hist_funcionarios
                                                                 where i_entidades = w_i_entidades
                                                                   and i_funcionarios = w_i_funcionarios
                                                                   and date(dt_alteracoes) = date(w_dt_alteracoes)
                                                                 union
                                                                 select dt_alteracoes from bethadba.hist_cargos
                                                                 where i_entidades = w_i_entidades
                                                                   and i_funcionarios = w_i_funcionarios
                                                                   and date(dt_alteracoes) = date(w_dt_alteracoes)) as T));
                   message '3-Passou aqui: ' || string(w_dt_alteracoes) to client;
                end if;
              end if;
              insert into bethadba.hist_cargos(i_entidades,i_funcionarios,dt_alteracoes,dt_saida,i_cargos,i_motivos_altcar,i_atos,i_concursos,dt_nomeacao,dt_posse)on existing skip
              values (w_i_entidades,w_i_funcionarios,w_dt_alteracoes,w_dtValidade,w_i_cargos,w_i_motivos_altcar,null,w_i_concursos,null,w_dt_posse);
            end if;
        end if;
		set w_i_funcionarios_aux=w_i_funcionarios;
	end for;
end;

--------------------------------------------------------------
--CM ABAIXO

ROLLBACK;

delete from bethadba.hist_cargos;

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
		select 1 as w_i_entidades,
			Funcionario.cdMatricula as w_cdMatricula,
			Funcionario.SqContrato as w_SqContrato,
			date(dtAdmissao) as w_dtAdmissao,
            DtHistorico as w_DtHistorico,
            cdMotivo as w_i_motivos_altcar,
            if HistoricoCargo.cdCargo is null then Funcionario.CdCargo else HistoricoCargo.cdCargo endif as w_i_cargos,
            (select date(date(min(a.DtHistorico)) - 1) from tecbth_delivery.GP001_HistoricoCargo a where a.CdMatricula = w_cdMatricula and a.DtHistorico > w_DtHistorico) as w_dtValidade,
            cast(HistoricoCargo.nrConcurso as integer) as w_i_concursos,
            date(if DtPosseCargo= 'NULL' THEN null else DtPosseCargo endif )as w_dt_posse,
            cdtipoferias as w_configferias,
            Funcionario.dtRescisao as w_dtRescisao
        from tecbth_delivery.GP001_Funcionario as Funcionario
        left outer join tecbth_delivery.GP001_HistoricoCargo AS HistoricoCargo on (Funcionario.CdMatricula = HistoricoCargo.CdMatricula and 
                                                                                     Funcionario.SqContrato = HistoricoCargo.SqContrato)                                                                         
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
		set w_number = w_number + 1;
		if w_number = 1 then
			set w_dt_alteracoes = hours(w_dtAdmissao, 0);
		else
          if date(isnull(w_dt_alteracoes,'1900-01-01')) < date(w_dtAdmissao) or w_DtHistorico is null  then
            set w_dt_alteracoes = dateadd(HOUR, 1, (select max(dt_alteracoes) from bethadba.hist_cargos
                                                    where i_entidades = w_i_entidades
                                                      and i_funcionarios = w_i_funcionarios));
          else
            set w_dt_alteracoes = w_DtHistorico;
          end if;
		end if;
	    --if w_dt_alteracoes is null or w_dt_alteracoes = '' then
	    	--set w_dt_alteracoes = hours(w_dtAdmissao, 0);
	    --end if;
		message 'w_dt_alteracoes:' || string(w_dt_alteracoes) || ' w_number: ' || string(w_number) || ' w_dtAdmissao: ' || w_dtAdmissao to client;
	
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
			set w_i_motivos_altcar=null;
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
				values (w_i_entidades,w_i_funcionarios,w_dt_alteracoes,w_dtValidade,w_i_cargos,w_i_motivos_altcar,null,w_i_concursos,null,w_dt_posse); 
			else
              if (w_dtRescisao is null) or (date(w_dt_alteracoes) < date(w_dtRescisao)) then
                set w_dt_alteracoes = dateadd(HOUR, 1, DATE(w_dt_alteracoes));
                if exists(select 1 from bethadba.hist_cargos where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) or
                   exists(select 1 from bethadba.hist_funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) or
                   exists(select 1 from bethadba.hist_salariais where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) then
                  set w_dt_alteracoes = dateadd(HOUR, 1, (select max(dt_alteracoes)
                                                          from (select dt_alteracoes from bethadba.hist_salariais
                                                                where i_entidades = w_i_entidades
                                                                  and i_funcionarios = w_i_funcionarios
                                                                  and date(dt_alteracoes) = date(w_dt_alteracoes)
                                                                union
                                                                select dt_alteracoes from bethadba.hist_funcionarios
                                                                where i_entidades = w_i_entidades
                                                                  and i_funcionarios = w_i_funcionarios
                                                                  and date(dt_alteracoes) = date(w_dt_alteracoes)
                                                                union
                                                                select dt_alteracoes from bethadba.hist_cargos
                                                                where i_entidades = w_i_entidades
                                                                  and i_funcionarios = w_i_funcionarios
                                                                  and date(dt_alteracoes) = date(w_dt_alteracoes)) as T));
                  message '1-Passou aqui: ' || string(w_dt_alteracoes) to client;
                end if;
              else
                set w_dt_alteracoes = dateadd(HOUR, 1, DATE(w_dtRescisao));
                if exists(select 1 from bethadba.hist_funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) or
                   exists(select 1 from bethadba.hist_cargos where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) or 
                   exists(select 1 from bethadba.hist_salariais where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) then
                   set w_dt_alteracoes = dateadd(HOUR, 1, (select max(dt_alteracoes)
                                                           from (select dt_alteracoes from bethadba.hist_salariais
                                                                 where i_entidades = w_i_entidades
                                                                   and i_funcionarios = w_i_funcionarios
                                                                   and date(dt_alteracoes) = date(w_dt_alteracoes)
                                                                 union
                                                                 select dt_alteracoes from bethadba.hist_funcionarios
                                                                 where i_entidades = w_i_entidades
                                                                   and i_funcionarios = w_i_funcionarios
                                                                   and date(dt_alteracoes) = date(w_dt_alteracoes)
                                                                 union
                                                                 select dt_alteracoes from bethadba.hist_cargos
                                                                 where i_entidades = w_i_entidades
                                                                   and i_funcionarios = w_i_funcionarios
                                                                   and date(dt_alteracoes) = date(w_dt_alteracoes)) as T));
                   message '3-Passou aqui: ' || string(w_dt_alteracoes) to client;
                end if;
              end if;
             message 'w_dt_alteracoes:' || string(w_dt_alteracoes) to client;
              insert into bethadba.hist_cargos(i_entidades,i_funcionarios,dt_alteracoes,dt_saida,i_cargos,i_motivos_altcar,i_atos,i_concursos,dt_nomeacao,dt_posse)on existing skip
              values (w_i_entidades,w_i_funcionarios,w_dt_alteracoes,w_dtValidade,w_i_cargos,w_i_motivos_altcar,null,w_i_concursos,null,w_dt_posse);
            end if;
        end if;
		set w_i_funcionarios_aux=w_i_funcionarios;
	end for;
end;
