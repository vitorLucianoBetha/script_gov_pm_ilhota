CREATE TABLE tecbth_delivery.gp001_FichaFinanceiraHeaderCalculo (
	ID int NOT NULL,
	cdMatricula int NOT NULL,
	sqContrato smallint NOT NULL,
	dtCompetencia datetime NOT NULL,
	tpCalculo smallint NOT NULL,
	sqHabilitacao smallint NOT NULL,
	nrReciboPagto smallint NULL,
	dtEnvioFila_eSocial datetime NULL,
	dtPagamento datetime NULL
);

CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;

-- INSERE TODOS OS CALCULOS MENOS OS TIPOS 1 E 2 QUE SÃO RESCISÃO POIS PODEM TER EVENTOS DUPLICADOS COM AS MENSAIS
-- NESTE CASO CRIAMOS OUTRA TABELA QUE PERMITE A INSERÇÃO DESTAS FOLHAS NA TABELA E ASSIM UNIFICAMOS NO ARJOB

begin
	// *****  Tabela bethadba.movimentos
	declare w_i_tipos_proc smallint;
	declare w_i_competencias date;
	declare w_i_processamentos smallint;
	declare w_i_funcionarios integer;
	declare w_i_eventos smallint;
	declare w_classif_evento tinyint;
	declare w_mov_resc char(1);
	
	// *****  Tabela bethadba.dados_calc
	declare w_dt_pagto date;
	
	// *****  Tabela bethabda.processamentos
	declare w_dt_fechamento date;
	
	// *****  Tabela bethadba.processamentos_lotes
	declare w_i_processamentos_lotes integer;
	
	// *****  Variaveis auxiliares
	declare w_evento smallint;
	declare w_i_eventos_aux smallint;
	declare w_nome_aux char(50);
	declare w_tipo_pd_aux char(1);
	declare w_taxa_aux decimal(10,4);
	declare w_unidade_aux char(15);
	declare w_sai_rais_aux char(1);
	declare w_compoe_liq_aux char(1);
	declare w_compoe_hmes_aux char(1);
	declare w_digitou_form_aux char(1);
	declare w_classif_evento_aux tinyint;
	ooLoop: for oo as cnv_movimentos dynamic scroll cursor for
		select 1 as w_i_entidades,
			f.cdMatricula as w_cdMatricula,
			f.sqContrato as w_sqContrato,
			f.dtCompetencia as w_dtCompetencia,
			f.tpcalculo as w_tpcalculo,
			f.sqHabilitacao as w_sqHabilitacao,
			f.cdVerba as w_cdVerba,
			f.inRetificacao as w_inRetificacao,
			f.dtPagamento as w_dtPagamento,
			tecbth_delivery.fu_convdecimal(tecbth_delivery.tira_caracter_1(vlComplemento),0) as w_vlr_inf,
			cast(vlMensal as decimal(12,2)) as w_vlr_calc,
			cast(vlAuxiliar as decimal(12,2)) as w_vlAuxiliar,
			cast(vlIntegral as decimal(12,2)) as w_vlIntegral,
			if v.TpCategoria in ('D','P','V') then 'S' else 'N' endif as w_compoe_liq,
			v.TpCategoria as w_tipo_pd
		from tecbth_delivery.gp001_FICHAFINANCEIRA f
		join tecbth_delivery.gp001_VERBA v on f.cdVerba = v.CdVerba
		join tecbth_delivery.gp001_FichaFinanceiraHeaderCalculo ff on f.cdMatricula = ff.cdMatricula and f.dtCompetencia = ff.dtCompetencia and f.sqHabilitacao = ff.sqHabilitacao and f.tpCalculo = ff.tpCalculo
		where f.sqHabilitacao = ff.sqHabilitacao
		and f.tpCalculo not in (1,2)
		order by 1, 2, 4, 9 asc
	do
		
		// *****  Tabela bethadba.movimentos
		set w_i_tipos_proc = null;
		set w_i_competencias = null;
		set w_i_processamentos = null;
		set w_i_funcionarios = null;
		set w_i_eventos = null;
		set w_tipo_pd = null;
		set w_classif_evento = null;
		set w_mov_resc = null;
		
		// *****  Tabela bethadba.dados_calc
		set w_dt_pagto = null;
		
		// *****  Tabela bethabda.processamentos
		set w_dt_fechamento = null;
		
		// *****  Tabela bethadba.processamentos_lotes
		set w_i_processamentos_lotes = null;
		
		// *****  Variaveis auxiliares
		set w_evento = null;
		set w_i_eventos_aux = null;
		set w_nome_aux = null;
		set w_tipo_pd_aux = null;
		set w_taxa_aux = null;
		set w_unidade_aux = null;
		set w_sai_rais_aux = null;
		set w_compoe_liq_aux = null;
		set w_compoe_hmes_aux = null;
		set w_digitou_form_aux = null;
		set w_classif_evento_aux = null;
		
		// *****  Converte bethadba.movimentos
		set w_i_funcionarios=cast(w_cdMatricula as integer);		
		if exists (select  1 from bethadba.funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios) then		
			if w_tpCalculo = 1 then -- 11-Mensal
				set w_i_tipos_proc=11;
				set w_mov_resc='S'
			elseif w_tpCalculo = 2 then --42-Complementar
				set w_i_tipos_proc=42;
				set w_mov_resc='S'
			elseif w_tpCalculo = 3 then --80-Férias
				set w_i_tipos_proc=80;
				set w_mov_resc='N'
			elseif w_tpCalculo = 5 then --51-13º Adiantamento
				set w_i_tipos_proc=51;
				set w_mov_resc='N'
			elseif w_tpCalculo = 6 then --52-13º Salário
				set w_i_tipos_proc=52;
				set w_mov_resc='N'
			elseif w_tpCalculo = 7 then --52-13º Salário
				set w_i_tipos_proc=52;
				set w_mov_resc='N'
			elseif w_tpCalculo = 8 then --41-Adiantamento
				set w_i_tipos_proc=41;
				set w_mov_resc='N'
			elseif w_tpCalculo = 9 then --11-Mensal
				set w_i_tipos_proc=11;
				set w_mov_resc='N'
			elseif w_tpCalculo = 10 then --42-Complementar
				set w_i_tipos_proc=42;
				set w_mov_resc='N'
			elseif w_tpCalculo = 11 then --11-Mensal
				set w_i_tipos_proc=11;
				set w_mov_resc='N'
			else --11-Mensal
				set w_i_tipos_proc=11;
				set w_mov_resc='N'
			end if;

		
				set w_i_competencias=date(w_dtCompetencia);
	
			
			set w_i_processamentos=1;


			
			if not w_cdVerba = any(select   evento from tecbth_delivery.evento_aux where tipo_pd = 'F' and w_i_entidades = w_i_entidades  ) then
				if not exists(select distinct  1 from tecbth_delivery.evento_aux where evento = w_cdVerba and retificacao = w_inRetificacao and w_i_entidades = w_i_entidades) then
					if w_inRetificacao in('C','D') then
						select   evento,nome,tipo_pd,taxa,unidade,sai_rais,compoe_liq,compoe_hmes,digitou_form,classif_evento 
						into w_i_eventos_aux,w_nome_aux,w_tipo_pd_aux,w_taxa_aux,w_unidade_aux,w_sai_rais_aux,w_compoe_liq_aux,w_compoe_hmes_aux,w_digitou_form_aux,w_classif_evento_aux 
						from tecbth_delivery.evento_aux 
						where evento = w_cdVerba 
						and retificacao = 'B' 
						and resc_mov = 'N' 
						and i_entidades = w_i_entidades
                        and i_eventos = w_i_eventos;
						-- AJUSTE
					
						
						select distinct coalesce(max(w_i_eventos),0)+1 
						into w_evento 
						  from tecbth_delivery.evento_aux 
						where tipo_pd in('P','D');
						
						insert into tecbth_delivery.evento_aux(evento,i_eventos,nome,tipo_pd,taxa,unidade,sai_rais,compoe_liq,compoe_hmes,digitou_form,classif_evento,retificacao,resc_mov,i_entidades) 
						values (w_i_eventos_aux,w_evento,w_nome_aux,w_tipo_pd_aux,w_taxa_aux,w_unidade_aux,w_sai_rais_aux,w_compoe_liq_aux,w_compoe_hmes_aux,w_digitou_form_aux,w_classif_evento_aux,
								w_inRetificacao,'N',w_i_entidades);
								
						message 'Eve.: '||w_evento||' Nom.: '||w_nome_aux||' Tip.: '||w_tipo_pd_aux to client;
						
						insert into bethadba.eventos(i_eventos,nome,tipo_pd,taxa,unidade,sai_rais,compoe_liq,compoe_hmes,digitou_form,classif_evento,cods_conversao)on existing skip
						values (w_evento,w_nome_aux,w_tipo_pd_aux,w_taxa_aux,w_unidade_aux,w_sai_rais_aux,w_compoe_liq_aux,w_compoe_hmes_aux,w_digitou_form_aux,w_classif_evento_aux,null);
					end if
				end if
			end if;			


            select first i_eventos
			into w_i_eventos 
 			from tecbth_delivery.evento_aux 
			where evento  = w_cdVerba
			and	i_entidades = w_i_entidades;

			select first tipo_pd,classif_evento 
				into w_tipo_pd,w_classif_evento 
			from bethadba.eventos  
			where i_eventos = w_i_eventos;

			if w_vlr_inf = '0' then
				set w_vlr_inf=0.0
			end if;
			
			if w_i_eventos = 1 then
				set w_vlr_inf=cast(w_vlAuxiliar as decimal(12,2))
			end if;
			
			if w_vlr_inf is null then
				set w_vlr_inf=w_vlr_calc
			end if;

			if(w_i_eventos < 9000) then
				// **** Processamentos
				set w_dt_fechamento=w_dtPagamento;
				set w_dt_pagto=w_dtPagamento;
				
				message 'Ent.: '||w_i_entidades||' Tip.: '||w_i_tipos_proc||' Com.: '||w_i_competencias||' Pro.: '||w_i_processamentos to client;
				 
				insert into bethadba.processamentos(i_entidades,i_tipos_proc,i_competencias,i_processamentos,dt_fechamento,dt_pagto,descricao)on existing skip
				values (w_i_entidades,w_i_tipos_proc,w_i_competencias,w_i_processamentos,w_dt_fechamento,w_dt_pagto,null);
				
						
				// *****  Converte tabela bethadba.processamentos_lotes
				if w_i_tipos_proc = 11 then
					set w_i_processamentos_lotes=1;
				
					message 'Ent.: '||w_i_entidades||' Tip.: '||w_i_tipos_proc||' Com.: '||w_i_competencias||' Pro. Lot.: '||w_i_processamentos_lotes to client;
				
					insert into bethadba.processamentos_lotes(i_entidades,i_competencias,i_processamentos_lotes,descricao)on existing skip
					values (w_i_entidades,w_i_competencias,w_i_processamentos_lotes,'Mensal Conversao');
				else 
					set w_i_processamentos_lotes=null;
				end if;				
					
				// *****  Converte tabela bethadba.dados_calc	
				set w_dt_pagto = null;
				set w_dt_pagto=w_dtPagamento;
				
				message 'Ent.: '||w_i_entidades||' Tip.: '||w_i_tipos_proc||' Com.: '||w_i_competencias||' Pro.: '||w_i_processamentos||' Fun.: '||w_i_funcionarios to client;
					
				insert into  bethadba.dados_calc(i_entidades,i_tipos_proc,i_competencias,i_processamentos,i_funcionarios,vlr_proventos,vlr_descontos,gerado_emp,movto_anterior,dt_pagto,i_processamentos_lotes)on existing skip
				values (w_i_entidades,w_i_tipos_proc,w_i_competencias,w_i_processamentos,w_i_funcionarios,0.0,0.0,'N','N',w_dt_pagto,w_i_processamentos_lotes);
				
				// **** Movimentos
				
				message 'Ent.: '||w_i_entidades||' Tip.: '||w_i_tipos_proc||' Com.: '||w_i_competencias||' Pro.: '||w_i_processamentos||' Fun.: '||w_i_funcionarios||' Eve.: '||w_i_eventos||
						' Vlr. Inf.: '||w_vlr_inf||' Vlr. Cal.: '||w_vlr_calc to client;
                insert into bethadba.movimentos(i_entidades,i_tipos_proc,i_competencias,i_processamentos,i_funcionarios,i_eventos,vlr_inf,vlr_calc,tipo_pd,compoe_liq,classif_evento,mov_resc) on existing skip
		    	values (w_i_entidades,w_i_tipos_proc,w_i_competencias,w_i_processamentos,w_i_funcionarios,w_i_eventos,w_vlr_inf,w_vlr_calc,w_tipo_pd,w_compoe_liq,w_classif_evento,w_mov_resc);
		    	 
			end if;
		end if;
	end for;
end;

-- INSERE OS CALCULOS DE RESCISÃO EFETIVAMENTE
-- MUDAR ARQJOB PARA UNION ALL E COLOCAR ESTA TABELA tecbth_delivery.gp001_MOVIMENTOS

CREATE TABLE tecbth_delivery.gp001_MOVIMENTOS (
    i_entidades int NOT NULL,
    i_tipos_proc smallint NOT NULL,
    i_competencias date NOT NULL,
    i_processamentos smallint NOT NULL,
    i_funcionarios int NOT NULL,
    i_eventos smallint NOT NULL,
    vlr_inf numeric(12,2) NOT NULL,
    vlr_calc numeric(12,2) NOT NULL,
    tipo_pd char(1) NOT NULL,
    compoe_liq char(1) NOT NULL,
    classif_evento tinyint DEFAULT 0 NOT NULL,
    mov_resc char(1) DEFAULT 'N' NOT NULL
);
CREATE INDEX idx_movimentos1 ON tecbth_delivery.gp001_MOVIMENTOS (i_entidades,i_funcionarios,classif_evento,i_competencias,i_tipos_proc);
CREATE INDEX idx_movimentos2 ON tecbth_delivery.gp001_MOVIMENTOS (i_entidades,i_funcionarios,i_eventos,i_tipos_proc,i_competencias);
CREATE INDEX idx_movimentos3 ON tecbth_delivery.gp001_MOVIMENTOS (i_entidades,i_funcionarios,i_eventos,i_competencias,i_tipos_proc);

ROLLBACK;

CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
call bethadba.pg_setoption('fire_triggers','on');
COMMIT;


begin
	// *****  Tabela bethadba.movimentos
	declare w_i_tipos_proc smallint;
	declare w_i_competencias date;
	declare w_i_processamentos smallint;
	declare w_i_funcionarios integer;
	declare w_i_eventos smallint;
	declare w_classif_evento tinyint;
	declare w_mov_resc char(1);
	
	// *****  Tabela bethadba.dados_calc
	declare w_dt_pagto date;
	
	// *****  Tabela bethabda.processamentos
	declare w_dt_fechamento date;
	
	// *****  Tabela bethadba.processamentos_lotes
	declare w_i_processamentos_lotes integer;
	
	// *****  Variaveis auxiliares
	declare w_evento smallint;
	declare w_i_eventos_aux smallint;
	declare w_nome_aux char(50);
	declare w_tipo_pd_aux char(1);
	declare w_taxa_aux decimal(10,4);
	declare w_unidade_aux char(15);
	declare w_sai_rais_aux char(1);
	declare w_compoe_liq_aux char(1);
	declare w_compoe_hmes_aux char(1);
	declare w_digitou_form_aux char(1);
	declare w_classif_evento_aux tinyint;
	ooLoop: for oo as cnv_movimentos dynamic scroll cursor for
		select 1 as w_i_entidades,
			f.cdMatricula as w_cdMatricula,
			f.sqContrato as w_sqContrato,
			f.dtCompetencia as w_dtCompetencia,
			f.tpcalculo as w_tpcalculo,
			f.sqHabilitacao as w_sqHabilitacao,
			f.cdVerba as w_cdVerba,
			f.inRetificacao as w_inRetificacao,
			f.dtPagamento as w_dtPagamento,
			tecbth_delivery.fu_convdecimal(tecbth_delivery.tira_caracter_1(vlComplemento),0) as w_vlr_inf,
			cast(vlMensal as decimal(12,2)) as w_vlr_calc,
			cast(vlAuxiliar as decimal(12,2)) as w_vlAuxiliar,
			cast(vlIntegral as decimal(12,2)) as w_vlIntegral,
			if v.TpCategoria in ('D','P','V') then 'S' else 'N' endif as w_compoe_liq,
			v.TpCategoria as w_tipo_pd
		from tecbth_delivery.gp001_FICHAFINANCEIRA f
		join tecbth_delivery.gp001_VERBA v on f.cdVerba = v.CdVerba
		join tecbth_delivery.gp001_FichaFinanceiraHeaderCalculo ff on f.cdMatricula = ff.cdMatricula and f.dtCompetencia = ff.dtCompetencia and f.sqHabilitacao = ff.sqHabilitacao and f.tpCalculo = ff.tpCalculo
		where f.tpCalculo in (1,2)
		order by 1, 2, 4, 9 asc
	do
		
		// *****  Tabela bethadba.movimentos
		set w_i_tipos_proc = null;
		set w_i_competencias = null;
		set w_i_processamentos = null;
		set w_i_funcionarios = null;
		set w_i_eventos = null;
		set w_classif_evento = null;
		set w_mov_resc = null;
		
		// *****  Tabela bethadba.dados_calc
		set w_dt_pagto = null;
		
		// *****  Tabela bethabda.processamentos
		set w_dt_fechamento = null;
		
		// *****  Tabela bethadba.processamentos_lotes
		set w_i_processamentos_lotes = null;
		
		// *****  Variaveis auxiliares
		set w_evento = null;
		set w_i_eventos_aux = null;
		set w_nome_aux = null;
		set w_tipo_pd_aux = null;
		set w_taxa_aux = null;
		set w_unidade_aux = null;
		set w_sai_rais_aux = null;
		set w_compoe_liq_aux = null;
		set w_compoe_hmes_aux = null;
		set w_digitou_form_aux = null;
		set w_classif_evento_aux = null;
		
		// *****  Converte bethadba.movimentos
		set w_i_funcionarios=cast(w_cdMatricula as integer);		
		if exists (select  1 from bethadba.funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios) then		
			if w_tpCalculo = 1 then -- 11-Mensal
				set w_i_tipos_proc=11;
				set w_mov_resc='S'
			elseif w_tpCalculo = 2 then --42-Complementar
				set w_i_tipos_proc=42;
				set w_mov_resc='S'
			elseif w_tpCalculo = 3 then --80-Férias
				set w_i_tipos_proc=80;
				set w_mov_resc='N'
			elseif w_tpCalculo = 5 then --51-13º Adiantamento
				set w_i_tipos_proc=51;
				set w_mov_resc='N'
			elseif w_tpCalculo = 6 then --52-13º Salário
				set w_i_tipos_proc=52;
				set w_mov_resc='N'
			elseif w_tpCalculo = 7 then --52-13º Salário
				set w_i_tipos_proc=52;
				set w_mov_resc='N'
			elseif w_tpCalculo = 8 then --41-Adiantamento
				set w_i_tipos_proc=41;
				set w_mov_resc='N'
			elseif w_tpCalculo = 9 then --11-Mensal
				set w_i_tipos_proc=11;
				set w_mov_resc='N'
			elseif w_tpCalculo = 10 then --42-Complementar
				set w_i_tipos_proc=42;
				set w_mov_resc='N'
			elseif w_tpCalculo = 11 then --11-Mensal
				set w_i_tipos_proc=11;
				set w_mov_resc='N'
			else --11-Mensal
				set w_i_tipos_proc=11;
				set w_mov_resc='N'
			end if;

		
				set w_i_competencias=date(w_dtCompetencia);
	
			
			set w_i_processamentos=1;


			
			if not w_cdVerba = any(select   evento from tecbth_delivery.evento_aux where tipo_pd = 'F' and w_i_entidades = w_i_entidades  ) then
				if not exists(select distinct  1 from tecbth_delivery.evento_aux where evento = w_cdVerba and retificacao = w_inRetificacao and w_i_entidades = w_i_entidades) then
					if w_inRetificacao in('C','D') then
						select   evento,nome,tipo_pd,taxa,unidade,sai_rais,compoe_liq,compoe_hmes,digitou_form,classif_evento 
						into w_i_eventos_aux,w_nome_aux,w_tipo_pd_aux,w_taxa_aux,w_unidade_aux,w_sai_rais_aux,w_compoe_liq_aux,w_compoe_hmes_aux,w_digitou_form_aux,w_classif_evento_aux 
						from tecbth_delivery.evento_aux 
						where evento = w_cdVerba 
						and retificacao = 'B' 
						and resc_mov = 'N' 
						and i_entidades = w_i_entidades
                        and i_eventos = w_i_eventos;
						-- AJUSTE
					
						
						select distinct coalesce(max(w_i_eventos),0)+1 
						into w_evento 
						  from tecbth_delivery.evento_aux 
						where tipo_pd in('P','D');
						
						insert into tecbth_delivery.evento_aux(evento,i_eventos,nome,tipo_pd,taxa,unidade,sai_rais,compoe_liq,compoe_hmes,digitou_form,classif_evento,retificacao,resc_mov,i_entidades) 
						values (w_i_eventos_aux,w_evento,w_nome_aux,w_tipo_pd_aux,w_taxa_aux,w_unidade_aux,w_sai_rais_aux,w_compoe_liq_aux,w_compoe_hmes_aux,w_digitou_form_aux,w_classif_evento_aux,
								w_inRetificacao,'N',w_i_entidades);
								
						message 'Eve.: '||w_evento||' Nom.: '||w_nome_aux||' Tip.: '||w_tipo_pd_aux to client;
						
						insert into bethadba.eventos(i_eventos,nome,tipo_pd,taxa,unidade,sai_rais,compoe_liq,compoe_hmes,digitou_form,classif_evento,cods_conversao)on existing skip
						values (w_evento,w_nome_aux,w_tipo_pd_aux,w_taxa_aux,w_unidade_aux,w_sai_rais_aux,w_compoe_liq_aux,w_compoe_hmes_aux,w_digitou_form_aux,w_classif_evento_aux,null);
					end if
				end if
			end if;			


            select first i_eventos
			into w_i_eventos 
 			from tecbth_delivery.evento_aux 
			where evento  = w_cdVerba
			--and	resc_mov = 'N' 
			and	i_entidades = w_i_entidades;

			--if w_tipo_pd not in ('D', 'P') then
				select first tipo_pd,classif_evento 
							into w_tipo_pd,w_classif_evento 
						from bethadba.eventos  
							where i_eventos = w_i_eventos;
			--end if;
	



			if w_vlr_inf = '0' then
				set w_vlr_inf=0.0
			end if;
			
			if w_i_eventos = 1 then
				set w_vlr_inf=cast(w_vlAuxiliar as decimal(12,2))
			end if;
			
			if w_vlr_inf is null then
				set w_vlr_inf=w_vlr_calc
			end if;

			if(w_i_eventos < 9000) then
				// **** Processamentos
				set w_dt_fechamento=w_dtPagamento;
				set w_dt_pagto=w_dtPagamento;
				
				message 'Ent.: '||w_i_entidades||' Tip.: '||w_i_tipos_proc||' Com.: '||w_i_competencias||' Pro.: '||w_i_processamentos|| ' w_classif_evento: ' || w_classif_evento to client;
				 
				insert into bethadba.processamentos(i_entidades,i_tipos_proc,i_competencias,i_processamentos,dt_fechamento,dt_pagto,descricao)on existing skip
				values (w_i_entidades,w_i_tipos_proc,w_i_competencias,w_i_processamentos,w_dt_fechamento,w_dt_pagto,null);
				
						
				// *****  Converte tabela bethadba.processamentos_lotes
				if w_i_tipos_proc = 11 then
					set w_i_processamentos_lotes=1;
				
					message 'Ent.: '||w_i_entidades||' Tip.: '||w_i_tipos_proc||' Com.: '||w_i_competencias||' Pro. Lot.: '||w_i_processamentos_lotes to client;
				
					insert into bethadba.processamentos_lotes(i_entidades,i_competencias,i_processamentos_lotes,descricao)on existing skip
					values (w_i_entidades,w_i_competencias,w_i_processamentos_lotes,'Mensal Conversao');
				else 
					set w_i_processamentos_lotes=null;
				end if;				
					
				// *****  Converte tabela bethadba.dados_calc	
				set w_dt_pagto = null;
				set w_dt_pagto=w_dtPagamento;
				
				message 'Ent.: '||w_i_entidades||' Tip.: '||w_i_tipos_proc||' Com.: '||w_i_competencias||' Pro.: '||w_i_processamentos||' Fun.: '||w_i_funcionarios to client;
					
				insert into  bethadba.dados_calc(i_entidades,i_tipos_proc,i_competencias,i_processamentos,i_funcionarios,vlr_proventos,vlr_descontos,gerado_emp,movto_anterior,dt_pagto,i_processamentos_lotes)on existing skip
				values (w_i_entidades,w_i_tipos_proc,w_i_competencias,w_i_processamentos,w_i_funcionarios,0.0,0.0,'N','N',w_dt_pagto,w_i_processamentos_lotes);
				
				// **** Movimentos
				
				message 'Ent.: '||w_i_entidades||' Tip.: '||w_i_tipos_proc||' Com.: '||w_i_competencias||' Pro.: '||w_i_processamentos||' Fun.: '||w_i_funcionarios||' Eve.: '||w_i_eventos||
						' Vlr. Inf.: '||w_vlr_inf||' Vlr. Cal.: '||w_vlr_calc to client;
		    
		    	if w_inRetificacao = 'C' 
		    		and exists(select first 1 from tecbth_delivery.gp001_MOVIMENTOS m where m.i_tipos_proc = w_i_tipos_proc and i_competencias = w_i_competencias and i_processamentos = w_i_processamentos 
		    							and i_funcionarios = w_i_funcionarios and i_eventos = w_i_eventos and compoe_liq = w_compoe_liq and tipo_pd = w_tipo_pd) then	
		    							
		    		update tecbth_delivery.gp001_MOVIMENTOS as m set m.vlr_inf = m.vlr_inf + w_vlr_inf, m.vlr_calc = m.vlr_calc + w_vlr_calc where m.i_tipos_proc = w_i_tipos_proc and i_competencias = w_i_competencias 
		    		and i_processamentos = w_i_processamentos and i_funcionarios = w_i_funcionarios and i_eventos = w_i_eventos and compoe_liq = w_compoe_liq and tipo_pd = w_tipo_pd;
		    	else
					insert into tecbth_delivery.gp001_MOVIMENTOS(i_entidades,i_tipos_proc,i_competencias,i_processamentos,i_funcionarios,i_eventos,vlr_inf,vlr_calc,tipo_pd,compoe_liq,classif_evento,mov_resc)
		    		values (w_i_entidades,w_i_tipos_proc,w_i_competencias,w_i_processamentos,w_i_funcionarios,w_i_eventos,w_vlr_inf,w_vlr_calc,w_tipo_pd,w_compoe_liq,w_classif_evento,w_mov_resc);
		    	end if;
		    	 
			end if;
		end if;
	end for;
end;

commit;

update bethadba.dados_calc set dt_fechamento = dt_pagto where dt_fechamento is null;

-- Atualiza composição líquidas dos eventos
update bethadba.movimentos m 
left join bethadba.eventos e 
on m.i_eventos = e.i_eventos 
set m.compoe_liq = e.compoe_liq;

-- Exclui eventos não informativos das folhas
delete from bethadba.movimentos where i_eventos in (1,128,243,242 ,301, 393, 12) and i_tipos_proc in (51,52);

CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;

-- Insere bethadba.processamentos + bethadba.rescisoes_proc
insert into bethadba.processamentos on existing skip
select 
1 as i_entidades,
11 as i_tipos_proc,
cast(left(dt_rescisao, 7) + '-01' as text) as i_competencias,
1 as i_processamentos,
i_competencias as dt_pagto,
i_competencias as dt_fechamento,
null as descricao,
'N' as simulado,
null as dt_liberacao,
'S' as pagto_realizado,
null as i_pagto_ant_rras,
null as i_pagto_ant_rras_parc,
null as pagto_ant_rras_13,
'N' as fechado_esocial 
from bethadba.rescisoes;

insert into bethadba.rescisoes_proc 
select 
i_entidades,
i_funcionarios,
i_rescisoes,
11 as i_tipos_proc,
dateformat(dt_rescisao, 'yyyy-MM-01 H:mm:ss')  as i_competencias,
1 as i_processamentos,
'N' as recolh_grfc
from bethadba.rescisoes;

-- Atualiza férias

delete from bethadba.movimentos where i_tipos_proc = 80 and i_eventos in (1,12,63,68,243,138,35, 393);

-- bethadba.ferias_proc | bethadba.processamentos
insert into bethadba.processamentos on existing skip
select 
1 as i_entidades,
80 as i_tipos_proc,
cast(left(dt_gozo_ini, 7) + '-01' as text) as i_competencias,
1 as i_processamentos,
i_competencias as dt_pagto,
i_competencias as dt_fechamento,
null as descricao,
'N' as simulado,
null as dt_liberacao,
'S' as pagto_realizado,
null as i_pagto_ant_rras,
null as i_pagto_ant_rras_parc,
null as pagto_ant_rras_13,
'N' as fechado_esocial 
from bethadba.ferias;

insert into bethadba.ferias_proc 
select 
i_entidades,
i_funcionarios,
i_ferias,
80 as i_tipos_proc,
left(dt_gozo_ini, 7) + '-01' as i_competencias,
1 as i_processamentos,
1 as mes_ferias
from bethadba.ferias;


-- Atualiza folhas finais para envio
update bethadba.dados_calc as t1 
set vlr_proventos = (select coalesce(sum(vlr_calc),0) 
				     from bethadba.movimentos as t2 
					 where t2.i_entidades = t1.i_entidades 
					 and t2.i_tipos_proc = t1.i_tipos_proc 
					 and t2.i_competencias = t1.i_competencias 
					 and t2.i_processamentos = t1.i_processamentos 
					 and t2.i_funcionarios = t1.i_funcionarios 
					 and t2.tipo_pd = 'P' 
					 and t2.compoe_liq = 'S'
					 and t2.mov_resc = 'N'),
	vlr_descontos = (select coalesce(sum(vlr_calc),0) 
					 from bethadba.movimentos as t2 
					 where t2.i_entidades = t1.i_entidades 
					 and t2.i_tipos_proc = t1.i_tipos_proc 
					 and t2.i_competencias = t1.i_competencias 
					 and t2.i_processamentos = t1.i_processamentos 
					 and t2.i_funcionarios = t1.i_funcionarios 
					 and t2.tipo_pd = 'D' 
					 and t2.compoe_liq = 'S'
					 and t2.mov_resc = 'N');

commit;

/* ATENÇÃO ADICIONAR NO ARJOB DE FOLHA

union all 

                 select rescisao = 'S' , 
                        300 as sistema, 
                        'folha' as tipo_registro,
                        dc.i_entidades         as chave_dsk1,
                        dc.i_funcionarios   as chave_dsk2,
                        dc.i_tipos_proc      as chave_dsk3,
                        dc.i_processamentos as chave_dsk4,
                        dc.i_competencias    as chave_dsk5,
                        tipoProcessamento = 'RESCISAO',
                        subTipoProcessamento = case dc.i_tipos_proc 
                                     when 11 then 'INTEGRAL' 
                                     when 42 then 'COMPLEMENTAR'
                                     end,
                        ehAposentado = if bethadba.dbf_gettipoafast (1,funcionarios.i_entidades,funcionarios.i_funcionarios,dc.i_competencias) = 9 then 'APOSENTADO' 
                                            else
                                        if bethadba.dbf_gettipoafast (1,funcionarios.i_entidades,funcionarios.i_funcionarios,dc.i_competencias) = 8 
                                                                                and exists (select 1 from bethadba.rescisoes r
                                                                                join bethadba.motivos_apos ma on r.i_motivos_apos = ma.i_motivos_apos
                                                                                join bethadba.tipos_afast ta on ma.i_tipos_afast = ta.i_tipos_afast
                                                                                where r.i_entidades = chave_dsk1 
                                                                                            and r.i_funcionarios = chave_dsk2 and r.dt_rescisao < chave_dsk5
                                                                                            and r.dt_canc_resc is null and r.i_motivos_apos is not null
                                                                                            and ta.classif = 9)
                                        then 'APOSENTADO' endif                                                               
                                   endif,
                        motivoRescisaoCessado = (select first i_motivos_resc from bethadba.rescisoes where rescisoes.i_funcionarios = dc.i_funcionarios and i_motivos_resc = 8),
                        matricula = bethadba.dbf_get_id_gerado(sistema, 'matricula', dc.i_entidades , dc.i_funcionarios ,(if ehAposentado = 'APOSENTADO' and motivoRescisaoCessado = 8 then ehAposentado endif)), 
                        competencia = dateformat(dc.i_competencias,'yyyy-MM'),
                        folhaPagamento = 'true',
                        identificadorFolhaDePagamento = dc.recibo_esocial,
                        dataPagamento = dc.dt_pagto,
                        dataFechamento = dc.dt_fechamento,
                        dataCalculo = dataPagamento ,
                        totalBruto = isnull((select sum(vlr_calc) from tecbth_delivery.gp001_movimentos m 
                                             where  m.i_entidades = dc.i_entidades and 
                                                    m.i_funcionarios = dc.i_funcionarios and 
                                                    m.i_tipos_proc = dc.i_tipos_proc and 
                                                    m.i_processamentos = dc.i_processamentos and 
                                                    m.i_competencias = dc.i_competencias and 
                                                    m.mov_resc = 'S' and 
                                                    m.tipo_pd = 'P' and 
                                                    m.compoe_liq = 'S'),0),
                        totalDesconto = isnull((select sum(vlr_calc) from tecbth_delivery.gp001_movimentos m 
                                             where  m.i_entidades = dc.i_entidades and 
                                                    m.i_funcionarios = dc.i_funcionarios and 
                                                    m.i_tipos_proc = dc.i_tipos_proc and 
                                                    m.i_processamentos = dc.i_processamentos and 
                                                    m.i_competencias = dc.i_competencias and 
                                                    m.mov_resc = 'S' and 
                                                    m.tipo_pd = 'D' and 
                                                    m.compoe_liq = 'S'),0),
                        totalLiquido = totalBruto - totalDesconto,
                        temRescisao = if exists (select 1 
                                                   from tecbth_delivery.gp001_movimentos m 
                                                  where m.i_entidades = dc.i_entidades and 
                                                        m.i_funcionarios = dc.i_funcionarios and 
                                                        m.i_tipos_proc = dc.i_tipos_proc and 
                                                        m.i_processamentos = dc.i_processamentos and 
                                                        m.i_competencias = dc.i_competencias and 
                                                        m.mov_resc = 'S') then 'S' else 'N' endif ,
                        dataLiberacao =  (select processamentos.dt_liberacao 
                                            from bethadba.processamentos 
                                           where processamentos.i_entidades = dc.i_entidades and 
                                                 processamentos.i_entidades = dc.i_funcionarios and 
                                                 processamentos.i_entidades = dc.i_tipos_proc and 
                                                 processamentos.i_entidades = dc.i_competencias and 
                                                 processamentos.i_entidades = dc.i_processamentos),
                        situacao = 'FECHADA',
                        NULL as ferias , 
                        NULL as periodo , 
                        NULL as periodoAquisitivo 
                   from bethadba.dados_calc dc, bethadba.funcionarios
                  where 
                        dc.i_entidades in (1) and
                        dc.i_entidades = funcionarios.i_entidades and
                        dc.i_funcionarios = funcionarios.i_funcionarios and
                        dc.dt_fechamento is not null and
                        temRescisao = 'S' and 
                        matricula is not null

*/