CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;


begin
	// *****  Tabela bethadba.movimentos
	declare w_i_tipos_proc smallint;
	declare w_i_competencias date;
	declare w_i_processamentos smallint;
	declare w_i_funcionarios integer;
	declare w_i_eventos smallint;
	declare w_tipo_pd char(1);
	declare w_compoe_liq char(1);
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
		select   1 as w_i_entidades,cdMatricula as w_cdMatricula,sqContrato as w_sqContrato,dtCompetencia as w_dtCompetencia,tpcalculo as w_tpcalculo,sqHabilitacao as w_sqHabilitacao,
			cdVerba as w_cdVerba,inRetificacao as w_inRetificacao,dtPagamento as w_dtPagamento,fu_convdecimal(tira_caracter_1(vlComplemento),0) as w_vlr_inf,
			cast(vlMensal as decimal(12,2)) as w_vlr_calc,cast(vlAuxiliar as decimal(12,2)) as w_vlAuxiliar,
			cast(vlIntegral as decimal(12,2)) as w_vlIntegral 
		from tecbth_delivery.gp001_fichafinanceira where dtCompetencia > '2020-01-01'
	do
		
		// *****  Tabela bethadba.movimentos
		set w_i_tipos_proc = null;
		set w_i_competencias = null;
		set w_i_processamentos = null;
		set w_i_funcionarios = null;
		set w_i_eventos = null;
		set w_tipo_pd = null;
		set w_compoe_liq = null;
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


            select distinct i_eventos
		into w_i_eventos 
 from tecbth_delivery.evento_aux 
			where evento  = w_cdVerba 
			and retificacao = w_inRetificacao 
			and	resc_mov = 'N' 
			and	i_entidades = w_i_entidades;


select first tipo_pd,compoe_liq,classif_evento 
			into w_tipo_pd,w_compoe_liq,w_classif_evento 
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
					
				insert into  bethadba.dados_calc(i_entidades,i_tipos_proc,i_competencias,i_processamentos,i_funcionarios,vlr_proventos,vlr_descontos,gerado_emp,movto_anterior,dt_pagto,
												i_processamentos_lotes)on existing skip
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

--update bethadba.movimentos set compoe_liq = 'N' WHERE i_eventos in (441,4) and i_tipos_proc in (51,52) and i_funcionarios in (192864)
--update bethadba.dados_calc set dt_fechamento = dt_pagto, recibo_esocial = cast(i_funcionarios as varchar(15)) + '/' + cast(i_competencias  as varchar(20))


update bethadba.dados_calc as t1 
set vlr_proventos = (select coalesce(sum(vlr_calc),0) 
				     from bethadba.movimentos as t2 
					 where t2.i_entidades = t1.i_entidades 
					 and t2.i_tipos_proc = t1.i_tipos_proc 
					 and t2.i_competencias = t1.i_competencias 
					 and t2.i_processamentos = t1.i_processamentos 
					 and t2.i_funcionarios = t1.i_funcionarios 
					 and t2.tipo_pd = 'P' 
					 and t2.compoe_liq = 'S'),
	vlr_descontos = (select coalesce(sum(vlr_calc),0) 
					 from bethadba.movimentos as t2 
					 where t2.i_entidades = t1.i_entidades 
					 and t2.i_tipos_proc = t1.i_tipos_proc 
					 and t2.i_competencias = t1.i_competencias 
					 and t2.i_processamentos = t1.i_processamentos 
					 and t2.i_funcionarios = t1.i_funcionarios 
					 and t2.tipo_pd = 'D' 
					 and t2.compoe_liq = 'S');

commit;