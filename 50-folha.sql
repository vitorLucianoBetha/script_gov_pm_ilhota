
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;
begin
	// *****  Tabela bethadba.movimentos
	declare w_i_tipos_proc smallint;
	declare w_i_competencias date;
	declare w_i_funcionarios integer;
	
	// ***** Tabela bethadba.bases_calc
	declare w_i_tipos_bases integer;
	declare w_i_sequenciais integer;
	declare w_vlbasecalculo numeric(12,2);
	
	ooLoop: for oo as cnv_bases_calc dynamic scroll cursor for
		select 1 as w_i_entidades,cdMatricula as w_cdMatricula,sqContrato as w_sqContrato,dtCompetencia as w_dtCompetencia,tpcalculo as w_tpcalculo,
			cdVerba as w_cdVerba,inRetificacao as w_inRetificacao,dtPagamento as w_dtPagamento,if cast(vlBaseCalculo as decimal(12,2)) = 0 then 
                                                                                                  cast(vlMensal as decimal(12,2))
                                                                                               else
                                                                                                  cast(vlBaseCalculo as decimal(12,2))  
                                                                                               endif as w_vlr_base, vlMensal as w_vlMensal, fu_convdecimal(tira_caracter_1(vlComplemento),0) as w_vlcomplemento
		from tecbth_delivery.gp001_fichafinanceira 
		where  w_vlr_base > 1
		order by 1,4,2,3,5 asc
	do
		
		// *****  Tabela bethadba.movimentos
		set w_i_tipos_proc = null;
		set w_i_competencias = null;
		set w_i_funcionarios = null;
				
		// ***** Tabela bethadba.bases_calc
		set w_i_tipos_bases = null;
		set w_i_sequenciais = null;
		set w_vlbasecalculo = null;
				
		// *****  Converte bethadba.movimentos
		set w_i_funcionarios=cast(w_cdMatricula as integer);
		
		if exists(select 1 from bethadba.funcionarios where	i_entidades = w_i_entidades and	i_funcionarios = w_i_funcionarios) then		
			if w_tpCalculo = 1 then -- 11-Mensal
				set w_i_tipos_proc=11;
			elseif w_tpCalculo = 2 then --42-Complementar
				set w_i_tipos_proc=42;
			elseif w_tpCalculo = 3 then --80-Férias
				set w_i_tipos_proc=80;
			elseif w_tpCalculo = 5 then --51-13º Adiantamento
				set w_i_tipos_proc=51;
			elseif w_tpCalculo = 6 then --52-13º Salário
				set w_i_tipos_proc=52;
			elseif w_tpCalculo = 7 then --52-13º Salário
				set w_i_tipos_proc=52;
			elseif w_tpCalculo = 8 then --41-Adiantamento
				set w_i_tipos_proc=41;
			elseif w_tpCalculo = 9 then --11-Mensal
				set w_i_tipos_proc=11;
			elseif w_tpCalculo = 10 then --42-Complementar
				set w_i_tipos_proc=42;
			elseif w_tpCalculo = 11 then --11-Mensal
				set w_i_tipos_proc=11;
			else --11-Mensal
				set w_i_tipos_proc=11;
			end if;
			if day(w_dtCompetencia) != 1 then
				set w_i_competencias=year(w_dtCompetencia)||'-'||month(w_dtCompetencia)||'-'||'01'
			else
				set w_i_competencias=date(w_dtCompetencia)
			end if;
          print '1'; 
         if exists (select 1 from bethadba.dados_calc where 
                     i_entidades = w_i_entidades and 
                     i_competencias = w_i_competencias and 
                     i_tipos_proc = w_i_tipos_proc and
                     i_funcionarios = w_i_funcionarios and 
                     i_processamentos = 1 ) then
		  if(w_i_tipos_proc in( 11,41,42,51,52) ) and(w_cdVerba = 1158) then
               set w_vlr_base=cast(w_vlMensal as decimal(12,2));
                 print 'i_competencias: '+string(w_i_competencias);
                 print 'i_funcionarios: '+string(w_i_funcionarios); 
                 print 'cdMatricula: '+string(w_cdMatricula); 
                   insert into bethadba.movimentos( i_entidades,i_tipos_proc,i_competencias,i_processamentos,i_funcionarios,i_eventos,
                    vlr_inf,vlr_calc,tipo_pd,compoe_liq,classif_evento,mov_resc) on existing skip values( w_i_entidades,w_i_tipos_proc,w_i_competencias,
                    1,w_i_funcionarios,138,w_vlcomplemento,w_vlr_base,'D','N',31,'N') 
            end if
          end if;	 
           print '3';
           if w_cdVerba in(526,527,528,529,530,531) then
						
				if exists(select 1 from bethadba.dados_calc where i_entidades = w_i_entidades and i_tipos_proc = w_i_tipos_proc and	i_competencias = w_i_competencias 
			              and i_processamentos = 1 and	i_funcionarios = w_i_funcionarios) then
					
					if (w_tpCalculo != 11) then	
					
						if w_vlr_base > 1 then
						
							if (w_i_tipos_proc in(11,41,42,51,52,80)) and (w_cdVerba in (530,531)) then
							
								set w_i_tipos_bases=8;
								
								/*select first vlbasecalculo
								into w_vlbasecalculo
								from tecbth_delivery.fichafinanceira 
								where (round(cdMatricula/1,0)*10)||SqContrato = w_i_funcionarios 
								and dtCompetencia = w_i_competencias
								and  cdverba = w_cdVerba;*/
								
								select coalesce(max(i_sequenciais),0)+1 
								into w_i_sequenciais 
								from bethadba.bases_calc 
								where i_entidades = w_i_entidades 
								and i_tipos_proc = w_i_tipos_proc 
								and i_competencias = w_i_competencias 
								and i_processamentos = 1 
								and i_funcionarios = w_i_funcionarios 
								and	i_tipos_bases = w_i_tipos_bases;
							
								message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Tip. Pro.: '||w_i_tipos_proc||' Com.: '||w_i_competencias||' Pro.: '||1||
										' Tip. Bas.: '||w_i_tipos_bases||' Verba: '||w_cdVerba||' Vlr.: '||w_vlr_base  to client;
							
								insert into bethadba.bases_calc(i_entidades,i_tipos_proc,i_competencias,i_processamentos,i_funcionarios,i_tipos_bases,i_sequenciais,i_eventos,
																vlr_base) on existing skip
								values (w_i_entidades,w_i_tipos_proc,w_i_competencias,1,w_i_funcionarios,w_i_tipos_bases,w_i_sequenciais,null,
										/*w_vlbasecalculo*/w_vlr_base);
							end if;
							
							if (w_i_tipos_proc in(11,41,42,51,52,80)) and (w_cdVerba in (529)) then
							
								set w_i_tipos_bases=9;
								
								/*select first vlbasecalculo
								into w_vlbasecalculo
								from tecbth_delivery.fichafinanceira 
								where (round(cdMatricula/1,0)*10)||SqContrato = w_i_funcionarios 
								and dtCompetencia = w_i_competencias
								and  cdverba = w_cdVerba;*/
								
								select coalesce(max(i_sequenciais),0)+1 
								into w_i_sequenciais 
								from bethadba.bases_calc 
								where i_entidades = w_i_entidades 
								and i_tipos_proc = w_i_tipos_proc 
								and i_competencias = w_i_competencias 
								and i_processamentos = 1 
								and i_funcionarios = w_i_funcionarios 
								and	i_tipos_bases = w_i_tipos_bases;
							
								message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Tip. Pro.: '||w_i_tipos_proc||' Com.: '||w_i_competencias||' Pro.: '||1||
										' Tip. Bas.: '||w_i_tipos_bases||' Verba: '||w_cdVerba||' Vlr.: '||w_vlr_base  to client;
							
								insert into bethadba.bases_calc(i_entidades,i_tipos_proc,i_competencias,i_processamentos,i_funcionarios,i_tipos_bases,i_sequenciais,i_eventos,
																vlr_base) on existing skip
								values (w_i_entidades,w_i_tipos_proc,w_i_competencias,1,w_i_funcionarios,w_i_tipos_bases,w_i_sequenciais,null,
										/*w_vlbasecalculo*/w_vlr_base);
							end if;
							
							if (w_i_tipos_proc in(11,41,42,51,52,80)) and (w_cdVerba in (527,528)) then
							
								set w_i_tipos_bases=11;
								
								/*select first vlbasecalculo
								into w_vlbasecalculo
								from tecbth_delivery.fichafinanceira 
								where (round(cdMatricula/1,0)*10)||SqContrato = w_i_funcionarios 
								and dtCompetencia = w_i_competencias
								and  cdverba = w_cdVerba;*/
								
								select coalesce(max(i_sequenciais),0)+1 
								into w_i_sequenciais 
								from bethadba.bases_calc 
								where i_entidades = w_i_entidades 
								and i_tipos_proc = w_i_tipos_proc 
								and i_competencias = w_i_competencias 
								and i_processamentos = 1 
								and i_funcionarios = w_i_funcionarios 
								and	i_tipos_bases = w_i_tipos_bases;
								
								message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Tip. Pro.: '||w_i_tipos_proc||' Com.: '||w_i_competencias||' Pro.: '||1||
										' Tip. Bas.: '||w_i_tipos_bases||' Verba: '||w_cdVerba||' Vlr.: '||w_vlr_base  to client;
							
								insert into bethadba.bases_calc(i_entidades,i_tipos_proc,i_competencias,i_processamentos,i_funcionarios,i_tipos_bases,i_sequenciais,i_eventos,
																vlr_base)on existing skip
								values (w_i_entidades,w_i_tipos_proc,w_i_competencias,1,w_i_funcionarios,w_i_tipos_bases,w_i_sequenciais,null,
										/*w_vlbasecalculo*/w_vlr_base);
							end if;								
							
							if (w_i_tipos_proc in(11,41,42,51,52,80)) and (w_cdVerba in (526)) then
							
								set w_i_tipos_bases=12;
								
								/*select first vlbasecalculo
								into w_vlbasecalculo
								from tecbth_delivery.fichafinanceira 
								where (round(cdMatricula/1,0)*10)||SqContrato = w_i_funcionarios 
								and dtCompetencia = w_i_competencias
								and  cdverba = w_cdVerba;*/
								
								select coalesce(max(i_sequenciais),0)+1 
								into w_i_sequenciais 
								from bethadba.bases_calc 
								where i_entidades = w_i_entidades 
								and i_tipos_proc = w_i_tipos_proc 
								and i_competencias = w_i_competencias 
								and i_processamentos = 1 
								and i_funcionarios = w_i_funcionarios 
								and	i_tipos_bases = w_i_tipos_bases;
								
								message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Tip. Pro.: '||w_i_tipos_proc||' Com.: '||w_i_competencias||' Pro.: '||1||
										' Tip. Bas.: '||w_i_tipos_bases||' Verba: '||w_cdVerba||' Vlr.: '||w_vlr_base  to client;
							
								insert into bethadba.bases_calc(i_entidades,i_tipos_proc,i_competencias,i_processamentos,i_funcionarios,i_tipos_bases,i_sequenciais,i_eventos,
																vlr_base)on existing skip
								values (w_i_entidades,w_i_tipos_proc,w_i_competencias,1,w_i_funcionarios,w_i_tipos_bases,w_i_sequenciais,null,
										/*w_vlbasecalculo*/w_vlr_base);
							end if;
						end if;
					end if;
				end if;
			end if;
		end if;
	end for;
end;

