ROLLBACK;
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
call bethadba.pg_setoption('fire_triggers','off');
COMMIT;

-- BTHSC-59216 ajuste organogramas - André
-- BTHSC-153502 Bug em Matrículas | Históricos duplicados

begin
	// **** Primeiro cursor
	declare cur_conver dynamic scroll cursor for 
		select 1,Funcionario.cdMatricula as w_i_funcionarios,
			Funcionario.SqContrato,
			CdPessoa,
			DtAdmissao,
			Funcionario.cdFilial,
			cdVinculoEmpregaticio,
			InPagamentoLiquido,
			tecbth_delivery.fu_tirachars(tecbth_delivery.fu_tirachars(tecbth_delivery.fu_tirachars(nrContaCorrente,'X'),'.'),','),dgContaCorrente,
			NrBancoContaCorrente,
			NrAgenciaCheque,
			NrBancoCheque,
			NrAgenciaContaCorrente,
			cdHorario,
			InOpcaoFgts,
			inContribuiINSS,
			inContribuiFundoPrev,
			Funcionario.cdAgenteNocivo,
			DtUltimoExame,
			dtFimContrato,
			isnull(DtTransferencia,DtAdmissao) as DtTransferencia,
			HistoricoLotacao.CdLocalTransf,
			if Lotacao.CdOrganograma = 99 then 2 else Lotacao.CdOrganograma endif,
			nivel1||nivel2,--||nivel3||nivel4,
			SqCartaoPonto,
			Funcionario.dtRescisao
		from tecbth_delivery.GP001_Funcionario as Funcionario
		left join tecbth_delivery.GP001_HistoricoLotacao as HistoricoLotacao on Funcionario.CdMatricula = HistoricoLotacao.CdMatricula and Funcionario.SqContrato = HistoricoLotacao.SqContrato and TpTransferencia not in('E','F')
		left join tecbth_delivery.GP001_Lotacao as Lotacao on HistoricoLotacao.CdLocalTransf = Lotacao.CdLocal
		left join tecbth_delivery.GP001_Empresa as Empresa on Lotacao.CdOrganograma = Empresa.CdOrganograma
		order by 1,2,3,22 asc; 
	 

	// **** Segundo cursor
	declare cur_conver3 dynamic scroll cursor for 
		select 1, Funcionario.cdMatricula as w_i_funcionarios, Funcionario.SqContrato, DtAdmissao, DtInicio,
               HistoricoVinculo.CdVinculoEmpregaticio, Funcionario.dtRescisao
		from tecbth_delivery.GP001_Funcionario as Funcionario,tecbth_delivery.GP001_HistoricoVinculo as HistoricoVinculo
		where Funcionario.CdMatricula = HistoricoVinculo.CdMatricula 
		and Funcionario.SqContrato = HistoricoVinculo.SqContrato 
		order by 1,2,3,5 asc;
		  
	// *****  Tabela bethadba.hist_funcionarios
	declare w_i_entidades integer;
	declare w_i_funcionarios integer;
	declare w_dt_alteracoes timestamp;
	declare w_i_config_organ smallint;
	declare w_i_organogramas char(16);
	declare w_i_grupos smallint;
	declare w_i_vinculos smallint;
	declare w_i_pessoas integer;
	declare w_i_bancos integer;
	declare w_i_agencias integer;
	declare w_i_pessoas_contas char(15);
	declare w_i_horarios smallint;
	declare w_func_princ integer;
	declare w_i_agentes_nocivos char(4);
	declare w_optante_fgts char(1);
	declare w_prev_federal char(1);
	declare w_prev_estadual char(1);
	declare w_fundo_ass char(1);
	declare w_fundo_prev char(1);
	declare w_ocorrencia_sefip tinyint;
	declare w_forma_pagto char(1);
	declare w_multiplic decimal(3,2);
	declare w_num_quadro_cp smallint;
	declare w_num_cp char(8);
	declare w_SqCartaoPonto varchar(8);
	
	// *****  Tabela tecbth_delivery.funcionario	
	declare w_CdMatricula integer;
	declare w_SqContrato smallint;
	declare w_CdPessoa double;
	declare w_DtAdmissao timestamp;
	declare w_dtFimContrato timestamp;
	declare w_NrBancoCheque smallint;
	declare w_NrAgenciaCheque smallint;
	declare w_NrBancoContaCorrente smallint;
	declare w_NrAgenciaContaCorrente smallint;
	declare w_NrContaCorrente varchar(12);
	declare w_dgContaCorrente varchar(2);
	declare w_InOpcaoFgts varchar(1);
	declare w_CdVinculoEmpregaticio smallint;
	declare w_CdFilial smallint;
	declare w_CdLocal smallint;
	declare w_cdHorario smallint;
	declare w_DtUltimoExame timestamp;
	declare w_cdAgenteNocivo smallint;
	declare w_inContribuiINSS smallint;
	declare w_inContribuiFundoPrev smallint;
	declare w_InPagamentoLiquido varchar(2);
	declare w_i_planos_previd integer;
	// *****  Tabela tecbth_delivery.historicolotacao
	declare w_cdLocalTransf smallint;
	
	// *****  Tabela tecbth_delivery.lotacao
	declare w_cdOrganograma smallint;
	declare w_cdLotacao varchar(100);
	
	// *****  Variaveis auxiliares
	declare w_data datetime;
	declare w_number integer;
	declare w_i_funcionarios_aux integer;
    declare w_dtRescisao date;
	open cur_conver with hold;
		set w_number=0;
        L_item: loop
			fetch next cur_conver into w_i_entidades,w_cdMatricula,w_SqContrato,w_CdPessoa,w_DtAdmissao,w_cdFilial,w_cdVinculoEmpregaticio,w_InPagamentoLiquido,w_nrContaCorrente,
									   w_dgContaCorrente,w_NrBancoContaCorrente,w_NrAgenciaCheque,w_NrBancoCheque,w_NrAgenciaContaCorrente,w_cdHorario,w_InOpcaoFgts,w_inContribuiINSS,
									   w_inContribuiFundoPrev,w_cdAgenteNocivo,w_DtUltimoExame,w_dtFimContrato,w_data,w_cdLocalTransf,w_CdOrganograma,w_CdLotacao,w_SqCartaoPonto,w_dtRescisao;
			
			if sqlstate = '02000' then
				leave L_item
			end if;
			
			// *****  Inicializa Variaveis
			set w_i_funcionarios=null;
			set w_dt_alteracoes=null;
			set w_i_config_organ=null;
			set w_i_organogramas=null;
			set w_i_grupos=null;
			set w_i_vinculos=null;
			set w_i_pessoas=null;
			set w_i_bancos=null;
			set w_i_agencias=null;
			set w_i_pessoas_contas=null;
			set w_i_horarios=null;
			set w_func_princ=null;
			set w_i_agentes_nocivos=null;
			set w_optante_fgts=null;
			set w_prev_federal=null;
			set w_prev_estadual=null;
			set w_fundo_ass=null;
			set w_fundo_prev=null;
			set w_ocorrencia_sefip=null;
			set w_forma_pagto=null;
			set w_multiplic=null;
			set w_num_quadro_cp=null;
			set w_num_cp=null;
			set w_i_planos_previd = null;
			
			// *****  Converte tabela bethadba.hist_funcionarios
			set w_i_funcionarios=cast(w_cdMatricula as integer);

			if w_i_funcionarios_aux != w_i_funcionarios then
				set w_number=0
			end if;
			
			set w_number=w_number+1;
			
			if w_number = 1 then
				set w_dt_alteracoes = hours(w_dtAdmissao,0)
			else
				fetch prior cur_conver into w_i_entidades,w_cdMatricula,w_SqContrato,w_CdPessoa,w_DtAdmissao,w_cdFilial,w_cdVinculoEmpregaticio,w_InPagamentoLiquido,w_nrContaCorrente,
											w_dgContaCorrente,w_NrBancoContaCorrente,w_NrAgenciaCheque,w_NrBancoCheque,w_NrAgenciaContaCorrente,w_cdHorario,w_InOpcaoFgts,w_inContribuiINSS,
											w_inContribuiFundoPrev,w_cdAgenteNocivo,w_DtUltimoExame,w_dtFimContrato,w_data,w_cdLocalTransf,w_CdOrganograma,w_CdLotacao,w_SqCartaoPonto,w_dtRescisao;
				
				if date(w_data) < w_dtAdmissao then
					set w_dt_alteracoes = dateadd(HOUR, 1, (select max(dt_alteracoes) from bethadba.hist_funcionarios
                                                            where i_entidades = w_i_entidades
                                                              and i_funcionarios = w_i_funcionarios
                                                              and date(dt_alteracoes) = w_dtAdmissao));
				else
					set w_dt_alteracoes = hours(date(w_data),1);
				end if;
				
				fetch next cur_conver into w_i_entidades,w_cdMatricula,w_SqContrato,w_CdPessoa,w_DtAdmissao,w_cdFilial,w_cdVinculoEmpregaticio,w_InPagamentoLiquido,w_nrContaCorrente,
										   w_dgContaCorrente,w_NrBancoContaCorrente,w_NrAgenciaCheque,w_NrBancoCheque,w_NrAgenciaContaCorrente,w_cdHorario,w_InOpcaoFgts,w_inContribuiINSS,
										   w_inContribuiFundoPrev,w_cdAgenteNocivo,w_DtUltimoExame,w_dtFimContrato,w_data,w_cdLocalTransf,w_CdOrganograma,w_CdLotacao,w_SqCartaoPonto,w_dtRescisao;
			end if;
			
			set w_i_config_organ = w_CdOrganograma;
			
			set w_i_organogramas = w_CdLotacao;
			set w_i_grupos = w_cdFilial;
			
			select first depois_1 
			into w_i_vinculos 
			from tecbth_delivery.antes_depois 
			where tipo = 'V'
			and antes_1 = w_i_entidades
			and antes_2 = w_cdVinculoEmpregaticio;
		
			if w_i_vinculos is null then
				set w_i_vinculos=1;
			end if;			
			
			if exists(select 1 from bethadba.funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_opcao_fgts is not null) then
				set w_optante_fgts='S';
			else
				set w_optante_fgts='N';
			end if;

			if w_inContribuiFundoPrev in(-1) then
              set w_fundo_prev='S';
              set w_prev_federal='N';
            else
              set w_prev_federal='S';
              set w_fundo_prev='N';
            end if;
			
            set w_prev_estadual ='N';
            set w_fundo_ass ='N';
			set w_i_pessoas=null;
			set w_i_pessoas_contas=null;
				select first depois_1 
				into w_i_pessoas 
				from tecbth_delivery.antes_depois 
				where tipo = 'P' 
				and antes_1 = w_i_entidades 
				and antes_2 = w_cdPessoa;
			if(w_InPagamentoLiquido = 'B') and(string(cast(trim(tecbth_delivery.fu_tirachars(w_nrContaCorrente,'.-,')) as decimal(15))) != 0) then
              set w_i_bancos=w_NrBancoContaCorrente;
              if not exists (select 1 from bethadba.bancos where i_bancos = w_i_bancos ) then
                set w_i_bancos = 800;
              end if;
              set w_i_agencias=w_NrAgenciaContaCorrente;

              select first i_pessoas_contas into w_i_pessoas_contas 
              from bethadba.pessoas_contas 
              where i_pessoas = w_i_pessoas 
                and i_bancos = w_i_bancos 
                and i_agencias = w_i_agencias 
                and num_conta = string(cast(trim(tecbth_delivery.fu_tirachars(w_NrContaCorrente,' .-,')) as decimal(15)))+string(trim(w_dgContaCorrente));

              set w_forma_pagto='R'
			elseif(w_InPagamentoLiquido = 'B') and(string(cast(trim(tecbth_delivery.fu_tirachars(w_nrContaCorrente,' .-,')) as decimal(15))) = 0) then
              set w_i_bancos=null;
              set w_i_agencias=null;
              set w_i_pessoas_contas=null;
              set w_forma_pagto='D'
			elseif w_InPagamentoLiquido = 'C' then
              if(w_NrAgenciaCheque != 0) and(w_NrBancoCheque != 0) then
                set w_i_bancos=w_NrBancoCheque;
                set w_i_agencias=w_NrAgenciaCheque;
                set w_i_pessoas_contas=null;
                set w_forma_pagto='C'
              else
                set w_i_bancos=null;
                set w_i_agencias=null;
                set w_i_pessoas_contas=null;
                set w_forma_pagto='D'
              end if
            elseif w_InPagamentoLiquido = 'D' then
              set w_i_bancos=null;
              set w_i_agencias=null;
              set w_i_pessoas_contas=null;
              set w_forma_pagto='D'
            end if;

			select first depois_1 into w_i_horarios 
            from tecbth_delivery.antes_depois
            where tipo = 'H' 
              and antes_1 = w_i_entidades 
              and antes_2 = w_cdHorario;
			
            set w_func_princ=null;
            set w_i_agentes_nocivos=null;
            set w_ocorrencia_sefip=1;
            set w_multiplic=1.0;
			
            if not exists(select 1 from bethadba.agencias where i_bancos = w_i_bancos and i_agencias = w_i_agencias) then
              set w_i_bancos=null;
              set w_i_agencias=null;
              set w_i_pessoas_contas=null;
              set w_forma_pagto='D'
            end if;

            set w_num_quadro_cp=null;
            if trim(w_SqCartaoPonto) in('0','') then
              set w_num_cp=null;
            else
              set w_num_cp=trim(w_SqCartaoPonto);
            end if;
			--BUG BTHSC-7952 Vincular o Plano de Previdência dos funcionários ocupantes do Fundo de Previdência
            if w_i_planos_previd is not null then
              insert into bethadba.hist_planos_previd_func (i_entidades,i_funcionarios,i_planos_previd,dt_inicial,dt_final,matricula) on existing skip
              values (w_i_entidades,w_i_funcionarios,w_i_planos_previd,'2023-01-01',null,null)
            end if;

			if w_dt_alteracoes is not null then
				if exists(select 1 from bethadba.funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios) then
					if not exists(select 1 from bethadba.hist_funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios) then
						message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Dt.: '||w_dt_alteracoes to client;
						message '1' to client;
						insert into bethadba.hist_funcionarios(i_entidades,i_funcionarios,dt_alteracoes,i_config_organ,i_organogramas,i_grupos,i_vinculos,i_pessoas,i_bancos,i_agencias,
															   i_pessoas_contas,i_horarios,func_princ,i_agentes_nocivos,optante_fgts,prev_federal,prev_estadual,fundo_ass,fundo_prev,
															   ocorrencia_sefip,forma_pagto,multiplic,fundo_financ,categoria)on existing skip
  					    values(w_i_entidades,w_i_funcionarios,w_dt_alteracoes,w_i_config_organ,w_i_organogramas,w_i_grupos,w_i_vinculos,w_i_pessoas,w_i_bancos,w_i_agencias,w_i_pessoas_contas,
							   w_i_horarios,w_func_princ,w_i_agentes_nocivos,w_optante_fgts,w_prev_federal,w_prev_estadual,w_fundo_ass,w_fundo_prev,w_ocorrencia_sefip,w_forma_pagto,w_multiplic,'N','M');
					
					else
                      if (w_dtRescisao is null) or (date(w_dt_alteracoes) < w_dtRescisao) then
                        set w_dt_alteracoes = dateadd(HOUR, 1, DATE(w_dt_alteracoes));
	                    message '1DtAlt.: ' || w_dt_alteracoes to client;
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
                          message '2DtAlt.: ' || w_dt_alteracoes to client;
                        end if; 
                      else
                        set w_dt_alteracoes = dateadd(HOUR, 1, DATE(w_dtRescisao));
                        message '5DtAlt.: ' || w_dt_alteracoes to client;
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
                           message '6DtAlt.: ' || w_dt_alteracoes to client;
                        end if;
                      end if;
                      message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Dt.: '||w_dt_alteracoes to client;	
                      message '2' to client;
                      insert into bethadba.hist_funcionarios(i_entidades,i_funcionarios,dt_alteracoes,i_config_organ,i_organogramas,i_grupos,i_vinculos,i_pessoas,i_bancos,i_agencias,
                                                             i_pessoas_contas,i_horarios,func_princ,i_agentes_nocivos,optante_fgts,prev_federal,prev_estadual,fundo_ass,fundo_prev,
                                                             ocorrencia_sefip,forma_pagto,multiplic,fundo_financ,categoria)on existing skip 
                      values(w_i_entidades,w_i_funcionarios,w_dt_alteracoes,w_i_config_organ,w_i_organogramas,w_i_grupos,w_i_vinculos,w_i_pessoas,w_i_bancos,w_i_agencias,w_i_pessoas_contas,
                      w_i_horarios,w_func_princ,w_i_agentes_nocivos,w_optante_fgts,w_prev_federal,w_prev_estadual,w_fundo_ass,w_fundo_prev,w_ocorrencia_sefip,w_forma_pagto,w_multiplic,'N','M'); 
					end if;
					
					if not exists(select 1 from bethadba.locais_mov where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_inicial = date(w_dt_alteracoes)) 
								  and(w_cdLocalTransf != 0) then
						message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Loc.: '||w_cdLocalTransf||' Hr.: '||w_i_horarios to client;
						insert into bethadba.locais_mov(i_entidades,i_funcionarios,i_locais_trab,dt_inicial,dt_final,i_horarios)on existing skip
						values(w_i_entidades,w_i_funcionarios,w_cdLocalTransf,date(w_dt_alteracoes),null,w_i_horarios);
					else
                      if exists(select 1 from bethadba.locais_mov where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios 
                                and dt_inicial = (select max(dt_inicial) 
                                                  from bethadba.locais_mov 
                                                  where i_entidades = w_i_entidades 
                                                    and i_funcionarios = w_i_funcionarios) 
                                and i_locais_trab != w_cdLocalTransf) and(w_cdLocalTransf != 0) then
                        update bethadba.locais_mov 
                          set dt_final = w_dt_alteracoes 
                        where i_entidades = w_i_entidades 
                          and i_funcionarios = w_i_funcionarios 
                          and dt_inicial = (select max(dt_inicial) 
                                            from bethadba.locais_mov 
                                            where i_entidades = w_i_entidades 
                                              and i_funcionarios = w_i_funcionarios);
						message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Loc.: '||w_cdLocalTransf||' Hr.: '||w_i_horarios to client;	
						insert into bethadba.locais_mov(i_entidades,i_funcionarios,i_locais_trab,dt_inicial,dt_final,i_horarios)on existing skip 
						values(w_i_entidades,w_i_funcionarios,w_cdLocalTransf,w_dt_alteracoes,null,w_i_horarios);
					  end if;
				    end if;
			    end if;
            end if;
			set w_i_funcionarios_aux = w_i_funcionarios;
		end loop L_item;
	close cur_conver;

	// **** Segundo Cursor
	open cur_conver3 with hold;
		set w_number=0;
		L_item3: loop
			fetch next cur_conver3 into w_i_entidades,w_cdMatricula,w_SqContrato,w_DtAdmissao,w_data,w_cdVinculoEmpregaticio, w_dtRescisao;
			
			if sqlstate = '02000' then
				leave L_item3
			end if;
		
			set w_i_funcionarios=w_cdMatricula;
			
			if w_i_funcionarios_aux != w_i_funcionarios then
				set w_number=0;
			end if;
			
			set w_number=w_number+1;
			set w_dt_alteracoes=hours(date(w_data),1);
			
			if w_number = 1 then
              set w_dt_alteracoes = hours(w_dtAdmissao, 0);
			else
              if date(w_dt_alteracoes) < w_dtAdmissao then
                set w_dt_alteracoes = dateadd(HOUR, 1, (select max(dt_alteracoes) from bethadba.hist_funcionarios
                                                        where i_entidades = w_i_entidades
                                                          and i_funcionarios = w_i_funcionarios
                                                          and date(dt_alteracoes) = w_dtAdmissao));
              else
                if date(w_dt_alteracoes) >= w_DtAdmissao then
                  set w_dt_alteracoes = dateadd(HOUR, 1, (select max(dt_alteracoes) from bethadba.hist_funcionarios
                                                          where i_entidades = w_i_entidades
                                                            and i_funcionarios = w_i_funcionarios
                                                            and date(dt_alteracoes) = w_dtRescisao));
                else
                  set w_dt_alteracoes=hours(date(w_data),1);
                end if;
              end if
            end if;

            if w_inContribuiFundoPrev in(-1) then
              set w_fundo_prev='S';
              set w_prev_federal='N';
            else
              set w_prev_federal='S';
              set w_fundo_prev='N';
            end if;
            set w_prev_estadual ='N';
            set w_fundo_ass ='N';

		if not exists(select 1 from bethadba.funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios) then
			if not exists(select 1 from bethadba.hist_funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios) then
				message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Dt.: '||w_dt_alteracoes to client;	
                message '3' to client;
				insert into bethadba.hist_funcionarios(i_entidades,i_funcionarios,dt_alteracoes,i_config_organ,i_organogramas,i_grupos,i_vinculos,i_pessoas,i_bancos,i_agencias,
													   i_pessoas_contas,i_horarios,func_princ,i_agentes_nocivos,optante_fgts,prev_federal,prev_estadual,fundo_ass,fundo_prev,ocorrencia_sefip,
													   forma_pagto,multiplic,fundo_financ,categoria) 
				values (w_i_entidades,w_i_funcionarios,w_dt_alteracoes,w_i_config_organ,w_i_organogramas,w_i_grupos,w_i_vinculos,w_i_pessoas,w_i_bancos,w_i_agencias,w_i_pessoas_contas,
						w_i_horarios,w_func_princ,w_i_agentes_nocivos,w_optante_fgts,w_prev_federal,w_prev_estadual,w_fundo_ass,w_fundo_prev,w_ocorrencia_sefip,w_forma_pagto,w_multiplic,'N','M') 
			elseif exists(select 1 from bethadba.hist_funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios 
                          and dt_alteracoes = (select max(dt_alteracoes) 
                                               from bethadba.hist_funcionarios 
                                               where i_entidades = w_i_entidades 
                                               and i_funcionarios = w_i_funcionarios) 
                          and i_vinculos != w_i_vinculos) then
				if exists(select 1 from bethadba.hist_funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) then
					update bethadba.hist_funcionarios 
					set i_vinculos = w_i_vinculos 
					where i_entidades = w_i_entidades 
					and i_funcionarios = w_i_funcionarios 
					and dt_alteracoes = w_dt_alteracoes
				else
					select first i_config_organ,i_organogramas,i_grupos,i_pessoas,i_bancos,i_agencias,i_pessoas_contas,i_horarios,func_princ,i_agentes_nocivos,optante_fgts,prev_federal,
								 prev_estadual,fundo_ass,fundo_prev,ocorrencia_sefip,forma_pagto,multiplic 
					into w_i_config_organ,w_i_organogramas,w_i_grupos,w_i_pessoas,w_i_bancos,w_i_agencias,w_i_pessoas_contas,w_i_horarios,w_func_princ,w_i_agentes_nocivos,w_optante_fgts,
					     w_prev_federal,w_prev_estadual,w_fundo_ass,w_fundo_prev,w_ocorrencia_sefip,w_forma_pagto,w_multiplic 
					from bethadba.hist_funcionarios 
					where i_entidades = w_i_entidades 
					and i_funcionarios = w_i_funcionarios 
					and dt_alteracoes < w_dt_alteracoes 
					order by dt_alteracoes desc;
					
					message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Dt.: '||w_dt_alteracoes to client;						
                    message '4' to client;
					insert into bethadba.hist_funcionarios(i_entidades,i_funcionarios,dt_alteracoes,i_config_organ,i_organogramas,i_grupos,i_vinculos,i_pessoas,i_bancos,i_agencias,
														   i_pessoas_contas,i_horarios,func_princ,i_agentes_nocivos,optante_fgts,prev_federal,prev_estadual,fundo_ass,fundo_prev,
														   ocorrencia_sefip,forma_pagto,multiplic,fundo_financ,categoria) 
					values (w_i_entidades,w_i_funcionarios,w_dt_alteracoes,w_i_config_organ,w_i_organogramas,w_i_grupos,w_i_vinculos,w_i_pessoas,w_i_bancos,w_i_agencias,w_i_pessoas_contas,
					        w_i_horarios,w_func_princ,w_i_agentes_nocivos,w_optante_fgts,w_prev_federal,w_prev_estadual,w_fundo_ass,w_fundo_prev,w_ocorrencia_sefip,w_forma_pagto,w_multiplic,'N','M');
				
					update bethadba.hist_funcionarios 
					set i_vinculos = w_i_vinculos 
					where i_entidades = w_i_entidades 
					and i_funcionarios = w_i_funcionarios 
					and dt_alteracoes >= w_dt_alteracoes
				end if
			end if
		end if;
		set w_i_funcionarios_aux=w_i_funcionarios;
	end loop L_item3;
	close cur_conver3
end;

insert into bethadba.horarios_ponto (i_entidades,i_horarios_ponto,descricao,classificacao,tipo,minima_hora,jornada_diaria,tolerancia_alocacao,enviar_esocial)
select 1,i_horarios,descricao,'N','F',entrada,1,1,'S'
 from bethadba.horarios