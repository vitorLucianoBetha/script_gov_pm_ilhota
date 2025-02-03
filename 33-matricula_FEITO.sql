ROLLBACK;
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;


if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_funcionarios') then
	drop procedure cnv_funcionarios;
end if
;

begin
	// **** Funcionarios
	declare w_i_funcionarios integer;
	declare w_dv tinyint;
	declare w_i_pessoas integer;
	declare w_tipo_admissao char(1);
	declare w_conta_fgts char(12);
	declare w_contrib_sindical char(1);
	declare w_i_sindicatos integer;
	declare w_tipo_func char(1);
	
	// **** Pessoas Contas
	declare w_i_pessoas_contas integer;
	declare w_num_conta char(15);
	declare w_tipo_conta char(1);
	declare w_status char(1);
	declare w_provimento integer;
	declare w_conselheiro char(1);
	
	ooLoop: for oo as cnv_funcionarios dynamic scroll cursor for
		select 1 as w_i_entidades,
			cdMatricula as w_cdMatricula,
			SqContrato as w_SqContrato,
			CdPessoa as w_CdPessoa,
			date(DtAdmissao) as w_dt_admissao,
			cdAdmissao as w_cdAdmissao,
			tpPagamento as w_categoria,
			date(dtOpcaoFgts) as w_dt_opcao_fgts,
			nrContaFgts as w_nrContaFgts,
			InContribuicaoSindical as w_InContribuicaoSindical,
			SqCartaoPonto as w_SqCartaoPonto,
			upper(inRais) as w_sai_rais,
			fu_tirachars(fu_tirachars(fu_tirachars(nrContaCorrente,'X'),'.'),',') as w_nrContaCorrente,
			dgContaCorrente as w_dgContaCorrente,
			NrBancoContaCorrente as w_i_bancos,
			NrAgenciaContaCorrente as w_i_agencias,
			cdTipoContaCorrente as w_cdTipoContaCorrente,
			NrFichaRegistro as w_NrFichaRegistro,
			dtFimContrato as w_dtFimContrato,
			inConcurso as w_inConcurso,
			dtConcurso as w_dtConcurso,
			CdSindicato as w_cdSindicato,
			DtPosseCargo as w_DtPosseCargo,
			nrConcurso as w_nrConcurso,
			inSituacaoFuncional as w_inSituacaoFuncional,
			date(dtAdmissao) as w_dt_base,
			cdvinculoEmpregaticio as vinculos,
			DtPosseCargo as w_dt_posse,
			CD_MATRICULA_ESOCIAL AS w_esocial,
			cdcargo as w_cargos
		from  tecbth_delivery.gp001_funcionario 
		order by 1,2,4 asc
	do



	
		// *****  Inicializa Variaveis
		set w_i_funcionarios=null;
		set w_dv=null;
		set w_i_pessoas=null;
		set w_conselheiro='N';
		
		// *****  Converte tabela bethadba.funcionarios
		-- BUG BTHSC-6292
		--bug BTHSC-7932
		set w_i_funcionarios=cast(w_cdMatricula as integer);
		--BUG BTHSC-6292
		set w_dv=bethadba.dbf_calcmod11(w_i_funcionarios);
		
		select first depois_1 
		into w_i_pessoas 
	    from tecbth_delivery.antes_depois 
		where tipo = 'P' 
		and	antes_1 = w_i_entidades 
		and antes_2 = w_cdPessoa
		and w_cdPessoa != 0;
		-- BUG BTHSC-7980 Migrar informação do Tipo de Provimento no cadastro do matriculas
select first 
provimento = ( case  when CdTpProvimento = '85100' then 1 
                     when CdTpProvimento = '85200' then 2
                     when CdTpProvimento = '85990' then 2 
                     when CdTpProvimento = '85700' then 2 
                     when CdTpProvimento = '0' then 1
else CdTpProvimento
end) into w_provimento
from gp001_CARGO  where cdcargo = w_cargos;
		if w_cdAdmissao = 3 then
			set w_tipo_admissao=5
		elseif w_cdAdmissao = 4 then
			set w_tipo_admissao=6
		elseif w_cdAdmissao = 5 then
			set w_tipo_admissao=4
		elseif w_cdAdmissao in(6) then
			set w_tipo_admissao=5
		elseif w_cdAdmissao = 0 then
			set w_tipo_admissao=1
        else
			set w_tipo_admissao= 1
		end if;
		
		if w_nrContaFgts = 0 then
			set w_conta_fgts=null
		else
			set w_conta_fgts=w_nrContaFgts
		end if;
		
		if w_InContribuicaoSindical = 'X' then
			set w_contrib_sindical='N'
		else
			set w_contrib_sindical=upper(w_InContribuicaoSindical)
		end if;
		
		if w_contrib_sindical = 'S' then
			select first depois_1 
			into w_i_sindicatos 
			from tecbth_delivery.antes_depois 
			where tipo = 'S'
		else
			set w_i_sindicatos=null
		end if;
		
		if w_inSituacaoFuncional = 1 then
			set w_tipo_func='F'
		elseif w_inSituacaoFuncional = 2 then
			set w_tipo_func='F'
		elseif w_inSituacaoFuncional = 3 then
			set w_tipo_func='B'
		elseif w_inSituacaoFuncional = 9 then
			set w_tipo_func='F'
		end if;
		--BUG BTHSC-8011 Não migrou registros de autonomos
		if vinculos in (17, 52, 51) then 
		set w_tipo_func='A'
		end if;
		-- BTHSC-142741 Bug em Matrículas | Conselheiro Tutelar
		if vinculos in (25) then 
			set w_tipo_func='A'
			set w_conselheiro = 'S'
		end if;
		--bug BTHSC-8074 Data da Posse
		if w_dt_base is null then
		set w_dt_base = w_dt_admissao
		end if;
		
		
		if w_i_pessoas != 0 then
			message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Pes.: '||w_i_pessoas||' Adm.: '||w_dt_admissao to client;
			insert into bethadba.funcionarios(i_entidades,i_funcionarios,dv,i_pessoas,dt_admissao,tipo_admissao,categoria,dt_opcao_fgts,conta_fgts,dt_base,contrib_sindical,i_sindicatos,conta_vaga,
											sai_rais,tipo_func,tipo_pens,conta_adicional,conta_licpremio,conta_temposerv,lei_contrato,codigo_esocial,tipo_provimento,conselheiro_tutelar)on existing skip
			values (w_i_entidades,w_i_funcionarios,w_dv,w_i_pessoas,w_dt_admissao,w_tipo_admissao,w_categoria,w_dt_opcao_fgts,w_conta_fgts,w_dt_base,w_contrib_sindical,w_i_sindicatos,'S',
					w_sai_rais,w_tipo_func,null,'N','N','N',null,w_esocial,w_provimento,w_conselheiro);
			
			// *****  Converte tabela bethadba.pessoas_contas
			set w_i_pessoas_contas=null;
			
			if(trim(w_nrContaCorrente) != '999999999999') and (trim(w_nrContaCorrente) != '99999999999') and (trim(w_nrContaCorrente) != '999999999')  then
				if(cast(trim(fu_tirachars(w_NrContaCorrente,'.-')) as decimal(15)) != 0) then				
					/*if w_i_bancos in(1,757,758,761,762,763,764) then
						set w_i_bancos = 1
					elseif w_i_bancos in(27) then
						set w_i_bancos = 27
					elseif w_i_bancos in(104) then
						set w_i_bancos = 104
					elseif w_i_bancos in(237) then
						set w_i_bancos = 237			
					elseif w_i_bancos in(756,765) then
						set w_i_bancos = 756		
					elseif w_i_bancos in(759,760) then
						set w_i_bancos = 800		
					end if;*/
                    
				    if not exists (select 1 from bethadba.bancos where i_bancos = w_i_bancos) then
                      set w_i_bancos = 800	
                    end if;
					set w_num_conta=string(cast(trim(fu_tirachars(fu_tirachars(fu_tirachars(w_NrContaCorrente,'.'),' .-'),'X')) as decimal(15)))+string(trim(w_dgContaCorrente));
					
					select coalesce((max(i_pessoas_contas)+1),1) 
					into w_i_pessoas_contas 
					from bethadba.pessoas_contas 
					where i_pessoas = w_i_pessoas;
					
					if w_cdTipoContaCorrente in(0,3) then
						set w_tipo_conta='1'
					elseif w_cdTipoContaCorrente in(2,13) then
						set w_tipo_conta='2'
					elseif w_cdTipoContaCorrente in(23) then
						set w_tipo_conta='3'
					else
						set w_tipo_conta='1'
					end if;
					
					set w_status='A'
				else
					set w_i_bancos=null;
					set w_i_agencias=null;
					set w_i_pessoas_contas=null;
					set w_tipo_conta=null;
					set w_status=null
				end if
			else
				set w_i_bancos=null;
				set w_i_agencias=null;
				set w_i_pessoas_contas=null;
				set w_tipo_conta=null;
				set w_status=null
			end if;
        if w_i_agencias is not null and  w_i_agencias <> 0 then  
         if not exists (select 1 from bethadba.agencias where i_agencias = w_i_agencias  and i_bancos = w_i_bancos) then
           insert into bethadba.agencias (i_bancos,i_agencias,dv_agencia,nome,qtde_zeros) values (w_i_bancos,w_i_agencias,bethadba.dbf_calcmod11(w_i_agencias),'Agencia: '+string(w_i_agencias),0)
          end if
         end if;  
          message 'Ban: '||w_i_bancos||' Age.: '||w_i_agencias||' Num. Con.: '||w_num_conta to client; 
			if(w_i_bancos is not null) and (w_i_agencias != 0) then
				if not exists (select 1 from bethadba.pessoas_contas where i_pessoas = w_i_pessoas and i_bancos = w_i_bancos and i_agencias = w_i_agencias and num_conta = w_num_conta and tipo_conta = w_tipo_conta) then
				
					message 'Pes.: '||w_i_pessoas||' Pes. Con.: '||w_i_pessoas_contas||' Ban: '||w_i_bancos||' Age.: '||w_i_agencias||' Num. Con.: '||w_num_conta to client; 
					insert into bethadba.pessoas_contas(i_pessoas,i_pessoas_contas,i_bancos,i_agencias,num_conta,tipo_conta,status)on existing skip
					values (w_i_pessoas,w_i_pessoas_contas,w_i_bancos,w_i_agencias,w_num_conta,w_tipo_conta,w_status);
				end if;
			end if;
		end if;
	end for;
end; 


-- BTHSC-57351
--update bethadba.funcionarios set tipo_func = 'F' where tipo_func = 'A'

-- BTHSC-136973 Dados Bancários

ROLLBACK;
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;


if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_contas') then
	drop procedure cnv_contas;
end if
;

begin
	// **** Funcionarios
	declare w_i_funcionarios integer;
	declare w_dv tinyint;
	declare w_i_pessoas integer;
	declare w_tipo_admissao char(1);
	declare w_conta_fgts char(12);
	declare w_contrib_sindical char(1);
	declare w_i_sindicatos integer;
	declare w_tipo_func char(1);
	
	// **** Pessoas Contas
	declare w_i_pessoas_contas integer;
	declare w_num_conta char(15);
	declare w_tipo_conta char(1);
	declare w_status char(1);
	declare w_provimento integer;
	declare w_conselheiro char(1);
	
	ooLoop: for oo as cnv_contas dynamic scroll cursor for
		select distinct 1 as w_i_entidades,
			HC.CdMatricula as w_cdMatricula, 
			F.CdPessoa as w_CdPessoa,
			HC.NrBancoCC as w_i_bancos, 
			HC.NrAgenciaCC as w_i_agencias,
			tecbth_delivery.fu_tirachars(tecbth_delivery.fu_tirachars(tecbth_delivery.fu_tirachars(HC.nrContaCorrente,'X'),'.'),',') as w_nrContaCorrente,
			HC.DgContaCorrente as w_dgContaCorrente,
			HC.TpContaCC as w_cdTipoContaCorrente
		from tecbth_delivery.gp001_HISTORICOBANCARIO HC
		join tecbth_delivery.gp001_FUNCIONARIO F on HC.CdMatricula = F.cdMatricula and HC.SqContrato = F.SqContrato
		where (HC.NrContaCorrente <> '' and HC.NrContaCorrente is not null)
		and w_CdPessoa = 511
		order by HC.CdMatricula asc
	do

		// *****  Inicializa Variaveis
		set w_i_funcionarios=null;
		set w_dv=null;
		set w_i_pessoas=null;
		set w_conselheiro='N';
		
		// *****  Converte tabela bethadba.funcionarios
		-- BUG BTHSC-6292
		--bug BTHSC-7932
		set w_i_funcionarios=cast(w_cdMatricula as integer);
		--BUG BTHSC-6292
		set w_dv=bethadba.dbf_calcmod11(w_i_funcionarios);
		
		select first depois_1 
		into w_i_pessoas 
	    from tecbth_delivery.antes_depois 
		where tipo = 'P' 
		and	antes_1 = w_i_entidades 
		and antes_2 = w_cdPessoa
		and w_cdPessoa != 0;
		
		
		if w_i_pessoas != 0 then	
			// *****  Converte tabela bethadba.pessoas_contas
			set w_i_pessoas_contas=null;
			
			if(trim(w_nrContaCorrente) != '999999999999') and (trim(w_nrContaCorrente) != '99999999999') and (trim(w_nrContaCorrente) != '999999999')  then
				if(cast(trim(tecbth_delivery.fu_tirachars(w_NrContaCorrente,'.-')) as decimal(15)) != 0) then				
					/*if w_i_bancos in(1,757,758,761,762,763,764) then
						set w_i_bancos = 1
					elseif w_i_bancos in(27) then
						set w_i_bancos = 27
					elseif w_i_bancos in(104) then
						set w_i_bancos = 104
					elseif w_i_bancos in(237) then
						set w_i_bancos = 237			
					elseif w_i_bancos in(756,765) then
						set w_i_bancos = 756		
					elseif w_i_bancos in(759,760) then
						set w_i_bancos = 800		
					end if;*/
                    
				    if not exists (select 1 from bethadba.bancos where i_bancos = w_i_bancos) then
                      set w_i_bancos = 800	
                    end if;
					set w_num_conta=string(cast(trim(tecbth_delivery.fu_tirachars(tecbth_delivery.fu_tirachars(tecbth_delivery.fu_tirachars(w_NrContaCorrente,'.'),' .-'),'X')) as decimal(15)))+string(trim(w_dgContaCorrente));
					
					select coalesce((max(i_pessoas_contas)+1),1) 
					into w_i_pessoas_contas 
					from bethadba.pessoas_contas 
					where i_pessoas = w_i_pessoas;
					
					if w_cdTipoContaCorrente in(0,3) then
						set w_tipo_conta='1'
					elseif w_cdTipoContaCorrente in(2,13) then
						set w_tipo_conta='2'
					elseif w_cdTipoContaCorrente in(23) then
						set w_tipo_conta='3'
					else
						set w_tipo_conta='1'
					end if;
					
					set w_status='A'
				else
					set w_i_bancos=null;
					set w_i_agencias=null;
					set w_i_pessoas_contas=null;
					set w_tipo_conta=null;
					set w_status=null
				end if
			else
				set w_i_bancos=null;
				set w_i_agencias=null;
				set w_i_pessoas_contas=null;
				set w_tipo_conta=null;
				set w_status=null
			end if;
        if w_i_agencias is not null and  w_i_agencias <> 0 then  
         if not exists (select 1 from bethadba.agencias where i_agencias = w_i_agencias  and i_bancos = w_i_bancos) then
           insert into bethadba.agencias (i_bancos,i_agencias,dv_agencia,nome,qtde_zeros) values (w_i_bancos,w_i_agencias,bethadba.dbf_calcmod11(w_i_agencias),'Agencia: '+string(w_i_agencias),0)
          end if
         end if;  
          message 'Ban: '||w_i_bancos||' Age.: '||w_i_agencias||' Num. Con.: '||w_num_conta to client; 
			if(w_i_bancos is not null) and (w_i_agencias != 0) then
				if not exists (select 1 from bethadba.pessoas_contas where i_pessoas = w_i_pessoas and i_bancos = w_i_bancos and i_agencias = w_i_agencias and num_conta = w_num_conta and tipo_conta = w_tipo_conta) then
				
					message 'Pes.: '||w_i_pessoas||' Pes. Con.: '||w_i_pessoas_contas||' Ban: '||w_i_bancos||' Age.: '||w_i_agencias||' Num. Con.: '||w_num_conta to client; 
					insert into bethadba.pessoas_contas(i_pessoas,i_pessoas_contas,i_bancos,i_agencias,num_conta,tipo_conta,status)on existing skip
					values (w_i_pessoas,w_i_pessoas_contas,w_i_bancos,w_i_agencias,w_num_conta,w_tipo_conta,w_status);
				end if;
			end if;
		end if;
	end for;
end; 