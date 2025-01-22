/* ==================================== SEGUIR TODOS OS SQLS PARA INSERÇÃO DE BENEFICIARIOS E PENSÕES ============================*/
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;
--------------------------------------------------
-- 31) Dependentes
--------------------------------------------------
if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_dependentes') then
	drop procedure cnv_dependentes;
end if
;
-- BUG BTHSC-7421  as datas finais relacionadas aos dependentes estão incorretas
begin
	// *****  Tabela bethadba.dependentes
	declare w_i_pessoas integer;
	declare w_i_dependentes integer;
	declare w_grau char(1);
	declare w_dt_casamento date;
	ooLoop: for oo as cnv_dependentes dynamic scroll cursor for 
			
select distinct 1 as w_i_entidades,d.cdPessoa as w_CdPessoa,d.cdDependente as w_cdDependente,d.CdPessoaDependente as w_CdPessoaDependente,d.cdGrauDependencia as w_CdGrauDependencia,
				   d.CdMotivoInicioRelacao as w_CdMotivoInicioRelacao,date(d.DtInicioRelacao) as w_DtInicioRelacao,dtsuspensao as dt_final
			FROM tecbth_delivery.GP001_PESSOA p
INNER JOIN tecbth_delivery.GP001_DEPENDENTE d ON d.cdPessoaDependente = p.CdPessoa
INNER JOIN tecbth_delivery.gp001_FUNCIONARIO f ON f.CdPessoa = d.CdPessoa
LEFT JOIN tecbth_delivery.gp001_DEPENDENTEVERBA dv ON f.cdMatricula = dv.cdMatricula
LEFT JOIN tecbth_delivery.gp001_VERBA v ON dv.CdVerba = v.CdVerba
WHERE d.CdPessoa IS NOT NULL
AND d.CdDependente = dv.CdDependente
			--and d.cdPessoa =14
			order by 1,2 asc
	do
		// *****  Inicializa Variaveis
		set w_i_pessoas=null;
		set w_i_dependentes=null;
		set w_grau=null;
		set w_dt_casamento=null;
		
		// *****  Converte tabela bethadba.dependentes
		select depois_1 
		into w_i_pessoas 
		from tecbth_delivery.antes_depois 
		where tipo = 'P' 
		and antes_1 = w_i_entidades 
		and antes_2 = w_CdPessoa;
		
		select depois_1 
		into w_i_dependentes 
		from tecbth_delivery.antes_depois 
		where tipo = 'P' 
		and antes_1 = w_i_entidades 
		and	antes_2 = w_CdPessoaDependente;

		if w_cdGrauDependencia = 1 then
			set w_grau=1
		elseif w_cdGrauDependencia in(2) then
			set w_grau=2
		elseif w_cdGrauDependencia in(3) then
			set w_grau=3
		elseif w_cdGrauDependencia = 8 then
			set w_grau=6
		elseif w_cdGrauDependencia = 11 then
			set w_grau=7
		elseif w_cdGrauDependencia = 21 then 
			set w_grau = 1
		else
			set w_grau=9
		end if;
		
		if w_CdMotivoInicioRelacao in(7,8) then
			set w_dt_casamento=w_DtInicioRelacao
		else
			set w_dt_casamento=null
		end if;
		
		if w_i_dependentes is not null and w_i_pessoas is not null then
			if not exists(select 1 from bethadba.dependentes where i_pessoas = w_i_pessoas and i_dependentes = w_i_dependentes) then
				message 'Pes.: '||w_i_pessoas||' Dep.: '||w_i_dependentes||' Grau.: '||w_grau to client;
				
				insert into bethadba.dependentes(i_pessoas,i_dependentes,grau,dt_casamento,dt_ini_depende,dt_fin_depende)on existing skip
				values(w_i_pessoas,w_i_dependentes,w_grau,w_dt_casamento,w_DtInicioRelacao,dt_final) 
			end if
		end if;
	end for;
end;


------ BTHSC-132078 (Cadastro de matrícula de beneficiários para pensão judicial. São apenas para cadastro na tabela de beneficiários) 

INSERT INTO Folharh.bethadba.pessoas
(i_pessoas, dv, nome, nome_fantasia, tipo_pessoa, ddd, telefone, fax, ddd_cel, celular, inscricao_municipal, email, cod_unificacao, nome_social, considera_nome_social_fly)
VALUES(6159, 1, 'MARIA JULIA PLOTEGHER', NULL, 'F', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO Folharh.bethadba.pessoas
(i_pessoas, dv, nome, nome_fantasia, tipo_pessoa, ddd, telefone, fax, ddd_cel, celular, inscricao_municipal, email, cod_unificacao, nome_social, considera_nome_social_fly)
VALUES(6160, 1, 'MARIA CLARA BATISTA', NULL, 'F', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);


INSERT INTO Folharh.bethadba.pessoas
(i_pessoas, dv, nome, nome_fantasia, tipo_pessoa, ddd, telefone, fax, ddd_cel, celular, inscricao_municipal, email, cod_unificacao, nome_social, considera_nome_social_fly)
VALUES(6161, 1, 'LUÍS GUSTAVO ELIAS DEBARBA', NULL, 'F', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO Folharh.bethadba.pessoas
(i_pessoas, dv, nome, nome_fantasia, tipo_pessoa, ddd, telefone, fax, ddd_cel, celular, inscricao_municipal, email, cod_unificacao, nome_social, considera_nome_social_fly)
VALUES(6162, 1, 'DAYANE CRISTINA ROSA', NULL, 'F', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- Insere pessoas físicas
insert into bethadba.pessoas_fisicas on existing skip
select
i_pessoas,
4207106 as i_cidades,
gp001_BENEFICIARIO.DtNascimento,
TpSexo as sexo,
2 as estado_civil,
8 as grau_instrucao,
'B' as nacionalidade ,
null as rg,
null as orgao_emis_rg,
null as dt_emis_rg,
null as uf_emis_rg,
null as num_pis,
null as dt_pis,
nRcpf as cpf,
null as carteira_prof,
null as serie_cart,
null as dt_emis_carteira,
null as uf_emis_carteira,
null as inscricao_estadual,
null as rig,
null as orgao_ric,
null as dt_emissao_ric,
null as nome_social,
'N' as considera_nome_social_fly 

from tecbth_delivery.gp001_BENEFICIARIO 
left join bethadba.pessoas 
on gp001_BENEFICIARIO.nmbENEFICIARIO = pessoas.nome

INSERT INTO Folharh.bethadba.hist_pessoas_fis
(i_pessoas, dt_alteracoes, dt_nascimento, sexo, rg, orgao_emis_rg, uf_emis_rg, dt_emis_rg, cpf, num_pis, dt_pis, carteira_prof, serie_cart, uf_emis_carteira, dt_emis_carteira, zona_eleitoral, secao_eleitoral, titulo_eleitor, grau_instrucao, estado_civil, cnh, categoria_cnh, dt_vencto_cnh, dt_primeira_cnh, observacoes_cnh, dt_emissao_cnh, i_estados_cnh, ric, orgao_ric, dt_emissao_ric, raca, certidao, ddd, telefone, ddd_cel, celular, email, tipo_validacao, tipo_pessoa, ident_estrangeiro, dt_validade_est, tipo_visto_est, cart_trab_est, serie_cart_est, dt_exp_cart_est, dt_val_cart_est, i_paises, orgao_emissor_est, dt_emissao_est, i_paises_nacionalidade, data_chegada_est, ano_chegada_est, casado_brasileiro_est, filho_brasileiro_est, i_situacao_estrangeiro, residencia_fiscal_exterior, i_pais_residencia_fiscal, indicativo_nif, numero_identificacao_fiscal, forma_tributacao)
VALUES(6158, '1970-01-01', '1970-01-01', 'M', '3R846.845', 'SSP', 22, '2003-05-09', '70821968904', '12303859478', '1985-07-08', '0009280', '00011', 22, '1985-07-08', '64', '67', '28577560965', 8, '2', '0.00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2', NULL, '47', '000000000', NULL, '92155534', NULL, NULL, 'F', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, NULL, NULL, NULL);


INSERT INTO Folharh.bethadba.hist_pessoas_fis
(i_pessoas, dt_alteracoes, dt_nascimento, sexo, rg, orgao_emis_rg, uf_emis_rg, dt_emis_rg, cpf, num_pis, dt_pis, carteira_prof, serie_cart, uf_emis_carteira, dt_emis_carteira, zona_eleitoral, secao_eleitoral, titulo_eleitor, grau_instrucao, estado_civil, cnh, categoria_cnh, dt_vencto_cnh, dt_primeira_cnh, observacoes_cnh, dt_emissao_cnh, i_estados_cnh, ric, orgao_ric, dt_emissao_ric, raca, certidao, ddd, telefone, ddd_cel, celular, email, tipo_validacao, tipo_pessoa, ident_estrangeiro, dt_validade_est, tipo_visto_est, cart_trab_est, serie_cart_est, dt_exp_cart_est, dt_val_cart_est, i_paises, orgao_emissor_est, dt_emissao_est, i_paises_nacionalidade, data_chegada_est, ano_chegada_est, casado_brasileiro_est, filho_brasileiro_est, i_situacao_estrangeiro, residencia_fiscal_exterior, i_pais_residencia_fiscal, indicativo_nif, numero_identificacao_fiscal, forma_tributacao)
VALUES(6160, '2009-08-27', '2009-08-27', 'M', '3R846.845', 'SSP', 22, '2003-05-09', '70821968904', '12303859478', '1985-07-08', '0009280', '00011', 22, '1985-07-08', '64', '67', '28577560965', 8, '2', '0.00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2', NULL, '47', '000000000', NULL, '92155534', NULL, NULL, 'F', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, NULL, NULL, NULL);


INSERT INTO Folharh.bethadba.hist_pessoas_fis
(i_pessoas, dt_alteracoes, dt_nascimento, sexo, rg, orgao_emis_rg, uf_emis_rg, dt_emis_rg, cpf, num_pis, dt_pis, carteira_prof, serie_cart, uf_emis_carteira, dt_emis_carteira, zona_eleitoral, secao_eleitoral, titulo_eleitor, grau_instrucao, estado_civil, cnh, categoria_cnh, dt_vencto_cnh, dt_primeira_cnh, observacoes_cnh, dt_emissao_cnh, i_estados_cnh, ric, orgao_ric, dt_emissao_ric, raca, certidao, ddd, telefone, ddd_cel, celular, email, tipo_validacao, tipo_pessoa, ident_estrangeiro, dt_validade_est, tipo_visto_est, cart_trab_est, serie_cart_est, dt_exp_cart_est, dt_val_cart_est, i_paises, orgao_emissor_est, dt_emissao_est, i_paises_nacionalidade, data_chegada_est, ano_chegada_est, casado_brasileiro_est, filho_brasileiro_est, i_situacao_estrangeiro, residencia_fiscal_exterior, i_pais_residencia_fiscal, indicativo_nif, numero_identificacao_fiscal, forma_tributacao)
VALUES(6161, '2006-12-30', '2006-12-30', 'M', '3R846.845', 'SSP', 22, '2003-05-09', '70821968904', '12303859478', '1985-07-08', '0009280', '00011', 22, '1985-07-08', '64', '67', '28577560965', 8, '2', '0.00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2', NULL, '47', '000000000', NULL, '92155534', NULL, NULL, 'F', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, NULL, NULL, NULL);


INSERT INTO Folharh.bethadba.hist_pessoas_fis
(i_pessoas, dt_alteracoes, dt_nascimento, sexo, rg, orgao_emis_rg, uf_emis_rg, dt_emis_rg, cpf, num_pis, dt_pis, carteira_prof, serie_cart, uf_emis_carteira, dt_emis_carteira, zona_eleitoral, secao_eleitoral, titulo_eleitor, grau_instrucao, estado_civil, cnh, categoria_cnh, dt_vencto_cnh, dt_primeira_cnh, observacoes_cnh, dt_emissao_cnh, i_estados_cnh, ric, orgao_ric, dt_emissao_ric, raca, certidao, ddd, telefone, ddd_cel, celular, email, tipo_validacao, tipo_pessoa, ident_estrangeiro, dt_validade_est, tipo_visto_est, cart_trab_est, serie_cart_est, dt_exp_cart_est, dt_val_cart_est, i_paises, orgao_emissor_est, dt_emissao_est, i_paises_nacionalidade, data_chegada_est, ano_chegada_est, casado_brasileiro_est, filho_brasileiro_est, i_situacao_estrangeiro, residencia_fiscal_exterior, i_pais_residencia_fiscal, indicativo_nif, numero_identificacao_fiscal, forma_tributacao)
VALUES(6162, '1970-01-01', '1970-01-01', 'M', '3R846.845', 'SSP', 22, '2003-05-09', '70821968904', '12303859478', '1985-07-08', '0009280', '00011', 22, '1985-07-08', '64', '67', '28577560965', 8, '2', '0.00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2', NULL, '47', '000000000', NULL, '92155534', NULL, NULL, 'F', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, NULL, NULL, NULL);

update bethadba.hist_pessoas_fis set rg = null, num_pis = null, cpf = null where i_pessoas in (6158, 6161,6162,6160)


----- Cria dependentes sem dados tecbth_delivery.gp001_dependente
  
                   insert into bethadba.dependentes (i_pessoas, i_dependentes, grau, dt_ini_depende)
                   select 
                   case cdMatricula
                   when 15881 then 727
                   when 31666 then 1301
                   when 73172 then 410
                   when 43265 then 1762
                   end as i_pessoas,
                   case cdMatricula
                   when 15881 then 6158
                   when 31666 then 6162
                   when 73172 then 6161
                   when 43265 then 6160
                   end as i_dependentes,
                   case cdGrauDependencia 
                   when 31 then 12 
                   when 21 then 1
                   when 11 then 1
                   end as grau,
                   dtInclusao as dt_ini_depende 
                   from tecbth_delivery.gp001_BENEFICIARIO
                   where i_pessoas is not null;
                   
                   


-- Cadastra matrículas de pensão
INSERT INTO Folharh.bethadba.funcionarios
(i_entidades, i_funcionarios, dv, i_pessoas, dt_admissao, tipo_admissao, categoria, dt_opcao_fgts, conta_fgts, dt_base, contrib_sindical, i_sindicatos, conta_vaga, sai_rais, tipo_func, tipo_pens, conta_adicional, conta_licpremio, conta_temposerv, lei_contrato, senha_teclado, tipo_trabalhador, total_pensao, total_pensao_fgts, func_original, conta_tempocarreira, i_pessoas_juridicas, conselheiro_tutelar, i_formacoes_estagio, i_atos_estagio, periodo_estagio, fase_estagio, num_contrato_estagio, dt_prorrog_estagio, objetivo_estagio, seguro_vida_estagio, codigo_esocial, situacao_admissional, provimento, motivo_contratacao, num_processo_judicial, just_contratacao, desc_salario_variavel, tipo_inclusao, possui_clausula, inicio_abono, entidade_sucedida, codigo_esocial_matricula_anterior, dt_transferencia, observacao, enviar_esocial, recebido_cessao, responsavel_contrato, i_pessoas_ent_origem, codigo_esocial_matricula_ent_origem, registro_preliminar_esocial, categoria_origem, dt_admissao_origem, processo_judicial_admissao, tipo_provimento, vinculado_saude_seg_publica, reconhec_jud_vinc_trab, dt_admissao_retificada, tipo_regime_prev_origem, tipo_regime_trab_origem)
VALUES(1, 92801, 1, 3260, '2025-01-01', '1', 'M', NULL, NULL, '2025-01-01', 'S', 6155, 'S', 'S', 'B', '2', 'N', 'N', 'N', NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'GOVB92802', 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'S', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, NULL, NULL);




INSERT INTO Folharh.bethadba.funcionarios
(i_entidades, i_funcionarios, dv, i_pessoas, dt_admissao, tipo_admissao, categoria, dt_opcao_fgts, conta_fgts, dt_base, contrib_sindical, i_sindicatos, conta_vaga, sai_rais, tipo_func, tipo_pens, conta_adicional, conta_licpremio, conta_temposerv, lei_contrato, senha_teclado, tipo_trabalhador, total_pensao, total_pensao_fgts, func_original, conta_tempocarreira, i_pessoas_juridicas, conselheiro_tutelar, i_formacoes_estagio, i_atos_estagio, periodo_estagio, fase_estagio, num_contrato_estagio, dt_prorrog_estagio, objetivo_estagio, seguro_vida_estagio, codigo_esocial, situacao_admissional, provimento, motivo_contratacao, num_processo_judicial, just_contratacao, desc_salario_variavel, tipo_inclusao, possui_clausula, inicio_abono, entidade_sucedida, codigo_esocial_matricula_anterior, dt_transferencia, observacao, enviar_esocial, recebido_cessao, responsavel_contrato, i_pessoas_ent_origem, codigo_esocial_matricula_ent_origem, registro_preliminar_esocial, categoria_origem, dt_admissao_origem, processo_judicial_admissao, tipo_provimento, vinculado_saude_seg_publica, reconhec_jud_vinc_trab, dt_admissao_retificada, tipo_regime_prev_origem, tipo_regime_trab_origem)
VALUES(1, 92802, 1, 6162, '2025-01-01', '1', 'M', NULL, NULL, '2025-01-01', 'S', 6155, 'S', 'S', 'B', '2', 'N', 'N', 'N', NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'GOVB92802', 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'S', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, NULL, NULL);


INSERT INTO Folharh.bethadba.funcionarios
(i_entidades, i_funcionarios, dv, i_pessoas, dt_admissao, tipo_admissao, categoria, dt_opcao_fgts, conta_fgts, dt_base, contrib_sindical, i_sindicatos, conta_vaga, sai_rais, tipo_func, tipo_pens, conta_adicional, conta_licpremio, conta_temposerv, lei_contrato, senha_teclado, tipo_trabalhador, total_pensao, total_pensao_fgts, func_original, conta_tempocarreira, i_pessoas_juridicas, conselheiro_tutelar, i_formacoes_estagio, i_atos_estagio, periodo_estagio, fase_estagio, num_contrato_estagio, dt_prorrog_estagio, objetivo_estagio, seguro_vida_estagio, codigo_esocial, situacao_admissional, provimento, motivo_contratacao, num_processo_judicial, just_contratacao, desc_salario_variavel, tipo_inclusao, possui_clausula, inicio_abono, entidade_sucedida, codigo_esocial_matricula_anterior, dt_transferencia, observacao, enviar_esocial, recebido_cessao, responsavel_contrato, i_pessoas_ent_origem, codigo_esocial_matricula_ent_origem, registro_preliminar_esocial, categoria_origem, dt_admissao_origem, processo_judicial_admissao, tipo_provimento, vinculado_saude_seg_publica, reconhec_jud_vinc_trab, dt_admissao_retificada, tipo_regime_prev_origem, tipo_regime_trab_origem)
VALUES(1, 92803, 1, 6161, '2025-01-01', '1', 'M', NULL, NULL, '2025-01-01', 'S', 6155, 'S', 'S', 'B', '2', 'N', 'N', 'N', NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'GOVB92802', 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'S', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, NULL, NULL);



INSERT INTO Folharh.bethadba.funcionarios
(i_entidades, i_funcionarios, dv, i_pessoas, dt_admissao, tipo_admissao, categoria, dt_opcao_fgts, conta_fgts, dt_base, contrib_sindical, i_sindicatos, conta_vaga, sai_rais, tipo_func, tipo_pens, conta_adicional, conta_licpremio, conta_temposerv, lei_contrato, senha_teclado, tipo_trabalhador, total_pensao, total_pensao_fgts, func_original, conta_tempocarreira, i_pessoas_juridicas, conselheiro_tutelar, i_formacoes_estagio, i_atos_estagio, periodo_estagio, fase_estagio, num_contrato_estagio, dt_prorrog_estagio, objetivo_estagio, seguro_vida_estagio, codigo_esocial, situacao_admissional, provimento, motivo_contratacao, num_processo_judicial, just_contratacao, desc_salario_variavel, tipo_inclusao, possui_clausula, inicio_abono, entidade_sucedida, codigo_esocial_matricula_anterior, dt_transferencia, observacao, enviar_esocial, recebido_cessao, responsavel_contrato, i_pessoas_ent_origem, codigo_esocial_matricula_ent_origem, registro_preliminar_esocial, categoria_origem, dt_admissao_origem, processo_judicial_admissao, tipo_provimento, vinculado_saude_seg_publica, reconhec_jud_vinc_trab, dt_admissao_retificada, tipo_regime_prev_origem, tipo_regime_trab_origem)
VALUES(1, 92804, 1, 6160, '2025-01-01', '1', 'M', NULL, NULL, '2025-01-01', 'S', 6155, 'S', 'S', 'B', '2', 'N', 'N', 'N', NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'GOVB92802', 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'S', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, NULL, NULL);



INSERT INTO Folharh.bethadba.funcionarios
(i_entidades, i_funcionarios, dv, i_pessoas, dt_admissao, tipo_admissao, categoria, dt_opcao_fgts, conta_fgts, dt_base, contrib_sindical, i_sindicatos, conta_vaga, sai_rais, tipo_func, tipo_pens, conta_adicional, conta_licpremio, conta_temposerv, lei_contrato, senha_teclado, tipo_trabalhador, total_pensao, total_pensao_fgts, func_original, conta_tempocarreira, i_pessoas_juridicas, conselheiro_tutelar, i_formacoes_estagio, i_atos_estagio, periodo_estagio, fase_estagio, num_contrato_estagio, dt_prorrog_estagio, objetivo_estagio, seguro_vida_estagio, codigo_esocial, situacao_admissional, provimento, motivo_contratacao, num_processo_judicial, just_contratacao, desc_salario_variavel, tipo_inclusao, possui_clausula, inicio_abono, entidade_sucedida, codigo_esocial_matricula_anterior, dt_transferencia, observacao, enviar_esocial, recebido_cessao, responsavel_contrato, i_pessoas_ent_origem, codigo_esocial_matricula_ent_origem, registro_preliminar_esocial, categoria_origem, dt_admissao_origem, processo_judicial_admissao, tipo_provimento, vinculado_saude_seg_publica, reconhec_jud_vinc_trab, dt_admissao_retificada, tipo_regime_prev_origem, tipo_regime_trab_origem)
VALUES(1, 92805, 1, 6158, '2025-01-01', '1', 'M', NULL, NULL, '2025-01-01', 'S', 6155, 'S', 'S', 'B', '2', 'N', 'N', 'N', NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'GOVB92802', 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'S', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, NULL, NULL);





INSERT INTO Folharh.bethadba.funcionarios
(i_entidades, i_funcionarios, dv, i_pessoas, dt_admissao, tipo_admissao, categoria, dt_opcao_fgts, conta_fgts, dt_base, contrib_sindical, i_sindicatos, conta_vaga, sai_rais, tipo_func, tipo_pens, conta_adicional, conta_licpremio, conta_temposerv, lei_contrato, senha_teclado, tipo_trabalhador, total_pensao, total_pensao_fgts, func_original, conta_tempocarreira, i_pessoas_juridicas, conselheiro_tutelar, i_formacoes_estagio, i_atos_estagio, periodo_estagio, fase_estagio, num_contrato_estagio, dt_prorrog_estagio, objetivo_estagio, seguro_vida_estagio, codigo_esocial, situacao_admissional, provimento, motivo_contratacao, num_processo_judicial, just_contratacao, desc_salario_variavel, tipo_inclusao, possui_clausula, inicio_abono, entidade_sucedida, codigo_esocial_matricula_anterior, dt_transferencia, observacao, enviar_esocial, recebido_cessao, responsavel_contrato, i_pessoas_ent_origem, codigo_esocial_matricula_ent_origem, registro_preliminar_esocial, categoria_origem, dt_admissao_origem, processo_judicial_admissao, tipo_provimento, vinculado_saude_seg_publica, reconhec_jud_vinc_trab, dt_admissao_retificada, tipo_regime_prev_origem, tipo_regime_trab_origem)
VALUES(1, 92806, 1, 3617, '2025-01-01', '1', 'M', NULL, NULL, '2025-01-01', 'S', 6155, 'S', 'S', 'B', '2', 'N', 'N', 'N', NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'GOVB92802', 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'S', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, NULL, NULL);



INSERT INTO Folharh.bethadba.funcionarios
(i_entidades, i_funcionarios, dv, i_pessoas, dt_admissao, tipo_admissao, categoria, dt_opcao_fgts, conta_fgts, dt_base, contrib_sindical, i_sindicatos, conta_vaga, sai_rais, tipo_func, tipo_pens, conta_adicional, conta_licpremio, conta_temposerv, lei_contrato, senha_teclado, tipo_trabalhador, total_pensao, total_pensao_fgts, func_original, conta_tempocarreira, i_pessoas_juridicas, conselheiro_tutelar, i_formacoes_estagio, i_atos_estagio, periodo_estagio, fase_estagio, num_contrato_estagio, dt_prorrog_estagio, objetivo_estagio, seguro_vida_estagio, codigo_esocial, situacao_admissional, provimento, motivo_contratacao, num_processo_judicial, just_contratacao, desc_salario_variavel, tipo_inclusao, possui_clausula, inicio_abono, entidade_sucedida, codigo_esocial_matricula_anterior, dt_transferencia, observacao, enviar_esocial, recebido_cessao, responsavel_contrato, i_pessoas_ent_origem, codigo_esocial_matricula_ent_origem, registro_preliminar_esocial, categoria_origem, dt_admissao_origem, processo_judicial_admissao, tipo_provimento, vinculado_saude_seg_publica, reconhec_jud_vinc_trab, dt_admissao_retificada, tipo_regime_prev_origem, tipo_regime_trab_origem)
VALUES(1, 92807, 1, 5934, '2025-01-01', '1', 'M', NULL, NULL, '2025-01-01', 'S', 6155, 'S', 'S', 'B', '2', 'N', 'N', 'N', NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'GOVB92802', 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'S', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, NULL, NULL);

------------------------------------- CADASTRA HISTÓRICO DE FUNCIONÁRIOS

INSERT INTO Folharh.bethadba.hist_funcionarios
(i_entidades, i_funcionarios, dt_alteracoes, i_config_organ, i_organogramas, i_grupos, i_vinculos, i_pessoas, i_bancos, i_agencias, i_pessoas_contas, i_horarios, func_princ, i_agentes_nocivos, optante_fgts, prev_federal, prev_estadual, fundo_ass, fundo_prev, ocorrencia_sefip, forma_pagto, multiplic, i_turmas, num_quadro_cp, num_cp, provisorio, bate_cartao, tipo_contrato, i_responsaveis, fundo_financ, i_pessoas_estagio, dt_final_estagio, nivel_curso_estagio, num_apolice_estagio, estagio_obrigatorio_estagio, i_agente_integracao_estagio, i_supervisor_estagio, controle_jornada, grau_exposicao, tipo_admissao, tipo_trabalhador, i_sindicatos, seguro_vida_estagio, aposentado, categoria, desc_salario_variavel, tipo_ingresso, remunerado_cargo_efetivo, duracao_ben, dt_vencto, tipo_beneficio, recebe_abono, valor_beneficio, cnpj_entidade_qualificadora, contratacao_aprendiz)
VALUES(1, 91715, '2025-01-01', 1, '10605', 1, 13, 174, NULL, NULL, NULL, 1, NULL, NULL, 'S', 'S', 'N', 'N', 'N', 1, 'D', 1.00, NULL, NULL, NULL, NULL, 'N', NULL, NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'M', NULL, NULL, NULL, 'V', NULL, NULL, 'N', NULL, NULL, NULL);

INSERT INTO Folharh.bethadba.hist_funcionarios
(i_entidades, i_funcionarios, dt_alteracoes, i_config_organ, i_organogramas, i_grupos, i_vinculos, i_pessoas, i_bancos, i_agencias, i_pessoas_contas, i_horarios, func_princ, i_agentes_nocivos, optante_fgts, prev_federal, prev_estadual, fundo_ass, fundo_prev, ocorrencia_sefip, forma_pagto, multiplic, i_turmas, num_quadro_cp, num_cp, provisorio, bate_cartao, tipo_contrato, i_responsaveis, fundo_financ, i_pessoas_estagio, dt_final_estagio, nivel_curso_estagio, num_apolice_estagio, estagio_obrigatorio_estagio, i_agente_integracao_estagio, i_supervisor_estagio, controle_jornada, grau_exposicao, tipo_admissao, tipo_trabalhador, i_sindicatos, seguro_vida_estagio, aposentado, categoria, desc_salario_variavel, tipo_ingresso, remunerado_cargo_efetivo, duracao_ben, dt_vencto, tipo_beneficio, recebe_abono, valor_beneficio, cnpj_entidade_qualificadora, contratacao_aprendiz)
VALUES(1, 92805, '2025-01-01', 1, '10605', 1, 13, 174, NULL, NULL, NULL, 1, NULL, NULL, 'S', 'S', 'N', 'N', 'N', 1, 'D', 1.00, NULL, NULL, NULL, NULL, 'N', NULL, NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'M', NULL, NULL, NULL, 'V', NULL, NULL, 'N', NULL, NULL, NULL);


INSERT INTO Folharh.bethadba.hist_funcionarios
(i_entidades, i_funcionarios, dt_alteracoes, i_config_organ, i_organogramas, i_grupos, i_vinculos, i_pessoas, i_bancos, i_agencias, i_pessoas_contas, i_horarios, func_princ, i_agentes_nocivos, optante_fgts, prev_federal, prev_estadual, fundo_ass, fundo_prev, ocorrencia_sefip, forma_pagto, multiplic, i_turmas, num_quadro_cp, num_cp, provisorio, bate_cartao, tipo_contrato, i_responsaveis, fundo_financ, i_pessoas_estagio, dt_final_estagio, nivel_curso_estagio, num_apolice_estagio, estagio_obrigatorio_estagio, i_agente_integracao_estagio, i_supervisor_estagio, controle_jornada, grau_exposicao, tipo_admissao, tipo_trabalhador, i_sindicatos, seguro_vida_estagio, aposentado, categoria, desc_salario_variavel, tipo_ingresso, remunerado_cargo_efetivo, duracao_ben, dt_vencto, tipo_beneficio, recebe_abono, valor_beneficio, cnpj_entidade_qualificadora, contratacao_aprendiz)
VALUES(1, 92802, '2025-01-01', 1, '10605', 1, 13, 174, NULL, NULL, NULL, 1, NULL, NULL, 'S', 'S', 'N', 'N', 'N', 1, 'D', 1.00, NULL, NULL, NULL, NULL, 'N', NULL, NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'M', NULL, NULL, NULL, 'V', NULL, NULL, 'N', NULL, NULL, NULL);


INSERT INTO Folharh.bethadba.hist_funcionarios
(i_entidades, i_funcionarios, dt_alteracoes, i_config_organ, i_organogramas, i_grupos, i_vinculos, i_pessoas, i_bancos, i_agencias, i_pessoas_contas, i_horarios, func_princ, i_agentes_nocivos, optante_fgts, prev_federal, prev_estadual, fundo_ass, fundo_prev, ocorrencia_sefip, forma_pagto, multiplic, i_turmas, num_quadro_cp, num_cp, provisorio, bate_cartao, tipo_contrato, i_responsaveis, fundo_financ, i_pessoas_estagio, dt_final_estagio, nivel_curso_estagio, num_apolice_estagio, estagio_obrigatorio_estagio, i_agente_integracao_estagio, i_supervisor_estagio, controle_jornada, grau_exposicao, tipo_admissao, tipo_trabalhador, i_sindicatos, seguro_vida_estagio, aposentado, categoria, desc_salario_variavel, tipo_ingresso, remunerado_cargo_efetivo, duracao_ben, dt_vencto, tipo_beneficio, recebe_abono, valor_beneficio, cnpj_entidade_qualificadora, contratacao_aprendiz)
VALUES(1, 92806, '2025-01-01', 1, '10605', 1, 13, 174, NULL, NULL, NULL, 1, NULL, NULL, 'S', 'S', 'N', 'N', 'N', 1, 'D', 1.00, NULL, NULL, NULL, NULL, 'N', NULL, NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'M', NULL, NULL, NULL, 'V', NULL, NULL, 'N', NULL, NULL, NULL);


INSERT INTO Folharh.bethadba.hist_funcionarios
(i_entidades, i_funcionarios, dt_alteracoes, i_config_organ, i_organogramas, i_grupos, i_vinculos, i_pessoas, i_bancos, i_agencias, i_pessoas_contas, i_horarios, func_princ, i_agentes_nocivos, optante_fgts, prev_federal, prev_estadual, fundo_ass, fundo_prev, ocorrencia_sefip, forma_pagto, multiplic, i_turmas, num_quadro_cp, num_cp, provisorio, bate_cartao, tipo_contrato, i_responsaveis, fundo_financ, i_pessoas_estagio, dt_final_estagio, nivel_curso_estagio, num_apolice_estagio, estagio_obrigatorio_estagio, i_agente_integracao_estagio, i_supervisor_estagio, controle_jornada, grau_exposicao, tipo_admissao, tipo_trabalhador, i_sindicatos, seguro_vida_estagio, aposentado, categoria, desc_salario_variavel, tipo_ingresso, remunerado_cargo_efetivo, duracao_ben, dt_vencto, tipo_beneficio, recebe_abono, valor_beneficio, cnpj_entidade_qualificadora, contratacao_aprendiz)
VALUES(1, 92804, '2025-01-01', 1, '10605', 1, 13, 174, NULL, NULL, NULL, 1, NULL, NULL, 'S', 'S', 'N', 'N', 'N', 1, 'D', 1.00, NULL, NULL, NULL, NULL, 'N', NULL, NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'M', NULL, NULL, NULL, 'V', NULL, NULL, 'N', NULL, NULL, NULL);


INSERT INTO Folharh.bethadba.hist_funcionarios
(i_entidades, i_funcionarios, dt_alteracoes, i_config_organ, i_organogramas, i_grupos, i_vinculos, i_pessoas, i_bancos, i_agencias, i_pessoas_contas, i_horarios, func_princ, i_agentes_nocivos, optante_fgts, prev_federal, prev_estadual, fundo_ass, fundo_prev, ocorrencia_sefip, forma_pagto, multiplic, i_turmas, num_quadro_cp, num_cp, provisorio, bate_cartao, tipo_contrato, i_responsaveis, fundo_financ, i_pessoas_estagio, dt_final_estagio, nivel_curso_estagio, num_apolice_estagio, estagio_obrigatorio_estagio, i_agente_integracao_estagio, i_supervisor_estagio, controle_jornada, grau_exposicao, tipo_admissao, tipo_trabalhador, i_sindicatos, seguro_vida_estagio, aposentado, categoria, desc_salario_variavel, tipo_ingresso, remunerado_cargo_efetivo, duracao_ben, dt_vencto, tipo_beneficio, recebe_abono, valor_beneficio, cnpj_entidade_qualificadora, contratacao_aprendiz)
VALUES(1, 92803, '2025-01-01', 1, '10605', 1, 13, 174, NULL, NULL, NULL, 1, NULL, NULL, 'S', 'S', 'N', 'N', 'N', 1, 'D', 1.00, NULL, NULL, NULL, NULL, 'N', NULL, NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'M', NULL, NULL, NULL, 'V', NULL, NULL, 'N', NULL, NULL, NULL);


INSERT INTO Folharh.bethadba.hist_funcionarios
(i_entidades, i_funcionarios, dt_alteracoes, i_config_organ, i_organogramas, i_grupos, i_vinculos, i_pessoas, i_bancos, i_agencias, i_pessoas_contas, i_horarios, func_princ, i_agentes_nocivos, optante_fgts, prev_federal, prev_estadual, fundo_ass, fundo_prev, ocorrencia_sefip, forma_pagto, multiplic, i_turmas, num_quadro_cp, num_cp, provisorio, bate_cartao, tipo_contrato, i_responsaveis, fundo_financ, i_pessoas_estagio, dt_final_estagio, nivel_curso_estagio, num_apolice_estagio, estagio_obrigatorio_estagio, i_agente_integracao_estagio, i_supervisor_estagio, controle_jornada, grau_exposicao, tipo_admissao, tipo_trabalhador, i_sindicatos, seguro_vida_estagio, aposentado, categoria, desc_salario_variavel, tipo_ingresso, remunerado_cargo_efetivo, duracao_ben, dt_vencto, tipo_beneficio, recebe_abono, valor_beneficio, cnpj_entidade_qualificadora, contratacao_aprendiz)
VALUES(1, 92807, '2025-01-01', 1, '10605', 1, 13, 174, NULL, NULL, NULL, 1, NULL, NULL, 'S', 'S', 'N', 'N', 'N', 1, 'D', 1.00, NULL, NULL, NULL, NULL, 'N', NULL, NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'M', NULL, NULL, NULL, 'V', NULL, NULL, 'N', NULL, NULL, NULL);

-- Ajuste inclusão de pensão
update bethadba.funcionarios set dt_admissao = '2014-09-01' where i_funcionarios = 92805;
update bethadba.funcionarios set dt_admissao = '2016-03-01' where i_funcionarios = 92801;
update bethadba.funcionarios set dt_admissao = '2014-09-01' where i_funcionarios = 92802;
update bethadba.funcionarios set dt_admissao = '2021-01-01' where i_funcionarios = 92806;
update bethadba.funcionarios set dt_admissao = '2022-12-15' where i_funcionarios = 92804;
update bethadba.funcionarios set dt_admissao = '2022-01-01' where i_funcionarios = 92803;
update bethadba.funcionarios set dt_admissao = '2024-05-03' where i_funcionarios = 92807;

INSERT INTO Folharh.bethadba.pessoas_contas
(i_pessoas, i_pessoas_contas, i_bancos, i_agencias, num_conta, tipo_conta, status)
VALUES(2518, 1, 27, 212, '23446          ', '1', 'A');
INSERT INTO Folharh.bethadba.pessoas_contas
(i_pessoas, i_pessoas_contas, i_bancos, i_agencias, num_conta, tipo_conta, status)
VALUES(3260, 1, 27, 212, '23446          ', '1', 'A');
INSERT INTO Folharh.bethadba.pessoas_contas
(i_pessoas, i_pessoas_contas, i_bancos, i_agencias, num_conta, tipo_conta, status)
VALUES(3617, 1, 27, 212, '23446          ', '1', 'A');
INSERT INTO Folharh.bethadba.pessoas_contas
(i_pessoas, i_pessoas_contas, i_bancos, i_agencias, num_conta, tipo_conta, status)
VALUES(5934, 1, 27, 212, '23446          ', '1', 'A');
INSERT INTO Folharh.bethadba.pessoas_contas
(i_pessoas, i_pessoas_contas, i_bancos, i_agencias, num_conta, tipo_conta, status)
VALUES(6158, 1, 27, 212, '23446          ', '1', 'A');
INSERT INTO Folharh.bethadba.pessoas_contas
(i_pessoas, i_pessoas_contas, i_bancos, i_agencias, num_conta, tipo_conta, status)
VALUES(6160, 1, 27, 212, '23446          ', '1', 'A');
INSERT INTO Folharh.bethadba.pessoas_contas
(i_pessoas, i_pessoas_contas, i_bancos, i_agencias, num_conta, tipo_conta, status)
VALUES(6162, 1, 27, 212, '23446          ', '1', 'A');

-- Contas bancárias
 update bethadba.hist_funcionarios hf 
                   left join bethadba.funcionarios f 
                   on hf.i_funcionarios = f.i_funcionarios 
                   set hf.i_pessoas = f.i_pessoas 
                   where f.i_funcionarios >= 92800



--- Ajusta contas bancárias
update bethadba.hist_funcionarios hf 
                   left join bethadba.pessoas_contas pc 
                   on hf.i_pessoas = pc.i_pessoas 
                   set hf.i_bancos = pc.i_bancos, hf.i_agencias = pc.i_agencias, hf.i_pessoas_contas = pc.i_pessoas_contas
                   where hf.i_funcionarios in (91715, 92802, 92803, 92804, 92805, 92806, 92807)
                   
				   


-- bethadba.beneficiarios
insert into bethadba.beneficiarios 
select 
i_entidades = 1,
i_funcionarios = case cdMatricula
	when 15881 then 92805
	when 31623 then 91715
	when 31666 then 92802
	when 39012 then 92806
	when 43265 then 92804
	when 73172 then 92803
	when 90484 then 92807
end,
1 as i_entidades_inst,
cdMatricula as i_instituidor,
null as i_atos,
'V' as duracao_ben,
null as dt_vencto,
0 as perc_recebto,
1 as config, 
null as alvara,
null as dt_alvara,
'0' as situacao,
null as dt_cessacao,
null as motivo_cessacao,
'N' as parecer_interno,
null as motivo_inicio,
null as origem_beneficio,
null as nr_beneficio,
'S' as acao_judicial,
NULL as matricula_instituidor,
null as cnpj_instituidor,
'0601'as tipo_beneficio,
null as data_recebido,
null as cnpj_ente_sucedido,
null as observacao_beneficio,
null as nr_beneficio_anterior 


from tecbth_delivery.gp001_BENEFICIARIO gb 
where i_instituidor not in (83976)

