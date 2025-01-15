

insert into bethadba.parametros(i_ponto,i_unidade,maskunidade,autoandamento,tiponaoresp,taxasnaopagas,diasant,diasantouv,andouvidoria,maskouvidoria,maskatendimento,estornar_enc,protecao_ole,
							    senha_ole,excluir_parec,mail_andamento,mail_padrao,mail_direto,mail_server,mail_protocol,usa_grupo_solicitacoes,grupo_solicitacoes_padrao,i_aplicacao,
								usa_grupo_ouv,grupo_padrao_ouv,cedente,cod_ouv,i_chave,alteracodigo,apensacao,estornar_retiradoc,aviso_proc_usr,diasrel,agencia,conta,perm_parecer,procedencia,
								convenio,perm_movimentacao,alterasolicitproc,perm_parado,autentica,usuario_email,senha_email,email_padrao,formato_inscricao,autenticadora,observacao,
								gerar_perm_pareceres,perm_mov_anexacao,prazo_andamento,altera_data_envio,altera_data_receb,formato_2avia,perm_proc_encerrado,sugerir_ano_proc,perm_solicit,
								tp_taxas,consulta_simpl,protocolo_ssl,porta_email)on existing skip
values (1,null,null,'S',1,'N',null,null,'D','#####/AA','#####/AA','N','0',null,'N','N',null,'N',null,null,'N',null,1,'S',null,null,null,1,'S','N','N','N',null,null,null,'T','3',null,'N','N',
		'P',null,null,null,null,null,null,null,'N','P',null,'N','N','1','N','S','T','N','N','N','25')
		;
		
	-- bug BTHSC-8141 Tabela de contribuição (segurado) a partir de 2014 não migrou.	
	-- bug BTHSC-8190 Dedução por dependente de 2015 à 2023 não migrou
	rollback;
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;

insert into bethadba.tabelas 
SELECT 

number(*) +1000as i_tabelas,
convert(varchar(7), dtReferencia, 15) as dt_vigencia_ini,
convert(varchar(7), dtReferencia, 15) as dt_vigencia_fin,
vlFinalIntervalo as vlr_sal_min,
VlDependente as vlr_dependente,
0 as vlr_min_irrf,
vlFinalIntervalo as vlr_piso_sal,
10 as vlr_min_prev_est,
80 as vlr_max_prev_est,
35 as vlr_sal_fam_est,
35  as vlr_sal_fam_apos,
35 as vlr_sal_fam_esp,
5000.00 as vlr_teto_salarial,
null as perc_sest_autonomo,
null as perc_senat_autonomo,
null as i_atos
from  gp001_inss ,gp001_IR
where gp001_inss.cdTabelaIR = gp001_IR.cdTabelaIR
ORDER BY dtReferencia DESC, sqIntervalo