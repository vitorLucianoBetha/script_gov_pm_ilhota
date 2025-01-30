CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;

-- Tabela | bethadba.candidatos
if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_candidatos') then
	drop procedure cnv_candidatos;
end if;



begin

ooLoop: for oo as cnv_candidatos dynamic scroll cursor for
		select distinct
1 as w_i_entidades,
LEFT(gcc.cdNroAno, LENGTH(gcc.cdNroAno) - 3) as w_i_concursos,
ROW_NUMBER() OVER (ORDER BY gcc.cdNroAno) AS w_i_candidatos,
gf.cdPessoa as w_i_pessoas,
gcc.cdCargo as w_i_cargos,
cast(gf.dtAdmissao as date) as w_dt_inscricao,
null as w_contato1,
null as w_contato2,
'N' as w_desempregado,
0 as w_salario,
cc.nrClassificacao as w_classif,
case when cc.inAprovado in (1) then 'S' else 'N' end as w_aprovado,
if gf.dtNomeacao is null or gf.dtNomeacao in ('NULL') then null else cast(gf.dtNomeacao as date) endif as w_dt_nomeacao,
if gf.DtPosseCargo  is null or gf.DtPosseCargo in ('NULL') then null else cast(gf.DtPosseCargo as date) endif as w_dt_posse,
null as w_mot_nao_nomeado,
null as w_mot_nao_posse,
case when cc.inHomologado in (1) then 'S' else 'N' end as w_i_faltou,
null as w_i_areas_atuacao,
null as w_dt_desabilitacao, 
null as w_dt_nao_posse,
null as w_dt_prorrog_posse,
null as w_classif_area,
null as w_sub_judice,
null as w_dt_doc_nao_posse,
null as w_arq_nao_posse,
'1' as w_status_deferimento_inscricao,
null as w_classif_cota_deffis_area,
null as w_classif_cota_afro_area,
null as w_classif_cota_indio_area,
null as w_vaga_especial,
cc.nrNota as nota

from tecbth_delivery.gp001_CONC_CONCURSOFUNCIONARIO gcc 
left join tecbth_delivery.gp001_FUNCIONARIO gf 
on gcc.cdMatricula = gf.cdMatricula
left join tecbth_delivery.gp001_pessoa p 
on GF.CdPessoa = p.CdPessoa 
left join tecbth_delivery.gp001_conc_classificacao cc 
on cc.dsNome = p.nmPessoa 

where LEFT(cc.cdNroAno, LENGTH(cc.cdNroAno) - 3) is not null
and w_i_cargos not in ('0')



	do
		message '(Inserindo os candidatos) ' || w_i_candidatos to client;
		
		insert into bethadba.candidatos (i_entidades, i_concursos, i_candidatos, i_pessoas, i_cargos, dt_inscricao, contato1, contato2, desempregado, salario, classif, aprovado, dt_nomeacao, dt_posse, mot_nao_nomeado, mot_nao_posse, faltou, i_areas_atuacao, dt_desabilitacao, dt_nao_posse, dt_prorrog_posse, classif_area, sub_judice, dt_doc_nao_posse, arq_nao_posse, status_deferimento_inscricao, classif_cota_deffis_area, classif_cota_afro_area, classif_cota_indio_area, vaga_especial, nota) 
		values (w_i_entidades, w_i_concursos, w_i_candidatos, w_i_pessoas, w_i_cargos, w_dt_inscricao, w_contato1, w_contato2, w_desempregado, w_salario, w_classif, w_aprovado, w_dt_nomeacao, w_dt_posse, w_mot_nao_nomeado, w_mot_nao_posse, w_i_faltou, w_i_areas_atuacao, w_dt_desabilitacao, w_dt_nao_posse, w_dt_prorrog_posse, w_classif_area, w_sub_judice, w_dt_doc_nao_posse, w_arq_nao_posse, w_status_deferimento_inscricao, w_classif_cota_deffis_area, w_classif_cota_afro_area, w_classif_cota_indio_area, w_vaga_especial, nota);
		

end for;

end;

commit;


