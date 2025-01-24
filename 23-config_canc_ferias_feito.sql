
-- Tabela | bethadba.mediasvant
if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_config_ferias') then
	drop procedure cnv_config_ferias;
end if;

delete from bethadba.config_ferias;

begin
	ooLoop: for oo as cnv_config_ferias dynamic scroll cursor for
		select 
CdTipoFerias as w_i_config_ferias,
DsTipoFerias as w_descricao,
if qtMesesAquisicao = 0 then 1 else qtMesesAquisicao endif as w_meses_aquisicao,
if qtMesesConcessao = 0 then 1 else qtMesesConcessao endif as w_meses_concessao,
if qtMesesCritica = 0 then 1 else qtMesesCritica endif as w_meses_critica,
case CdTipoFerias 
when 1 then 30
when 2 then 45
end as w_num_dias_ferias,
1 as w_inicio_periodo,
null as w_diames_inicio_per,
case CdTipoFerias 
when 1 then 'S'
when 2 then 'N'
end as w_ferias_prop,
case CdTipoFerias 
when 1 then 'I'
when 2 then 'P'
end as w_pagto_ferias_prop,
case CdTipoFerias 
when 1 then 30
when 2 then 20
end as w_num_dias_abono,
'D' as w_controle_abono,
'N' as w_trunca_dias
from tecbth_delivery.gp001_instalacaoferias
	do
		message '(Configuração de Férias) ' || w_i_config_ferias to client;
		insert into bethadba.config_ferias(i_config_ferias, descricao, meses_aquisicao, meses_concessao, meses_critica, num_dias_ferias, inicio_periodo, diames_inicio_per, ferias_prop, pagto_ferias_prop, num_dias_abono, controle_abono, trunca_dias)
		values (w_i_config_ferias, w_descricao, w_meses_aquisicao, w_meses_concessao, w_meses_critica, w_num_dias_ferias, w_inicio_periodo, w_diames_inicio_per, w_ferias_prop, w_pagto_ferias_prop,  w_num_dias_abono, w_controle_abono, w_trunca_dias)
		
	end for;
end;



CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;

 -- Cadastro de faltas

insert into bethadba.desc_faltas (i_config_ferias, i_num_faltas, num_dias_desc)
values (1, 5, 0);
insert into bethadba.desc_faltas (i_config_ferias, i_num_faltas, num_dias_desc)
values (1, 14, 6);
insert into bethadba.desc_faltas (i_config_ferias, i_num_faltas, num_dias_desc)
values (1, 23, 12);
insert into bethadba.desc_faltas (i_config_ferias, i_num_faltas, num_dias_desc)
values (1, 32, 18);
insert into bethadba.desc_faltas (i_config_ferias, i_num_faltas, num_dias_desc)
values (1, 999, 30);

insert into bethadba.desc_faltas (i_config_ferias, i_num_faltas, num_dias_desc)
values (2, 5, 0);
insert into bethadba.desc_faltas (i_config_ferias, i_num_faltas, num_dias_desc)
values (2, 14, 15);
insert into bethadba.desc_faltas (i_config_ferias, i_num_faltas, num_dias_desc)
values (2, 23, 25);
insert into bethadba.desc_faltas (i_config_ferias, i_num_faltas, num_dias_desc)
values (2, 32, 35);
insert into bethadba.desc_faltas (i_config_ferias, i_num_faltas, num_dias_desc)
values (2, 999, 45);






insert into bethadba.config_ferias_canc values (1, 1);
insert into bethadba.config_ferias_canc values (1, 2);