CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
commit;

-- Tabela | bethadba.turmas 
if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_jornada_trabalho') then
	drop procedure cnv_jornada_trabalho;
end if;

begin
	ooLoop: for oo as cnv_medias_vant dynamic scroll cursor for
	select 
	1 as w_i_entidades,
	cdEscala as w_i_turmas,
	DsEscala as w_descricao,
	date(dtEscala) as w_dt_inicio_turma,
	'N' as w_busca_autom,
	'N' as w_revezamento,
	'P'	as w_tipo_revezamento,
	'9' as w_tipo_jornada,
	nrHorasDia * 5 as w_media_horas_semanais,
	3 as w_limite_jornada_parcial,
	'N' as w_jornada_noturna
	from tecbth_delivery.gp001_ESCALA
	
	do
		message '(Inserindo as Jornadas de Trabalho) ' || w_i_turmas to client;
		insert into bethadba.turmas (i_entidades, i_turmas, descricao, dt_inicio_turma, busca_autom, revezamento, tipo_revezamento, tipo_jornada, media_horas_semanais, limite_jornada_parcial, jornada_noturna)
		values (w_i_entidades, w_i_turmas, w_descricao, w_dt_inicio_turma, w_busca_autom, w_revezamento, w_tipo_revezamento, w_tipo_jornada, w_media_horas_semanais, w_limite_jornada_parcial, w_jornada_noturna )
		
	end for;
end;
commit;

-- bethadba.periodos_trab
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
commit;

-- Tabela | bethadba.turmas | bethadba.periodos_trab | bethadba.periodos_trab_turmas
if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_jornada_trabalho') then
	drop procedure cnv_jornada_trabalho;
end if;

begin
	ooLoop: for oo as cnv_medias_vant dynamic scroll cursor for
	select 
	1 as w_i_entidades,
	cdEscala as w_i_periodos_trab,
	DsEscala as w_descricao,
	'S' as w_tipo,
	'00:00' as w_horas_dsr,
	'N' as w_estender_hora_noturna
	from tecbth_delivery.gp001_ESCALA
	
	do
		message '(Inserindo as Jornadas de Trabalho) ' || w_i_periodos_trab to client;
		insert into bethadba.periodos_trab (i_entidades, i_periodos_trab, descricao, tipo, horas_dsr, estender_hora_noturna)
		values (w_i_entidades, w_i_periodos_trab, w_descricao, w_tipo, w_horas_dsr, w_estender_hora_noturna)
		
	end for;
end;


commit;


-- bethadba.periodos_trab_turmas

CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
commit;

-- Tabela | bethadba.turmas | bethadba.periodos_trab | bethadba.periodos_trab_turmas
if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_jornada_trabalho') then
	drop procedure cnv_jornada_trabalho;
end if;

begin
	ooLoop: for oo as cnv_medias_vant dynamic scroll cursor for
	select 
	1 as w_i_entidades,
	cdEscala as w_i_turmas,
	1 as w_i_sequencial,
	cdEscala as w_i_periodos_trab,
	'S' as w_principal
	from tecbth_delivery.gp001_ESCALA
	
	do
		message '(Inserindo as Jornadas de Trabalho) ' || w_i_turmas || w_i_periodos_trab to client;
		insert into bethadba.periodos_trab_turmas (i_entidades, i_turmas, i_sequencial, i_periodos_trab, principal)
		values (w_i_entidades, w_i_turmas, w_i_sequencial, w_i_periodos_trab, w_principal)
		
	end for;
end;


commit;




