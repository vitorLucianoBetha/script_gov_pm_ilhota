
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;

if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_cargos_concursos') then
	drop procedure cnv_cargos_concursos;
end if;

begin
	ooLoop: for oo as cnv_medias_vant dynamic scroll cursor for
		select 
		1 as w_i_entidades,
		cast(cdNroAno as int) as w_i_concursos,
		cdCargo as w_i_cargos,
		nrVagas as w_num_vagas,
		nrVagasDeficientes as w_num_vagas_deffis,
		0 as w_vlr_tx_insc,
		7 AS w_media_aprov,
		0 as w_salario_inicial,
		null as w_reg_juridico,
		null as w_tipo_previd, 
		NrVagasCotaRacial as w_num_vagas_afrodescendentes,
		null as w_num_vagas_indio,
		null as w_num_vagas_comp_afrod_indio
			from tecbth_delivery.gp001_CONC_CARGO 

	do
		message '(Inserindo os Cargos dos concursos) ' || w_i_concursos || w_i_cargos to client;
		insert into bethadba.cargos_concursos(i_entidades, i_concursos, i_cargos, num_vagas, num_vagas_deffis, vlr_tx_insc, media_aprov, salario_inicial, reg_juridico, tipo_previd, num_vagas_afrodescendentes, num_vagas_indio) on existing skip
		values (w_i_entidades, w_i_concursos, w_i_cargos, w_num_vagas, w_num_vagas_deffis, w_vlr_tx_insc, w_media_aprov, w_salario_inicial, w_reg_juridico, w_tipo_previd, w_num_vagas_afrodescendentes, w_num_vagas_indio)

	end for;
end;
