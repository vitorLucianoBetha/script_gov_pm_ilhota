CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;

if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_concursos') then
	drop procedure cnv_concursos;
end if;

begin
	ooLoop: for oo as cnv_concursos dynamic scroll cursor for
		select 1 as w_i_entidades, 
		cast(cdNroAno as int) as w_i_concursos, 
		'Concurso' + ' - ' + cast(w_i_concursos as text) as w_descricao, 
		date(dtPublicacao) as w_dt_ini_insc, 
		date(dtPublicacao) as w_dt_fin_insc, 
		date(dtProrrogacao) as w_dt_prorrog,
		date(dtValidade) as w_dt_validade,
		null as w_dt_prorrog_validade,
		date(dtHomologacao) as w_dt_homolog,
		null as w_justificativa,
		null as w_dt_encerra,
		'M' as w_info_cand,
		-- bug BTHSC-139782 Bug em Concursos e Processos Seletivos
		(case cdTipoConcurso
        when 0  then    'C'
        ELSE 'P'
        END)
 as w_tipo_concurso
		from tecbth_delivery.gp001_conc_concurso
	do

    
		message '(Inserindo os concursos) ' || w_i_concursos to client;
		insert into bethadba.concursos(i_entidades, i_concursos, descricao, dt_ini_insc, dt_fin_insc, dt_prorrog, dt_validade, dt_prorrog_validade, dt_homolog, justificativa, dt_encerra, info_cand, tipo_concurso)
		values (w_i_entidades, w_i_concursos, w_descricao, w_dt_ini_insc, w_dt_fin_insc, w_dt_prorrog, w_dt_validade, w_dt_prorrog_validade, w_dt_homolog, w_justificativa, w_dt_encerra, w_info_cand, w_tipo_concurso)
		
	end for;
end;


commit;
