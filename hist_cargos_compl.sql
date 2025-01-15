if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_cargos_compl') then
	drop procedure cnv_cargos_compl;
end if
;


CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;



begin
	// ***** bethadba.cargos
	--BUG BTHSC-7633
	declare cnv_cargos_compl dynamic scroll cursor for
	-- BUG NOME CARGO BTHSC-7894
    	select cdCargo,nrsequenciafaixa,cdestruturasalarial,cdgrupofaixasalarial
		from tecbth_delivery.gp001_cargosalariofaixa where cdCargo = 99;
		
        declare w_i_cbo char(6);
    	declare w_i_tipos_cargos smallint;
    	declare w_cdfaixasalarial char(50);
        declare w_descricao char(50);
        declare w_i_niveis integer;
        declare w_tot integer;
    	declare w_nrsequenciafaixa integer;
        declare w_i_cargos integer;
    	declare w_cdestruturasalarial integer;
    	declare w_cdgrupofaixasalarial integer;
    	declare w_dt_alteracoes_aux datetime;
    	declare w_dedic_exclu char(50);
        declare w_i_clas_niveisd char(1);
        declare w_faixa char(50);
        declare w_totd integer;
        declare w_i_referencias char(50);
    	
    	// ***** bethadba.hist_cargos_compl
    	declare w_i_config_ferias_subst smallint;
		set w_i_cbo = null;
		set w_i_tipos_cargos = null;
		set w_i_config_ferias_subst = null;
		set w_nrsequenciafaixa = null;
		set w_cdestruturasalarial = null;
		set w_cdgrupofaixasalarial = null;
        set w_dt_alteracoes_aux = '1990-01-01';
        

        open cnv_cargos_compl with hold;
        L_item: loop
            fetch next cnv_cargos_compl into w_i_cargos,w_nrsequenciafaixa,w_cdestruturasalarial,w_cdgrupofaixasalarial;
            if sqlstate = '02000' then
              leave L_item
            end if;

        set w_dt_alteracoes_aux = w_dt_alteracoes_aux + 1;

		--select * from gp001_salarioestruturanivel
		--select * from gp001_salarioestrutura
			
		select case when tot > 6 then left(salariofaixa.cdfaixasalarial,6)
                    when tot <= 6 and tot > 3 then left(salariofaixa.cdfaixasalarial,3)
                    else salariofaixa.cdfaixasalarial
                end,
               length(salariofaixa.cdfaixasalarial) as tot,
               cdfaixasalarial as faixa,
               case when tot > 6 then 3
                    when tot <= 6 and tot > 3 then 3
                    else 2
                end as totd
		into w_cdfaixasalarial, w_tot, w_faixa, w_totd
    	from gp001_salariofaixa as salariofaixa
    	where nrsequenciafaixa    = w_nrsequenciafaixa
    	and cdestruturasalarial = w_cdestruturasalarial
    	and cdgrupofaixasalarial = w_cdgrupofaixasalarial;

        select dsFaixaSalarial as descfim,
               (select i_niveis from bethadba.niveis where nome = descfim) as i_niveis 
		into w_descricao, w_i_niveis
    	from gp001_salariofaixa as salariofaixa
    	where cdestruturasalarial = w_cdestruturasalarial
    	and cdfaixasalarial = w_cdfaixasalarial;

        select if isnumeric(left(sgFaixaSalarial,1)) <> 1 then upper(left(sgFaixaSalarial,1)) else upper(left(dsFaixaSalarial,1)) endif as clas_niveisd 
        into w_i_clas_niveisd
	    from tecbth_delivery.gp001_SALARIOFAIXA where cdEstruturaSalarial = w_cdestruturasalarial and nrSequenciaFaixa = w_nrsequenciafaixa and cdGrupoFaixaSalarial = w_cdgrupofaixasalarial;

        select i_referencias 
        into w_i_referencias
        from bethadba.clas_niveis cn where cn.i_niveis = w_i_niveis 
        and cn.i_clas_niveis = w_i_clas_niveisd 
        and cn.i_referencias = RIGHT(w_faixa,w_totd);
		

		if exists(select 1 from bethadba.niveis where i_entidades = 1 and i_niveis = w_i_niveis) then
			insert into bethadba.hist_cargos_compl(i_entidades,i_cargos,dt_alteracoes,i_niveis,i_clas_niveis_ini,i_referencias_ini,i_clas_niveis_fin,i_referencias_fin) 
			values (1,w_i_cargos,w_dt_alteracoes_aux,w_i_niveis,w_i_clas_niveisd,w_i_referencias,w_i_clas_niveisd,w_i_referencias);
		end if;
  end loop L_item;
  close cnv_cargos_compl
end
;
commit;
s