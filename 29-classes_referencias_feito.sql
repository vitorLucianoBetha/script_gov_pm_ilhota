CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;

-- BTHSC-133097 - Bug em Níveis salariais | Classes e referências, Motivo, Carga Horaria e Vigencia

if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_clas_niveis') then
  drop procedure cnv_clas_niveis;
end if;

begin
  declare w_i_entidades integer;
  declare w_i_niveis integer;
  declare w_i_clas_niveis varchar(9);
  declare w_i_referencias varchar(9);
  declare w_ordem integer;
--
  ooLoop: for oo as dynamic scroll cursor for 
    select cdestruturasalarial as w_cdestruturasalarial,
    		cdfaixasalarial as w_cdfaixasalarial,
    		nrsequenciafaixa as w_nrsequenciafaixa,
    		dsfaixasalarial as w_descricao,
           vlfaixasalarial as w_vlfaixasalarial,
           if substring(sgFaixaSalarial,1,1) = '' then 'A' else substring(replace(sgFaixaSalarial,'-',''),1,1) endif as w_classe,
           if substring(sgFaixaSalarial,2,2) = '' then substring(w_cdfaixasalarial,8,2) else  substring(replace(sgFaixaSalarial,'-',''),2,2) endif as w_referencia
    from tecbth_delivery.gp001_salariofaixa
    where nrnivelsalarial = 3 --and w_cdfaixasalarial like '0920920%'
    order by cdestruturasalarial, cdfaixasalarial, nrsequenciafaixa
  do


    message 'CdEstrutura.: ' || w_cdestruturasalarial || ' - CdFaixa.: ' || w_cdfaixasalarial || ' - NrSequencia.: ' || w_nrsequenciafaixa || ' - Descricao.: ' || w_descricao to client;
    set w_i_entidades = 1;
    set w_i_niveis = null;
    set w_i_clas_niveis = null;
    set w_i_referencias = null;
    set w_ordem = null;
    if w_cdestruturasalarial = 1 then
      set w_i_niveis = (select i_niveis
                        from tecbth_delivery.gp001_salariofaixa
                        where trim(cdfaixasalarial) = substr(trim(w_cdfaixasalarial), 1, 3)
                          and nrnivelsalarial = 1);
    else
      if w_cdestruturasalarial = 3 then
        set w_i_niveis = (select i_niveis
                          from tecbth_delivery.gp001_salariofaixa
                          where trim(cdfaixasalarial) = substr(trim(w_cdfaixasalarial), 1, 1)
                            and nrnivelsalarial = 1);
      end if;
    end if;
    message 'I_Niveis.: ' || w_i_niveis to client;
    if w_vlfaixasalarial = 0 then
      set w_vlfaixasalarial = 0.01;
    end if;
    if length(w_cdfaixasalarial) = 9 then
      set w_i_clas_niveis = substr(w_cdfaixasalarial, 4, 3);
      set w_i_referencias = substr(w_cdfaixasalarial, 7, 3);
    else
      if length(w_cdfaixasalarial) = 6 then
        set w_i_clas_niveis = substr(w_cdfaixasalarial, 2, 2);
        set w_i_referencias = substr(w_cdfaixasalarial, 4, 3);
      end if;
    end if;
    set w_ordem = coalesce((select max(ordem)
                            from bethadba.clas_niveis
                            where i_entidades = w_i_entidades
                              and i_niveis = w_i_niveis), 0) + 1;
if w_i_niveis is not null  then
	if exists(select first 1 from bethadba.clas_niveis cn where cn.i_niveis = w_i_niveis and cn.i_clas_niveis = w_classe and cn.i_referencias =  w_referencia) then
		set w_referencia = substring(w_i_referencias, 2,2);
	end if;

	if exists(select first 1 from bethadba.clas_niveis cn where cn.i_niveis = w_i_niveis and cn.i_clas_niveis = w_classe and cn.i_referencias =  w_referencia) then
		select max(cn.i_referencias) + 1 into w_referencia from bethadba.clas_niveis cn where cn.i_niveis = w_i_niveis and cn.i_clas_niveis = w_classe;
	end if;

	message 'I_Niveis.: ' || w_i_niveis||' - Classe.: ' || w_classe || ' - Referencia.: ' || w_referencia to client;
    insert into bethadba.clas_niveis(i_entidades,
                                     i_niveis,
                                     i_clas_niveis,
                                     i_referencias,
                                     valor,
                                     ordem,
                                     classe_tce,
                                     referencia_tce)
    values(w_i_entidades,      -- i_entidades integer NOT NULL,
           w_i_niveis,         -- i_niveis integer NOT NULL,
           w_classe,    -- i_clas_niveis char(9) NOT NULL,
           w_referencia,    -- i_referencias char(9) NOT NULL,
           w_vlfaixasalarial,  -- valor numeric(10,4) NOT NULL,
           w_ordem,            -- ordem smallint NOT NULL,
           null,               -- classe_tce char(3) NULL,
           null);              -- referencia_tce char(3) NULL,
end if;
  end for;
end;