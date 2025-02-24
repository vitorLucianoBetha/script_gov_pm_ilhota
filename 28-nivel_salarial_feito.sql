--ROLLBACK;

--- ATENÇÃO UTILIZAR DE ACORDO COM A ENTIDADE
--- PM ABAIXO

CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
call bethadba.pg_setoption('fire_triggers','off');
COMMIT;


--delete from bethadba.niveis;
--delete from bethadba.clas_niveis;
--delete from bethadba.hist_niveis;
--delete from bethadba. bethadba.hist_salariais ;

-- BTHSC-133097 Bug em Níveis salariais | Classes e referências, Motivo, Carga Horaria e Vigencia

begin
  declare w_i_entidades integer;
  declare w_i_niveis integer;
  declare w_nome char(50);
--
  ooLoop: for oo as dynamic scroll cursor for 
    select cdestruturasalarial as w_cdestruturasalarial,
    		cdfaixasalarial as w_cdfaixasalarial,
    		nrsequenciafaixa as w_nrsequenciafaixa,
    		dsfaixasalarial as w_descricao,
           	vlfaixasalarial as w_vlfaixasalarial,
            (select first sf.nrHorasReferencia from tecbth_delivery.gp001_salariofaixa sf where sf.cdEstruturaSalarial = w_cdestruturasalarial
            and sf.nrNivelSalarial = 2 and SUBSTRING(sf.cdFaixaSalarial,1,3) = w_cdfaixasalarial) as w_carga_hor,
            (select first sf.cdMotivoReajuste from tecbth_delivery.gp001_salariofaixa sf where sf.cdEstruturaSalarial = w_cdestruturasalarial
            and sf.nrNivelSalarial = 3 and SUBSTRING(sf.cdFaixaSalarial,1,3) = w_cdfaixasalarial) as w_motivos_alt_sal,
            isnull((select first date(sf.dtInicioValidade) from tecbth_delivery.gp001_salariofaixa sf where sf.cdEstruturaSalarial = w_cdestruturasalarial
            and sf.nrNivelSalarial = 3 and SUBSTRING(sf.cdFaixaSalarial,1,3) = w_cdfaixasalarial),date(dtInicioValidade)) as w_dt_alteracoes
    from tecbth_delivery.gp001_salariofaixa
    where nrnivelsalarial = 1 
    order by cdestruturasalarial, cdfaixasalarial, nrsequenciafaixa

--CdEstrutura.: 1 - CdFaixa.: 165002002 - NrSequencia.: 2 - Descricao.: DENTE ILHOTAPREV

--select * from niveis where nome = 'DENTE ILHOTAPREV'
  do
    set w_i_entidades = 1;
    set w_i_niveis = null;
    message 'CdEstrutura.: ' || w_cdestruturasalarial || ' - CdFaixa.: ' || w_cdfaixasalarial || ' - NrSequencia.: ' || w_nrsequenciafaixa || ' - Descricao.: ' || w_descricao to client;
    set w_i_niveis = w_cdestruturasalarial || cast(w_cdfaixasalarial as integer);
    message 'I_Niveis.: ' || w_i_niveis to client;
    set w_nome = w_descricao;
    if w_vlfaixasalarial = 0 then
      set w_vlfaixasalarial = 0.01;
    end if;
    if w_motivos_alt_sal = 0 then
    	set w_motivos_alt_sal = 1;
    end if;
    insert into bethadba.niveis(i_entidades,
                                i_niveis,
                                nome,
                                valor,
                                carga_hor,
                                coeficiente,
                                i_planos_salariais,
                                codigo_tce,
                                i_atos,
                                dt_criacao) on existing update
      values(w_i_entidades,     -- i_entidades integer NOT NULL,
             w_i_niveis,        -- i_niveis integer NOT NULL,
             w_nome,            -- nome char(50) NOT NULL,
             w_vlfaixasalarial, -- valor numeric(12,2) NOT NULL,
             w_carga_hor,             -- carga_hor numeric(5,2) NOT NULL,
             'N',               -- coeficiente char(1) NOT NULL DEFAULT 'N',
             1,                 -- i_planos_salariais smallint NOT NULL,
             null,              -- codigo_tce char(16) NULL,
             null,              -- i_atos integer NULL,
             null);             -- dt_criacao date NULL,
             insert into bethadba.hist_niveis(i_entidades,i_niveis,dt_alteracoes,i_motivos_altsal,vlr_anterior,vlr_novo,perc_aumento,i_planos_salariais,carga_hor) on existing skip
      		values(w_i_entidades, w_i_niveis, w_dt_alteracoes, w_motivos_alt_sal, w_vlfaixasalarial, w_vlfaixasalarial, 0, 1, w_carga_hor);
    update tecbth_delivery.gp001_salariofaixa
      set i_niveis = w_i_niveis
    where cdestruturasalarial = w_cdestruturasalarial
      and cdfaixasalarial = w_cdfaixasalarial
      and cdgrupofaixasalarial = cdgrupofaixasalarial
      and nrsequenciafaixa = nrsequenciafaixa
  end for;
end;

---------------------------------------------------------------------------------------------------
---CM ABAIXO
--ROLLBACK;


CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
call bethadba.pg_setoption('fire_triggers','off');
COMMIT;


--delete from bethadba.niveis;
--delete from bethadba.clas_niveis;
--delete from bethadba.hist_niveis;
--delete from bethadba. bethadba.hist_salariais ;

-- BTHSC-133097 Bug em Níveis salariais | Classes e referências, Motivo, Carga Horaria e Vigencia

begin
  declare w_i_entidades integer;
  declare w_i_niveis integer;
  declare w_nome char(50);
--
  ooLoop: for oo as dynamic scroll cursor for 
    select cdestruturasalarial as w_cdestruturasalarial,
    		cdfaixasalarial as w_cdfaixasalarial,
    		nrsequenciafaixa as w_nrsequenciafaixa,
    		dsfaixasalarial as w_descricao,
           	vlfaixasalarial as w_vlfaixasalarial,
            (select first sf.nrHorasReferencia from tecbth_delivery.gp001_salariofaixa sf where sf.cdEstruturaSalarial = w_cdestruturasalarial
            and sf.nrNivelSalarial = 2 and SUBSTRING(sf.cdFaixaSalarial,1,1) = w_cdfaixasalarial) as w_carga_hor,
            (select first sf.cdMotivoReajuste from tecbth_delivery.gp001_salariofaixa sf where sf.cdEstruturaSalarial = w_cdestruturasalarial
            and sf.nrNivelSalarial = 2 and SUBSTRING(sf.cdFaixaSalarial,1,1) = w_cdfaixasalarial) as w_motivos_alt_sal,
            isnull((select first date(sf.dtInicioValidade) from tecbth_delivery.gp001_salariofaixa sf where sf.cdEstruturaSalarial = w_cdestruturasalarial
            and sf.nrNivelSalarial = 2 and SUBSTRING(sf.cdFaixaSalarial,1,1) = w_cdfaixasalarial),date(dtInicioValidade)) as w_dt_alteracoes
    from tecbth_delivery.gp001_salariofaixa
    where nrnivelsalarial = 1 
    order by cdestruturasalarial, cdfaixasalarial, nrsequenciafaixa

--CdEstrutura.: 1 - CdFaixa.: 165002002 - NrSequencia.: 2 - Descricao.: DENTE ILHOTAPREV

--select * from niveis where nome = 'DENTE ILHOTAPREV'
  do
    set w_i_entidades = 1;
    set w_i_niveis = null;
    message 'CdEstrutura.: ' || w_cdestruturasalarial || ' - CdFaixa.: ' || w_cdfaixasalarial || ' - NrSequencia.: ' || w_nrsequenciafaixa || ' - Descricao.: ' || w_descricao to client;
    set w_i_niveis = w_cdestruturasalarial || cast(w_cdfaixasalarial as integer);
    message 'I_Niveis.: ' || w_i_niveis to client;
    set w_nome = w_descricao;
    if w_vlfaixasalarial = 0 then
      set w_vlfaixasalarial = 0.01;
    end if;
    if w_motivos_alt_sal = 0 then
    	set w_motivos_alt_sal = 1;
    end if;
    insert into bethadba.niveis(i_entidades,
                                i_niveis,
                                nome,
                                valor,
                                carga_hor,
                                coeficiente,
                                i_planos_salariais,
                                codigo_tce,
                                i_atos,
                                dt_criacao) on existing update
      values(w_i_entidades,     -- i_entidades integer NOT NULL,
             w_i_niveis,        -- i_niveis integer NOT NULL,
             w_nome,            -- nome char(50) NOT NULL,
             w_vlfaixasalarial, -- valor numeric(12,2) NOT NULL,
             w_carga_hor,             -- carga_hor numeric(5,2) NOT NULL,
             'N',               -- coeficiente char(1) NOT NULL DEFAULT 'N',
             1,                 -- i_planos_salariais smallint NOT NULL,
             null,              -- codigo_tce char(16) NULL,
             null,              -- i_atos integer NULL,
             null);             -- dt_criacao date NULL,
             insert into bethadba.hist_niveis(i_entidades,i_niveis,dt_alteracoes,i_motivos_altsal,vlr_anterior,vlr_novo,perc_aumento,i_planos_salariais,carga_hor) on existing skip
      		values(w_i_entidades, w_i_niveis, w_dt_alteracoes, w_motivos_alt_sal, w_vlfaixasalarial, w_vlfaixasalarial, 0, 1, w_carga_hor);
    update tecbth_delivery.gp001_salariofaixa
      set i_niveis = w_i_niveis
    where cdestruturasalarial = w_cdestruturasalarial
      and cdfaixasalarial = w_cdfaixasalarial
      and cdgrupofaixasalarial = cdgrupofaixasalarial
      and nrsequenciafaixa = nrsequenciafaixa
  end for;
end;