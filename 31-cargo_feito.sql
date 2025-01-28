ROLLBACK;
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
call bethadba.pg_setoption('fire_triggers','off');
COMMIT;

-- BTHSC-139752 Bug em Cargos | Campos adicionais
INSERT INTO caracteristicas (i_caracteristicas, nome, tipo_dado, tamanho, formato, valor_padrao, obrigatorio, observacao, deletar) VALUES(15001, 'Tipo de cargo p/ fins de Acumu', 6, NULL, NULL, NULL, 'N', 'Conforme Tabela TCE nº77', 0);
INSERT INTO itens_lista (i_caracteristicas, i_itens_lista, descricao, dt_expiracao) VALUES(15001, '1   ', 'Professor', '2999-12-31');
INSERT INTO itens_lista (i_caracteristicas, i_itens_lista, descricao, dt_expiracao) VALUES(15001, '2   ', 'Cargo privativo de profissionais de saúde, com profissão regulamentada;', '2999-12-31');
INSERT INTO itens_lista (i_caracteristicas, i_itens_lista, descricao, dt_expiracao) VALUES(15001, '99  ', 'Não se aplica', '2999-12-31');

DELETE FROM bethadba.cargos;
DELETE FROM bethadba.mov_cargos;
DELETE FROM  bethadba.hist_cargos_compl;
DELETE FROM  bethadba.cargos_compl;
DELETE FROM  bethadba.hist_cargos_cadastro;
DELETE FROM  bethadba.hist_cargos;
---------------------------------------------------------------------------
-- 30) Inserindo os cargos (cargos, mov_cargos, hist_cargos_compl)
---------------------------------------------------------------------------
if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_cargos') then
  drop procedure cnv_cargos;
end if;

begin
	// ***** bethadba.cargos
	declare w_i_cbo char(6);
	declare w_i_tipos_cargos smallint;
	declare w_cdestruturasalarial integer;
	declare w_dt_alteracoes_aux datetime;
	declare w_dt_alteracoes_hcc datetime;
	
	// ***** bethadba.hist_cargos_compl
	declare w_i_config_ferias_subst smallint;
    declare w_i_niveis integer;
    declare w_i_clas_niveis_ini varchar(9);
    declare w_i_referencias_ini varchar(9);
    declare w_i_clas_niveis_fin varchar(9);
    declare w_i_referencias_fin varchar(9);
	declare w_i_config_ferias integer;
	
	ooLoop: for oo as cnv_cargos dynamic scroll cursor for
	-- BTHSC-139752 Bug em Cargos | Campos adicionais
		select 1 as w_i_entidades,
			A.CdCargo as w_i_cargos,
			cdGrupoCboCargo as w_cdGrupoCboCargo,
			cdCboCargo as w_cdCboCargo,
			A.TpCargo as w_TpCargo,
			upper(DsCargo) as w_nome,
			QtVagas_OLD as w_vagas_acresc,
			QtVagas_OLD as w_qtd_vagas,
			date(DtLeiCargo) as w_dt_lei,
			date(DtLeiCargo) as w_dt_vigorar,
			NrLeiCargo as w_num_lei,
			CdTribunal as w_CdTribunal,
			DtDesativacao as w_dt_leii,
			DtDesativacao as w_dt_vigorarr,
			ymd(year(DtLeiCargo),month(DtLeiCargo),1) as w_dt_alteracoes,
			DtDesativacao as w_DtDesativacao,
			B.tpQuadro as w_tpquadro, 
			B.tpCargo as w_tpcargo
		 from tecbth_delivery.gp001_cargo A
		 join tecbth_delivery.gp001_TCSCCARGOS B on A.CdCargo = B.CdCargo
	do
		set w_i_cbo = null;
		set w_i_tipos_cargos = null;
		set w_i_config_ferias_subst = null;
		set w_cdestruturasalarial = null;
        set w_i_niveis = null;
        set w_i_clas_niveis_ini = null;
        set w_i_referencias_ini = null;
        set w_i_clas_niveis_fin = null;
        set w_i_referencias_fin = null;
        set w_dt_alteracoes_hcc = null;

		// *****  Converte tabela bethadba.cargos		
		if w_cdGrupoCboCargo != 0 then
			if length(w_cdGrupoCboCargo) = 4 then
				if length(w_cdCboCargo) = 2 then
					set w_i_cbo=string(w_cdGrupoCboCargo)+string(w_cdCboCargo)
				else
					set w_i_cbo=string(w_cdGrupoCboCargo)+'0'+string(w_cdCboCargo)
				end if
			else
				if length(w_cdCboCargo) = 2 then
					set w_i_cbo='0'+string(w_cdGrupoCboCargo)+string(w_cdCboCargo)
				else
					set w_i_cbo='0'+string(w_cdGrupoCboCargo)+'0'+string(w_cdCboCargo)
				end if
			end if		
		else
			set w_i_cbo=null
		end if;
		
		if(w_i_cbo = '000000') or(trim(w_i_cbo) = '') then
			set w_i_cbo=null
		end if;
		
		if not exists(select 1 from bethadba.cbo where i_cbo = w_i_cbo) then
			set w_i_cbo=null
		end if;
		
		if w_TpCargo in(99,0) then
			set w_i_tipos_cargos=99
		else
			set w_i_tipos_cargos=w_TpCargo
		end if;
		
		message 'Car.: '||w_i_cargos||' Nom.: '||w_nome||' CBO.: '||w_i_cbo to client;
		
		insert into bethadba.cargos(i_entidades,i_cargos,i_cbo,i_tipos_cargos,nome)on existing skip
		values(w_i_entidades,w_i_cargos,w_i_cbo,w_i_tipos_cargos,w_nome);

		-- BTHSC-139752 Bug em Cargos | Campos adicionais
		insert into bethadba.cargos_prop_adic(i_caracteristicas,i_entidades,i_cargos,valor_caracter) on existing skip
		values(20092,w_i_entidades,w_i_cargos,w_tpquadro);
		-- BTHSC-139752 Bug em Cargos | Campos adicionais
		insert into bethadba.cargos_prop_adic(i_caracteristicas,i_entidades,i_cargos,valor_caracter) on existing skip
		values(15001,w_i_entidades,w_i_cargos,w_tpcargo);

		-- BTHSC-138631
		if w_i_cargos in(61,68,162,176,156,67,69,160) then
			set w_i_config_ferias = 2
		else
			set w_i_config_ferias = 1
		end if;
		
		// *****  Converte tabela bethadba.cargos_compl 		
		if w_i_tipos_cargos = any(select i_tipos_cargos from bethadba.tipos_cargos where classif = 2) then
			set w_i_config_ferias_subst=null
		else
			set w_i_config_ferias_subst=1
		end if;
		if w_qtd_vagas is null  then
        set w_qtd_vagas = 0
        end if;
		insert into bethadba.cargos_compl(i_entidades,i_cargos,i_config_ferias,i_config_ferias_subst,qtd_vagas,rol,grau_instrucao,codigo_tce,decimo_terc,requisitos,atividades)on existing skip
		values (w_i_entidades,w_i_cargos,w_i_config_ferias,w_i_config_ferias_subst,w_qtd_vagas,null,1,w_i_cargos,'S',null,null);

		// *****  Converte tabela bethadba.hist_cargos_compl
        message 'Verificando se o cargo passou pela configuração 1' to client;
        if exists(select 1 from tecbth_delivery.gp001_cargosalariofaixa
                  where cdestruturasalarial = 1
                    and cdcargo = w_i_cargos) then
          set w_cdestruturasalarial = 1;
          set w_dt_alteracoes_hcc = '1900-01-01';
          -- INICIAL
          select first a.cdestruturasalarial || cast(substr(b.cdfaixasalarial, 1, 3) as integer), substr(cdfaixasalarial, 4, 3), substr(cdfaixasalarial, 7, 3)
          into w_i_niveis, w_i_clas_niveis_ini, w_i_referencias_fin
          from tecbth_delivery.gp001_cargosalariofaixa a, tecbth_delivery.gp001_salariofaixa b
          where a.cdestruturasalarial = b.cdestruturasalarial
            and a.cdgrupofaixasalarial = b.cdgrupofaixasalarial
            and a.nrsequenciafaixa = b.nrsequenciafaixa
            and a.cdcargo = w_i_cargos
            and a.cdestruturasalarial = w_cdestruturasalarial
          order by b.cdestruturasalarial, b.cdfaixasalarial, b.cdgrupofaixasalarial, b.nrsequenciafaixa;
          -- FINAL
          select first substr(cdfaixasalarial, 4, 3) as w_i_clas_niveis_fin, substr(cdfaixasalarial, 7, 3) as w_i_referencias_fin
          into w_i_clas_niveis_fin, w_i_referencias_fin
          from tecbth_delivery.gp001_cargosalariofaixa a, tecbth_delivery.gp001_salariofaixa b
          where a.cdestruturasalarial = b.cdestruturasalarial
            and a.cdgrupofaixasalarial = b.cdgrupofaixasalarial
            and a.nrsequenciafaixa = b.nrsequenciafaixa
            and a.cdcargo = w_i_cargos
            and a.cdestruturasalarial = w_cdestruturasalarial
          order by b.cdestruturasalarial desc, b.cdfaixasalarial desc, b.cdgrupofaixasalarial desc, b.nrsequenciafaixa desc;
          if exists(select 1 from bethadba.niveis where i_entidades = w_i_entidades and i_niveis = w_i_niveis) then
            insert into bethadba.hist_cargos_compl(i_entidades,i_cargos,dt_alteracoes,i_niveis,i_clas_niveis_ini,i_referencias_ini,i_clas_niveis_fin,i_referencias_fin) 
            values (w_i_entidades,w_i_cargos,w_dt_alteracoes_hcc,w_i_niveis,w_i_clas_niveis_ini,w_i_referencias_ini,w_i_clas_niveis_fin,w_i_referencias_fin);
          end if;
        end if;
        message 'Verificando se o cargo passou pela configuração 3' to client;
        if exists(select 1 from tecbth_delivery.gp001_cargosalariofaixa
                  where cdestruturasalarial = 3
                    and cdcargo = w_i_cargos) then
          set w_cdestruturasalarial = 3;
          set w_dt_alteracoes_hcc = '1900-01-02';
          -- INICIAL
          select first a.cdestruturasalarial || cast(substr(b.cdfaixasalarial, 1, 1) as integer), substr(cdfaixasalarial, 2, 2), substr(cdfaixasalarial, 4, 3)
          into w_i_niveis, w_i_clas_niveis_ini, w_i_referencias_ini
          from tecbth_delivery.gp001_cargosalariofaixa a, tecbth_delivery.gp001_salariofaixa b
          where a.cdestruturasalarial = b.cdestruturasalarial
            and a.cdgrupofaixasalarial = b.cdgrupofaixasalarial
            and a.nrsequenciafaixa = b.nrsequenciafaixa
            and a.cdcargo = w_i_cargos
            and a.cdestruturasalarial = w_cdestruturasalarial
          order by b.cdestruturasalarial, b.cdfaixasalarial, b.cdgrupofaixasalarial, b.nrsequenciafaixa;
          -- FINAL
          select first substr(cdfaixasalarial, 2, 2), substr(cdfaixasalarial, 4, 3)
          into w_i_clas_niveis_fin, w_i_referencias_fin
          from tecbth_delivery.gp001_cargosalariofaixa a, tecbth_delivery.gp001_salariofaixa b
          where a.cdestruturasalarial = b.cdestruturasalarial
            and a.cdgrupofaixasalarial = b.cdgrupofaixasalarial
            and a.nrsequenciafaixa = b.nrsequenciafaixa
            and a.cdcargo = w_i_cargos
            and a.cdestruturasalarial = w_cdestruturasalarial
          order by b.cdestruturasalarial desc, b.cdfaixasalarial desc, b.cdgrupofaixasalarial desc, b.nrsequenciafaixa desc;
          if exists(select 1 from bethadba.niveis where i_entidades = w_i_entidades and i_niveis = w_i_niveis  ) then
            insert into bethadba.hist_cargos_compl(i_entidades,i_cargos,dt_alteracoes,i_niveis,i_clas_niveis_ini,i_referencias_ini,i_clas_niveis_fin,i_referencias_fin) 
            values (w_i_entidades,w_i_cargos,w_dt_alteracoes_hcc,w_i_niveis,w_i_clas_niveis_ini,w_i_referencias_ini,w_i_clas_niveis_fin,w_i_referencias_fin);
          end if;
        end if;

		if w_dt_alteracoes is not null then
			if w_dt_alteracoes = w_dt_alteracoes_aux then
				set w_dt_alteracoes = dateadd(hour,1,w_dt_alteracoes);
			end if;
		end if;

		// *****  Converte tabela bethadba.mov_cargos
		if w_dt_alteracoes is null then
			set w_dt_alteracoes='2000-01-01'
		end if;
		
		insert into bethadba.mov_cargos(i_entidades,i_cargos,dt_alteracoes,tipo_atualiz,num_lei,dt_lei,dt_vigorar,vagas_acresc,vagas_reduzir)on existing skip
		values (w_i_entidades,w_i_cargos,w_dt_alteracoes,1,w_num_lei,w_dt_lei,w_dt_vigorar,w_vagas_acresc,null);
		
		if w_DtDesativacao is not null then
			set w_dt_alteracoes=ymd(year(w_DtDesativacao),month(w_DtDesativacao),1);
		
			insert into bethadba.mov_cargos(i_entidades,i_cargos,dt_alteracoes,tipo_atualiz,num_lei,dt_lei,dt_vigorar,vagas_acresc,vagas_reduzir)on existing update
			values (w_i_entidades,w_i_cargos,w_dt_alteracoes,3,null,w_dt_leii,w_dt_vigorarr,w_vagas_acresc,null) 
		end if;
		
		set w_dt_alteracoes_aux = w_dt_alteracoes;
		
	end for;
end;



