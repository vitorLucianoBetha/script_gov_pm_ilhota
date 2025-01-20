-- Rodar em ordem (Parte 1 até a Parte 9 + eventos_prop_adic)

-- Parte 1 BTHSC-134414
begin 
	declare w_i_eventos integer;
	declare w_tipo_pd char(1);
	declare w_unidade char(15);
	declare w_sai_rais char(1);
	declare w_cont integer;
	set w_cont=0;

	select 1 as entidade,CdVerba 
	into #tempo1 
	from tecbth_delivery.gp001_VerbaTipoProcesso 
	where CdTipoProcesso = 2 
	union
	select 2 as entidade,CdVerba 
	from tecbth_delivery.gp001_VerbaTipoProcesso 
	where CdTipoProcesso = 2;

	ooLoop: for oo as cnv_evento_aux dynamic scroll cursor for
		select 1 as w_i_entidades,cdVerba as w_evento,upper(trim(DsVerba)) as w_nome,TpCategoria as w_TpCategoria,TpReferencia as w_TpReferencia 
		from tecbth_delivery.gp001_Verba 
		order by 2 asc
	do
		set w_cont = w_cont + 1;
		
		set w_tipo_pd=null;
		set w_unidade=null;
		set w_sai_rais=null;

		-- BTHSC-56554 / Ajuste no tipo do evento
		if w_TpCategoria  in ('P', 'V') then
			set w_tipo_pd = 'P'
		else
			set w_tipo_pd = 'D';
		end if;		
		
		if w_TpReferencia = 'H' then
			set w_unidade='Horas'
		elseif w_TpReferencia = 'P' then
			set w_unidade='Percentual'
		elseif w_TpReferencia = 'V' then
			set w_unidade='Valor'
		elseif w_TpReferencia = 'D' then
			set w_unidade='Dias'
		elseif w_TpReferencia = 'U' then
			set w_unidade='Unidade'
		end if;
		
		if exists(select 1 from #tempo1 where entidade = w_i_entidades and cdVerba = w_evento) then
			set w_sai_rais='S'
		else
			set w_sai_rais='N'
		end if;
		
		message 'Ent.: '||w_i_entidades||' Eve.: '||w_evento||' Nom.: '||w_nome||' Tip.: '||w_tipo_pd to client;
		
		insert into tecbth_delivery.evento_aux(evento,i_eventos,nome,tipo_pd,taxa,unidade,sai_rais,compoe_liq,compoe_hmes,digitou_form,classif_evento,retificacao,resc_mov,i_entidades) 
		values(w_evento,w_evento,w_nome,w_tipo_pd,0.0,w_unidade,w_sai_rais,'S','N','N',0,'B','N',w_i_entidades);
		
		if w_cont = 500 then
			set w_cont = 0;
			commit;
			message 'commit' to client;
		end if;		
	end for;
end
;

if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_evento_atu') then
	drop procedure cnv_evento_atu;
end if
;


;

call tecbth_delivery.cnv_evento_atu()
;

commit
;



if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_eventos') then
	drop procedure cnv_eventos;
end if
;

begin
	
	ooLoop: for oo as cnv_eventos dynamic scroll cursor for
		select distinct i_eventos as w_i_eventos,nome as w_nome,tipo_pd as w_tipo_pd,taxa as w_taxa,unidade as w_unidade,sai_rais as w_sai_rais,compoe_liq as w_compoe_liq,compoe_hmes as w_compoe_hmes,
		       	        digitou_form as w_digitou_form,classif_evento as w_classif_evento,evento as w_cods_conver,i_entidades as w_i_entidades 
		from tecbth_delivery.evento_aux 
	do	
-- BUG BTHSC-8214	
       --if  w_i_eventos >= 400 and w_tipo_pd in('P','D') then
		message 'Eve.: '||w_i_eventos||' Nom.: '||w_nome||' Tip.: '||w_tipo_pd to client;
		insert into bethadba.eventos(i_eventos,nome,tipo_pd,taxa,unidade,sai_rais,compoe_liq,compoe_hmes,digitou_form,classif_evento,cods_conversao,desativado,seq_impressao,
									codigo_tce,caracteristica, deduc_fundo_financ, natureza, i_atos, envia_fly_transparencia, montar_base_fgts_integral_afast, enviar_esocial)on existing skip
		values (w_i_eventos,w_nome,w_tipo_pd,w_taxa,w_unidade,w_sai_rais,w_compoe_liq,w_compoe_hmes,w_digitou_form,w_classif_evento,null,'N',null,w_i_eventos, 'F' ,'N', null, null, 'N', 'N', 'S'); 
      --end if;
	  -- BUG BTHSC-8214

	  insert into bethadba.hist_eventos(i_eventos,i_competencias, nome,tipo_pd,taxa,unidade,sai_rais,compoe_liq,compoe_hmes,digitou_form,classif_evento,cods_conversao,desativado,seq_impressao,
									codigo_tce,caracteristica, deduc_fundo_financ)on existing skip
		values (w_i_eventos,'2024-01-01', w_nome,w_tipo_pd,w_taxa,w_unidade,w_sai_rais,w_compoe_liq,w_compoe_hmes,w_digitou_form,w_classif_evento,null,'N',null,w_i_eventos, 'F','N'); 
	
	end for;
end;

commit;



-- PARTE 2 BTHSC-134414
insert into bethadba.eventos_hist_agrup_esocial (i_eventos, i_competencia_inicio ,codigo_esocial, nome_esocial, tabela_rubrica, rubrica_esocial, incidencia_previdencia, incidencia_irrf, incidencia_fgts, incidencia_contrato_sindical, incidencia_rpps)
on existing skip   SELECT
	 d.i_eventos AS ID_CLOUD,
	 '1990-01-01',
	id_cloud,
	nome as nome_esocial,
	1,
	if v.cdNaturezaRubrica = 0 then null else v.cdNaturezaRubrica endif as rubrica_esocial,
	case v.codIncCP 
	when 18003 then '11' 
	when 18001 then '00'
	when 18004 then '12'
	when 18015 then '51'
	when 18012 then '32'
	when 18006 then '22'
	END AS incidencia_previdencia,
	
	
	case v.codIncIRRF 
	when 19003 then '11'
	when 19046 then '79'
	when 19004 then '12'
	when 19005 then '13'
	when 19014 then '42'
	when 19020 then '52'
	when 19019 then '51'
	when 19013  then '79'
	when 19037 then '79'
	else '00'
	end as incidencia_irrf,
	
	case codIncFgts
	when 20002 then '11'
	when 20001 then '00'
	when 20003 then '12'
	end as incidencia_fgts,
	
	case codIncSind
	when 21001 then '00'
	when 21003 then '31'
	when 21002 then '11'	
	end as incidencia_contrato_sindical,

	case codIncCPRP
	when 22001 then '00'
	when 22002 then '11'
	when 22003 then '12'
	when 22004 then '31'
	when 22005 then '32'
	end as incidencia_rpps

FROM tecbth_delivery.gp001_verba v
LEFT JOIN tecbth_delivery.evento_aux d on d.evento = v.CdVerba
where id_cloud is not null 
commit;

-- PARTE 3 BTHSC-134414
insert into bethadba.itens_lista (i_caracteristicas, i_itens_lista, descricao, dt_expiracao)
values (23905, 'E', 'Eventual', '2999-12-31');
insert into bethadba.itens_lista (i_caracteristicas, i_itens_lista, descricao, dt_expiracao)
values (23905, 'P', 'Percentual', '2999-12-31');

-- PARTE 4 BTHSC-134414

INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20361, 12, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20358, 12, 'N', '2999-12-31');

INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20151, 12, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20166, 1, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20167, 2, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20278, 3, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20279, 4, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20280, 6, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20281, 7, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20282, 5, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20305, 8, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20312, 9, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20329, 10, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20330, 11, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20376, 13, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20377, 14, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20394, 15, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20396, 16, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20402, 17, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20416, 19, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20417, 20, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20513, 18, 'N', '2999-12-31');


-- Parte 5 BTHSC-134414

update bethadba.hist_eventos ev 
left join tecbth_delivery.gp001_verba v 
on ev.i_eventos = v.CdVerba 
left join tecbth_delivery.gp001_constanteCalculo cc 
on SIMILAR(V.DsVerba, cc.dsConstante) >= 35
set taxa = isnull(cc.vlConstante, 0);

update bethadba.EVENTOS ev 
left join tecbth_delivery.gp001_verba v 
on ev.i_eventos = v.CdVerba 
left join tecbth_delivery.gp001_constanteCalculo cc 
on SIMILAR(V.DsVerba, cc.dsConstante) >= 35
set taxa = isnull(cc.vlConstante, 0);


-- Parte 6 BTHSC-134414

if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_eventos_prop_adic') then
	drop procedure cnv_eventos_prop_adic;
end if;

begin
	ooLoop: for oo as cnv_eventos_prop_adic dynamic scroll cursor for
		select distinct i_eventos as w_i_eventos from bethadba.eventos
	do
		message '(Inserindo os adicionais dos eventos) ' || w_i_eventos to client;
	
		
			INSERT INTO Folharh.bethadba.eventos_prop_adic
			(i_caracteristicas, i_eventos, valor_numerico, valor_decimal, valor_data, valor_caracter, valor_hora, valor_texto)
			VALUES(20361, w_i_eventos, NULL, NULL, NULL, 'S', NULL, NULL);


			INSERT INTO Folharh.bethadba.eventos_prop_adic
			(i_caracteristicas, i_eventos, valor_numerico, valor_decimal, valor_data, valor_caracter, valor_hora, valor_texto)
			VALUES(20358, w_i_eventos, NULL, NULL, NULL, '1', NULL, NULL);


			INSERT INTO Folharh.bethadba.eventos_prop_adic
			(i_caracteristicas, i_eventos, valor_numerico, valor_decimal, valor_data, valor_caracter, valor_hora, valor_texto)
			VALUES(23905, w_i_eventos, NULL, NULL, NULL, 'P', NULL, NULL);

	end for;
end;
commit;




-- Parte 7 BTHSC-134414

update bethadba.eventos e
left join tecbth_delivery.gp001_MOVIMENTOATOLEGALTABELA mat 
on e.i_eventos = mat.cdchave
set e.descricao = mat.dsMovimentoAtoLegal
where e.descricao is null

-- Parte 8 BTHSC-134414
update bethadba.eventos e 
left join bethadba.eventos_prop_adic epc 
on e.i_eventos = epc.i_eventos 
set e.caracteristica = 'V'
where epc.valor_caracter ='E'


update bethadba.hist_eventos e 
left join bethadba.eventos_prop_adic epc 
on e.i_eventos = epc.i_eventos 
set e.caracteristica = 'V'
where epc.valor_caracter ='E'

update bethadba.eventos e 
left join bethadba.eventos_prop_adic epc 
on e.i_eventos = epc.i_eventos 
set e.caracteristica = 'F'
where epc.valor_caracter ='P'

update bethadba.hist_eventos e 
left join bethadba.eventos_prop_adic epc 
on e.i_eventos = epc.i_eventos 
set e.caracteristica = 'F'
where epc.valor_caracter ='P'