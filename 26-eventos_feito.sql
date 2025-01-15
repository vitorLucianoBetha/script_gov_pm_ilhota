if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_evento_aux') then
	drop procedure cnv_evento_aux;
end if
;
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

create procedure tecbth_delivery.cnv_evento_atu()
begin
	
end
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
									codigo_tce,deduc_fundo_financ, natureza, i_atos, envia_fly_transparencia, montar_base_fgts_integral_afast, enviar_esocial)on existing skip
		values (w_i_eventos,w_nome,w_tipo_pd,w_taxa,w_unidade,w_sai_rais,w_compoe_liq,w_compoe_hmes,w_digitou_form,w_classif_evento,null,'N',null,w_i_eventos,'N', null, null, 'N', 'N', 'S'); 
      --end if;
	  -- BUG BTHSC-8214

	  insert into bethadba.hist_eventos(i_eventos,i_competencias, nome,tipo_pd,taxa,unidade,sai_rais,compoe_liq,compoe_hmes,digitou_form,classif_evento,cods_conversao,desativado,seq_impressao,
									codigo_tce,deduc_fundo_financ)on existing skip
		values (w_i_eventos,'2024-01-01', w_nome,w_tipo_pd,w_taxa,w_unidade,w_sai_rais,w_compoe_liq,w_compoe_hmes,w_digitou_form,w_classif_evento,null,'N',null,w_i_eventos,'N'); 
	
	end for;
end

commit;
-- RUBRICA BUG BTHSC-7812
insert into bethadba.eventos_hist_agrup_esocial (i_eventos,i_competencia_inicio,codigo_esocial)
on existing skip   SELECT
	 d.i_eventos AS ID_CLOUD,'1990-01-01',cdnaturezarubrica AS NAT_RUBRICA
FROM gp001_verba v
LEFT JOIN tecbth_delivery.evento_aux d on d.evento = v.CdVerba
where id_cloud is not null and nat_rubrica <>0






UPDATE eventos
SET compoe_liq = 'N'
where nome like '%base%' AND compoe_liq = 'S'

