
-- Tabela | bethadba.mediasvant
if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_medias_vant') then
	drop procedure cnv_medias_vant;
end if;

begin
	ooLoop: for oo as cnv_medias_vant dynamic scroll cursor for
		select distinct
tp.cdVerbaMedia as w_i_eventos,
'S' as w_agrupa, 
1 as w_classif,
2 as w_periodo,
case tp.cdTipoMedia
when 3 then 1
when 1 then 2
when 2 then 4
when 5 then 6
else 7 end as w_grupo,
'N' as w_mes_janeiro,
null as w_buscar_base,
CASE WHEN EXISTS (
            SELECT 1 
            FROM bethadba.eventos 
            WHERE nome LIKE '%13%') 
        THEN 'S' 
        ELSE 'N' 
    END AS w_eh_13adiant,
    'N' as w_media_sobre_media
from tecbth_delivery.gp001_TIPOMEDIA tp
left join tecbth_delivery.gp001_TIPOMEDIAINCIDENCIA gt 
on tp.cdTipoMedia = gt.cdTipoMedia 
order by tp.cdVerbaMedia

	do
		message '(Inserindo as médias e vantagens) ' || w_i_eventos to client;
	
		insert into bethadba.mediasvant (i_eventos, agrupa, classif, periodo, grupo, mes_janeiro, buscar_base, eh_13adiant, media_sobre_media) on existing skip
		values (w_i_eventos, w_agrupa, w_classif, w_periodo, w_grupo, w_mes_janeiro, w_buscar_base, w_eh_13adiant, w_media_sobre_media)
	end for;
end;


commit;

---------------------------------------------------------------------------


-- Tabela | bethadba.mediasvant_eve
if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_medias_vant_eve') then
	drop procedure cnv_medias_vant_eve;
end if;

begin
	ooLoop: for oo as cnv_medias_vant_eve dynamic scroll cursor for
select 
	tp2.cdVerbaMedia as w_i_eventos_medias,
	tp.cdVerba as w_i_eventos,
	1 as w_periodo,
	1 as w_valor,
	'S' as w_proporcional,
	null as w_i_tipos_bases,
	1 as w_atualizar_valor 
	from tecbth_delivery.gp001_TIPOMEDIAINCIDENCIA tp
	left join tecbth_delivery.gp001_tipoMedia tp2
	on tp.cdTipoMedia = tp2.cdTipoMedia

	do
		message '(Eventos de composição) ' || w_i_eventos to client;
	
		insert into bethadba.mediasvant_eve (i_eventos_medias, i_eventos, periodo, valor, proporcional, i_tipos_bases, atualizar_valor) on existing skip 
		values (w_i_eventos_medias, w_i_eventos, w_periodo, w_valor, w_proporcional, w_i_tipos_bases, w_atualizar_valor)
	
		
	end for;
end;
commit;

