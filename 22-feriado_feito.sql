insert into bethadba.feriados 
select distinct
'F' as i_tipo_feriado,
cast(DtFeriado as date) as i_feriados,
'Feriado SURG' + ' - ' + cast(i_feriados as varchar(20)) as descricao,
'N' as ponto_facultativo 


from tecbth_delivery.gp001_FERIADO gf 