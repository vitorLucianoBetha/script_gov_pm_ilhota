insert into bethadba.tipos_atos(i_tipos_atos,nome,classif,gera_tc)
select distinct CdTipoDocumentoLegal+10,DsTipoDocumentoLegal,
if DsTipoDocumentoLegal like '%decre%' then 1 else
if DsTipoDocumentoLegal like '%Port%' then 4 else
if DsTipoDocumentoLegal like '%Resol%' then 5 else
if DsTipoDocumentoLegal like '%Edit%' then 2 else
if DsTipoDocumentoLegal = 'Lei' then 3 else
if DsTipoDocumentoLegal like '%conv%' then 41 else
if DsTipoDocumentoLegal like '%medi%provis%' then 42 else
if DsTipoDocumentoLegal like '%lei%aut%' then 43 else
if DsTipoDocumentoLegal like '%Contra%' then 7 else
if DsTipoDocumentoLegal like '%Outro%' then 11  else
if DsTipoDocumentoLegal like '%lei%compl%' then 18 else
if DsTipoDocumentoLegal like 'lei%mun%' then 21 else
if DsTipoDocumentoLegal like '%term%poss%' then 14 else 1  endif endif endif endif endif endif endif endif endif endif endif endif endif,'S' 
 from tecbth_delivery.gp001_TipoDocumentoLegal 
where CdTipoDocumentoLegal != 0;  
commit
; 


