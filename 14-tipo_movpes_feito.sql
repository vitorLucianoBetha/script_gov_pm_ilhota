insert into bethadba.tipos_movpes(i_tipos_movpes,descricao,classif,codigo_tce)on existing skip
select CdTipoMovimentoAto,DsTipoMovimentoAto,0,null 
from tecbth_delivery.gp001_AtoTipoMovimento
;            
 
commit
;


