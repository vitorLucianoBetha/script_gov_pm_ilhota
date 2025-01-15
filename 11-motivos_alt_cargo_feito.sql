delete bethadba.motivos_altcar
;

insert into bethadba.motivos_altcar(i_motivos_altcar,descricao)on existing skip
select CdMotivo,DsMotivo 
from tecbth_delivery.gp001_motivoevolucao
;
commit;