insert into bethadba.tipos_cargos(i_tipos_cargos,descricao,classif)on existing skip
select CdTipoCArgo,DsTipoCargo,if CdTipoCargo = 1 then 
											2 
							   else 
									if CdTipoCargo in(2,5) then 
										1 
									else 
										0 
									endif
							   endif
from tecbth_delivery.gp001_TipoCargo
;

commit
;