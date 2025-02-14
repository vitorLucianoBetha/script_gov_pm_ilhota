-- BTHSC-138611 Bug em Matrículas | Afastamento, Lotação, Horas por mês, Parecer do Controle Interno
insert into bethadba.locais_mov(i_entidades,i_funcionarios,i_locais_trab,dt_inicial,dt_final,i_horarios,principal) on existing skip
select 1,
			a.CdMatricula,
			CdLocalTransf,
			dataInicial = date(isnull((select max(c.DtTransferencia) from tecbth_delivery.gp001_HISTORICOLOTACAO c where a.CdMatricula = c.CdMatricula and a.SqContrato = c.SqContrato and c.DtTransferencia < a.DtTransferencia),b.DtAdmissao)),
			dataFinal = date(a.DtTransferencia),
			1,
			'N'
from tecbth_delivery.gp001_HISTORICOLOTACAO a
join tecbth_delivery.gp001_FUNCIONARIO b on a.CdMatricula = b.cdMatricula and a.SqContrato = b.SqContrato
union all 
select 1,
			b.cdMatricula,
			b.CdLocal,
			dataInicial = date(isnull((select max(c.DtTransferencia) from tecbth_delivery.gp001_HISTORICOLOTACAO c where b.CdMatricula = c.CdMatricula and b.SqContrato = c.SqContrato),b.DtAdmissao)),
			dataFinal = null,
			1,
			'N'
from tecbth_delivery.gp001_FUNCIONARIO b