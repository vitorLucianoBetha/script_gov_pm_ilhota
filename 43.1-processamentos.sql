-- Insere os processamentos para inserir as datas de pagamento após
insert into bethadba.processamentos on existing skip
select distinct 1,
			80,
			left(w_DtPagamento,8) + '01' as competencia,
			1,
			w_DtPagamento,
			w_DtPagamento,
			null,
			'N',
			null,
			'S',
			null,
			null,
			null,
			'N'
from (
select 1 as w_i_entidades,
			cdMatricula as w_CdMatricula,
			Sqcontrato as w_Sqcontrato,
			(select first i_periodos 
			into w_i_periodos 
			from bethadba.periodos 
			where i_entidades = 1 
			and i_funcionarios = w_CdMatricula 
			and dt_aquis_ini = w_DtInicioPeriodo) as periodo,
			date(DtInicioPeriodo) as w_DtInicioPeriodo,
			date(DtInicioConcessao) as w_dt_gozo_ini,
			date(dtFimConcessao) as w_dt_gozo_fin,
			CASE 
        		WHEN ISNUMERIC(NrDiasFeriasConcedidas) = 0 
        		THEN cast(LEFT(NrDiasFeriasConcedidas, CHARINDEX(',', NrDiasFeriasConcedidas) - 1) as int)
        		ELSE NrDiasFeriasConcedidas 
    		END AS numero_arredondado,
			if isnull(numero_arredondado,0) = 0 then datediff(day,w_dt_gozo_ini,w_dt_gozo_fin) + 1 else isnull(numero_arredondado,0) endif as w_NrDiasFeriasConcedidas,
      		NrDiasAbonoConcedidas as w_NrDiasAbonoConcedidas,
      		isnull(date(DtPagamento),w_dt_gozo_ini) as w_DtPagamento
from tecbth_delivery.gp001_PERIODOCONCESSAO
order by 1,2,3,4 asc) as test

-- Insere os processamentos de ferias para inserir as datas de pagamento
insert into bethadba.ferias_proc on existing skip
select 1,
			w_CdMatricula,
			(select first i_ferias from bethadba.periodos_ferias pf where i_entidades = 1 and i_funcionarios = w_CdMatricula and i_periodos = periodo
			and tipo = 2 and dt_periodo = w_dt_gozo_ini) as ferias,
			80,
			left(w_DtPagamento,8) + '01' as competencia,
			1,
			1
from (
select 1 as w_i_entidades,
			cdMatricula as w_CdMatricula,
			Sqcontrato as w_Sqcontrato,
			(select first i_periodos 
			into w_i_periodos 
			from bethadba.periodos 
			where i_entidades = 1 
			and i_funcionarios = w_CdMatricula 
			and dt_aquis_ini = w_DtInicioPeriodo) as periodo,
			date(DtInicioPeriodo) as w_DtInicioPeriodo,
			date(DtInicioConcessao) as w_dt_gozo_ini,
			date(dtFimConcessao) as w_dt_gozo_fin,
			CASE 
        		WHEN ISNUMERIC(NrDiasFeriasConcedidas) = 0 
        		THEN cast(LEFT(NrDiasFeriasConcedidas, CHARINDEX(',', NrDiasFeriasConcedidas) - 1) as int)
        		ELSE NrDiasFeriasConcedidas 
    		END AS numero_arredondado,
			if isnull(numero_arredondado,0) = 0 then datediff(day,w_dt_gozo_ini,w_dt_gozo_fin) + 1 else isnull(numero_arredondado,0) endif as w_NrDiasFeriasConcedidas,
      		NrDiasAbonoConcedidas as w_NrDiasAbonoConcedidas,
      		isnull(date(DtPagamento),w_dt_gozo_ini) as w_DtPagamento
from tecbth_delivery.gp001_PERIODOCONCESSAO
order by 1,2,3,4 asc) as test
where ferias is not null