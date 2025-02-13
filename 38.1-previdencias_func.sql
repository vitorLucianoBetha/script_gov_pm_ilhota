-- ATENÇÃO NECESSARIO VERIFICAR A TABELA ABAIXO PARA IDENTIFICAR OS TIPOS DE PREVIDENCIA DO CONCORRENTE
-- NESTE CASO O UNICO PROBLEMA DE ILHOTA ERA O TIPO DE SEGREGAÇÃO NO PLANO DE PREVIDENCIA MUNICIPAL 
-- NO QUAL FOI CRIADO UM NOVO PLANO DE PREVIDENCIA NA PRODUÇÃO PARA A RESOLUÇÃO DESTE PROBLEMA

-- BTHSC-151481 Bug em Matrículas | Nível e Faixa salarial e Previdencia

--select * from tecbth_delivery.gp001_TIPOFUNDOPREVIDENCIA gt 

insert into bethadba.hist_planos_previd_func(i_entidades,i_funcionarios,i_planos_previd,dt_inicial,dt_final) on existing skip
select 1, gh.cdMatricula, 5, date(gh.dtInicio) as dataInicio, date(gh.dtFim)  as dataFim from tecbth_delivery.gp001_HISTORICOFUNDOPREVIDENCIA gh