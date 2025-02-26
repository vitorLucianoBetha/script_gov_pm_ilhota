-- ATENÇÃO NECESSARIO VERIFICAR A TABELA ABAIXO PARA IDENTIFICAR OS TIPOS DE PREVIDENCIA DO CONCORRENTE
-- NESTE CASO O UNICO PROBLEMA DE ILHOTA ERA O TIPO DE SEGREGAÇÃO NO PLANO DE PREVIDENCIA MUNICIPAL 
-- NO QUAL FOI CRIADO UM NOVO PLANO DE PREVIDENCIA NA PRODUÇÃO PARA A RESOLUÇÃO DESTE PROBLEMA

-- BTHSC-151481 Bug em Matrículas | Nível e Faixa salarial e Previdencia

--select * from tecbth_delivery.gp001_TIPOFUNDOPREVIDENCIA gt 
-- NECESSARIO VERIFICAR SE NÃO EXISTEM MAIS PLANOS DE PREVIDENCIA, NESTE CASO SÓ EXISTIA O 1, O 0 ERA PARA PREVIDENCIA FEDERAL

insert into bethadba.hist_planos_previd_func(i_entidades,i_funcionarios,i_planos_previd,dt_inicial,dt_final) on existing skip
select 1, gh.cdMatricula, 5, date(gh.dtInicio) as dataInicio, date(gh.dtFim)  as dataFim from tecbth_delivery.gp001_HISTORICOFUNDOPREVIDENCIA gh
-- no caso de existir planos 0 adicionar (where cdFundoPrevidencia = 1) também rodar o update abaixo

update bethadba.hist_funcionarios as f set f.fundo_prev = 'S' where exists(select first 1 from tecbth_delivery.gp001_HISTORICOFUNDOPREVIDENCIA a 
where a.cdMatricula = f.i_funcionarios and a.cdFundoPrevidencia = 1 and f.dt_alteracoes between a.dtInicio and a.dtFim)

/* somente quando há planos 0
update bethadba.hist_funcionarios as f set f.prev_federal = 'S' where exists(select first 1 from tecbth_delivery.gp001_HISTORICOFUNDOPREVIDENCIA a 
where a.cdMatricula = f.i_funcionarios and a.cdFundoPrevidencia = 0 and f.dt_alteracoes between a.dtInicio and a.dtFim)
*/