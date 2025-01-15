-- BUG - BTHSC-8179  Planos de cargos e salários
insert into bethadba.planos_salariais(i_planos_salariais,nome,data_inicio,masc_classe,masc_referencia, limite_classe, limite_referencia)on existing skip
--values(1,'PLANO GERAL','1','!','###');
select cdEstruturaSalarial,dsEstruturaSalarial,'1','!',dsMascara, 1, 3 from gp001_SALARIOESTRUTURA

commit
;

--------------------------------------------------
-- 07) Planos Faixas 
--------------------------------------------------
insert into bethadba.planos_faixas(i_planos_salariais,i_faixas,num_direito_altsal)on existing skip
values (1,99,1);

commit
;
