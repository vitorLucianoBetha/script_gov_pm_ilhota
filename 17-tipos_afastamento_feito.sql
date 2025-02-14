--------------------------------------------------
-- 09) Tipos Afast 
--------------------------------------------------

-- BTHSC-141362 Bug em Matrículas | Afastamentos e Faltas não migraram
--- CRIA UMA TABELA AUXILIAR PARA INSERIR OS DADOS DE ANTES E DEPOIS DO CONCORRENTE PARA BETHA
--- CAMPOS faltas_antes, afastamento_antes

CREATE TABLE tecbth_delivery.gp001_MOTIVOAFASTAMENTOax (
	i_tipos_afast int NULL,
	i_tipos_movpes int NULL,
	descricao varchar(250) NULL,
	classif int NULL,
	perde_temposerv char(1) NULL,
	busca_var char(1) NULL,
	dias_prev int NULL,
	faltas_antes int NULL,
	afastamento_antes int NULL
);

--BUG BTHSC-8050 Concatenar código do afastamento do banco concorrente com o nome do afastamento no concorrente
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
call bethadba.pg_setoption('fire_triggers','off');
COMMIT;

insert into tecbth_delivery.gp001_MOTIVOAFASTAMENTOax(i_tipos_afast,i_tipos_movpes,descricao,classif,perde_temposerv,busca_var,dias_prev,faltas_antes,afastamento_antes)
WITH max_code AS (
    SELECT MAX(cdAfastamento) AS max_cdAfastamento
    FROM tecbth_delivery.gp001_motivoafastamento
),
ausencia_with_rownum AS  (
    SELECT 
        cdAusencia, 
        dsAusencia,
        cdAusencia as faltas_antes,
        ROW_NUMBER() OVER (ORDER BY cdAusencia) AS rn
    FROM tecbth_delivery.gp001_AUSENCIA
)
SELECT 
    cdAfastamento AS cdAfastamento,
    NULL AS i_tipos_movpes,
    cdAfastamento || ' - ' || DsAfastamento AS descricao,
    1 AS classif,
    'N' AS perde_temposerv,
    'N' AS busca_var,
    NULL AS dias_prev,
    null as faltas_antes,
    cdAfastamento as afastamento_antes
FROM tecbth_delivery.gp001_motivoafastamento

UNION ALL

SELECT 
    (SELECT max_cdAfastamento FROM max_code) + rn AS cdAusencia,
    NULL AS  i_tipos_movpes,
    cdAusencia || ' - ' || dsAusencia AS descricao,
    1 AS col4,
    'N' AS perde_temposerv,
    'N' AS busca_var,
    NULL AS dias_prev,
    faltas_antes,
    null as afastamento_antes
FROM ausencia_with_rownum

commit


--- ATUALIZA O i_tipos_afast PARA O ULTIMO ID PRESENTE NA TABELA DE TIPOS DE AFASTAMENTO
--- PARA POSTERIORMENTE FAZER O DE PARA NO AFASTAMENTO
update tecbth_delivery.gp001_MOTIVOAFASTAMENTOax set i_tipos_afast = (select max(ta.i_tipos_afast) + gp001_MOTIVOAFASTAMENTOax.i_tipos_afast from bethadba.tipos_afast ta)
where afastamento_antes is not null or faltas_antes = 2;

--- INSERE EFETIVAMENTE OS AFASTAMENTOS DO CONCORRENTE PARA BETHA
insert into bethadba.tipos_afast(i_tipos_afast,i_tipos_movpes,descricao,classif,perde_temposerv,busca_var,dias_prev) on existing skip
select i_tipos_afast,
			i_tipos_movpes,
			descricao,
			classif,
			perde_temposerv,
			busca_var,
			dias_prev
from tecbth_delivery.gp001_MOTIVOAFASTAMENTOax gm
where gm.afastamento_antes is not null or faltas_antes = 2

--- FALTAS JA ESTAVAM INSERIDAS NESTE CASO SOMENTE ATUALIZEI A TABELA AUXILIAR
update tecbth_delivery.gp001_MOTIVOAFASTAMENTOax set i_tipos_afast = 4 where i_tipos_afast = 36;
update tecbth_delivery.gp001_MOTIVOAFASTAMENTOax set i_tipos_afast = 5 where i_tipos_afast = 37;
update tecbth_delivery.gp001_MOTIVOAFASTAMENTOax set i_tipos_afast = 7 where i_tipos_afast = 39;
update tecbth_delivery.gp001_MOTIVOAFASTAMENTOax set i_tipos_afast = 8 where i_tipos_afast = 40;
update tecbth_delivery.gp001_MOTIVOAFASTAMENTOax set i_tipos_afast = 9 where i_tipos_afast = 41;
update tecbth_delivery.gp001_MOTIVOAFASTAMENTOax set i_tipos_afast = 10 where i_tipos_afast = 42;
update tecbth_delivery.gp001_MOTIVOAFASTAMENTOax set i_tipos_afast = 11 where i_tipos_afast = 43;

-- atualizando as classificações de acordo com o depara repassado pela implantação
update bethadba.tipos_afast set classif = 2 where i_tipos_afast = 22;
update bethadba.tipos_afast set classif = 2 where i_tipos_afast = 23;
update bethadba.tipos_afast set classif = 4 where i_tipos_afast = 24;
update bethadba.tipos_afast set classif = 5 where i_tipos_afast = 25;
update bethadba.tipos_afast set classif = 6 where i_tipos_afast = 26;
update bethadba.tipos_afast set classif = 2 where i_tipos_afast = 27;
update bethadba.tipos_afast set classif = 5 where i_tipos_afast = 28;
update bethadba.tipos_afast set classif = 9 where i_tipos_afast = 29;
update bethadba.tipos_afast set classif = 14 where i_tipos_afast = 30;
update bethadba.tipos_afast set classif = 5 where i_tipos_afast = 31;
update bethadba.tipos_afast set classif = 2 where i_tipos_afast = 32;
update bethadba.tipos_afast set classif = 2 where i_tipos_afast = 33;
update bethadba.tipos_afast set classif = 11 where i_tipos_afast = 35;
update bethadba.tipos_afast set classif = 7 where i_tipos_afast = 36;
update bethadba.tipos_afast set classif = 18 where i_tipos_afast = 38;
update bethadba.tipos_afast set classif = 18 where i_tipos_afast = 39;
update bethadba.tipos_afast set classif = 18 where i_tipos_afast = 40;
update bethadba.tipos_afast set classif = 2 where i_tipos_afast = 41;
update bethadba.tipos_afast set classif = 2 where i_tipos_afast = 42;
update bethadba.tipos_afast set classif = 2 where i_tipos_afast = 43;
update bethadba.tipos_afast set classif = 2 where i_tipos_afast = 44;
update bethadba.tipos_afast set classif = 2 where i_tipos_afast = 45;
update bethadba.tipos_afast set classif = 2 where i_tipos_afast = 46;
update bethadba.tipos_afast set classif = 5 where i_tipos_afast = 47;
update bethadba.tipos_afast set classif = 21 where i_tipos_afast = 49;
update bethadba.tipos_afast set classif = 14 where i_tipos_afast = 51;
update bethadba.tipos_afast set classif = 2 where i_tipos_afast = 52;
update bethadba.tipos_afast set classif = 21 where i_tipos_afast = 56;
update bethadba.tipos_afast set classif = 2 where i_tipos_afast = 59;