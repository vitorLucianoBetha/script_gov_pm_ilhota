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
update tecbth_delivery.gp001_MOTIVOAFASTAMENTOax set i_tipos_afast = (select max(ta.i_tipos_afast) + gp001_MOTIVOAFASTAMENTOax.i_tipos_afast from bethadba.tipos_afast ta);

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