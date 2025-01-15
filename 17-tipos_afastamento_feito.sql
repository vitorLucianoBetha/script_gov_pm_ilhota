--------------------------------------------------
-- 09) Tipos Afast 
--------------------------------------------------
--BUG BTHSC-8050 Concatenar código do afastamento do banco concorrente com o nome do afastamento no concorrente
insert into bethadba.tipos_afast(i_tipos_afast,i_tipos_movpes,descricao,classif,perde_temposerv,busca_var,dias_prev)on existing skip
WITH max_code AS (
    SELECT MAX(cdAfastamento) AS max_cdAfastamento
    FROM tecbth_delivery.gp001_motivoafastamento
),
ausencia_with_rownum AS  (
    SELECT 
        cdAusencia, 
        dsAusencia,
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
    NULL AS dias_prev
FROM tecbth_delivery.gp001_motivoafastamento

UNION ALL

SELECT 
    (SELECT max_cdAfastamento FROM max_code) + rn AS cdAusencia,
    NULL AS  i_tipos_movpes,
    cdAusencia || ' - ' || dsAusencia AS descricao,
    1 AS col4,
    'N' AS perde_temposerv,
    'N' AS busca_var,
    NULL AS dias_prev
FROM ausencia_with_rownum

commit;

UPDATE Folharh.bethadba.tipos_afast
SET classif=3
WHERE i_tipos_afast=1;
UPDATE Folharh.bethadba.tipos_afast
SET classif=3
WHERE i_tipos_afast=2;
UPDATE Folharh.bethadba.tipos_afast
SET classif=4
WHERE i_tipos_afast=3;
UPDATE Folharh.bethadba.tipos_afast
SET classif=5
WHERE i_tipos_afast=4;
UPDATE Folharh.bethadba.tipos_afast
SET classif=7
WHERE i_tipos_afast=5;
UPDATE Folharh.bethadba.tipos_afast
SET classif=2
WHERE i_tipos_afast=6;
UPDATE Folharh.bethadba.tipos_afast
SET classif=2
WHERE i_tipos_afast=7;
UPDATE Folharh.bethadba.tipos_afast
SET classif=7
WHERE i_tipos_afast=8;
UPDATE Folharh.bethadba.tipos_afast
SET classif=2
WHERE i_tipos_afast=9;
UPDATE Folharh.bethadba.tipos_afast
SET classif=5
WHERE i_tipos_afast=10;
UPDATE Folharh.bethadba.tipos_afast
SET classif=2
WHERE i_tipos_afast=11;
UPDATE Folharh.bethadba.tipos_afast
SET classif=2
WHERE i_tipos_afast=12;
UPDATE Folharh.bethadba.tipos_afast
SET classif=2
WHERE i_tipos_afast=13;
UPDATE Folharh.bethadba.tipos_afast
SET classif=2
WHERE i_tipos_afast=14;
UPDATE Folharh.bethadba.tipos_afast
SET classif=2
WHERE i_tipos_afast=15;
UPDATE Folharh.bethadba.tipos_afast
SET classif=2
WHERE i_tipos_afast=16;
UPDATE Folharh.bethadba.tipos_afast
SET classif=2
WHERE i_tipos_afast=17;