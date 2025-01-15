DECLARE @cdMatricula INT,
        @dtCompetencia DATE,
        @previous_cdMatricula INT,
        @row_number INT

-- Inicializando variáveis
SET @previous_cdMatricula = NULL
SET @row_number = 0

-- Cursor para percorrer as linhas da tabela
DECLARE cursor_hist CURSOR FOR
SELECT cdMatricula, dtCompetencia
FROM tecbth_delivery.gp001_HISTORICOALTERACAOCADASTRAL
ORDER BY cdMatricula, dtCompetencia

-- Abrindo o cursor
OPEN cursor_hist

-- Fetch a primeira linha do cursor
FETCH cursor_hist INTO @cdMatricula, @dtCompetencia

-- Loop até que não haja mais linhas
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Incrementar ou resetar o row_number baseado no valor atual e anterior de cdMatricula
    IF @previous_cdMatricula = @cdMatricula
        SET @row_number = @row_number + 1
    ELSE
        SET @row_number = 1

    -- Inserir os dados na tabela alvo
    INSERT INTO bethadba.atos_func (i_entidades, i_funcionarios, i_atos_func, i_ATOS, i_tipos_movpes, manual, dt_movimento)
    VALUES (1, @cdMatricula, @row_number, 12, 1, 'N', @dtCompetencia)

    -- Atualizar a variável previous_cdMatricula
    SET @previous_cdMatricula = @cdMatricula

    -- Fetch a próxima linha do cursor
    FETCH cursor_hist INTO @cdMatricula, @dtCompetencia
END

-- Fechar e desalocar o cursor
CLOSE cursor_hist
DEALLOCATE cursor_hist

select * from bethadba.atos_func af 
