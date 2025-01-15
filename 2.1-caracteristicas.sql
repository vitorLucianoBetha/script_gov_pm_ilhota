insert into bethadba.caracteristicas(i_caracteristicas,nome,tipo_dado,obrigatorio,tamanho)on existing skip 
values(1,'Ficha de Registro',2,'N',null) 
;

insert into bethadba.caracteristicas(i_caracteristicas,nome,tipo_dado,obrigatorio,tamanho)on existing skip 
values(2,'Data Final do Contrato',4,'N',null) 
;

insert into bethadba.caracteristicas(i_caracteristicas,nome,tipo_dado,obrigatorio,tamanho)on existing skip 
values(3,'Concursado (S-Sim/N-Nao)',7,'N',null) 
;

insert into bethadba.caracteristicas(i_caracteristicas,nome,tipo_dado,obrigatorio,tamanho)on existing skip 
values(4,'Data Concurso',4,'N',null) 
;

insert into bethadba.caracteristicas(i_caracteristicas,nome,tipo_dado,obrigatorio,tamanho)on existing skip 
values(5,'Nr Inscricao Concurso',7,'N',null) 
;

insert into bethadba.caracteristicas(i_caracteristicas,nome,tipo_dado,obrigatorio,tamanho)on existing skip 
values(6,'Conselho Regional',7,'N',null) 
;

insert into bethadba.caracteristicas(i_caracteristicas,nome,tipo_dado,obrigatorio,tamanho)on existing skip 
values(7,'Contribuinte Individual(INSS)',7,'N',null) 
;

commit
;

CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;