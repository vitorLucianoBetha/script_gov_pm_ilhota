CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
call bethadba.pg_setoption('fire_triggers','off');	
commit;

if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_formacao') then
	drop procedure cnv_formacao;
end if;

begin
	// *****  Tabela bethadba.pessoas
	declare w_i_formacao integer;
	declare w_dv tinyint;
	
	// *****  Tabela bethadba.pessoas_enderecos
	declare w_tipo_endereco char(1);
	declare w_i_ruas integer;
	
	ooLoop: for oo as cnv_formacao dynamic scroll cursor for
		select 1 as w_entidade,
					A.cdMatricula as w_funcionario,
					(select first i_formacoes from bethadba.formacoes where nome = A.dsareaEstagio) as w_formacao,
					(select first depois_1 from tecbth_delivery.antes_depois ad where ad.tipo = 'E' and antes_2 = A.ID_ENT_EXT_INST_ENSINO) as w_pessoa,
					(select first f.dt_admissao from bethadba.funcionarios f where f.i_funcionarios = A.cdMatricula) as w_dt_inicial,
					date(A.dtPrevistaTerminoEstagio) as w_dt_final,
					'S' as w_seguro,
					A.nrApoliceSeguroEstagio as w_numero_apolice,
					'S' as w_obrigatorio,
					if A.cdnivEstagio = 128002 then 2 else 3 endif as w_nivel
		from tecbth_delivery.gp001_FuncionarioEstagio A
	do
		// *****  Tabela bethadba.pessoas
		set w_i_formacao = null;
		set w_dv = null;
		
		// *****  Tabela bethadba.pessoas_enderecos
		set w_tipo_endereco = null;
		set w_i_ruas = null;
		
		
		// *****  Converte tabela bethadba.pessoas
		insert into bethadba.estagios(i_entidades,i_funcionarios,i_formacoes,i_pessoas,dt_inicial,dt_final,seguro_vida,num_apolice,estagio_obrigatorio,nivel_curso,periodo,fase)on existing skip 
		values(w_entidade,w_funcionario,w_formacao,w_pessoa,w_dt_inicial,w_dt_final,w_seguro,w_numero_apolice,w_obrigatorio,w_nivel,1,1);	
	
	end for;
end;