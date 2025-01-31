CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
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
		select distinct A.dsareaEstagio as w_nome,
					if A.cdnivEstagio = 128002 then 1 else 2 endif as w_nivel
		from tecbth_delivery.gp001_FuncionarioEstagio A
	do
		// *****  Tabela bethadba.pessoas
		set w_i_formacao = null;
		set w_dv = null;
		
		// *****  Tabela bethadba.pessoas_enderecos
		set w_tipo_endereco = null;
		set w_i_ruas = null;
		
		
		// *****  Converte tabela bethadba.pessoas
		select coalesce(max(i_formacoes),0)+1 
		into w_i_formacao
		from bethadba.formacoes;			

		insert into bethadba.formacoes(i_formacoes,nome,nivel_formacao)on existing skip 
		values(w_i_formacao,w_nome,w_nivel);	
	
	end for;
end;