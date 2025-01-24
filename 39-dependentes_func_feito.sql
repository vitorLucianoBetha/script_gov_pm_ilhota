
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;

if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_dependentes_func') then
	drop procedure cnv_dependentes_func;
end if
;

begin
	// *****  Tabela bethadba.dependentes_func
	declare w_i_funcionarios integer;
	declare w_i_dependentes integer;
	declare w_fundo_ass char(1);
	declare w_fundo_prev char(1);
	declare w_curso_tec_sup char(1);

	ooLoop: for oo as cnv_dependentes_func dynamic scroll cursor for
	
SELECT 
    1 AS w_i_entidades, 
    dv.CdMatricula AS w_CdMatricula, 
    f.CdMatricula AS w_cdmatriculadep, 
    f.SqContrato AS w_SqContrato, 
    dv.CdPessoa AS w_CdPessoa,
    d.CdPessoaDependente AS w_CdPessoaDependente, 
    dv.CdDependente AS w_CdDependente, 
    MIN(CASE WHEN dv.cdVerba = 1004 THEN 1 ELSE 5 END) AS w_i_tipos_salfam, 
    MAX(CASE WHEN dv.cdVerba = 1158 THEN 'S' ELSE 'N' END) AS w_dep_irrf,
    inEstuda as w_inEstuda
FROM tecbth_delivery.gp001_DependenteVerba AS dv
JOIN tecbth_delivery.gp001_Dependente AS d 
    ON dv.CdPessoa = d.CdPessoa 
    AND dv.CdDependente = d.CdDependente  
JOIN tecbth_delivery.gp001_funcionario AS f 
    ON dv.CdMatricula = f.CdMatricula 
	WHERE F.dtRescisao is null
GROUP BY 
    dv.CdMatricula, 
    f.CdMatricula, 
    f.SqContrato, 
    dv.CdPessoa,
    d.CdPessoaDependente, 
    dv.CdDependente,
    inEstuda

	do
		
		// *****  Inicializa Variaveis
		set w_i_funcionarios=null;
		set w_i_dependentes=null;
		set w_fundo_ass=null;
		set w_fundo_prev=null;
		set w_curso_tec_sup=null;
		
		// *****  Converte tabela bethadba.dependentes_func		
		set w_i_funcionarios=cast(w_cdMatricula as integer);
		
		select first depois_1 
		into w_i_dependentes 
		from tecbth_delivery.antes_depois 
		where tipo = 'P' 
		and antes_1 = w_i_entidades 
		and antes_2 = w_CdPessoaDependente;
		
		set w_fundo_ass='N';
		set w_fundo_prev='N';
		
		if w_inEstuda != 0 then
			set w_curso_tec_sup='S'
		else
			set w_curso_tec_sup='N'
		end if;
		if w_i_dependentes is not null then
				message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Dep.: '||w_i_dependentes to client;
				insert into bethadba.dependentes_func(i_entidades,i_funcionarios,i_dependentes,dep_irrf,i_tipos_salfam,fundo_ass,fundo_prev,curso_tec_sup,dt_inicio_curso,dt_final_curso,fundo_financ)on existing update
				values (w_i_entidades,w_i_funcionarios,w_i_dependentes,w_dep_irrf,w_i_tipos_salfam,w_fundo_ass,w_fundo_prev,w_curso_tec_sup,null,null,'N');
		end if;
		
	end for;
end;

commit;