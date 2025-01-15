
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
	declare w_dep_irrf char(1);
	declare w_i_tipos_salfam char(1);
	declare w_fundo_ass char(1);
	declare w_fundo_prev char(1);
	declare w_curso_tec_sup char(1);

	ooLoop: for oo as cnv_dependentes_func dynamic scroll cursor for
		select distinct 1 as w_i_entidades, DependenteVerba.CdMatricula as w_CdMatricula,funcionario.CdMatricula as w_cdmatriculadep,funcionario.SqContrato as w_SqContrato,DependenteVerba.CdPessoa as w_CdPessoa,
			            Dependente.CdPessoaDependente as w_CdPessoaDependente,DependenteVerba.CdDependente as w_CdDependente,inEstuda as w_inEstuda
		from tecbth_delivery.gp001_DependenteVerba as DependenteVerba,tecbth_delivery.gp001_Dependente as Dependente,tecbth_delivery.gp001_funcionario as funcionario 
		where DependenteVerba.CdPessoa = Dependente.CdPessoa 
		and DependenteVerba.CdMatricula = funcionario.CdMatricula 
		and DependenteVerba.CdDependente = Dependente.CdDependente  
		order by 1,2,3 asc	
	do
		
		// *****  Inicializa Variaveis
		set w_i_funcionarios=null;
		set w_i_dependentes=null;
		set w_dep_irrf=null;
		set w_i_tipos_salfam=null;
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
		
		if exists(select 1 from tecbth_delivery.gp001_dependenteverba where CdMatricula = w_cdmatriculadep and SqContrato = Sqcontrato and CdPessoa = w_cdPessoa and	CdDependente = w_CdDependente and CdVerba = 1158) then
			set w_dep_irrf='S'
		else
			set w_dep_irrf='N'
		end if;
		
		if exists(select 1 from tecbth_delivery.gp001_dependenteverba where CdMatricula = w_cdmatriculadep and SqContrato = Sqcontrato and CdPessoa = w_cdPessoa and CdDependente = w_CdDependente and CdVerba = 1004) then
			set w_i_tipos_salfam=1
		elseif exists(select 1 from tecbth_delivery.gp001_dependenteverba where CdMatricula = w_cdmatriculadep and SqContrato = Sqcontrato and CdPessoa = w_cdPessoa and CdDependente = w_CdDependente and CdVerba = 1166) then
				set w_i_tipos_salfam=2
			else
				set w_i_tipos_salfam=5			
		end if;
		
		if exists(select 1 from tecbth_delivery.gp001_dependenteverba where CdMatricula = w_cdmatriculadep and SqContrato = Sqcontrato and CdPessoa = w_cdPessoa and CdDependente = w_CdDependente and CdVerba = 1004) then
			set w_i_tipos_salfam=1
		elseif exists(select 1 from tecbth_delivery.gp001_dependenteverba where CdMatricula = w_cdmatriculadep and SqContrato = Sqcontrato and CdPessoa = w_cdPessoa and	CdDependente = w_CdDependente and CdVerba = 1166) then
			set w_i_tipos_salfam=2
		else
			set w_i_tipos_salfam=5		
		end if;
		
		set w_fundo_ass='N';
		set w_fundo_prev='N';
		
		if w_inEstuda != 0 then
			set w_curso_tec_sup='S'
		else
			set w_curso_tec_sup='N'
		end if;
		
		if exists(select 1 from bethadba.hist_funcionarios as b where b.i_entidades = w_i_entidades and b.i_funcionarios = w_i_funcionarios and b.fundo_prev = 'S' and b.dt_alteracoes = (select max(a.dt_alteracoes) from bethadba.hist_funcionarios as a where a.i_entidades = b.i_entidades and a.i_funcionarios = b.i_funcionarios)) then set w_fundo_ass='N'; set w_fundo_prev='S'; set w_i_tipos_salfam=2 end if; if exists(select 1 from bethadba.hist_funcionarios as b where b.i_entidades = w_i_entidades and	b.i_funcionarios = w_i_funcionarios and	b.prev_federal = 'S' and b.dt_alteracoes = (select max(a.dt_alteracoes) from bethadba.hist_funcionarios as a where a.i_entidades = b.i_entidades and a.i_funcionarios = b.i_funcionarios)) then set w_fundo_ass='N'; set w_fundo_prev='N'; set w_i_tipos_salfam=1 end if;
		
		
			if w_i_dependentes is not null then
				message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Dep.: '||w_i_dependentes to client;
				insert into bethadba.dependentes_func(i_entidades,i_funcionarios,i_dependentes,dep_irrf,i_tipos_salfam,fundo_ass,fundo_prev,curso_tec_sup,dt_inicio_curso,dt_final_curso,fundo_financ)on existing update
				values (w_i_entidades,w_i_funcionarios,w_i_dependentes,w_dep_irrf,w_i_tipos_salfam,w_fundo_ass,w_fundo_prev,w_curso_tec_sup,null,null,'N');
			
		end if;
		
	end for;
end;

commit
