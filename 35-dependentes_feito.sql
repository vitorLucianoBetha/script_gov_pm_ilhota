
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;
--------------------------------------------------
-- 31) Dependentes
--------------------------------------------------
if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_dependentes') then
	drop procedure cnv_dependentes;
end if
;
-- BUG BTHSC-7421  as datas finais relacionadas aos dependentes estão incorretas
begin
	// *****  Tabela bethadba.dependentes
	declare w_i_pessoas integer;
	declare w_i_dependentes integer;
	declare w_grau char(1);
	declare w_dt_casamento date;
	ooLoop: for oo as cnv_dependentes dynamic scroll cursor for 
			
select distinct 1 as w_i_entidades,d.cdPessoa as w_CdPessoa,d.cdDependente as w_cdDependente,d.CdPessoaDependente as w_CdPessoaDependente,d.cdGrauDependencia as w_CdGrauDependencia,
				   d.CdMotivoInicioRelacao as w_CdMotivoInicioRelacao,date(d.DtInicioRelacao) as w_DtInicioRelacao,dtsuspensao as dt_final
			FROM tecbth_delivery.GP001_PESSOA p
INNER JOIN tecbth_delivery.GP001_DEPENDENTE d ON d.cdPessoaDependente = p.CdPessoa
INNER JOIN tecbth_delivery.gp001_FUNCIONARIO f ON f.CdPessoa = d.CdPessoa
LEFT JOIN tecbth_delivery.gp001_DEPENDENTEVERBA dv ON f.cdMatricula = dv.cdMatricula
LEFT JOIN tecbth_delivery.gp001_VERBA v ON dv.CdVerba = v.CdVerba
WHERE d.CdPessoa IS NOT NULL
AND d.CdDependente = dv.CdDependente
			--and d.cdPessoa =14
			order by 1,2 asc
	do
		// *****  Inicializa Variaveis
		set w_i_pessoas=null;
		set w_i_dependentes=null;
		set w_grau=null;
		set w_dt_casamento=null;
		
		// *****  Converte tabela bethadba.dependentes
		select depois_1 
		into w_i_pessoas 
		from tecbth_delivery.antes_depois 
		where tipo = 'P' 
		and antes_1 = w_i_entidades 
		and antes_2 = w_CdPessoa;
		
		select depois_1 
		into w_i_dependentes 
		from tecbth_delivery.antes_depois 
		where tipo = 'P' 
		and antes_1 = w_i_entidades 
		and	antes_2 = w_CdPessoaDependente;

		if w_cdGrauDependencia = 1 then
			set w_grau=1
		elseif w_cdGrauDependencia in(2) then
			set w_grau=2
		elseif w_cdGrauDependencia in(3) then
			set w_grau=3
		elseif w_cdGrauDependencia = 8 then
			set w_grau=6
		elseif w_cdGrauDependencia = 11 then
			set w_grau=7
		elseif w_cdGrauDependencia = 21 then 
			set w_grau = 1
		else
			set w_grau=9
		end if;
		
		if w_CdMotivoInicioRelacao in(7,8) then
			set w_dt_casamento=w_DtInicioRelacao
		else
			set w_dt_casamento=null
		end if;
		
		if w_i_dependentes is not null and w_i_pessoas is not null then
			if not exists(select 1 from bethadba.dependentes where i_pessoas = w_i_pessoas and i_dependentes = w_i_dependentes) then
				message 'Pes.: '||w_i_pessoas||' Dep.: '||w_i_dependentes||' Grau.: '||w_grau to client;
				
				insert into bethadba.dependentes(i_pessoas,i_dependentes,grau,dt_casamento,dt_ini_depende,dt_fin_depende)on existing skip
				values(w_i_pessoas,w_i_dependentes,w_grau,w_dt_casamento,w_DtInicioRelacao,dt_final) 
			end if
		end if;
	end for;
end;


