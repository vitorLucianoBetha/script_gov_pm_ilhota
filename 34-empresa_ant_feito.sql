
begin
	// *****  Tabela bethadba.empresas_ant
	declare w_i_pessoas integer;
	declare w_i_empresas_ant integer;
	declare w_i_empresas integer;
	declare w_observacao char(200);

	// *****  Variaveis auxiliares
	declare w_i_pessoas_aux integer;
	
	// **** Insere a pessoa da empresa anterior
	if not exists(select 1 from bethadba.pessoas where nome = 'EMPRESA EXPERIENCIA ANTERIOR') then
		set w_i_pessoas_aux=null;
		
		select coalesce(max(i_pessoas),0)+1 
		into w_i_pessoas_aux 
		from bethadba.pessoas;
		
		insert into bethadba.pessoas(i_pessoas,dv,nome,nome_fantasia,tipo_pessoa)on existing skip
		values (w_i_pessoas_aux,bethadba.dbf_calcmod11(w_i_pessoas_aux),'EMPRESA EXPERIENCIA ANTERIOR',null,'J');
		
		insert into bethadba.pessoas_juridicas(i_pessoas)on existing skip
		values(w_i_pessoas_aux);
	end if;
	ooLoop: for oo as cnv_empresas_ant dynamic scroll cursor for
		select 1 as w_i_entidades,HistoricoTempoServico.cdPessoa as w_cdPessoa,dtDe as w_dt_inicial,dtAte as w_dt_final,cdmatricula as w_cdmatricula,SqContrato as w_SqContrato,
		  	  cdVinculoEmpregaticio as w_cdVinculoEmpregaticio,trim(upper(NmCargo)) as w_desc_funcao,NmEmpresa as w_NmEmpresa 
		from tecbth_delivery.gp001_HistoricoTempoServico as HistoricoTempoServico,tecbth_delivery.gp001_Pessoa as Pessoa  
		where HistoricoTempoServico.CdPessoa = Pessoa.CdPessoa  
		order by 1,2,4 asc	
	do
		
		// **** Inicializa
		set w_i_pessoas = null;
		set w_i_empresas_ant = null;
		set w_i_empresas = null;
		set w_observacao = null;
		
		// *****  Converte bethadba.empresas_ant
		select depois_1 
		into w_i_pessoas 
		from tecbth_delivery.antes_depois 
		where tipo = 'P' 
		and antes_1 = w_i_entidades 
		and antes_2 = w_cdPessoa;
		
		select coalesce(max(i_empresas_ant),0)+1 
		into w_i_empresas_ant 
		from bethadba.empresas_ant 
		where i_pessoas = w_i_pessoas;
		
		select i_pessoas 
		into w_i_empresas 
		from bethadba.pessoas 
		where nome = 'EMPRESA EXPERIENCIA ANTERIOR';
		
		if w_cdmatricula != 0 then
			set w_observacao=string('Funcionario : ')+string(w_cdMatricula||'0'||w_SqContrato)
		else
			set w_observacao=null
		end if;
		
		if(w_NmEmpresa is not null) and (trim(w_NmEmpresa) != '') then
			if trim(w_observacao) != '' then
				set w_observacao=w_observacao+' Empresa: '+trim(w_NmEmpresa)
			else
				set w_observacao='Empresa: '+trim(w_NmEmpresa)
			end if
		end if;		

		message 'Pes.: '||w_i_pessoas||' Emp. Ant.: '||w_i_empresas_ant||' Emp Atu.: '||w_i_empresas||' Ini.: '||w_dt_inicial||' Fin.: '||w_dt_final to client;
		
		insert into bethadba.empresas_ant(i_pessoas,i_empresas_ant,i_empresas,i_cbo,dt_inicial,dt_final,observacao,contato,trab_rural,multiplic,dt_ini_prevfed,dt_fin_prevfed,dt_ini_prevest,
										  dt_fin_prevest,dt_ini_prevmun,dt_fin_prevmun,dt_ini_outras,dt_fin_outras)on existing skip
		values (w_i_pessoas,w_i_empresas_ant,w_i_empresas,null,w_dt_inicial,w_dt_final,w_observacao,null,'N',1.0,null,null,null,null,null,null,null,null);
				
	end for;
end
;   

--where  cdMatricula in( 159) and w_dtCompetencia between '1900-01-01 00:00:00.000000' and '2002-01-01 00:00:00.000000'
rollback;
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;