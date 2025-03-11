if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_sindicatos') then
	drop procedure cnv_sindicatos;
end if;

begin
	// *****  Tabela bethadba.pessoas
	declare w_i_pessoas integer;
	declare w_dv tinyint;
	declare w_ddd char(2);	
	
	// *****  Tabela bethadba.pessoas_enderecos
	declare w_tipo_endereco char(1);
	declare w_i_ruas integer;
	
	ooLoop: for oo as cnv_sindicados dynamic scroll cursor for
		select 1 as w_i_entidades,
			cdSindicato as w_cdSindicato,
			upper(DsSindicato) as w_nome,
			DsEndereco as w_nome_rua,
			CdCep as w_cep,
			DsComplemento as w_complemento,
			string(cast(NrCgc as decimal(15))) as w_cnpj,
	    	cast((CdUf*100000)+cdMunicipio as int) as w_i_cidades,
	        CdBairro as w_i_bairros,
	        NrEndereco as w_numero,
	        CdLogradouro as w_CdLogradouro
		from tecbth_delivery.gp001_sindicato
		order by 1,2,3 asc
	do
		// *****  Tabela bethadba.pessoas
		set w_i_pessoas = null;
		set w_dv = null;
		set w_ddd = null;	
		
		// *****  Tabela bethadba.pessoas_enderecos
		set w_tipo_endereco = null;
		set w_i_ruas = null;
		
		
		// *****  Converte tabela bethadba.pessoas
		select coalesce(max(i_pessoas),0)+1 
		into w_i_pessoas 
		from bethadba.pessoas;
		
		set w_dv=bethadba.dbf_calcmod11(w_i_pessoas);
		

		insert into bethadba.pessoas(i_pessoas,dv,nome,nome_fantasia,tipo_pessoa,ddd,telefone,fax,ddd_cel,celular,inscricao_municipal,email)on existing skip 
		values(w_i_pessoas,w_dv,w_nome,null,'J',w_ddd,null,null,null,null,null,null);
		
		// *****  Converte tabela bethadba.pessoas_juridicas		
		insert into bethadba.pessoas_juridicas(i_pessoas,i_naturezas,responsavel,cnpj,inscricao_estadual) 
		values (w_i_pessoas,null,null,w_cnpj,null);
		
		// *****  Converte tabela bethadba.pessoas_enderecos
		set w_tipo_endereco='P';
		
		if w_i_bairros = 0 then
			set w_i_bairros=null
		else
			set w_i_bairros=w_i_bairros
		end if;

		if not exists(select first 1 from bethadba.ruas r where r.nome like w_nome_rua and r.i_cidades = w_i_cidades) then
			set w_i_ruas = (select max(i_ruas) + 1 from bethadba.ruas);
			insert into bethadba.ruas(i_ruas,i_cidades,nome,tipo,extensao) on existing skip values(w_i_ruas, w_i_cidades, w_nome_rua, 67, 0);
		else
			set w_i_ruas = (select i_ruas from bethadba.ruas r where r.nome like w_nome_rua and r.i_cidades = w_i_cidades);
		end if;
		
	
		if w_CdLogradouro = 0 then
			select depois_1 
			into w_i_bairros 
			from antes_depois 
			where tipo = 'B' 
			and antes_1 = w_i_entidades 
			and antes_2 = w_i_cidades 
			and antes_3 = w_i_bairros
		else
			select depois_1 
			into w_i_ruas 
			from antes_depois 
			where tipo = 'R' 
			and antes_1 = w_i_cidades 
			and antes_2 = w_CdLogradouro;
			
			select first i_bairros 
			into w_i_bairros 
			from bethadba.bairros_ruas 
			where i_ruas = w_i_ruas;
			
			select cep 
			into w_cep 
			from bethadba.ruas 
			where i_ruas = w_i_ruas;
		end if;
		
		insert into bethadba.pessoas_enderecos(i_pessoas,tipo_endereco,i_ruas,i_bairros,i_distritos,i_loteamentos,i_cidades,i_condominios,nome_rua,complemento,numero,bloco,apartamento,
											   nome_bairro,nome_distrito,nome_cidade_conv,cep) 
		values (w_i_pessoas,w_tipo_endereco,w_i_ruas,w_i_bairros,null,null,w_i_cidades,null,null,w_complemento,w_numero,null,null,null,
			   null,null,w_cep);
		
		message 'Pes.: '||w_i_pessoas||' Rua.: '||w_i_ruas||' Bai.: '||w_i_bairros||' CNPJ.: '||w_cnpj to client;
		
		// *****  Converte tabela bethadba.sindicatos		
		insert into bethadba.sindicatos(i_sindicatos,mes_contrib,tipo_sind) 
		values (w_i_pessoas,3,'2');
		
		// *****  Converte tabela tecbth_delivery.pessoa_aux
		insert into tecbth_delivery.antes_depois 
		values('S',w_i_entidades,w_cdSindicato,null,null,w_i_pessoas,null,null,null,null);
	end for;
end;

------- CADASTRO DE INSTITUIÇÃO DE ENSINO PARA ESTAGIARIO

CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
commit;

if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_estagios') then
	drop procedure cnv_estagios;
end if;

begin
	// *****  Tabela bethadba.pessoas
	declare w_i_pessoas integer;
	declare w_dv tinyint;
	
	// *****  Tabela bethadba.pessoas_enderecos
	declare w_tipo_endereco char(1);
	declare w_i_ruas integer;
	
	ooLoop: for oo as cnv_estagios dynamic scroll cursor for
		select distinct B.ID as w_id,
					B.NmEntidade as w_nome,
					isnull(left(B.NrFone,2),47) as w_ddd,
					SUBSTRING(B.NrFone, 3, DATALENGTH(B.NrFone)) as w_telefone,
					B.NrEndereco as w_numero,
					B.NrInscricao as w_cnpj
		from tecbth_delivery.gp001_FuncionarioEstagio A
		join tecbth_delivery.gp001_ENTIDADEEXTERNA B on A.ID_ENT_EXT_INST_ENSINO = B.ID 
	do
		// *****  Tabela bethadba.pessoas
		set w_i_pessoas = null;
		set w_dv = null;
		
		// *****  Tabela bethadba.pessoas_enderecos
		set w_tipo_endereco = null;
		set w_i_ruas = null;
		
		
		// *****  Converte tabela bethadba.pessoas
		select coalesce(max(i_pessoas),0)+1 
		into w_i_pessoas 
		from bethadba.pessoas;
		
		set w_dv=bethadba.dbf_calcmod11(w_i_pessoas);
		

		insert into bethadba.pessoas(i_pessoas,dv,nome,nome_fantasia,tipo_pessoa,ddd,telefone,fax,ddd_cel,celular,inscricao_municipal,email)on existing skip 
		values(w_i_pessoas,w_dv,w_nome,null,'J',w_ddd,w_telefone,null,null,null,null,null);
		
		// *****  Converte tabela bethadba.pessoas_juridicas		
		insert into bethadba.pessoas_juridicas(i_pessoas,i_naturezas,responsavel,cnpj,inscricao_estadual) 
		values (w_i_pessoas,null,null,w_cnpj,null);
		
		// *****  Converte tabela tecbth_delivery.pessoa_aux
		insert into tecbth_delivery.antes_depois 
		values('E',null,w_id,null,null,w_i_pessoas,null,null,null,null);
	
	end for;
end;

------ CADASTRO DE AGENTE PARA ESTAGIARIO

CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
commit;

if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_agente') then
	drop procedure cnv_agente;
end if;

begin
	// *****  Tabela bethadba.pessoas
	declare w_i_pessoas integer;
	declare w_dv tinyint;
	
	// *****  Tabela bethadba.pessoas_enderecos
	declare w_tipo_endereco char(1);
	declare w_i_ruas integer;
	
	ooLoop: for oo as cnv_agente dynamic scroll cursor for
		select distinct B.ID as w_id,
					B.NmEntidade as w_nome,
					isnull(left(B.NrFone,2),47) as w_ddd,
					SUBSTRING(B.NrFone, 3, DATALENGTH(B.NrFone)) as w_telefone,
					B.NrEndereco as w_numero,
					B.NrInscricao as w_cnpj
		from tecbth_delivery.gp001_FuncionarioEstagio A
		join tecbth_delivery.gp001_ENTIDADEEXTERNA B on A.ID_ENT_EXT_AGENTE_INTEGR = B.ID 
	do
		// *****  Tabela bethadba.pessoas
		set w_i_pessoas = null;
		set w_dv = null;
		
		// *****  Tabela bethadba.pessoas_enderecos
		set w_tipo_endereco = null;
		set w_i_ruas = null;
		
		
		// *****  Converte tabela bethadba.pessoas
		select coalesce(max(i_pessoas),0)+1 
		into w_i_pessoas 
		from bethadba.pessoas;
		
		set w_dv=bethadba.dbf_calcmod11(w_i_pessoas);
		

		insert into bethadba.pessoas(i_pessoas,dv,nome,nome_fantasia,tipo_pessoa,ddd,telefone,fax,ddd_cel,celular,inscricao_municipal,email)on existing skip 
		values(w_i_pessoas,w_dv,w_nome,null,'J',w_ddd,w_telefone,null,null,null,null,null);
		
		// *****  Converte tabela bethadba.pessoas_juridicas		
		insert into bethadba.pessoas_juridicas(i_pessoas,i_naturezas,responsavel,cnpj,inscricao_estadual) 
		values (w_i_pessoas,null,null,w_cnpj,null);
		
		// *****  Converte tabela tecbth_delivery.pessoa_aux
		insert into tecbth_delivery.antes_depois 
		values('E',null,w_id,null,null,w_i_pessoas,null,null,null,null);
	
	end for;
end
;