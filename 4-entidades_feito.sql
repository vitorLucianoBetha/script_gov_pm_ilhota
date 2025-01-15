if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_entidades') then
	drop procedure cnv_entidades;
end if
;

begin
	// *****  Tabela bethadba.Entidades
	declare w_i_ruas integer;
	declare w_cnpj char(14);
		
	ooLoop: for oo as cnv_entidades dynamic scroll cursor for
		select 1 as w_i_entidades,
			Filial.CdLogradouro as w_CdLogradouro,
			trim(EnderecoFilial.DsEndereco) as w_nome_rua,
			coalesce(EnderecoFilial.CdBairro,0) as w_i_bairros,
			(CdUF *100000) + (cdMunicipio) as w_i_cidades,
			Empresa.CdNaturezaJuridica as w_i_naturezas,
			trim(RolEmpresas.NaEmpresa) as w_apelido,
			EnderecoFilial.CdCep as w_cep,
			Filial.NrEndereco as w_numero,
			Filial.DsComplemento as w_complemento,
			substr(NrFone,1,2) as w_ddd,
			substr(NrFone,3,8) as w_telefone,
			substr(NrFoneFax,3,8)as w_fax,
			lower(Filial.Email) as w_email,
			tecbth_delivery.fu_tirachars(Empresa.NrInscricao,'.') as w_NrInscricao,
			Empresa.CnaePreponderante as w_i_cnae,
			Filial.NrBancoContaFGTS as w_i_bancos,
			Filial.NrAgenciaContaFGTS as w_i_agencias
		from tecbth_delivery.gp001_RolEmpresas as RolEmpresas,tecbth_delivery.gp001_Empresa as Empresa,tecbth_delivery.gp001_Filial as Filial,tecbth_delivery.gp001_EnderecoFilial as EnderecoFilial 
		where RolEmpresas.CdEmpresa = Empresa.CdEmpresa 
		and Empresa.CdFilial = Filial.CdFilial 
		and Filial.CdFilial = EnderecoFilial.cdFilial  
		order by 1,2 asc		
	do	
		// *****  Inicializa Variaveis
		set w_i_ruas=null;
		set w_cnpj=null;
			
		if not exists(select 1 from bethadba.cidades where i_cidades = w_i_cidades) then
			set w_i_cidades=null
		end if;
		
		if w_i_cidades = 0 then
			set w_i_cidades=null
		end if;
		
		if w_cdLogradouro = 0 then
			select depois_1 
			into w_i_bairros 
			from tecbth_delivery.antes_depois 
			where tipo = 'B' 
			and antes_1 = w_i_entidades 
			and antes_2 = w_i_cidades 
			and antes_3 = w_i_bairros;

			if not exists(select 1 from bethadba.ruas where trim(nome) = trim(w_nome_rua)) then
				select coalesce(max(i_ruas),0)+1 
				into w_i_ruas 
				from bethadba.ruas;
			
				message 'Rua.: '||w_i_ruas||' Cid.: '||w_i_cidades||' Nom.: '||w_nome_rua to client;
				
				insert into bethadba.ruas(i_ruas,i_ruas_ini,i_ruas_fim,i_cidades,nome,tipo,cep,epigrafe,lei,zona_fiscal) 
				values(w_i_ruas,null,null,w_i_cidades,w_nome_rua,null,w_cep,null,null,null);
			
				if w_i_bairros is not null and w_i_ruas is not null then
					insert into bethadba.bairros_ruas(i_ruas,i_bairros) 
					values(w_i_ruas,w_i_bairros) 
				else
					select i_ruas 
					into w_i_ruas 
					from bethadba.ruas 
					where trim(nome) = trim(w_nome_rua)
				end if
			end if
		else	
			select depois_1 
			into w_i_ruas 
			from tecbth_delivery.antes_depois 
			where tipo = 'R' 
			and antes_1 = w_i_cidades 
			and antes_2 = w_CdLogradouro;

			select i_bairros 
			into w_i_bairros 
			from bethadba.bairros_ruas 
			where i_ruas = w_i_ruas;
		
			select cep 
			into w_cep 
			from bethadba.ruas 
			where i_ruas = w_i_ruas
		end if;
				
		if w_i_naturezas = '' then
			set w_i_naturezas=null
		end if;
						
		/*if length(cast(w_cnpj as decimal(14))) < 14 then
			set w_cnpj=repeat('0',14-length(cast(w_NrInscricao as decimal(14))))+string(cast(w_NrInscricao as decimal(14)))
		else
			set w_cnpj=string(cast(w_NrInscricao as decimal(14)))
		end if;*/
		set w_cnpj=string(cast(left(w_NrInscricao,14) as decimal(14))) ; --w_cnpj = '80637424000109';
		
		
		if not exists(select 1 from bethadba.entidades where i_entidades = w_i_entidades) then
			message 'Ent.: '||w_i_entidades||' Nom.: '||w_apelido to client;
		
			insert into bethadba.entidades(i_entidades,i_ruas,i_bairros,i_cidades,i_entidades_princ,apelido,cep,numero,complemento,codigo_tce,ddd,telefone,fax,email,cnpj,
										   i_tipos_adm) 
			values (w_i_entidades,w_i_ruas,w_i_bairros,w_i_cidades,1,w_apelido,w_cep,w_numero,w_complemento,w_i_entidades,w_ddd,w_telefone,w_fax,null,w_cnpj,8); 

			insert into bethadba.hist_entidades(i_entidades,i_competencias, i_ruas,i_bairros,i_cidades,cep,numero,complemento,ddd,telefone,fax,email,cnpj, i_tipos_adm) 
			values (w_i_entidades,'2024-01-01', w_i_ruas,w_i_bairros,w_i_cidades,w_cep,w_numero,w_complemento,w_ddd,w_telefone,w_fax,null,w_cnpj, 8); 
		
			 		-- BUG BTHSC-8213
		--BUG BTHSC-8194 CNAE incorreto
			insert into bethadba.hist_entidades_software_house(i_entidades,cnpj,i_competencias,razao_social,nome_contato,ddd,telefone,email)
			values (w_i_entidades ,w_cnpj,'2024-01-01','BETHA SISTEMAS','BETHA SISTEMAS','048','34310733','betha@betha.com.br');
		
		else
			message 'Ent.: '||w_i_entidades||' Nom.: '||w_apelido to client;
			update bethadba.entidades 
			set i_ruas = w_i_ruas,
				i_bairros = w_i_bairros,
				i_cidades = w_i_cidades,
				i_entidades_princ = 1,
				apelido = w_apelido,
				cep = w_cep,
				numero = w_numero,
				complemento = w_complemento,
				codigo_tce = w_i_entidades,
				ddd = w_ddd,
				telefone = w_telefone,
				fax = w_fax,
				email = w_email,
				cnpj = w_cnpj 
			where i_entidades = w_i_entidades
		end if;
			-- BUG BTHSC-8213
		// *****  Converte tabela bethadba.entidades_folha		
		if w_i_bancos = 0 then
			set w_i_bancos=null
		end if;

		if w_i_agencias = 0 then
			set w_i_agencias=null
		end if;
		
		if not exists(select 1 from bethadba.cnae where	i_cnae = w_i_cnae) then
			set w_i_cnae=null
		end if;
		
		if not exists(select 1 from bethadba.entidades_folha where i_entidades = w_i_entidades) then
			insert into bethadba.entidades_folha(i_entidades,i_bancos,i_agencias,i_cnae,cod_fgts,sigla,autonomos) 
			values(w_i_entidades,w_i_bancos,w_i_agencias,w_i_cnae,null,null,'N');
		else
			update bethadba.entidades_folha 
			set i_bancos = w_i_bancos,
				i_agencias = w_i_agencias,
				i_cnae = w_i_cnae,
				cod_fgts = null,
				sigla = null,
				autonomos = 'N' 
			where i_entidades = w_i_entidades
		end if;
	end for;
end
;  



 		--BUG BTHSC-8215 /BTHSC-8209
		insert into bethadba.hist_entidades_compl (i_entidades,i_competencias,pagto_previdenciario,i_pessoas,cnpj_efr,i_entidades_efr,indicativo_entidade_educativa
select top 1  1,'2023-05-01',1,(select i_pessoas from bethadba.pessoas where nome = nmresponsavel) as pessoas,(SELECT FIRST CNPJ_EFR FROM gp001_EMPRESA) cnpjef,1,'S'  from gp001_RESPONSAVEL ;
		--BUG BTHSC-8215 /BTHSC-8209 


	
-- Ajustes

/*
update bethadba.hist_entidades_compl 
set reg_eletronico_empregados = 0,
situacao_entidade = 0,
entidade_possui_rpps = 'N',
tipo_entidade = 1,
indicativo_entidade_educativa = 'S',
entidade_gestora_rpps = 'N',
entidade_regime_prev_compl = 'N'
update bethadba.hist_tipos_adm set vlr_sub_teto = 50000
update bethadba.hist_parametros_previd set classificacao_tributaria = 99

		
INSERT INTO Folharh.bethadba.hist_parametros_previd
(i_entidades, i_competencias, cod_terceiros, perc_terceiros, perc_inss, perc_acid_trab, cod_rat, processo_rat, tipo_processo_rat, cod_fpas, cod_previdencia, cod_gps, perc_isencao, cod_gps_cat15, cod_gps_obra, cod_rat_obra, perc_acid_trab_obra, fap, processo_fap, tipo_processo_fap, qualificacao_isencao, numero_certificado, dt_emissao, dt_vencto, numero_renovacao, dt_protocolo_renovacao, dt_publicacao_dou, numero_pagina_dou, classificacao_tributaria, codigo_suspensao_rat, codigo_suspensao_fap, desoneracao_folha, perc_inss_aut)
VALUES(1, '1990-01-01', '0000', 0.00, 0.00, 1.00, '1    ', '1', 1, '639', '1', '2305', 1.00, '2402', '2402', '1    ', 1.00, 1.0000, '1', 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 80, 1, 1, '0', 0);


 

 INSERT INTO Folharh.tecbth_delivery.gp001_RolEmpresas
(CdEmpresa, NaEmpresa, NmEmpresa, nmFisicoDB)
VALUES(1, N'Instituto de Previdência Municipal Ilhota', N'Instituto de Previdencia ilhota', NULL);