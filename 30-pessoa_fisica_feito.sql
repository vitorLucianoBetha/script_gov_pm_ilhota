
---------------------------------------------------------------------------
-- 24) Inserindo as pessoas(pessoas, pessoas_fisicas, pessoas_juridicas, pessoas_nomes, hist_enderecos, pessoas_fis_compl, pessoas_estrangeiras)
---------------------------------------------------------------------------
rollback;
begin
	// *****  Tabela bethadba.pessoas
	declare w_i_pessoas integer;
	declare w_dv tinyint;	
	declare w_tipo_pessoa char(1);
	declare w_email char(60);
	declare w_cpf char(11);
	
	// **** Pessoas Fisicas
	declare w_i_cidades_nasc integer;
	declare w_estado_civil char(1);
	declare w_grau_instrucao tinyint;
	declare w_nacionalidade char(1);
	declare w_uf_emis_rg char(2);
	declare w_uf_emis_carteira char(2);
	
	// **** Pessoas Fisicas Complemento
	declare w_profissao_pai char(30);
	declare w_profissao_mae char(30);
	declare w_grupo_sanguineo char(2);
	declare w_fator_rh char(1);
	declare w_raca char(1);
	declare w_deficiencia_fisica char(1);
	declare w_grau_invalidez char(1);
	declare w_num_reg char(25);
	// **** Pessoas Endereços
	declare w_i_cidades integer;
	declare w_i_ruas integer;
	declare w_i_bairros integer;
     //aux
    declare w_valida_cpf integer; 
    declare w_valida_Pis integer;
    declare w_dt_alteracoes date;
	ooLoop: for oo as cnv_pessoas dynamic scroll cursor for
		select  1 as w_i_entidades,
				Pessoa.CdPessoa as w_CdPessoa,
				trim(upper(nmPessoa)) as w_nome,
				right(tecbth_delivery.fu_tirachars(trim(nrddd),','),2) as w_ddd,
				tecbth_delivery.fu_tirachars(trim(NrFone),'-') as w_telefone,
			    cdCidadeNascimento as w_cdCidadeNascimento,
			    sgEstadoNascimento as w_sgEstadoNascimento,
			    date(dtNascimento) as w_dt_nascimento,
			    tpSexo as w_sexo,
			    cdEstadoCivil as w_cdEstadoCivil,
			    cdNacionalidade as w_cdNacionalidade,
			    trim(NrIdentidade) as w_rg,
			    trim(NmOrgaoIdentidade) as w_orgao_emis_rg,
			    DtExpedicaoIdentidade as w_dt_emis_rg,
			    SgEstadoIdentidade as w_SgEstadoIdentidade,
			    cast(NrPis as decimal(15)) as w_num_pis,
			    dtCadastroPisPasep as w_dt_pis,
			    cast(NrCpf as decimal(15)) as w_NrCpf,
			    trim(NrCarteiraProf) as w_carteira_prof,
			    trim(NrSerieCarteiraProf) as w_serie_cart,
			    DtExpedicaoCartProf as w_dt_emis_carteira,
			    sgEstadoCarteira as w_sgEstadoCarteira,
			    trim(NmPai) as w_nome_pai,
			    trim(NmMae) as w_nome_mae,
			    trim(NrZonaEleitoral) as w_zona_eleitoral,
			    trim(NrSecaoEleitoral) as w_secao_eleitoral,
			    cast(NrTituloEleitoral as decimal(15)) as w_titulo_eleitor,
			    trim(NrCarteiraReservista) as w_num_reservista,
			    TipoSanguineo as w_TipoSanguineo,
			    FatorRh as w_FatorRh,
			    trim(NrCarteiraHabilitacao) as w_cnh,
			    dtValidadeHabilitacao as w_dt_vencto_cnh,
			    dsCategoriaHabilitacao as w_categoria_cnh,
			    cdRacaCor as w_cdRacaCor,
			    cdDeficiente as w_cdDeficiente,
			    cdUf as w_cdUf,
			    cdMunicipio as w_cdMunicipio, 
			    trim(DsComplLogradouro) as w_complemento,
			    NrEndereco as w_numero,
			    dtAnoChegada as w_ano_chegada,
			    trim(nrIdentidadeEstrangeiro) as w_nrIdentidadeEstrangeiro,
			    date(dtValidadeIdentEstrang) as w_dtValidadeIdentEstrang,
			    trim(tpVistoEstrangeiro) as w_tpVistoEstrangeiro,
			    trim(nrCartProfEstrangeiro) as w_nrCartProfEstrangeiro,
			    trim(nrSerieCartProfEstrang) as w_nrSerieCartProfEstrang,
			    date(dtExpedCartProfEstrang) as w_dtExpedCartProfEstrang,
			    date(dtValidadeCartProfEstrang) as w_dtValidadeCartProfEstrang,
			    CdLogradouro as w_CdLogradouro,
			    trim(DsEndereco) as w_nome_rua,
			    CdCep as w_cep,
			    CdBairro as w_CdBairro,
			    InDependente as w_InDependente,
			    CdTipoDeficiencia as w_CdTipoDeficiencia,
			    left(nrCelular,2) as w_ddd_cel,
			    SUBSTRING(nrCelular, 3, DATALENGTH(nrCelular)) as w_celular,
			    DtObito as w_dt_obito,
			    NmCartorio as w_cartorio_reg,
			    NrRegistro as w_NrRegistro,
			    Email as w_EmailFunc,
			    EmailPessoal as w_EmailPessoal,
			    dsCategoriaHabilitacao as w_dsCategoriaHabilitacao,
			    inCertidao as w_inCertidao,
			    nrFolha as w_nrFolha,
			    nrLivro as w_nrLivro,
			    trim(dsApelido) as w_nome_fantasia,
			    null as w_NrFoneFax,
			    2 as w_TpInscricao
		from tecbth_delivery.gp001_pessoa as pessoa,
		tecbth_delivery.gp001_enderecopessoa as enderecopessoa 
        where pessoa.cdpessoa = enderecopessoa.cdpessoa 
	    order by 1,2 asc
	do
		// *****  Tabela bethadba.pessoas
        set w_valida_cpf = null; 
        set w_valida_Pis = null;
		set w_i_pessoas = null;
		set w_dv = null;	
		set w_tipo_pessoa = null;
		set w_email = null;
		set w_cpf = null;
        set w_dt_alteracoes = null;

		// **** Pessoas Fisicas
		set w_i_cidades_nasc = null;
		set w_estado_civil = null;
		set w_grau_instrucao = null;
		set w_nacionalidade = null;
		set w_uf_emis_rg = null;
		set w_uf_emis_carteira = null;
		
		// **** Pessoas Fisicas Complemento
		set w_profissao_pai = null;
		set w_profissao_mae = null;
		set w_grupo_sanguineo = null;
		set w_fator_rh = null;
		set w_raca = null;
		set w_deficiencia_fisica = null;
		set w_grau_invalidez = null;
		set w_num_reg = null;
		
		// **** Pessoas Endereços
		set w_i_cidades = null;
		set w_i_ruas = null;
		set w_i_bairros = null;
		set w_valida_cpf = null; 
        set w_valida_Pis = null;
        if length(cast(w_NrCpf as decimal(15))) < 11 then
			set w_cpf=repeat('0',11-length(cast(w_NrCpf as decimal(15))))+string(cast(w_NrCpf as decimal(15)))
		else
			set w_cpf=cast(w_NrCpf as decimal(15))
		end if;
        set w_valida_cpf = bethadba.dbf_chk_cnpj_cpf (null,w_cpf,'F');
		// *****  Converte tabela bethadba.pessoas
		/*if w_InDependente != 0 then
			set w_tipo_pessoa='O'*/
        if w_valida_cpf =1 then  
          set w_tipo_pessoa='F'
        else
          set w_tipo_pessoa='O';
          set w_cpf = null
        end if;  
        
        if w_ddd in('0','') then
			set w_ddd=null
		end if;
		
		if w_telefone in('0', '') then
			set w_telefone=null
		end if;
		if w_ddd_cel in('0','') then
			set w_ddd_cel=null
		end if;
		if w_celular in('0','') then
			set w_celular=null
		end if;
		if locate(w_EmailFunc,'@') != 0 then
			set w_email=trim(w_EmailFunc)
		elseif locate(w_EmailPessoal,'@') != 0 then
			set w_email=trim(w_EmailPessoal)
		else
			set w_email=null
		end if;
			
		/*select coalesce(max(i_pessoas),0)+1 
		into w_i_pessoas 
		from bethadba.pessoas;*/
		set w_i_pessoas = w_CdPessoa; 
		
		set w_dv=bethadba.dbf_calcmod11(w_i_pessoas);
		
		message 'Pes.: '||w_i_pessoas||' Dv.: '||w_dv||' Nom.: '||w_nome||' Tip.: '||w_tipo_pessoa to client;
		
        if w_dt_nascimento is null then
          set w_dt_alteracoes = (select min(dtadmissao) from gp001_funcionario where cdpessoa = w_CdPessoa);
        else
          set w_dt_alteracoes = w_dt_nascimento;
        end if;
        if w_dt_alteracoes is null then
          set w_dt_alteracoes = '1900-01-01';
        end if;
		
		insert into bethadba.pessoas(i_pessoas,dv,nome,nome_fantasia,tipo_pessoa,ddd,telefone,fax,ddd_cel,celular,inscricao_municipal,email)/*on existing skip*/
		values (w_i_pessoas,w_dv,w_nome,w_nome_fantasia,w_tipo_pessoa,w_ddd,w_telefone,null,w_ddd_cel,w_celular,null,w_email);
		
		message 'Pes.: '||w_i_pessoas||' Dt.: '||w_dt_nascimento||' Nom.: '||w_nome to client;
		
		insert into bethadba.pessoas_nomes(i_pessoas,dt_alteracoes,nome,documento)/*on existing skip*/ values (w_i_pessoas,w_dt_alteracoes,w_nome,null);
		
		insert into antes_depois values('P',w_i_entidades,w_CdPessoa,null,null,w_i_pessoas,null,null,null,null);
		--select * from antes_depois
		// *****  Converte tabela bethadba.pessoas_fisicas
		set w_i_cidades_nasc=null;
		if w_cdCidadeNascimento != '' then
			select(cod_nacional*100000) 
			into w_i_cidades_nasc 
			from bethadba.estados 
			where sigla = trim(w_sgEstadoNascimento);
			
			set w_i_cidades_nasc=w_i_cidades_nasc+w_CdCidadeNascimento
		else
			set w_i_cidades_nasc=null
		end if;
		
		if not exists(select 1 from bethadba.cidades where i_cidades = w_i_cidades_nasc) then
			set w_i_cidades_nasc=null
		end if;
		
		if w_sexo = '' then
			set w_sexo='M'
		end if;

		if w_categoria_cnh = '' then
			set w_categoria_cnh = null
		end if;
		
		if w_CdEstadoCivil in(5) then
			set w_estado_civil=6
		elseif w_CdEstadoCivil = 0 then
			set w_estado_civil=1
		else
			set w_estado_civil=w_CdEstadoCivil
		end if;

		if w_i_entidades = 1 then
			select first cdOcorrencia 
			into w_grau_instrucao 
			from gp001_eventofuncional 
			where cdPessoa = w_cdPessoa and cdEvento = 1
		end if;

		if w_grau_instrucao not in(1,2,3,4,5,6,7,8,9) then
			set w_grau_instrucao=1
		end if;

		if w_CdNacionalidade = 10 then
			set w_nacionalidade='B'
		elseif w_CdNacionalidade = 52 then
			set w_nacionalidade='E'
		end if;

		if w_rg = '' then
			set w_rg=null
		end if;

		if w_orgao_emis_rg = '' then
			set w_orgao_emis_rg=null
		end if;

		if w_SgEstadoIdentidade != '' then
			select first i_estados 
			into w_uf_emis_rg 
			from bethadba.estados 
			where sigla = trim(w_SgEstadoIdentidade)
		else
			set w_uf_emis_rg=null
		end if;
		
		if(w_carteira_prof = '000000') or(w_carteira_prof = '') then
			set w_carteira_prof=null
		end if;
		
		if(w_serie_cart = '0000') or(w_carteira_prof is null) then
			set w_serie_cart=null
		end if;

		if w_sgEstadoCarteira != '' and(w_carteira_prof is not null) then
			select first i_estados 
			into w_uf_emis_carteira 
			from bethadba.estados 
			where sigla = trim(w_sgEstadoCarteira)
		else
			set w_uf_emis_carteira=null
		end if;
		
		message 'Pes.: '||w_i_pessoas||' Nas.: '||w_dt_nascimento||' Sex.: '||w_sexo||' Nas.: '||w_nacionalidade||' RG.: '||w_rg||' CPF: '||w_cpf to client;
		
		insert into bethadba.pessoas_fisicas(i_pessoas,i_cidades,dt_nascimento,sexo,estado_civil,grau_instrucao,nacionalidade,rg,orgao_emis_rg,dt_emis_rg,uf_emis_rg,num_pis,dt_pis,cpf,
											 carteira_prof,serie_cart,dt_emis_carteira,uf_emis_carteira) on existing skip 
		values(w_i_pessoas,w_i_cidades_nasc,w_dt_nascimento,w_sexo,w_estado_civil,w_grau_instrucao,w_nacionalidade,w_rg,w_orgao_emis_rg,w_dt_emis_rg,w_uf_emis_rg,w_num_pis,w_dt_pis,w_cpf,
			   w_carteira_prof,w_serie_cart,w_dt_emis_carteira,w_uf_emis_carteira);
		
		// *****  Converte tabela tabela bethadba.pessaos_fis_compl
		if w_tipo_pessoa != 'J' then
			
			if w_nome_pai = '' then
				set w_nome_pai=null
			end if;
			
			if w_nome_mae = '' then
				set w_nome_mae=null
			end if;
			
			set w_profissao_pai=null;
			set w_profissao_mae=null;			

			if w_cartorio_reg = '' then
				set w_cartorio_reg=null
			end if;
		
			if w_nrLivro not in( '0','') then
				set w_num_reg=trim(w_num_reg+' L: '+string(w_nrLivro))
			end if;
				
			if w_nrFolha not in( '0','') then
				set w_num_reg=trim(w_num_reg+' F: '+string(w_nrFolha))
			end if;
		
			if w_NrRegistro not in( '0','') then
				set w_num_reg=trim(w_num_reg+' R: '+string(w_NrRegistro))
			end if;
		
			if w_zona_eleitoral = '0' then
				set w_zona_eleitoral=null
			end if;
		
			if w_secao_eleitoral = '0' then
				set w_secao_eleitoral=null
			end if;

			if w_titulo_eleitor = '0' then
				set w_titulo_eleitor=null
			end if;

			if(w_num_reservista = '0') or(w_sexo = 'F') or(w_num_reservista = '') then
				set w_num_reservista=null
			end if;

			if trim(w_TipoSanguineo) = '' or(trim(w_TipoSanguineo) not in( 'A','B','AB','O') ) then
				set w_grupo_sanguineo=null
			else
				set w_grupo_sanguineo=w_TipoSanguineo
			end if;
		
			if w_FatorRh = 'P' then
				set w_fator_rh='+'
			elseif w_FatorRh = 'N' then
				set w_fator_rh='-'
			elseif trim(w_FatorRh) = '' then
					set w_fator_rh=null
			end if;
			
			if w_cnh = '0' then
				set w_cnh=null
			end if;
	
			if w_cdRacaCor in('','9') then
				set w_raca='2'
			else
				set w_raca=w_cdRacaCor
			end if;
			
			if w_raca not in('2','4','6','8') then
				set w_raca = 2
			end if;

			if w_cdDeficiente = 1 then
				set w_deficiencia_fisica='6';
				set w_grau_invalidez=null
			else
				set w_deficiencia_fisica='0';
				set w_grau_invalidez='N'
			end if;	
			
			message 'Pes.: '||w_i_pessoas||' Nom.: '||w_nome||' Nom Pai.: '||w_nome_pai||' Nom. Mae.: '||w_nome_mae to client;
			
			insert into bethadba.pessoas_fis_compl(i_pessoas,nome_pai,nome_mae,profissao_pai,profissao_mae,cartorio_reg,num_reg,zona_eleitoral,secao_eleitoral,titulo_eleitor,num_reservista,
												grupo_sanguineo,fator_rh,doador,cnh,categoria_cnh,dt_vencto_cnh,raca,estatura,peso,cor_olhos)on existing skip 
			values(w_i_pessoas,w_nome_pai,w_nome_mae,w_profissao_pai,w_profissao_mae,w_cartorio_reg,w_num_reg,w_zona_eleitoral,w_secao_eleitoral,w_titulo_eleitor,w_num_reservista,w_grupo_sanguineo,
				w_fator_rh,'N',w_cnh,w_categoria_cnh,w_dt_vencto_cnh,w_raca,null,null,null);
	
			// *****  Converte tabela bethadba.pessoas_fis_obito
			if w_dt_obito is not null then
				message 'Pes.: '||w_i_pessoas||' Dt. Obt.: '||w_dt_obito to client;
				
				insert into bethadba.pessoas_fis_obito(i_pessoas,dt_obito,certidao,causa_mortis)on existing skip 
				values (w_i_pessoas,w_dt_obito,null,null) 
			end if;		
					
			// **** Histórico de pessoas_fisicas
			message 'Pes.: '||w_i_pessoas||' Dt. Nas.: '||w_dt_nascimento||' Dt. Alt.: '||w_dt_nascimento||' Sex.: '||w_sexo to client;
			
			insert into bethadba.hist_pessoas_fis(tipo_pessoa,i_pessoas,dt_alteracoes,dt_nascimento,sexo,rg,orgao_emis_rg,uf_emis_rg,dt_emis_rg,cpf,num_pis,dt_pis,carteira_prof,serie_cart,uf_emis_carteira,
												  dt_emis_carteira,zona_eleitoral,secao_eleitoral,titulo_eleitor)on existing skip
			values (w_tipo_pessoa,w_i_pessoas,w_dt_alteracoes,w_dt_nascimento,w_sexo,w_rg,w_orgao_emis_rg,w_uf_emis_rg,w_dt_emis_rg,w_cpf,w_num_pis,w_dt_pis,w_carteira_prof,w_serie_cart,w_uf_emis_carteira,
					w_dt_emis_carteira,w_zona_eleitoral,w_secao_eleitoral,w_titulo_eleitor);
			
			// *****  Converte tabela bethadba.pessoas_estrangeiros
			if w_nacionalidade != 'B' then
				message 'Pes.: '||w_i_pessoas||' Ano. Che.: '||w_ano_chegada to client;
			
				insert into bethadba.pessoas_estrangeiras(i_pessoas,ano_chegada,ident_estrangeiro,dt_validade_est,tipo_visto_est,cart_trab_est,serie_cart_est,dt_exp_cart_est,dt_val_cart_est)on existing skip 
				values (w_i_pessoas,w_ano_chegada,w_ident_estrangeiro,w_dt_validade_est,w_tipo_visto_est,w_cart_trab_est,w_serie_cart_est,w_dt_exp_cart_est,w_dt_val_cart_est) 
			end if;
		else
			// *****  Converte tabela bethadba.pessoas_juridicas
			if length(cast(w_NrCpf as decimal(15))) < 14 then
				set w_cnpj=repeat('0',14-length(cast(w_NrCpf as decimal(15))))+string(cast(w_NrCpf as decimal(15)))
			else
				set w_cnpj=cast(w_NrCpf as decimal(15))
			end if;
			
			message 'Pes.: '||w_i_pessoas||' CNPJ.: '||w_cnpj to client;

			insert into bethadba.pessoas_juridicas(i_pessoas,i_naturezas,responsavel,cnpj,inscricao_estadual) on existing skip 
			values (w_i_pessoas,null,null,w_cnpj,null) 
		end if;
		
		// *****  Converte tabela bethadba.pessoas_enderecos
		set w_i_cidades=null;
		set w_i_ruas=null;

		if w_cdUf != 0 then
			set w_i_cidades=(w_CdUf*100000)+w_cdMunicipio
		else
			set w_i_cidades=null
		end if;
		
		if not exists(select 1 from bethadba.cidades where i_cidades = w_i_cidades) then
			set w_i_cidades=null
		end if;
		
		if w_cdMunicipio = 0 then
			set w_i_cidades=null
		end if;
		
		if w_complemento = '' then
			set w_complemento=null
		end if;

		if w_numero = 0 then
			set w_numero=null
		end if;

		if w_CdLogradouro = 0 then
			select first depois_1 
			into w_i_bairros 
			from antes_depois 
			where tipo = 'B' 
			and antes_1 = w_i_entidades 
			and antes_2 = w_i_cidades 
			and antes_3 = w_cdBairro;
			
			if w_nome_rua != '' then
				if not exists(select 1 from bethadba.ruas where trim(nome) = trim(w_nome_rua)) then
					select coalesce(max(i_ruas),0)+1 
					into w_i_ruas 
					from bethadba.ruas;
					
					message 'Rua.: '||w_i_ruas||' Cid.: '||w_i_cidades||' Nom.: '||w_nome_rua||' CEP.: '||w_cep to client;
					
					insert into bethadba.ruas(i_ruas,i_ruas_ini,i_ruas_fim,i_cidades,nome,tipo,cep,epigrafe,lei,zona_fiscal)on existing skip
					values(w_i_ruas,null,null,w_i_cidades,w_nome_rua,57,null,null,null,null);
					
					if w_i_bairros is not null then
						message 'Bai.: '||w_i_bairros||' Rua.: '||w_i_ruas to client;
						insert into bethadba.bairros_ruas(i_ruas,i_bairros) 
						values(w_i_ruas,w_i_bairros) 
					end if
				else
					select first i_ruas 
					into w_i_ruas 
					from bethadba.ruas 
					where trim(nome) = trim(w_nome_rua)
				end if;
				set w_nome_rua=null
			end if
		else
			select first depois_1 
			into w_i_ruas 
			from antes_depois 
			where tipo = 'R' 
			and antes_1 = w_i_entidades 
			and antes_2 = w_i_cidades 
			and antes_3 = w_CdLogradouro;
		
			select first i_bairros 
			into w_i_bairros 
			from bethadba.bairros_ruas 
			where i_ruas = w_i_ruas;
				
		end if;

		if trim(w_nome_rua) in('R','R;','R:','R.','Rf','Rua:','Rua:.') then
			set w_nome_rua=null
		end if;
		
		message 'Pes.: '||w_i_pessoas||' Rua.: '||w_i_ruas||' Bai.: '||w_i_bairros||' Nom.: '||w_nome_rua to client;
		
		insert into bethadba.pessoas_enderecos(i_pessoas,tipo_endereco,i_ruas,i_bairros,i_distritos,i_loteamentos,i_cidades,i_condominios,nome_rua,complemento,numero,bloco,apartamento,
											   nome_bairro,nome_distrito,nome_cidade_conv,cep) on existing skip 
		values (w_i_pessoas,'P',w_i_ruas,w_i_bairros,null,null,w_i_cidades,null,w_nome_rua,w_complemento,w_numero,null,null,null,null,null,w_cep);
		
				
		insert into bethadba.hist_enderecos(i_pessoas,tipo_endereco,dt_alteracoes,i_ruas,i_condominios,i_distritos,i_loteamentos,i_bairros,i_cidades,nome_rua,complemento,numero,bloco,
											apartamento,nome_bairro,nome_distrito,nome_cidade_conv,cep)on existing skip
		values (w_i_pessoas,'P',w_dt_alteracoes,w_i_ruas,null,null,null,w_i_bairros,w_i_cidades,w_nome_rua,w_complemento,w_numero,null,null,null,null,null,w_cep);
	end for
end;


/*



begin
	// *****  Tabela bethadba.pessoas
	declare w_i_pessoas integer;
	declare w_dv tinyint;	
	declare w_tipo_pessoa char(1);
	declare w_email char(60);
	declare w_cpf char(11);
	
	// **** Pessoas Fisicas
	declare w_i_cidades_nasc integer;
	declare w_estado_civil char(1);
	declare w_grau_instrucao tinyint;
	declare w_nacionalidade char(1);
	declare w_uf_emis_rg char(2);
	declare w_uf_emis_carteira char(2);
	
	// **** Pessoas Fisicas Complemento
	declare w_profissao_pai char(30);
	declare w_profissao_mae char(30);
	declare w_grupo_sanguineo char(2);
	declare w_fator_rh char(1);
	declare w_raca char(1);
	declare w_deficiencia_fisica char(1);
	declare w_grau_invalidez char(1);
	declare w_num_reg char(25);
	// **** Pessoas Endereços
	declare w_i_cidades integer;
	declare w_i_ruas integer;
	declare w_i_bairros integer;
     //aux
    declare w_valida_cpf integer; 
    declare w_valida_Pis integer;
	--BTHSC-7236 bug
	ooLoop: for oo as cnv_pessoas dynamic scroll cursor for
		select  1 as w_i_entidades,
			Pessoa.CdPessoa as w_CdPessoa,
			trim(upper(nmPessoa)) as w_nome,
			right(tecbth_delivery.fu_tirachars(trim(nrddd),','),2) as w_ddd,
			right(tecbth_delivery.fu_tirachars(trim(NrFone),'-'),8) as w_telefone,
			cdCidadeNascimento as w_cdCidadeNascimento,
			sgEstadoNascimento as w_sgEstadoNascimento,
			date(dtNascimento) as w_dt_nascimento,
			tpSexo as w_sexo,
			cdEstadoCivil as w_cdEstadoCivil,
			cdNacionalidade as w_cdNacionalidade,
			trim(NrIdentidade) as w_rg,
			trim(NmOrgaoIdentidade) as w_orgao_emis_rg,
			DtExpedicaoIdentidade as w_dt_emis_rg,
			SgEstadoIdentidade as w_SgEstadoIdentidade,
			cast(NrPis as decimal(15)) as w_num_pis,
			dtCadastroPisPasep as w_dt_pis,
			cast(NrCpf as decimal(15)) as w_NrCpf,
			trim(NrCarteiraProf) as w_carteira_prof,
			trim(NrSerieCarteiraProf) as w_serie_cart,
			DtExpedicaoCartProf as w_dt_emis_carteira,
			sgEstadoCarteira as w_sgEstadoCarteira,
			trim(NmPai) as w_nome_pai,
			trim(NmMae) as w_nome_mae,
			trim(NrZonaEleitoral) as w_zona_eleitoral,
			trim(NrSecaoEleitoral) as w_secao_eleitoral,
			cast(NrTituloEleitoral as decimal(15)) as w_titulo_eleitor,
			trim(NrCarteiraReservista) as w_num_reservista,
			TipoSanguineo as w_TipoSanguineo,
			FatorRh as w_FatorRh,
			trim(NrCarteiraHabilitacao) as w_cnh,
			dtValidadeHabilitacao as w_dt_vencto_cnh,
			cdRacaCor as w_cdRacaCor,
			cdDeficiente as w_cdDeficiente,
			cdUf as w_cdUf,
			cdMunicipio as w_cdMunicipio, 
			trim(DsComplLogradouro) as w_complemento,
			NrEndereco as w_numero,
			dtAnoChegada as w_ano_chegada,
			trim(nrIdentidadeEstrangeiro) as w_nrIdentidadeEstrangeiro,
			date(dtValidadeIdentEstrang) as w_dtValidadeIdentEstrang,
			trim(tpVistoEstrangeiro) as w_tpVistoEstrangeiro,
			trim(nrCartProfEstrangeiro) as w_nrCartProfEstrangeiro,
			trim(nrSerieCartProfEstrang) as w_nrSerieCartProfEstrang,
			date(dtExpedCartProfEstrang) as w_dtExpedCartProfEstrang,
			date(dtValidadeCartProfEstrang) as w_dtValidadeCartProfEstrang,
			CdLogradouro as w_CdLogradouro,
			trim(DsEndereco) as w_nome_rua,
			CdCep as w_cep,
			CdBairro as w_CdBairro,
			InDependente as w_InDependente,
			CdTipoDeficiencia as w_CdTipoDeficiencia,
			right(NrCelular,8) as w_celular,
			DtObito as w_dt_obito,
			NmCartorio as w_cartorio_reg,
			NrRegistro as w_NrRegistro,
			Email as w_EmailFunc,
			EmailPessoal as w_EmailPessoal,
			dsCategoriaHabilitacao as w_dsCategoriaHabilitacao,
			inCertidao as w_inCertidao,
			nrFolha as w_nrFolha,
			nrLivro as w_nrLivro,
			trim(dsApelido) as w_nome_fantasia,
			null as w_NrFoneFax,
			2 as w_TpInscricao
		from tecbth_delivery.gp001_pessoa as pessoa, tecbth_delivery.gp001_enderecopessoa as enderecopessoa 
       where pessoa.cdpessoa *= enderecopessoa.cdpessoa 
	   order by 1,2 asc
	do
		// *****  Tabela bethadba.pessoas
        set w_valida_cpf = null; 
        set w_valida_Pis = null;
		set w_i_pessoas = null;
		set w_dv = null;	
		set w_tipo_pessoa = null;
		set w_email = null;
		set w_cpf = null;

		// **** Pessoas Fisicas
		set w_i_cidades_nasc = null;
		set w_estado_civil = null;
		set w_grau_instrucao = null;
		set w_nacionalidade = null;
		set w_uf_emis_rg = null;
		set w_uf_emis_carteira = null;
		
		// **** Pessoas Fisicas Complemento
		set w_profissao_pai = null;
		set w_profissao_mae = null;
		set w_grupo_sanguineo = null;
		set w_fator_rh = null;
		set w_raca = null;
		set w_deficiencia_fisica = null;
		set w_grau_invalidez = null;
		set w_num_reg = null;
		
		// **** Pessoas Endereços
		set w_i_cidades = null;
		set w_i_ruas = null;
		set w_i_bairros = null;
		set w_valida_cpf = null; 
        set w_valida_Pis = null;
        if length(cast(w_NrCpf as decimal(15))) < 11 then
			set w_cpf=repeat('0',11-length(cast(w_NrCpf as decimal(15))))+string(cast(w_NrCpf as decimal(15)))
		else
			set w_cpf=cast(w_NrCpf as decimal(15))
		end if;
        set w_valida_cpf = bethadba.dbf_chk_cnpj_cpf (null,w_cpf,'F');
		// *****  Converte tabela bethadba.pessoas
		if w_InDependente != 0 then
			set w_tipo_pessoa='O'
 end if;
        if w_valida_cpf =1 then  
          set w_tipo_pessoa='F'
         else
          set w_tipo_pessoa='O';
          set w_cpf = null
        end if;  
        if w_ddd in('0','') then
			set w_ddd=null
		end if;
		
		if w_telefone in('0', '') then
			set w_telefone=null
		end if;
		
		if w_celular in('0','') then
			set w_celular=null
		end if;
		
		if locate(w_EmailFunc,'@') != 0 then
			set w_email=trim(w_EmailFunc)
		elseif locate(w_EmailPessoal,'@') != 0 then
			set w_email=trim(w_EmailPessoal)
		else
			set w_email=null
		end if;
			
		select coalesce(max(i_pessoas),0)+1 
		into w_i_pessoas 
		from bethadba.pessoas;
		
		set w_dv=bethadba.dbf_calcmod11(w_i_pessoas);
		
		message 'Pes.: '||w_i_pessoas||' Dv.: '||w_dv||' Nom.: '||w_nome||' Tip.: '||w_tipo_pessoa to client;
		
		insert into bethadba.pessoas(i_pessoas,dv,nome,nome_fantasia,tipo_pessoa,ddd,telefone,fax,ddd_cel,celular,inscricao_municipal,email)/*on existing skip*/
		values (w_i_pessoas,w_dv,w_nome,w_nome_fantasia,w_tipo_pessoa,w_ddd,w_telefone,null,null,w_celular,null,w_email);
		
		message 'Pes.: '||w_i_pessoas||' Dt.: '||w_dt_nascimento||' Nom.: '||w_nome to client;
		
		insert into bethadba.pessoas_nomes(i_pessoas,dt_alteracoes,nome,documento)/*on existing skip*/ values (w_i_pessoas,coalesce(w_dt_nascimento,date('2001-01-01')),w_nome,null);
		
		insert into tecbth_delivery.antes_depois values('P',w_i_entidades,w_CdPessoa,null,null,w_i_pessoas,null,null,null,null);
		
		// *****  Converte tabela bethadba.pessoas_fisicas
		set w_i_cidades_nasc=null;
		if w_cdCidadeNascimento != '' then
			select(cod_nacional*100000) 
			into w_i_cidades_nasc 
			from bethadba.estados 
			where sigla = trim(w_sgEstadoNascimento);
			
			set w_i_cidades_nasc=w_i_cidades_nasc+w_CdCidadeNascimento
		else			set w_i_cidades_nasc=null
		end if;
		
		if not exists(select 1 from bethadba.cidades where i_cidades = w_i_cidades_nasc) then
			set w_i_cidades_nasc=null
		end if;
		
		if w_sexo = '' then
			set w_sexo='M'
		end if;
		
		if w_CdEstadoCivil = 6 then
			set w_estado_civil=5
		elseif w_CdEstadoCivil in( 5,7) then
			set w_estado_civil=6
		elseif w_CdEstadoCivil = 0 then
			set w_estado_civil=1
		else
			set w_estado_civil=w_CdEstadoCivil
		end if;

		if w_i_entidades = 1 then
			select first cdOcorrencia 
			into w_grau_instrucao 
			from tecbth_delivery.gp001_eventofuncional 
			where cdPessoa = w_cdPessoa and cdEvento = 1
		end if;

		if w_grau_instrucao not in( 1,2,3,4,5,6,7,8,9) then
			set w_grau_instrucao=1
		end if;

		if w_CdNacionalidade = 10 then
			set w_nacionalidade='B'
		elseif w_CdNacionalidade = 52 then
			set w_nacionalidade='E'
		end if;

		if w_rg = '' then
			set w_rg=null
		end if;

		if w_orgao_emis_rg = '' then
			set w_orgao_emis_rg=null
		end if;

		if w_SgEstadoIdentidade != '' then
			select first i_estados 
			into w_uf_emis_rg 
			from bethadba.estados 
			where sigla = trim(w_SgEstadoIdentidade)
		else
			set w_uf_emis_rg=null
		end if;
		
		if(w_carteira_prof = '000000') or(w_carteira_prof = '') then
			set w_carteira_prof=null
		end if;
		
		if(w_serie_cart = '0000') or(w_carteira_prof is null) then
			set w_serie_cart=null
		end if;

		if w_sgEstadoCarteira != '' and(w_carteira_prof is not null) then
			select first i_estados 
			into w_uf_emis_carteira 
			from bethadba.estados 
			where sigla = trim(w_sgEstadoCarteira)
		else
			set w_uf_emis_carteira=null
		end if;
		
		message 'Pes.: '||w_i_pessoas||' Nas.: '||w_dt_nascimento||' Sex.: '||w_sexo||' Nas.: '||w_nacionalidade||' RG.: '||w_rg||' CPF: '||w_cpf to client;
		
		insert into bethadba.pessoas_fisicas(i_pessoas,i_cidades,dt_nascimento,sexo,estado_civil,grau_instrucao,nacionalidade,rg,orgao_emis_rg,dt_emis_rg,uf_emis_rg,num_pis,dt_pis,cpf,
											 carteira_prof,serie_cart,dt_emis_carteira,uf_emis_carteira) on existing skip 
		values(w_i_pessoas,w_i_cidades_nasc,w_dt_nascimento,w_sexo,w_estado_civil,w_grau_instrucao,w_nacionalidade,w_rg,w_orgao_emis_rg,w_dt_emis_rg,w_uf_emis_rg,w_num_pis,w_dt_pis,w_cpf,
			   w_carteira_prof,w_serie_cart,w_dt_emis_carteira,w_uf_emis_carteira);
		
		// *****  Converte tabela tabela bethadba.pessaos_fis_compl
		if w_tipo_pessoa != 'J' then
			
			if w_nome_pai = '' then
				set w_nome_pai=null
			end if;
			
			if w_nome_mae = '' then
				set w_nome_mae=null
			end if;
			
			set w_profissao_pai=null;
			set w_profissao_mae=null;			

			if w_cartorio_reg = '' then
				set w_cartorio_reg=null
			end if;
		
			if w_nrLivro not in( '0','') then
				set w_num_reg=trim(w_num_reg+' L: '+string(w_nrLivro))
			end if;
				
			if w_nrFolha not in( '0','') then
				set w_num_reg=trim(w_num_reg+' F: '+string(w_nrFolha))
			end if;
		
			if w_NrRegistro not in( '0','') then
				set w_num_reg=trim(w_num_reg+' R: '+string(w_NrRegistro))
			end if;
		
			if w_zona_eleitoral = '0' then
				set w_zona_eleitoral=null
			end if;
		
			if w_secao_eleitoral = '0' then
				set w_secao_eleitoral=null
			end if;

			if w_titulo_eleitor = '0' then
				set w_titulo_eleitor=null
			end if;

			if(w_num_reservista = '0') or(w_sexo = 'F') or(w_num_reservista = '') then
				set w_num_reservista=null
			end if;

			if trim(w_TipoSanguineo) = '' or(trim(w_TipoSanguineo) not in( 'A','B','AB','O') ) then
				set w_grupo_sanguineo=null
			else
				set w_grupo_sanguineo=w_TipoSanguineo
			end if;
		
			if w_FatorRh = 'P' then
				set w_fator_rh='+'
			elseif w_FatorRh = 'N' then
				set w_fator_rh='-'
			elseif trim(w_FatorRh) = '' then
					set w_fator_rh=null
			end if;
			
			if w_cnh = '0' then
				set w_cnh=null
			end if;
	
			if w_cdRacaCor in('','9') then
				set w_raca='2'
			else
				set w_raca=w_cdRacaCor
			end if;
			
			if w_raca not in('2','4','6','8') then
				set w_raca = 2
			end if;
			
			
			if w_cdDeficiente = 1 then
				set w_deficiencia_fisica='6';
				set w_grau_invalidez=null
			else
				set w_deficiencia_fisica='0';
				set w_grau_invalidez='N'
			end if;	
			
			message 'Pes.: '||w_i_pessoas||' Nom.: '||w_nome||' Nom Pai.: '||w_nome_pai||' Nom. Mae.: '||w_nome_mae to client;
			
			insert into bethadba.pessoas_fis_compl(i_pessoas,nome_pai,nome_mae,profissao_pai,profissao_mae,cartorio_reg,num_reg,zona_eleitoral,secao_eleitoral,titulo_eleitor,num_reservista,
												grupo_sanguineo,fator_rh,doador,cnh,categoria_cnh,dt_vencto_cnh,raca,estatura,peso,cor_olhos)on existing skip 
			values(w_i_pessoas,w_nome_pai,w_nome_mae,w_profissao_pai,w_profissao_mae,w_cartorio_reg,w_num_reg,w_zona_eleitoral,w_secao_eleitoral,w_titulo_eleitor,w_num_reservista,w_grupo_sanguineo,
				w_fator_rh,'N',w_cnh,null,w_dt_vencto_cnh,w_raca,null,null,null);
	
			// *****  Converte tabela bethadba.pessoas_fis_obito
			if w_dt_obito is not null then
				message 'Pes.: '||w_i_pessoas||' Dt. Obt.: '||w_dt_obito to client;
				
				insert into bethadba.pessoas_fis_obito(i_pessoas,dt_obito,certidao,causa_mortis)on existing skip 
				values (w_i_pessoas,w_dt_obito,null,null) 
			end if;		
					
			// **** Histórico de pessoas_fisicas
			message 'Pes.: '||w_i_pessoas||' Dt. Nas.: '||w_dt_nascimento||' Dt. Alt.: '||w_dt_nascimento||' Sex.: '||w_sexo to client;
			
			insert into bethadba.hist_pessoas_fis(tipo_pessoa,i_pessoas,dt_alteracoes,dt_nascimento,sexo,rg,orgao_emis_rg,uf_emis_rg,dt_emis_rg,cpf,num_pis,dt_pis,carteira_prof,serie_cart,uf_emis_carteira,
												  dt_emis_carteira,zona_eleitoral,secao_eleitoral,titulo_eleitor)on existing skip
			values (w_tipo_pessoa,w_i_pessoas,w_dt_nascimento,w_dt_nascimento,w_sexo,w_rg,w_orgao_emis_rg,w_uf_emis_rg,w_dt_emis_rg,w_cpf,w_num_pis,w_dt_pis,w_carteira_prof,w_serie_cart,w_uf_emis_carteira,
					w_dt_emis_carteira,w_zona_eleitoral,w_secao_eleitoral,w_titulo_eleitor);
			
			// *****  Converte tabela bethadba.pessoas_estrangeiros
			if w_nacionalidade != 'B' then
				message 'Pes.: '||w_i_pessoas||' Ano. Che.: '||w_ano_chegada to client;
			
				insert into bethadba.pessoas_estrangeiras(i_pessoas,ano_chegada,ident_estrangeiro,dt_validade_est,tipo_visto_est,cart_trab_est,serie_cart_est,dt_exp_cart_est,dt_val_cart_est)on existing skip 
				values (w_i_pessoas,w_ano_chegada,w_ident_estrangeiro,w_dt_validade_est,w_tipo_visto_est,w_cart_trab_est,w_serie_cart_est,w_dt_exp_cart_est,w_dt_val_cart_est) 
			end if;
		else
			// *****  Converte tabela bethadba.pessoas_juridicas
			if length(cast(w_NrCpf as decimal(15))) < 14 then
				set w_cnpj=repeat('0',14-length(cast(w_NrCpf as decimal(15))))+string(cast(w_NrCpf as decimal(15)))
			else
				set w_cnpj=cast(w_NrCpf as decimal(15))
			end if;
			
			message 'Pes.: '||w_i_pessoas||' CNPJ.: '||w_cnpj to client;

			insert into bethadba.pessoas_juridicas(i_pessoas,i_naturezas,responsavel,cnpj,inscricao_estadual) on existing skip 
			values (w_i_pessoas,null,null,w_cnpj,null) 
		end if;
		
		// *****  Converte tabela bethadba.pessoas_enderecos
		set w_i_cidades=null;
		set w_i_ruas=null;

		if w_cdUf != 0 then
			set w_i_cidades=(w_CdUf*100000)+w_cdMunicipio
		else
			set w_i_cidades=null
		end if;
		
		if not exists(select 1 from bethadba.cidades where i_cidades = w_i_cidades) then
			set w_i_cidades=null
		end if;
		
		if w_cdMunicipio = 0 then
			set w_i_cidades=null
		end if;
		
		if w_complemento = '' then
			set w_complemento=null
		end if;

		if w_numero = 0 then
			set w_numero=null
		end if;

		if w_CdLogradouro = 0 then
			select first depois_1 
			into w_i_bairros 
			from tecbth_delivery.antes_depois 
			where tipo = 'B' 
			and antes_1 = w_i_entidades 
			and antes_2 = w_i_cidades 
			and antes_3 = w_cdBairro;
			
			if w_nome_rua != '' then
				if not exists(select 1 from bethadba.ruas where trim(nome) = trim(w_nome_rua)) then
					select coalesce(max(i_ruas),0)+1 
					into w_i_ruas 
					from bethadba.ruas;
					
					message 'Rua.: '||w_i_ruas||' Cid.: '||w_i_cidades||' Nom.: '||w_nome_rua||' CEP.: '||w_cep to client;
					
					insert into bethadba.ruas(i_ruas,i_ruas_ini,i_ruas_fim,i_cidades,nome,tipo,cep,epigrafe,lei,zona_fiscal)on existing skip
					values(w_i_ruas,null,null,w_i_cidades,w_nome_rua,67,w_cep,null,null,null);
					
					if w_i_bairros is not null then
						message 'Bai.: '||w_i_bairros||' Rua.: '||w_i_ruas to client;
						insert into bethadba.bairros_ruas(i_ruas,i_bairros) 
						values(w_i_ruas,w_i_bairros) 
					end if
				else
					select first i_ruas 
					into w_i_ruas 
					from bethadba.ruas 
					where trim(nome) = trim(w_nome_rua)
				end if;
				set w_nome_rua=null
			end if
		else
			select first depois_1 
			into w_i_ruas 
			from tecbth_delivery.antes_depois 
			where tipo = 'R' 
			and antes_1 = w_i_cidades 
			and antes_2 = w_CdLogradouro;
		
			select first i_bairros 
			into w_i_bairros 
			from bethadba.bairros_ruas 
			where i_ruas = w_i_ruas;
				
		end if;
      
		
		if trim(w_nome_rua) in('R','R;','R:','R.','Rf','Rua:','Rua:.') then
			set w_nome_rua=null
		end if;
		
		message 'Pes.: '||w_i_pessoas||' Rua.: '||w_i_ruas||' Bai.: '||w_i_bairros||' Nom.: '||w_nome_rua to client;
		
		insert into bethadba.pessoas_enderecos(i_pessoas,tipo_endereco,i_ruas,i_bairros,i_distritos,i_loteamentos,i_cidades,i_condominios,nome_rua,complemento,numero,bloco,apartamento,
											   nome_bairro,nome_distrito,nome_cidade_conv,cep) on existing skip 
		values (w_i_pessoas,'P',w_i_ruas,w_i_bairros,null,null,w_i_cidades,null,w_nome_rua,w_complemento,w_numero,null,null,null,null,null,w_cep);
		
				
		insert into bethadba.hist_enderecos(i_pessoas,tipo_endereco,dt_alteracoes,i_ruas,i_condominios,i_distritos,i_loteamentos,i_bairros,i_cidades,nome_rua,complemento,numero,bloco,
											apartamento,nome_bairro,nome_distrito,nome_cidade_conv,cep)on existing skip
		values (w_i_pessoas,'P',w_dt_nascimento,w_i_ruas,null,null,null,w_i_bairros,w_i_cidades,w_nome_rua,w_complemento,w_numero,null,null,null,null,null,w_cep);
	end for;
end
;

*/

