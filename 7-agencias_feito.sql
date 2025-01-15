

--------------------------------------------------
-- 17) Agências
--------------------------------------------------
insert into bethadba.bancos(i_bancos,nome,sigla,mascara,dia_vcto,situacao) on existing skip
values (800,'CONVERSÃO','CONV',null,null,'A')
;



if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_agencias') then
	drop procedure cnv_agencias;
end if
;
begin
	// *****  Tabela bethadba.agencias
	declare w_dv_agencia char(2);
	declare w_i_ruas integer;
	declare w_i_bairros integer;
	
	ooLoop: for oo as cnv_agencias dynamic scroll cursor for
		select 1 as w_i_entidades,Agencia.NrBanco as w_i_bancos,Agencia.NrAgencia as w_i_agencias,trim(Agencia.NmAgencia) as w_nome,
			   trim(EnderecoAgencia.DsEndereco) as w_nome_rua,trim(EnderecoAgencia.DsComplemento) as w_complemento,EnderecoAgencia.CdLogradouro as w_CdLogradouro,EnderecoAgencia.cdUF as w_cdUF,
			   EnderecoAgencia.cdBairro as w_cdBairro,(EnderecoAgencia.cdUF *100000)+EnderecoAgencia.CdMunicipio as w_i_cidades,EnderecoAgencia.CdMunicipio as w_CdMunicipio, Agencia.dgagencia as w_dgagencia
		from tecbth_delivery.gp001_AGENCIA as Agencia,tecbth_delivery.gp001_EnderecoAgencia as EnderecoAgencia
		where Agencia.NrBanco = EnderecoAgencia.NrBanco 
		and Agencia.NrAgencia = EnderecoAgencia.NrAgencia  
		order by 1,2,3 asc	
	do
		// *****  Inicializa Variaveis
		set w_i_ruas=null;
		set w_i_bairros=null;

		// *****  Converte tabela bethadba.ruas
		if w_CdBairro = 0 then
			set w_i_bairros=null
		else
			select first depois_1 
			into w_i_bairros 
			from tecbth_delivery.antes_depois 
			where tipo = 'B' 
			and antes_1 = w_i_entidades 
			and antes_2 = (w_CdUF*100000)+w_CdMunicipio 
			and antes_3 = w_cdBairro
		end if;

		if w_i_cidades = 0 then
			set w_i_cidades=null
		end if;
		
		if w_CdLogradouro = 0 then
			if w_nome_rua != '' then
				if not exists(select 1 from bethadba.ruas where trim(nome) = trim(w_nome_rua)) then
					select coalesce(max(i_ruas),0)+1 
					into w_i_ruas 
					from bethadba.ruas;
					
					message 'Rua.: '||w_i_ruas||' Cid.: '||w_i_cidades||' Nom.: '||w_nome_rua to client;
					
					insert into bethadba.ruas(i_ruas,i_ruas_ini,i_ruas_fim,i_cidades,nome,tipo,cep,epigrafe,lei,zona_fiscal)on existing skip
					values(w_i_ruas,null,null,w_i_cidades,w_nome_rua,null,null,null,null,null);
				
					if w_i_bairros is not null then
						insert into bethadba.bairros_ruas(i_ruas,i_bairros) 
						values(w_i_ruas,w_i_bairros);
					end if
				else
					message 'Rua.: '||w_i_ruas||' Cid.: '||w_i_cidades||' Nom.: '||w_nome_rua to client;
					
					select i_ruas 
					into w_i_ruas 
					from bethadba.ruas 
					where trim(nome) = trim(w_nome_rua)
				end if
			end if
		else
			select first depois_1 
			into w_i_ruas 
			from antes_depois 
			where tipo = 'R' 
			and antes_1 = w_i_cidades 
			and antes_2 = w_CdLogradouro
		end if;
		
		// *****  Converte tabela bethadba.agencias
		if w_i_bancos in(1,757,758,761,762,763,764) then
			set w_i_bancos = 1
		elseif w_i_bancos in(27) then
			set w_i_bancos = 27
		elseif w_i_bancos in(38) then
			set w_i_bancos = 37	
		elseif w_i_bancos in(104) then
			set w_i_bancos = 104
		elseif w_i_bancos in(237) then
			set w_i_bancos = 237			
		elseif w_i_bancos in(756,765) then
			set w_i_bancos = 756		
		elseif w_i_bancos in(759,760) then
			set w_i_bancos = 800		
		end if;
		if trim(w_dgagencia) <> '' then
           set w_dv_agencia =  	 trim(w_dgagencia)
        else
		set w_dv_agencia = bethadba.dbf_calcmod11(w_i_agencias)
        end if;
		if(w_i_bancos != 0) and (w_i_agencias != 0) then
			if not exists(select 1 from bethadba.agencias where	i_bancos = w_i_bancos and i_agencias = w_i_agencias) then
				message 'Ban.: '||w_i_bancos||' Age.: '||w_i_agencias to client;
				
				insert into bethadba.agencias(i_bancos,i_agencias,i_ruas,i_bairros,i_cidades,dv_agencia,nome,cep,numero,complemento)on existing skip
				values(w_i_bancos,w_i_agencias,w_i_ruas,w_i_bairros,w_i_cidades,w_dv_agencia,w_nome,null,null,null); 
			end if
		end if;
	end for;
end;

