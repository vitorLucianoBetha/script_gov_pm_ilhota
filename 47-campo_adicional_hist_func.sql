
begin
	// *****  Tabela bethadba.funcionarios_compl
	declare w_i_funcionarios integer;
	declare w_texto long varchar;

	ooLoop: for oo as cnv_funcionarios_prop_adic dynamic scroll cursor for
		select 1 as w_i_entidades,cdMatricula as w_cdMatricula,SqContrato as w_SqContrato,CdPessoa as w_CdPessoa,date(DtAdmissao) as w_DtAdmissao,NrFichaRegistro as w_num_inteiro,
			   date(dtFimContrato) as w_dt_data,inConcurso as w_inConcurso,date(dtConcurso) as w_dtConcurso,dsConselhoRegional as w_dsConselhoRegional,nrInscricaoConcurso as w_nrInscricaoConcurso,
			   cdCI as w_cdCI 
		from tecbth_delivery.gp001_funcionario 
	do
		// **** Inicia as variaveis
		set w_texto = null;
		
		// *****  Converte tabela bethadba.funcionarios_compl 
		set w_i_funcionarios=cast(w_cdMatricula as integer);
		 // *****  Item 1 - Ficha de Registro
			if(w_num_inteiro != 0) and (w_num_inteiro is not null) then
				message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Car.: 1' to client;
				
                insert into bethadba.hist_funcionarios_prop_adic(i_caracteristicas,i_entidades,i_funcionarios,dt_alteracoes,valor_numerico,valor_decimal,valor_data,valor_caracter,valor_hora,
																valor_texto)on existing skip
				values (1,w_i_entidades,w_i_funcionarios,w_DtAdmissao,w_num_inteiro,null,null,null,null,null);

				
				
			end if;
			
			// *****  Item 2 - Data Final de contrato		
			if w_dt_data is not null then
				message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Car.: 2' to client;
			
				insert into bethadba.hist_funcionarios_prop_adic(i_caracteristicas,i_entidades,i_funcionarios,dt_alteracoes,valor_numerico,valor_decimal,valor_data,valor_caracter,valor_hora,
																valor_texto)on existing skip
				values (2,w_i_entidades,w_i_funcionarios,w_DtAdmissao,null,null,w_dt_data,null,null,null);
				
			end if;
			
			// *****  Item 3 - Concursado (S/N)	
			if w_inConcurso = 'S' then
				set w_texto='S'
			elseif w_inConcurso = 'N' then
				set w_texto='N'
			else
				set w_texto=null
			end if;
			
			if w_texto is not null then
				message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Car.: 3' to client;
			
			    
				
                insert into bethadba.hist_funcionarios_prop_adic(i_caracteristicas,i_entidades,i_funcionarios,dt_alteracoes,valor_numerico,valor_decimal,valor_data,valor_caracter,valor_hora,
																valor_texto)on existing skip
				values (3,w_i_entidades,w_i_funcionarios,w_DtAdmissao,null,null,w_dtConcurso,null,null,null);
		
			end if;
			
			// *****  Item 4 - Data Concurso	
			if w_dtConcurso is not null then		
				message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Car.: 4' to client;
			
							insert into bethadba.hist_funcionarios_prop_adic(i_caracteristicas,i_entidades,i_funcionarios,dt_alteracoes,valor_numerico,valor_decimal,valor_data,valor_caracter,valor_hora,
                valor_texto)on existing skip
values (4,w_i_entidades,w_i_funcionarios,w_DtAdmissao,null,null,null,null,null,w_texto);
				
			end if;
			
			// *****  Item 5 - Nr Inscricao Concurso
            set w_texto = null;
			set w_texto=w_nrInscricaoConcurso;
			
			if trim(w_texto) in( '0','') then
				set w_texto=null
			end if;
			
			if w_texto is not null then
				message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Car.: 5' to client;
			
			insert into bethadba.hist_funcionarios_prop_adic(i_caracteristicas,i_entidades,i_funcionarios,dt_alteracoes,valor_numerico,valor_decimal,valor_data,valor_caracter,valor_hora,
																valor_texto)on existing skip
				values (5,w_i_entidades,w_i_funcionarios,w_DtAdmissao,null,null,null,null,null,w_texto);	

				
			end if;
			
			// *****  Item 6 - Conselho Regional
            set w_texto = null;
			set w_texto=w_dsConselhoRegional;
			
			if trim(w_texto) in( '0','') then
				set w_texto=null
			end if;
			
			if w_texto is not null then
				message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Car.: 6' to client;
				
                insert into bethadba.hist_funcionarios_prop_adic(i_caracteristicas,i_entidades,i_funcionarios,dt_alteracoes,valor_numerico,valor_decimal,valor_data,valor_caracter,valor_hora,
                valor_texto)on existing skip
values (6,w_i_entidades,w_i_funcionarios,w_DtAdmissao,null,null,null,null,null,w_texto);
	
						
			end if;
			
			// *****  Item 7 - Contribuinte Individual
            set w_texto = null;
			set w_texto=cast(w_cdCI as decimal(20));
			
			if trim(w_texto) in( '0','') then
				set w_texto=null
			end if;
			
			if w_texto is not null then
				message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Car.: 7' to client;
			

                insert into bethadba.hist_funcionarios_prop_adic(i_caracteristicas,i_entidades,i_funcionarios,dt_alteracoes,valor_numerico,valor_decimal,valor_data,valor_caracter,valor_hora,
																valor_texto)on existing skip
				values (7,w_i_entidades,w_i_funcionarios,w_DtAdmissao,null,null,null,null,null,w_texto);
				
			end if; 
	end for;
end;





