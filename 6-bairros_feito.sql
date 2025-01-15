
if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_bairros') then
	drop procedure cnv_bairros;
end if
;


begin
  // *****  Tabela bethadba.bairros
  declare w_i_bairros integer;
	ooLoop: for oo as cnv_bairro dynamic scroll cursor for 
		select 1 as w_i_entidades,
			(CdUf*100000)+cdMunicipio as w_cdMunicipio,
			cdBairro as w_cdBairro,
			trim(nmBairro) as w_nome 
		from tecbth_delivery.gp001_BAIRRO  
		order by 1,2 asc
	do
		
		// *****  Inicializa Variaveis
		set w_i_bairros=null;
		
		// *****  Converte tabela bethadba.bairros
		select coalesce(max(i_bairros),0)+1 
		into w_i_bairros 
		from bethadba.bairros;
		
		if not exists(select 1 from bethadba.bairros where bairros.nome = trim(w_nome)) then
			message 'Bai.: '||w_i_bairros||' Nom.: '||w_nome to client;
			
			insert into bethadba.bairros(i_bairros,nome)on existing skip
			values(w_i_bairros,w_nome);
			
			insert into tecbth_delivery.antes_depois 
			values('B',w_i_entidades,w_cdMunicipio,w_cdBairro,null,w_i_bairros,null,null,null,null); 
		else
			message 'Bai.: '||w_i_bairros||' Nom.: '||w_nome to client;
			
			select i_bairros 
			into w_i_bairros 
			from bethadba.bairros 
			where bairros.nome = trim(w_nome);
			
			insert into tecbth_delivery.antes_depois 
			values('B',w_i_entidades,w_cdMunicipio,w_cdBairro,null,w_i_bairros,null,null,null,null) ;
		end if;
	end for;
end
;


commit
