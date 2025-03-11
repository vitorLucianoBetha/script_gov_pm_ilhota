CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
commit;

--- ATENÇÃO PARA CADA BASE DIFERENTE DEVE SER ADEQUADO O CODIGO ABAIXO

if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_config_organ') then
	drop procedure cnv_config_organ;
end if
;


begin
	// *****  Tabela bethadba.config_organ
	declare w_i_config_organ smallint;
	
	ooLoop: for oo as cnv_config_organ dynamic scroll cursor for
		select 1 as w_i_entidades,cdOrganograma as w_cdOrganograma,dsOrganograma as w_descricao
		from tecbth_delivery.GP001_organograma  
		order by 1,2 asc	
	do
		// *****  Inicializa Variaveis
		set w_i_config_organ=null;
		
		// *****  Converte tabela bethadba.config_organ
		if w_cdOrganograma = 99 then
			set w_i_config_organ=2
		elseif w_cdOrganograma = 101 then
			set w_i_config_organ=3
		else
			set w_i_config_organ=w_cdOrganograma
		end if;
		
		message 'Con. Org.: '||w_i_config_organ||' Des.: '||w_descricao to client;
		insert into bethadba.config_organ(i_config_organ,i_atos,descricao) on existing skip
		values (w_i_config_organ,null,w_descricao);		
		
		insert into tecbth_delivery.antes_depois 
		values('O',w_i_entidades,w_cdOrganograma,null,null,w_i_config_organ,null,null,null,null,null,null,null,null);
		
	end for;
end;

-- BTHSC-59216 - ajustes de niveis - André

insert into bethadba.niveis_organ(i_config_organ,i_niveis_organ,descricao,num_digitos,separador_nivel,tot_digitos)on existing skip 
values(1,1,'PM',2,'.',2);

insert into bethadba.niveis_organ(i_config_organ,i_niveis_organ,descricao,num_digitos,separador_nivel,tot_digitos)on existing skip 
values(1,2,'DEPTO',2,'.',4);

insert into bethadba.niveis_organ(i_config_organ,i_niveis_organ,descricao,num_digitos,separador_nivel,tot_digitos)on existing skip 
values(2,1,'PM',2,'.',2);

insert into bethadba.niveis_organ(i_config_organ,i_niveis_organ,descricao,num_digitos,separador_nivel,tot_digitos)on existing skip 
values(2,2,'DEPTO',2,'.',4);

insert into bethadba.niveis_organ(i_config_organ,i_niveis_organ,descricao,num_digitos,separador_nivel,tot_digitos)on existing skip 
values(3,1,'PM',2,'.',2);

insert into bethadba.niveis_organ(i_config_organ,i_niveis_organ,descricao,num_digitos,separador_nivel,tot_digitos)on existing skip 
values(3,2,'DEPTO',2,'.',4);

insert into bethadba.niveis_organ(i_config_organ,i_niveis_organ,descricao,num_digitos,separador_nivel,tot_digitos)on existing skip 
values(102,1,'PM',2,'.',2);

insert into bethadba.niveis_organ(i_config_organ,i_niveis_organ,descricao,num_digitos,separador_nivel,tot_digitos)on existing skip 
values(102,2,'DEPTO',2,'.',4);

insert into bethadba.niveis_organ(i_config_organ,i_niveis_organ,descricao,num_digitos,separador_nivel,tot_digitos)on existing skip 
values(103,1,'PM',2,'.',2);

insert into bethadba.niveis_organ(i_config_organ,i_niveis_organ,descricao,num_digitos,separador_nivel,tot_digitos)on existing skip 
values(103,2,'DEPTO',2,'.',4);

insert into bethadba.niveis_organ(i_config_organ,i_niveis_organ,descricao,num_digitos,separador_nivel,tot_digitos)on existing skip 
values(104,1,'PM',2,'.',2);

insert into bethadba.niveis_organ(i_config_organ,i_niveis_organ,descricao,num_digitos,separador_nivel,tot_digitos)on existing skip 
values(104,2,'DEPTO',2,'.',4);

insert into bethadba.niveis_organ(i_config_organ,i_niveis_organ,descricao,num_digitos,separador_nivel,tot_digitos)on existing skip 
values(105,1,'PM',1,'.',1);

insert into bethadba.niveis_organ(i_config_organ,i_niveis_organ,descricao,num_digitos,separador_nivel,tot_digitos)on existing skip 
values(105,2,'DEPTO',1,'.',1);



--------------------------------------------------
-- 41) Entidades Organ
--------------------------------------------------
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,1988,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,1989,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,1990,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,1991,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,1992,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,1993,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,1994,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,1995,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,1996,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,1997,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,1998,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,1999,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2000,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2001,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2002,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2003,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2004,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2005,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2006,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2007,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2008,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2009,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip 
values(1,2010,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2011,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2012,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2013,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2014,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2015,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2016,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2017,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2018,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2019,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2020,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2021,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2022,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2023,1)
;
insert into bethadba.entidades_organ(i_entidades,i_exercicios,i_config_organ)on existing skip  
values(1,2024,1)
;
