--24/10/2023 18:44
--------------------------------------------------
-- 01) Tabelas Auxiliares
--------------------------------------------------
if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cria_tabela') then
	drop procedure cria_tabela;
end if
;

create procedure tecbth_delivery.cria_tabela()
begin
	// ***** Cria a tabela conversao                                                   
	create table tecbth_delivery.conversao(
		ini_fim smallint null,
		descricao char(50) null,
		datahora DATETIME null);
	
	// ***** Cria a tabela antes_depois                                                  
	create table tecbth_delivery.antes_depois(
		tipo char(1) null,
		antes_1 integer null,
		antes_2 integer null,
		antes_3 integer null,
		antes_4 date null,
		depois_1 integer null,
		depois_2 integer null,
		depois_3 integer null,
		depois_4 date null,
		sistema char(25) null);
	
	// ***** Cria a tabela evento_aux                                               
	create table tecbth_delivery.evento_aux(
		evento smallint null,
		i_eventos smallint null,
		nome char(50) null,
		tipo_pd char(1) null,
		taxa decimal(10,4) null,
		unidade char(15) null,
		sai_rais char(1) null,
		compoe_liq char(1) null,
		compoe_hmes char(1) null,
		digitou_form char(1) null,
		classif_evento tinyint null,
		retificacao char(1) null,
		resc_mov char(1) null,
		i_entidades integer null);
end
;

call tecbth_delivery.cria_tabela()
;


--------------------------------------------------
-- 02) Funções
--------------------------------------------------
if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'fu_substrlim') then
	drop procedure fu_substrlim;
end if
;

create function tecbth_delivery.fu_substrlim(in string long varchar,in inicio integer,in caracter char(1))
returns long varchar
begin
	declare w_final long varchar;
	declare w_i integer;
	declare w_char long varchar;
	set w_i=1;
	set w_char=substr(string,inicio,length(string));
	
	while w_i <= length(w_char) loop
		if substr(w_char,w_i,1) != caracter then
			if w_i = 1 then
				set w_final=substr(w_char,w_i,1)
			else
				set w_final=w_final+substr(w_char,w_i,1)
			end if
		else
			set w_i=length(w_char)+1
		end if;
		
		set w_i=w_i+1
	end loop;
	return(w_final)
end
; 

if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'fu_tirachars') then
	drop procedure fu_tirachars;
end if
;

create function tecbth_delivery.fu_tirachars(in string long varchar,in caracter long varchar)
returns long varchar
begin
	declare w_final long varchar;
	declare w_string long varchar;
	declare w_letra char(1);
	declare w_i integer;
	declare w_j integer;
	set w_i=1;
	set w_j=1;
	set w_string=string;
	
	while w_j <= length(caracter) loop
		set w_letra=substr(caracter,w_j,1);
	
		while w_i <= length(w_string) loop
			if substr(w_string,w_i,1) not like(w_letra) then
				if w_i = 1 then
					set w_final=substr(w_string,w_i,1)
				else
					set w_final=w_final+substr(w_string,w_i,1)
				end if
			end if;
			set w_i=w_i+1
		end loop;
		
		set w_string=w_final;
		set w_i=1;
		set w_j=w_j+1
	end loop;
	return(w_final)
end 
;

if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'tira_caracter_1') then
	drop procedure tira_caracter_1;
end if
;

create function tecbth_delivery.tira_caracter_1(in x_valor char(22)) returns char(22)
begin
	declare w_k integer;
	declare w_trata_valor char(22);
	declare w_valores char(22);
	
	set w_k=1;
	set w_trata_valor='';
	set w_valores=x_valor;
	
	while(w_k <= (length(w_valores))) loop
		if(substr(w_valores,w_k,1) in('0','1','2','3','4','5','6','7','8','9',',')) then
			set w_trata_valor=w_trata_valor+substr(w_valores,w_k,1)
		end if;
		set w_k=w_k+1
	end loop;
	return(w_trata_valor)
end;
commit
;

if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'fu_convdecimal') then
	drop procedure fu_convdecimal;
end if
;

create function fu_convdecimal(in numero long varchar,in nDecimal integer) returns decimal
begin
	declare retorno decimal;
	declare cont integer;
	declare aux long varchar;
	declare pDecimal char(1);
	
	set cont=1;
	set pDecimal='N';
	set numero=trim(numero);
	
	while cont <= length(trim(numero)) loop
		if substr(numero,cont,1) != ',' and substr(numero,cont,1) != '.' and substr(numero,cont,1) != ' ' then
			set aux=aux+substr(numero,cont,1)
		elseif substr(numero,cont,1) = ',' then
			set aux=aux+'.';
			set pDecimal='S'
		end if;
		
		set cont=cont+1
	end loop;
	
	if PDecimal = 'S' then
		set retorno=convert(decimal,aux)
	else
		set retorno=convert(decimal,aux)/power(10,nDecimal)
	end if;
	
	return(retorno)
end
;
commit
;

if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'tira_caracter') then
	drop procedure tira_caracter;
end if
;

create function tira_caracter(in x_valor char(100))
returns char(100)
begin
  declare w_k integer;
  declare w_trata_valor char(100);
  declare w_valores char(100);
  set w_k=1;
  set w_trata_valor='';
  set w_valores=x_valor;
  while(w_k <= (length(w_valores))) loop
    if(substr(w_valores,w_k,1) in('X','.') ) then
      set w_trata_valor=w_trata_valor+substr(w_valores,w_k,1)
    end if;
    set w_k=w_k+1
  end loop;
  return(w_trata_valor)
end
;

if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'exists_func') then
	drop procedure exists_func;
end if
;

create function exists_func(in w_i_entidades integer,in w_i_funcionarios integer)
returns integer
begin
	if exists (select 1 from bethadba.funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios) then
		return 1
	else
		return 0
	end if;
end
;
