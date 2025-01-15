

CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;


---------------------------------------------------------------------------
-- 32) Inserindo os organogramas
---------------------------------------------------------------------------
if exists (select 1 from sys.syscolumns where creator = current user  and tname = 'gp001_lotacao' and cname = 'i_config_organ') then 
  alter table gp001_LOTACAO drop i_config_organ;
  alter table gp001_LOTACAO drop nivel1;
  alter table gp001_LOTACAO drop nivel2;
  alter table gp001_LOTACAO drop nivel3;
  alter table gp001_LOTACAO drop nivel4;
  alter table gp001_LOTACAO drop nivel5;
end if;

alter table gp001_lotacao add(i_config_organ integer, nivel1 varchar(4), nivel2 varchar(4), nivel3 varchar(4), nivel4 varchar(4), nivel5 varchar(4));



update gp001_lotacao
  set i_config_organ = cdorganograma,
      nivel1 = '1',
      nivel2 = cdlotacao
where cdorganograma = 1;

update gp001_lotacao
  set i_config_organ = cdorganograma,
      nivel1 = if trim(substr(cdlotacao,1,2))  = '' then '00' else substr(cdlotacao,1,2) endif,
      nivel2 = if trim(substr(cdlotacao,3,2))  = '' then '00' else substr(cdlotacao,3,2) endif,
      nivel3 = if trim(substr(cdlotacao,5,2))  = '' then '00' else substr(cdlotacao,5,2) endif,
      nivel4 = if trim(substr(cdlotacao,7,2))  = '' then '00' else substr(cdlotacao,7,2) endif,
      nivel5 = if trim(substr(cdlotacao,9,2))  = '' then '00' else substr(cdlotacao,9,2) endif
where cdorganograma = 2;

update gp001_lotacao
  set i_config_organ = cdorganograma,
      nivel1 = if trim(substr(cdlotacao,1,1))  = '' then '00' else substr(cdlotacao,1,1) endif,
      nivel2 = if trim(substr(cdlotacao,2,2))  = '' then '00' else substr(cdlotacao,2,2) endif,
      nivel3 = if trim(substr(cdlotacao,4,2))  = '' then '00' else substr(cdlotacao,4,2) endif,
      nivel4 = if trim(substr(cdlotacao,6,2))  = '' then '00' else substr(cdlotacao,6,2) endif,
      nivel5 = if trim(substr(cdlotacao,8,2))  = '' then '00' else substr(cdlotacao,8,2) endif
where cdorganograma = 3;

update gp001_lotacao
  set i_config_organ = cdorganograma,
      nivel1 = if trim(substr(cdlotacao,1,1))  = '' then '00' else substr(cdlotacao,1,1) endif,
      nivel2 = if trim(substr(cdlotacao,2,2))  = '' then '00' else substr(cdlotacao,2,2) endif,
      nivel3 = if trim(substr(cdlotacao,4,2))  = '' then '00' else substr(cdlotacao,4,2) endif,
      nivel4 = if trim(substr(cdlotacao,6,2))  = '' then '00' else substr(cdlotacao,6,2) endif
where cdorganograma = 5;

update gp001_lotacao
  set i_config_organ = cdorganograma,
      nivel1 = '101',
      nivel2 = cdlotacao
where cdorganograma = 101;

call bethadba.pg_setoption('fire_triggers','off');

if not exists(select 1 from bethadba.organogramas where i_config_organ = 1 and i_organogramas = '10000') then
  insert into bethadba.organogramas values(1,'10000',null,null,null,'PM ILHOTA',1,'S',null,null,null,null,null,null,null,null,null);
  insert into bethadba.organogramas values(1,'10100',null,null,null,'Teste',1,'S',null,null,null,null,null,null,null,null,null);
end if;


if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_organogramas') then
  drop procedure cnv_organogramas;
end if;

create procedure tecbth_conv.cnv_organogramas()
begin
	// *****  Tabela bethadba.organogramas
	declare w_i_config_organ smallint;
	declare w_i_organogramas char(16);
	declare w_i_parametros_previd smallint;
	declare w_tipo char(1);
    declare w_i_entidades integer;
	
	ooLoop: for oo as cnv_organogramas dynamic scroll cursor for
		select CdOrganograma as w_CdOrganograma, CdLotacao as w_CdLotacao, NmLotacao as w_descricao, NrNivel as w_nivel, nivel1 as w_nivel1, nivel2 as w_nivel2, nivel3 as w_nivel3,
               nivel4 as w_nivel4, nivel5 as w_nivel5
		from gp001_lotacao 
		order by 1,2,3 asc	
	do
		// *****  Inicializa Variaveis
		set w_i_config_organ = null;
		set w_i_organogramas = null;
		set w_i_parametros_previd = null;
		set w_tipo = null;
        set w_i_entidades = 1;
		
		// *****  Converte tabela bethadba.organogramas
		set w_i_config_organ = w_cdorganograma;
        if w_cdorganograma = 1 then
          set w_i_organogramas = w_nivel1 || w_nivel2;
          if w_nivel = 2 then
            set w_tipo = 'A';
          else
            set w_tipo = 'S';
          end if;
        else
          if w_cdorganograma = 2 then
            set w_i_organogramas = w_nivel1 || w_nivel2 || w_nivel3 || w_nivel4 || w_nivel5;
            if w_nivel = 5 then
              set w_tipo = 'A';
            else
              set w_tipo = 'S';
            end if;
          else
            if w_cdorganograma = 3 then
              set w_i_organogramas = w_nivel1 || w_nivel2 || w_nivel3 || w_nivel4 || w_nivel5;
              if w_nivel = 5 then
                set w_tipo = 'A';
              else
                set w_tipo = 'S';
              end if;
            else
              if w_cdorganograma = 5 then
                set w_i_organogramas = w_nivel1 || w_nivel2 || w_nivel3 || w_nivel4;
                if w_nivel = 4 then
                  set w_tipo = 'A';
                else
                  set w_tipo = 'S';
                end if;
              else
                if w_cdorganograma = 101 then
                  set w_i_organogramas = w_nivel1 || w_nivel2;
                  if w_nivel = 2 then
                    set w_tipo = 'A';
                  else
                    set w_tipo = 'S';
                  end if;
                end if;
              end if;
            end if;
          end if;
        end if;

		if w_nivel = 1 then
			set w_i_parametros_previd = 1;
		else
			set w_i_parametros_previd = null;
		end if;
		
		message 'Org.: '||w_i_organogramas||' Con.: '||w_i_config_organ||' Des.: '||w_descricao||' Niv.: '||w_nivel to client;
		if w_i_organogramas is not null  then 
		insert into bethadba.organogramas(i_config_organ,i_organogramas,i_ruas,i_bairros,descricao,nivel,tipo,cnpj,numero,complemento,cep,codigo_tce) on existing skip
		values (w_i_config_organ,w_i_organogramas,null,null,w_descricao,w_nivel,w_tipo,null,null,null,null,null);
        end if;
	end for;
end;


/*

-- BTHSC-59216 - ajustes de niveis - André

if not exists (select 1 from sys.syscolumns where creator = current user  and tname = 'tecbth_delivery.gp001_LOTACAO' and cname = 'i_config_organ') then 
	alter table tecbth_delivery.gp001_LOTACAO add(i_config_organ integer null, nivel1 char(1) null, nivel2 char(2) null,nivel3 char(3) null, nivel4 char(5) null );
end if
;

update tecbth_delivery.gp001_LOTACAO 
set nivel1 = substr(cdlotacao,1,1), 
    nivel2 = if trim(substr(cdlotacao,2,1)) = '' then 
				'0' 
			else 
				substr(cdlotacao,2,1) 
			endif,
    nivel3 = if trim(substr(cdlotacao,3,1)) = '' then 
				'0' 
			else 
				substr(cdlotacao,3,1) 
			endif, 
	nivel4 = if trim(substr(cdlotacao,4,2)) = '' then 
				'00' 
			else 
				substr(cdlotacao,4,2) 
			endif 		
;
commit
;	


if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_organogramas') then
	drop procedure cnv_organogramas;
end if
;


begin
	// *****  Tabela bethadba.organogramas
	declare w_i_config_organ smallint;
	declare w_i_organogramas char(16);
	declare w_i_parametros_previd smallint;
	declare w_tipo char(1);
	declare w_digitos integer;
	declare w_nivel_tipo integer;
	
	ooLoop: for oo as cnv_organogramas dynamic scroll cursor for
		select 1 as w_i_entidades,
			CdOrganograma as w_CdOrganograma,
			CdLotacao as w_CdLotacao,
			NmLotacao as w_descricao,
			sgLotacao as w_sigla,
			NrNivel as w_nivel,
			nivel1 as w_nivel1,
			nivel2 as w_nivel2,
			nivel3 as w_nivel3,
			nivel4 as w_nivel4
		from tecbth_delivery.GP001_lotacao 
		order by 1,2,3 asc
	do
		// *****  Inicializa Variaveis
		set w_i_config_organ=null;
		set w_i_organogramas=null;
		set w_i_parametros_previd=null;
		set w_tipo=null;
		
		// *****  Converte tabela bethadba.organogramas
		 set w_i_config_organ= 1 ;
		Select sum(num_digitos) 
		into w_digitos 
		from bethadba.niveis_organ 
		where i_config_organ = w_i_config_organ;
		
		if length(w_CdLotacao) < w_digitos then
			set w_i_organogramas=string(w_nivel1)+string(w_nivel2)+string(w_nivel3)+string(w_nivel4)  
		else
			set w_i_organogramas=string(w_nivel1)+string(w_nivel2)+string(w_nivel3)+string(w_nivel4)
		end if;
		
		if w_nivel = 1 then
			set w_i_parametros_previd=1
		else
			set w_i_parametros_previd=null
		end if;
		
		select max(i_niveis_organ) 
		into w_nivel_tipo 
		from bethadba.niveis_organ 
		where i_config_organ = w_i_config_organ;
		
		if w_nivel = w_nivel_tipo then
			set w_tipo='A'
		else
			set w_tipo='S'
		end if;
		
		message 'Org.: '||w_i_organogramas||' Con.: '||w_i_config_organ||' Des.: '||w_descricao||' Niv.: '||w_nivel to client;
		
		insert into bethadba.organogramas(i_config_organ,i_organogramas,i_ruas,i_bairros,descricao,sigla,nivel,tipo,cnpj,numero,complemento,cep,codigo_tce)on existing skip
		values (w_i_config_organ,w_i_organogramas,null,null,w_descricao,w_sigla,w_nivel,w_tipo,null,null,null,null,null);
		
	end for;
end
;

commit

*/

