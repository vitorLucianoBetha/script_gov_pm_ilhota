CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;

---------------------------------------------------------------------------
-- 32) Inserindo os organogramas
---------------------------------------------------------------------------
update tecbth_delivery.gp001_lotacao
 set i_config_organ = cdorganograma,
	nivel1 = if trim(substr(cdlotacao,1,2))  = '' then '00' else substr(cdlotacao,1,2) endif,
	nivel2 = if trim(substr(cdlotacao,3,2))  = '' then '00' else substr(cdlotacao,3,2) endif
where cdorganograma = 1;

update tecbth_delivery.gp001_lotacao
  set i_config_organ = 2,
      nivel1 = if trim(substr(cdlotacao,1,2))  = '' then '00' else substr(cdlotacao,1,2) endif,
      nivel2 = if trim(substr(cdlotacao,3,2))  = '' then '00' else substr(cdlotacao,3,2) endif
where cdorganograma = 99;

update tecbth_delivery.gp001_lotacao
  set i_config_organ = 3,
      nivel1 = if trim(substr(cdlotacao,1,2))  = '' then '00' else substr(cdlotacao,1,1) endif,
      nivel2 = if trim(substr(cdlotacao,3,2))  = '' then '00' else substr(cdlotacao,2,2) endif
where cdorganograma = 101;

update tecbth_delivery.gp001_lotacao
  set i_config_organ = cdorganograma,
      nivel1 = if trim(substr(cdlotacao,1,2))  = '' then '00' else substr(cdlotacao,1,1) endif,
      nivel2 = if trim(substr(cdlotacao,3,2))  = '' then '00' else substr(cdlotacao,2,2) endif
where cdorganograma = 102;

update tecbth_delivery.gp001_lotacao
  set i_config_organ = cdorganograma,
      nivel1 = if trim(substr(cdlotacao,1,2))  = '' then '00' else substr(cdlotacao,1,1) endif,
      nivel2 = if trim(substr(cdlotacao,3,2))  = '' then '00' else substr(cdlotacao,2,2) endif
where cdorganograma = 103;

update tecbth_delivery.gp001_lotacao
  set i_config_organ = cdorganograma,
      nivel1 = if trim(substr(cdlotacao,1,2))  = '' then '00' else substr(cdlotacao,1,1) endif,
      nivel2 = if trim(substr(cdlotacao,3,2))  = '' then '00' else substr(cdlotacao,2,2) endif
where cdorganograma = 104;

update tecbth_delivery.gp001_lotacao
  set i_config_organ = cdorganograma,
      nivel1 = if trim(substr(cdlotacao,1,1))  = '' then '00' else substr(cdlotacao,1,1) endif,
      nivel2 = if trim(substr(cdlotacao,2,2))  = '' then '00' else substr(cdlotacao,2,2) endif
where cdorganograma = 105;


call bethadba.pg_setoption('fire_triggers','off');


CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
commit;

if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_organogramas_b') then
  drop procedure cnv_organogramas_b;
end if;

begin
	// *****  Tabela bethadba.organogramas
	declare w_i_config_organ smallint;
	declare w_i_organogramas char(16);
	declare w_i_parametros_previd smallint;
	declare w_tipo char(1);
    declare w_i_entidades integer;
	
	ooLoop: for oo as cnv_organogramas_b dynamic scroll cursor for
		select CdOrganograma as w_CdOrganograma, 
			CdLotacao as w_CdLotacao,
			NmLotacao as w_descricao,
			NrNivel as w_nivel,
			nivel1 as w_nivel1,
			nivel2 as w_nivel2,
			nivel3 as w_nivel3,
            nivel4 as w_nivel4,
            nivel5 as w_nivel5
		from tecbth_delivery.gp001_lotacao 
		order by 1,2,3 asc	
	do
		// *****  Inicializa Variaveis
		set w_i_config_organ = null;
		set w_i_organogramas = null;
		set w_i_parametros_previd = null;
		set w_tipo = null;
        set w_i_entidades = 1;
       
        if w_cdorganograma = 99 then
        	set w_cdorganograma = 2
        else
        	if w_cdorganograma = 101 then
        		set w_cdorganograma = 3
        	end if;
        end if;
		
		// *****  Converte tabela bethadba.organogramas
		set w_i_config_organ = w_cdorganograma;
	
        if w_cdorganograma in (1,2,3,102,103,104,105) then
          set w_i_organogramas = w_nivel1 || w_nivel2;
          if w_nivel = 2 then
            set w_tipo = 'A';
          else
            set w_tipo = 'S';
          end if;
        else
          if w_cdorganograma = 2 then
            set w_i_organogramas = w_nivel1 || w_nivel2;
            if w_nivel = 2 then
              set w_tipo = 'A';
            else
              set w_tipo = 'S';
            end if;
          else
            if w_cdorganograma = 3 then
              set w_i_organogramas = w_nivel1 || w_nivel2;
              if w_nivel = 2 then
                set w_tipo = 'A';
              else
                set w_tipo = 'S';
              end if;
            else
              if w_cdorganograma = 102 then
                set w_i_organogramas = w_nivel1 || w_nivel2;
                if w_nivel = 2 then
                  set w_tipo = 'A';
                else
                  set w_tipo = 'S';
                end if;
              else
                if w_cdorganograma = 103 then
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

