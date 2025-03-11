CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;

--select * from  tecbth_delivery.gp001_ORGANOGRAMA
-- BTHSC-59216 - ajustes de niveis - André

if not exists (select 1 from sys.syscolumns where creator = current user  and tname = 'tecbth_delivery.gp001_LOTACAO' and cname = 'i_config_organ') then 
    alter table tecbth_delivery.gp001_LOTACAO add(i_config_organ integer null, nivel1 varchar(5) null, nivel2 varchar(5) null,nivel3 varchar(5) null, nivel4 varchar(5) null );
end if;

-- PM
update tecbth_delivery.gp001_LOTACAO 
set nivel1 = substr(cdlotacao,1,2), 
    nivel2 = if trim(substr(cdlotacao,3,2)) = '' then 
                '00' 
            else 
                substr(cdlotacao,3,2) 
            endif;
commit;

-- CM
update tecbth_delivery.gp001_LOTACAO 
set nivel1 = substr(cdlotacao,1,1), 
    nivel2 = if trim(substr(cdlotacao,2,2)) = '' then 
                '00' 
            else 
                substr(cdlotacao,2,2) 
            endif;
commit;   




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
            set w_i_organogramas=string(w_nivel1)+string(w_nivel2)
        else
            set w_i_organogramas=string(w_nivel1)+string(w_nivel2)
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
end;

