--------------------------------------------------
-- 14) Convertendo as ruas (ruas, bairros_ruas)
--------------------------------------------------
if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_logradouros') then
  drop procedure cnv_logradouros;
end if;

rollback;
begin
  declare cur_logradouros dynamic scroll cursor for
  	select cdPais,
	cdUF,
	(CdUf*100000)+cdMunicipio,
	cdLogradouro,
	nmLogradouro,
	isnull((select cast(gs.CdCep as char(8)) from tecbth_delivery.gp001_SINDICATO gs where gs.CdMunicipio = gl.cdMunicipio and gs.cdPais = gl.cdPais and gs.cdUF = gl.cdUF and gs.cdLogradouro = gl.cdLogradouro),'')
from tecbth_delivery.gp001_LOGRADOURO gl;

--
  declare w_pais integer;
  declare w_uf integer;
  declare w_i_ruas_format integer;
  declare w_logradouro integer;
  declare w_nome char(60);
  declare w_i_cidades char(60);
  declare w_cep char(8);
  declare w_cidade_nv integer;
--
  open cur_logradouros with hold;
  l_item: loop
    fetch next cur_logradouros into w_pais, w_uf,w_i_cidades,w_logradouro, w_nome, w_cep;
    if sqlstate='02000' then
      leave l_item; 
    end if;
    set w_i_ruas_format=null;
    set w_cidade_nv = null;
    message 'Ruas:'||w_logradouro||' Nome:'||w_nome to client;
   
    select  first isnull(i_cidades,3170701) into w_cidade_nv from bethadba.cidades where i_cidades = w_i_cidades;
   
	message 'Ruas:'||w_logradouro||' Nome:'||w_nome to client;

    if not exists(select 1 from bethadba.ruas where nome = w_nome and i_cidades = w_cidade_nv) then
      select isnull(max(i_ruas),0)+1 into w_i_ruas_format from bethadba.ruas;
      insert into bethadba.ruas(i_ruas,i_ruas_ini,i_ruas_fim,i_cidades,nome,tipo,cep,epigrafe,lei,zona_fiscal,extensao,dia_vcto,i_sefaz)
      values(w_i_ruas_format,null,null,w_cidade_nv,w_nome,67,w_cep,null,null,null,null,null,null);
    else
        select i_ruas into w_i_ruas_format from bethadba.ruas where nome = w_nome and i_cidades = w_cidade_nv;
    end if;
    --tecbth_delivery.antes_depois
	message 'Ruas:'||w_i_ruas_format||' Nome:'||w_nome to client;
    insert into tecbth_delivery.antes_depois(tipo,antes_1,antes_2,depois_1)   values('R',w_cidade_nv,w_logradouro,w_i_ruas_format);
  end loop;
end;


--rollback;


if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_bairros_ruas') then
  drop procedure cnv_bairros_ruas;
end if;


begin
  declare w_logradouro integer;
  declare w_cod_bairros integer;
  declare w_i_ruas integer;
  declare w_i_bairros integer;
  declare w_i_cidades varchar(60);
  declare w_cidade_nv integer;
--
  declare cur_bairros_ruas dynamic scroll cursor for 
	 select CdLogradouro,
		cdBairro,
		(CdUf*100000)+cdMunicipio
	 from tecbth_delivery.gp001_SINDICATO gs2 ;
--
  open cur_bairros_ruas with hold;
  l_item: loop
    fetch next cur_bairros_ruas into w_logradouro,w_cod_bairros,w_i_cidades;
    if sqlstate = '02000' then
      leave l_item;
    end if;
    set w_i_ruas=null;
    set w_i_bairros=null;
    select first isnull(i_cidades,3170701) into w_cidade_nv from bethadba.cidades where i_cidades = w_i_cidades;
    message 'Bairros/Cidades: ' || w_cod_bairros || '/' || w_i_cidades to client;
    if w_logradouro is not null and w_logradouro <> 0 and w_cod_bairros is not null and w_cod_bairros <> 0 then
      select distinct depois_1 into w_i_ruas from tecbth_delivery.antes_depois where tipo = 'R' and antes_1 = w_cidade_nv and antes_2 = w_logradouro;
      select distinct depois_1 into w_i_bairros from tecbth_delivery.antes_depois where tipo = 'B' and antes_1 = 1 and antes_2 = w_cidade_nv and antes_3 = w_cod_bairros;
      if w_i_bairros is not  null and w_i_ruas is not null then
        if not exists(select 1 from bethadba.bairros_ruas where i_ruas = w_i_ruas and i_bairros = w_i_bairros) then
          insert into bethadba.bairros_ruas(i_ruas,i_bairros) values(w_i_ruas,w_i_bairros);
        end if;
      end if; 
    end if;   
  end loop;
end;

commit