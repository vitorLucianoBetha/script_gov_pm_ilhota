CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;



insert into bethadba.motivos_resc(i_motivos_resc,i_tipos_movpes,descricao,dispensados,sair_fumbesc,num_caged,motivo_rais,cod_saque_fgts,movto_gfip,i_tipos_afast,
								  i_tipos_movpes_subst)on existing skip
select cdDesligamento,null,DsDesligamento,5,'N',CdCaged,CdRais,null,null,7,null from tecbth_delivery.gp001_tipodesligamento 

;

update gp001_tipodesligamento 
set i_motivos_resc = if CdDesligamento = 1 then 2 else
                      if CdDesligamento = 2 then 1 else  
                      if CdDesligamento = 3 then 16 else
                      if CdDesligamento =  4 then 7 else  
                      if CdDesligamento = 5 then 4 else  
                      if CdDesligamento = 6 then 17 else 
                      if CdDesligamento = 7 then 12 else 
                      if CdDesligamento = 8 then 18 else 
                      if CdDesligamento = 9 then 19 else 
                      if CdDesligamento = 10 then 8 else 
                      if CdDesligamento = 11 then 20 else 
                      if CdDesligamento = 12 then 21 else 
                      if CdDesligamento = 13 then 22 else 
                      if CdDesligamento = 14 then 23 endif endif endif endif endif endif endif endif endif endif endif endif endif endif
where i_motivos_resc is null;
commit
;

update gp001_tipodesligamento set i_motivos_apos = 3 where CdDesligamento = 4;
commit
;

update gp001_tipodesligamento , bethadba.motivos_resc
set categoria_esocial = right('00'||cdmtvdeslig_esocial,2)
where cddesligamento = i_motivos_resc
select * from bethadba.motivos_apos


update gp001_tipodesligamento , bethadba.motivos_apos
set categoria_esocial = 38
where cddesligamento = i_motivos_apos