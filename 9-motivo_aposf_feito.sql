      CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;
            insert into bethadba.motivos_apos 
        select 
		cdDesligamento as i_motivos_apos,
		null as i_tipo_movpes,
		dsDesligamento as descricao,
		cast(cdRais as varchar(10)) as motivo_rais,
		cast(cdCaged as varchar(10)) as motivo_gifp,
		'N' as mantem_vinc,
		case cdDesligamento 
		when 4 then 2
		when 32 then 1
		when 33 then 3
		end as tipo_apos,
		8 as i_tipos_afast,
		null as i_tipos_mov_resc,
		null as categoria_esocial
		from tecbth_delivery.gp001_tipodesligamento
		where cdDesligamento in (4,33,32)