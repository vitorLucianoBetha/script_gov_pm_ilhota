insert into bethadba.licpremio_config(i_licpremio_config,i_tipos_afast,descricao,tipo_intervalo,lanc_licpremio_afast)on existing skip
values(1,null,'Configuração Licença Premio','A','N')
;

commit
;


--------------------------------------------------
-- 46) Licpremio Faixas
--------------------------------------------------
insert into bethadba.licpremio_faixas(i_licpremio_config,i_faixas,num_direito_licenca,num_dias_licenca,num_max_abonar)on existing skip
values(1,99,5,90,30)
;

commit