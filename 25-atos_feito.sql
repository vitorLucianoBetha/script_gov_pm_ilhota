
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;
// ***** teste
begin
	// *****  Tabela bethadba.atos
	declare w_i_atos integer;
	declare w_num_ato char(10);
	declare w_num_diariooficial char(10);
	
	ooLoop: for oo as cnv_atos dynamic scroll cursor for
		select 12 as w_i_tipos_atos,nrleicargo as w_NrDocumentoLegal,coalesce(dtleicargo,'2000-01-01') as w_DtAnoDocumentoLegal,coalesce(dtleicargo,'2000-01-01') as w_dt_inicial,
			   coalesce(dtleicargo,'2000-01-01') as w_dt_vigorar,nrleicargo as w_ementa,dtleicargo as w_dt_publicacao,dtleicargo as w_DtAnoOficial,nrleicargo as w_NrOficial,
			   null as w_num_proc_tce,null as w_num_resolucao,null as w_i_fontes_divulga 
		from tecbth_delivery.gp001_CARGO
	do
		// *****  Inicializa Variaveis
		set w_i_atos=null;
		set w_num_ato=null;
		set w_num_diariooficial=null;
		
		// *****  Converte tabela bethadba.atos
		select coalesce(max(i_atos),0)+1 
		into w_i_atos 
		from bethadba.atos;
		
		set w_num_ato=w_NrDocumentoLegal;

	
			set w_num_diariooficial=w_NrOficial;
	
		
		if w_dt_vigorar is null then
			set w_dt_vigorar = w_dt_inicial;
		end if;
		
			message 'Ato.: '||w_i_atos||' Tip. Ato.: '||w_i_tipos_atos||' Dt Ini.: '||w_dt_inicial||' Dt Vig.: '||w_dt_vigorar to client;
			
			insert into bethadba.atos(i_atos,i_tipos_atos,num_ato,dt_inicial,dt_final,ementa,dt_publicacao,num_diariooficial,num_proc_tce,dt_vigorar,num_resolucao,origem_docto)on existing skip
			values (w_i_atos,w_i_tipos_atos,w_num_ato,w_dt_inicial,null,w_ementa,w_dt_publicacao,null,w_num_proc_tce,w_dt_vigorar,w_num_resolucao,300);
			

	end for;
end;
