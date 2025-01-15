CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;
begin
	// *****  Tabela bethadba.variaveis
	declare w_i_funcionarios integer;
	declare w_i_tipos_proc smallint;
	declare w_i_eventos smallint;
	declare w_dt_inicial date;
	declare w_dt_final date;
	declare w_observacao char(50);
	
	// *****  Variaveis auxiliares
	declare w_dt_admissao date;
	ooLoop: for oo as cnv_variaveis dynamic scroll cursor for
		select 1 as w_i_entidades,CdMatricula as w_CdMatricula,SqContrato as w_SqContrato,CdVerba as w_CdVerba,tpcalculo as w_tpCalculo,DtInicioMovto as w_DtInicioMovto,
			   cast(VlMovimento as decimal(12,2)) as w_vlr_variavel,NrOcorrencia as w_NrOcorrencia,1 as w_i_processamentos,dtfinalValidade as w_dtfinalValidade
		from tecbth_delivery.gp001_movimentofixo   
		order by 1,2,3 asc	
	do
		
		// *****  Inicializa Variaveis
		set w_i_funcionarios=null;
		set w_i_tipos_proc=null;
		set w_i_eventos=null;
		set w_dt_inicial=null;
		set w_dt_final=null;
		set w_observacao=null;
		
		// *****
		set w_dt_admissao=null;
		
		// *****  Converte a tabela bethabda.variaveis
		set w_i_funcionarios=cast(w_cdMatricula as integer);
		
		if w_tpCalculo in (1,9,11) then 
			set w_i_tipos_proc=11
		elseif w_tpCalculo in (2,10) then 
			set w_i_tipos_proc=42
		elseif w_tpCalculo = 3 then 
			set w_i_tipos_proc=80
		elseif w_tpCalculo = 5 then set 
			w_i_tipos_proc=51
		elseif w_tpCalculo in(6,7) then 
			set w_i_tipos_proc=52
		elseif w_tpCalculo = 8 then 
			set w_i_tipos_proc=41
		end if;
		
		if w_i_tipos_proc is null or w_i_tipos_proc = 0 then
			set w_i_tipos_proc=11
		end if;
		
		select first i_eventos 
		into w_i_eventos 
		from tecbth_delivery.evento_aux 
		where evento = w_cdVerba
		and	retificacao = 'B' 
		and	resc_mov = 'N' 
		and	i_entidades = w_i_entidades;
		
		set w_dt_inicial = ymd(year(w_DtInicioMovto), month(w_DtInicioMovto),01);		
		set w_dt_final = ymd(year(w_dtfinalValidade),month(w_dtfinalValidade),1);		
		
		select dt_admissao 
		into w_dt_admissao 
		from bethadba.funcionarios 
		where i_entidades = w_i_entidades 
		and i_funcionarios = w_i_funcionarios;
		
		if w_dt_inicial < ymd(year(w_dt_admissao),month(w_dt_admissao),1) then
			set w_dt_inicial=ymd(year(w_dt_admissao),month(w_dt_admissao),1)
		end if;
		
		if (w_i_tipos_proc != 42) and (w_dt_final is not null) and (w_i_eventos is not null) then
			message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Tip. Pro.: '||w_i_tipos_proc||' Eve.: '||w_i_eventos||' Vlr.: '||w_vlr_variavel to client;
			
			insert into bethadba.variaveis(i_entidades,i_funcionarios,i_tipos_proc,i_processamentos,i_eventos,dt_inicial,dt_final,vlr_variavel)on existing skip
			values (w_i_entidades,w_i_funcionarios,w_i_tipos_proc,w_i_processamentos,w_i_eventos,w_dt_inicial,w_dt_final,w_vlr_variavel);
		end if;
		
	end for;
end;
commit;
