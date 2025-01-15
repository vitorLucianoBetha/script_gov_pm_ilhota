if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_ferias') then
	drop procedure cnv_ferias;
end if
;

begin
	declare cnv_ferias dynamic scroll cursor for 
		select 1 as w_i_entidades,
					left(cdMatricula, length(cdMatricula) - 1) as w_CdMatricula,
					Sqcontrato as w_Sqcontrato,
					DtInicioPeriodo as w_DtInicioPeriodo,
					date(DtInicioConcessao) as w_dt_gozo_ini,
			   		date(dtFimConcessao) as w_dt_gozo_fin,
			   		NrDiasFeriasConcedidas as w_NrDiasFeriasConcedidas,
      				NrDiasAbonoConcedidas as w_NrDiasAbonoConcedidas,
      				isnull(date(DtPagamento),w_dt_gozo_ini) as w_DtPagamento
  	from tecbth_delivery.gp001_PERIODOCONCESSAO
		order by 1,2,3,4 asc;
	
		// *****  Tabela bethadba.ferias
		declare w_i_entidades integer;
		declare w_cdMatricula integer;
		declare w_SqContrato integer;
		declare w_NrDiasFeriasConcedidas integer;
		declare w_NrDiasAbonoConcedidas integer;
		declare w_DtPagamento date;
		declare w_DtInicioPeriodo date;
		declare w_dt_gozo_ini date;
		declare w_dt_gozo_ini_ant date;
		declare w_dt_gozo_fin date;
		declare w_i_funcionarios integer;
		declare w_i_funcionarios_ant integer;
		declare w_i_ferias smallint;
		declare w_i_periodos smallint;
		declare w_i_periodos_ant smallint;
		declare w_num_dias_abono tinyint;
		declare w_comp_abono date;
		declare w_num_dias_desc tinyint;
		declare w_num_dias_dir tinyint;
		declare w_diferenca integer;
		declare w_count integer;
		
		// *****  Tabela bethadba.ferias
		declare w_i_periodos_ferias smallint;
		declare w_i_periodos_ferias_ant smallint;
		set w_count = 1;
	
	open cnv_ferias with hold;
	  L_item: loop
	    fetch next cnv_ferias into w_i_entidades,w_cdMatricula,w_SqContrato,w_DtInicioPeriodo,w_dt_gozo_ini,w_dt_gozo_fin,w_NrDiasFeriasConcedidas,w_NrDiasAbonoConcedidas,w_DtPagamento;
	    if sqlstate = '02000' then
	      leave L_item
	    end if;	   

		// *****  Inicializa Variaveis
		set w_i_funcionarios=null;
		set w_i_ferias=null;
		set w_i_periodos=null;
		set w_num_dias_abono=null;
		set w_comp_abono=null;
		set w_num_dias_desc=null;
		set w_num_dias_dir=null;
		
		// *****
		set w_i_periodos_ferias=null;
		
		// *****  Converte tabela bethadba.ferias
		set w_i_funcionarios=cast(w_cdMatricula as integer);
		select coalesce(max(i_ferias),0)+1 
		into w_i_ferias 
		from bethadba.ferias 
		where i_entidades = w_i_entidades 
		and i_funcionarios = w_i_funcionarios;
		
		select first i_periodos 
		into w_i_periodos 
		from bethadba.periodos 
		where i_entidades = w_i_entidades 
		and i_funcionarios = w_i_funcionarios 
		and dt_aquis_ini = w_DtInicioPeriodo;
		
		if w_i_periodos is null then
			select first i_periodos 
			into w_i_periodos 
			from bethadba.periodos 
			where i_entidades = w_i_entidades 
			and i_funcionarios = w_i_funcionarios 
			and year(dt_aquis_ini) = year(w_DtInicioPeriodo)
		end if;
		
		if w_NrDiasAbonoConcedidas is null then
			set w_NrDiasAbonoConcedidas = 0;
		end if;
		
		if w_NrDiasAbonoConcedidas > 0 then 
			set w_num_dias_abono= w_NrDiasAbonoConcedidas;
			set w_comp_abono=ymd(year(w_DtPagamento),month(w_DtPagamento),'01');
		else
			set w_num_dias_abono= 0;
			set w_comp_abono=null;
		end if;		
		
		if  w_i_funcionarios <> w_i_funcionarios_ant and w_i_periodos <> w_i_periodos_ant then 
			set w_i_funcionarios_ant=w_i_funcionarios;
			set w_i_periodos_ant=w_i_periodos;
		end if; 
		
		set w_num_dias_desc = 0;
		if w_num_dias_desc < 0 then
			set w_num_dias_desc = 0
		end if; 
		
		if w_NrDiasFeriasConcedidas > 30 then
			set w_num_dias_dir =  30 
		else
			set w_num_dias_dir = w_NrDiasFeriasConcedidas
		end if;
		
	
		if w_i_periodos is not null then
			message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Fer.: '||w_i_ferias||' Per.: '||w_i_periodos to client;
			
			insert into bethadba.ferias(i_entidades,i_funcionarios,i_ferias,i_periodos,i_ferias_progr,i_atos,dt_gozo_ini,dt_gozo_fin,num_dias_abono,comp_abono,saldo_dias,desc_faltas,
									    num_faltas,num_dias_desc,num_dias_dir,desc_ferias,adiant_13sal,pagto_ferias)on existing skip
			values (w_i_entidades,w_i_funcionarios,w_i_ferias,w_i_periodos,null,null,w_dt_gozo_ini,w_dt_gozo_fin,w_num_dias_abono,w_comp_abono,30,'N',
					0,w_num_dias_desc,w_num_dias_dir,0,'N',1);				
		
			// *****  Converte tabela bethadba.periodos_ferias
			select coalesce(max(i_periodos_ferias),0)+1 
			into w_i_periodos_ferias 
			from bethadba.periodos_ferias 
			where i_entidades = w_i_entidades 
			and i_funcionarios = w_i_funcionarios 
			and	i_periodos = w_i_periodos;	
		
		end if;
		
  end loop L_item;
  close cnv_ferias
end; 

rollback;
CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;