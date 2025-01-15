if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_ferias') then
	drop procedure cnv_ferias;
end if
;

begin
	// *****  Tabela bethadba.ferias
	declare w_i_funcionarios integer;
	declare w_i_ferias smallint;
	declare w_i_periodos smallint;
	declare w_num_dias_abono tinyint;
	declare w_comp_abono date;
	declare w_num_dias_desc tinyint;
	declare w_num_dias_dir tinyint;
	
	// *****  Tabela bethadba.ferias
	declare w_i_periodos_ferias smallint;
	
	ooLoop: for oo as cnv_ferias dynamic scroll cursor for
		select 1 as w_i_entidades,cdMatricula as w_CdMatricula,Sqcontrato as w_Sqcontrato,DtInicioPeriodo as w_DtInicioPeriodo,date(DtInicioConcessao) as w_dt_gozo_ini,
			   date(dtFimConcessao) as w_dt_gozo_fin,NrDiasFeriasConcedidas as w_NrDiasFeriasConcedidas,
      NrDiasAbonoConcedidas as w_NrDiasAbonoConcedidas,isnull(date(DtPagamento),w_dt_gozo_ini) as w_DtPagamento
  from gp001_PERIODOCONCESSAO
		order by 1,2,3,4 asc
	do
		
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
			set w_NrDiasAbonoConcedidas = 0
		end if;
		
		if w_NrDiasAbonoConcedidas > 0 then 
			set w_num_dias_abono= w_NrDiasAbonoConcedidas;
			set w_comp_abono=ymd(year(w_DtPagamento),month(w_DtPagamento),'01');
		else
			set w_num_dias_abono= 0;
			set w_comp_abono=null
		end if;
		
		if (30-(w_dt_gozo_fin+1-w_dt_gozo_ini)) < 0 then
			set w_num_dias_desc=0
		else
			set w_num_dias_desc=(30-(w_dt_gozo_fin+1-w_dt_gozo_ini))
		end if;
		
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
			values (w_i_entidades,w_i_funcionarios,w_i_ferias,w_i_periodos,null,null,w_dt_gozo_ini,w_dt_gozo_fin,w_num_dias_abono,w_comp_abono,30,'S',
					0,w_num_dias_desc,w_num_dias_dir,0,'N',1);
		
			// *****  Converte tabela bethadba.periodos_ferias
			select coalesce(max(i_periodos_ferias),0)+1 
			into w_i_periodos_ferias 
			from bethadba.periodos_ferias 
			where i_entidades = w_i_entidades 
			and i_funcionarios = w_i_funcionarios 
			and	i_periodos = w_i_periodos;
		
			message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Per.: '||w_i_periodos||' Per. Fer.: '||w_i_periodos_ferias to client;
			insert into bethadba.periodos_ferias(i_entidades,i_funcionarios,i_periodos,i_periodos_ferias,tipo,dt_periodo,num_dias,observacao,i_ferias,i_rescisoes,manual)on existing skip
			values (w_i_entidades,w_i_funcionarios,w_i_periodos,w_i_periodos_ferias,2,w_dt_gozo_ini,w_num_dias_dir,null,w_i_ferias,null,'S');
		
			// *****  Converte tabela bethadba.periodos_ferias	
			if w_num_dias_abono > 0 then
				select coalesce(max(i_periodos_ferias),0)+1 
				into w_i_periodos_ferias 
				from bethadba.periodos_ferias 
				where i_entidades = w_i_entidades 
				and i_funcionarios = w_i_funcionarios 
				and i_periodos = w_i_periodos;
				
				message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Per.: '||w_i_periodos||' Per. Fer.: '||w_i_periodos_ferias||' Abo.: '||w_num_dias_abono to client;
				insert into bethadba.periodos_ferias( i_entidades,i_funcionarios,i_periodos,i_periodos_ferias,tipo,dt_periodo,num_dias,observacao,i_ferias,i_rescisoes,manual)on existing skip
				values (w_i_entidades,w_i_funcionarios,w_i_periodos,w_i_periodos_ferias,3,w_comp_abono,w_num_dias_abono,null,w_i_ferias,null,'S');
			end if;
		end if;
		
	end for;
end;




commit