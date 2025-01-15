
if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_ferias') then
	drop procedure cnv_ferias;
end if
;

begin
	declare cnv_ferias dynamic scroll cursor for 
		select pf.i_entidades,
               pf.i_funcionarios,
               pf.i_periodos,
               (select max(i_periodos_ferias) + 1 from bethadba.periodos_ferias pf2 where pf2.i_entidades = pf.i_entidades 
                and pf2.i_funcionarios = pf.i_funcionarios and pf2.i_periodos = pf.i_periodos) as periodos_ferias,
               (select date(max(dt_periodo) + 1) from bethadba.periodos_ferias pf2 where pf2.i_entidades = pf.i_entidades 
                and pf2.i_funcionarios = pf.i_funcionarios and pf2.i_periodos = pf.i_periodos) as dt_periodo,
               (select sum(num_dias) from bethadba.periodos_ferias pf2 where pf2.i_entidades = pf.i_entidades 
                and pf2.i_funcionarios = pf.i_funcionarios and pf2.i_periodos = pf.i_periodos and pf2.tipo <> 1) as total,
               num_dias_dir - total as num_dias,
               (select num_dias from bethadba.periodos_ferias pf2 where pf2.i_entidades = pf.i_entidades 
                and pf2.i_funcionarios = pf.i_funcionarios and pf2.i_periodos = pf.i_periodos and pf2.tipo = 1) as num_dias_dir,
               date(getdate()) as dataAgora
  	from bethadba.periodos pf where num_dias_dir - total > 0 and pf.dt_aquis_fin < dataAgora
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
		declare w_data_agora date;
		declare w_num_dias_desc tinyint;
		declare w_diferenca integer;
		declare w_count integer;
        declare w_total float;
        declare w_num_dias float;
        declare w_num_dias_dir float;
        declare w_periodo integer;
		
		// *****  Tabela bethadba.ferias
		declare w_i_periodos_ferias smallint;
		declare w_i_periodos_ferias_ant smallint;
		set w_count = 1;
	
	open cnv_ferias with hold;
	  L_item: loop
	    fetch next cnv_ferias into w_i_entidades,w_cdMatricula, w_periodo, w_i_periodos_ferias, w_DtInicioPeriodo,w_total, w_num_dias, w_num_dias_dir, w_data_agora;
	    if sqlstate = '02000' then
	      leave L_item
	    end if;	   

		// *****  Inicializa Variaveis
		set w_i_funcionarios=null;
		set w_i_ferias=null;
		set w_i_periodos=null;
		set w_num_dias_abono=null;
		set w_num_dias_desc=null;
		set w_num_dias_dir=null;
	
		if w_periodo is not null then
			message 'Ent.: '||w_i_entidades||' Fun.: '||w_CdMatricula||' Per.: '||w_periodo to client;
			
			insert into bethadba.periodos_ferias(i_entidades,i_funcionarios,i_periodos,i_periodos_ferias,tipo,dt_periodo,num_dias,observacao,manual,i_ferias,i_rescisoes)on existing skip
			values (w_i_entidades,w_CdMatricula,w_periodo,w_i_periodos_ferias,2,w_DtInicioPeriodo,w_num_dias,null,'S',null,null);	
		end if;
		
  end loop L_item;
  close cnv_ferias
end;