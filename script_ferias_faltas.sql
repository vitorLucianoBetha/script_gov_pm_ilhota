

CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit', 'on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;
if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_ferias') then
	drop procedure cnv_ferias;
end if
;

begin
	declare cnv_ferias dynamic scroll cursor for 
		select 1 as w_i_entidades,
					cdMatricula  as w_CdMatricula,
					Sqcontrato as w_Sqcontrato,
                    NrPeriodo as w_periodo,
					(select date(max(dt_periodo) + 1) from bethadba.periodos_ferias where i_funcionarios = w_CdMatricula and i_periodos = nrPeriodo and tipo = 2) as w_DtInicioPeriodo,                    
                    (select sum(num_dias) from bethadba.periodos_ferias where i_funcionarios = w_CdMatricula and i_periodos = nrPeriodo and tipo = 2) as w_total,
                    (select max(i_periodos_ferias) + 1 from bethadba.periodos_ferias where i_funcionarios = w_CdMatricula and i_periodos = nrPeriodo) as w_i_periodos_ferias, 
                    NrDiasPerdeAusencia as w_faltas
  	from tecbth_delivery.gp001_PERIODOAQUISICAO where w_faltas > 0 and w_faltas + w_total <= 30
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
        declare w_total float;
        declare w_faltas float;
        declare w_periodo integer;
		
		// *****  Tabela bethadba.ferias
		declare w_i_periodos_ferias smallint;
		declare w_i_periodos_ferias_ant smallint;
		set w_count = 1;
	
	open cnv_ferias with hold;
	  L_item: loop
	    fetch next cnv_ferias into w_i_entidades,w_cdMatricula,w_SqContrato, w_periodo, w_DtInicioPeriodo,w_total, w_i_periodos_ferias, w_faltas;
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
	
		if w_periodo is not null then
			message 'Ent.: '||w_i_entidades||' Fun.: '||w_CdMatricula||' Per.: '||w_periodo to client;
			
			insert into bethadba.periodos_ferias(i_entidades,i_funcionarios,i_periodos,i_periodos_ferias,tipo,dt_periodo,num_dias,observacao,manual,i_ferias,i_rescisoes)on existing skip
			values (w_i_entidades,w_CdMatricula,w_periodo,w_i_periodos_ferias,4,w_DtInicioPeriodo,w_faltas,null,'S',null,null);	
		end if;
		
  end loop L_item;
  close cnv_ferias
end; 

CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
COMMIT;

