ROLLBACK;

CALL bethadba.dbp_conn_gera(1, 2019, 300);
CALL bethadba.pg_setoption('wait_for_commit','on');
CALL bethadba.pg_habilitartriggers('off');
call bethadba.pg_setoption('fire_triggers','off');
COMMIT;
begin
  declare cur_conver dynamic scroll cursor for 
    select 1,Funcionario.CdMatricula,Funcionario.SqContrato,dtAdmissao,
           Nrdias,NrHorasDia,if dtgravacao is null then dtAdmissao else dtgravacao endif,HistoricoSalarial.cdEstruturaSalarial,HistoricoSalarial.CdGrupoFaixaSalarial,
           HistoricoSalarial.NrSequenciaFaixa,cdMotivo,VlSalarioFaixa,VlSalario,dtgravacao,1, HistoricoSalarial.cdcargo
     from  tecbth_delivery.gp001_Funcionario as Funcionario,tecbth_delivery.gp001_HistoricoSalarial as HistoricoSalarial,tecbth_delivery.gp001_Escala as Escala 
    where  Funcionario.CdMatricula *= HistoricoSalarial.CdMatricula 
      and  Funcionario.SqContrato *= HistoricoSalarial.SqContrato 
      and  Funcionario.CdEscalaTrabalho = Escala.CdEscala 
/*
Os comandos abaixo foram comentados pois a CM Araucaria possui apenas uma entidade
    UNION
    select 1,Funcionario.CdMatricula,Funcionario.SqContrato,dtAdmissao,
           Nrdias,NrHorasDia,if dtgravacao is null then dtAdmissao else dtgravacao endif,HistoricoSalarial.cdEstruturaSalarial,HistoricoSalarial.CdGrupoFaixaSalarial,
           HistoricoSalarial.NrSequenciaFaixa,cdMotivo,VlSalarioFaixa,VlSalario,dtgravacao,1 
     from  tecbth_delivery.gp001_Funcionario as Funcionario,tecbth_delivery.gp001_HistoricoSalarial as HistoricoSalarial,tecbth_delivery.gp001_Escala as Escala 
    where  Funcionario.CdMatricula *= HistoricoSalarial.CdMatricula 
      and  Funcionario.SqContrato *= HistoricoSalarial.SqContrato 
      and  Funcionario.CdEscalaTrabalho = Escala.CdEscala 
    UNION
    select 1,Funcionario.CdMatricula,Funcionario.SqContrato,dtAdmissao,
           Nrdias,NrHorasDia,if dtgravacao is null then dtAdmissao else dtgravacao endif,HistoricoSalarial.cdEstruturaSalarial,HistoricoSalarial.CdGrupoFaixaSalarial,
           HistoricoSalarial.NrSequenciaFaixa,cdMotivo,VlSalarioFaixa,VlSalario,dtgravacao,1 
     from  tecbth_delivery.gp001_Funcionario as Funcionario,tecbth_delivery.gp001_HistoricoSalarial as HistoricoSalarial,tecbth_delivery.gp001_Escala as Escala 
    where  Funcionario.CdMatricula *= HistoricoSalarial.CdMatricula 
      and  Funcionario.SqContrato *= HistoricoSalarial.SqContrato 
      and  Funcionario.CdEscalaTrabalho = Escala.CdEscala 
    UNION
    select 1,Funcionario.CdMatricula,Funcionario.SqContrato,dtAdmissao,
           Nrdias,NrHorasDia,if dtgravacao is null then dtAdmissao else dtgravacao endif,HistoricoSalarial.cdEstruturaSalarial,HistoricoSalarial.CdGrupoFaixaSalarial,
           HistoricoSalarial.NrSequenciaFaixa,cdMotivo,VlSalarioFaixa,VlSalario,dtgravacao,1 
     from  tecbth_delivery.gp001_Funcionario as Funcionario,tecbth_delivery.gp001_HistoricoSalarial as HistoricoSalarial,tecbth_delivery.gp001_Escala as Escala 
    where  Funcionario.CdMatricula *= HistoricoSalarial.CdMatricula 
      and  Funcionario.SqContrato *= HistoricoSalarial.SqContrato 
      and  Funcionario.CdEscalaTrabalho = Escala.CdEscala 
*/
    order by 1 asc,2 asc,3 asc,7 asc,13 asc;
  // *****  Tabela bethadba.hist_salariais
  declare w_i_entidades integer;
  declare w_i_funcionarios integer;
  declare w_dt_alteracoes timestamp;
  declare w_i_niveis integer;
  declare w_i_clas_niveis char(3);
  declare w_i_referencias char(3);
  declare w_i_motivos_altsal smallint;
  declare w_i_atos integer;
  declare w_salario decimal(12,2);
  declare w_horas_mes decimal(5,2);
  declare w_horas_sem decimal(5,2);
  // *****  Tabela tecbth_conv.funcionario
  declare w_CdMatricula integer;
  declare w_SqContrato smallint;
  declare w_DtAdmissao timestamp;
  declare w_cdEstruturaSalarial integer;
  declare w_nrSequenciaFaixa smallint;
  declare w_cdGrupoFaixaSalarial smallint;
  // *****  Tabela tecbth_conv.historicosalarial
  declare w_DtHistorico timestamp;
  declare w_VlSalarioFaixa double;
  declare w_VlSalario double;
  declare w_cdMotivo smallint;
  declare w_DtGravacao timestamp;
  // *****  Tabela tecbth_conv.escala
  declare w_Nrdias smallint;
  declare w_NrHorasDia double;
  // *****  Variaveis auxiliares
  declare w_cont integer;
  declare w_number integer;
  declare w_i_funcionarios_aux integer;
  declare w_config smallint;
  declare w_hora_alt integer;
  declare w_cdcargo integer;
  set w_cont=0;
  set w_i_funcionarios_aux=null;
  set w_number=0;
  open cur_conver with hold;
  L_item: loop
    fetch next cur_conver into w_i_entidades,w_cdMatricula,w_SqContrato,w_dtAdmissao,w_Nrdias,w_NrHorasDia,
      w_DtHistorico,w_cdEstruturaSalarial,w_cdGrupoFaixaSalarial,w_NrSequenciaFaixa,w_cdMotivo,w_VlSalarioFaixa,w_VlSalario,w_dtGravacao,
      w_config, w_cdcargo;
    if sqlstate = '02000' then
      leave L_item
    end if;
    set w_cont=w_cont+1;
    // *****  Inicializa Variaveis
    set w_i_funcionarios=null;
    set w_dt_alteracoes=null;
    set w_i_niveis=null;
    set w_i_clas_niveis=null;
    set w_i_referencias=null;
    set w_i_motivos_altsal=null;
    set w_i_atos=null;
    set w_salario=null;
    set w_horas_mes=null;
    set w_horas_sem=null;
    // *****  Converte tabela bethadba.hist_salariais
    set w_i_funcionarios =  w_cdmatricula;
/*
    set w_i_funcionarios=(round(w_cdMatricula/1,0)*10)||w_SqContrato;
*/
    set w_dt_alteracoes=hours("date"(w_DtHistorico),0);
    if w_i_funcionarios_aux <> w_i_funcionarios then
      set w_number=0
    end if;
    set w_number=w_number+1;
    if w_number = 1 then
      set w_dt_alteracoes=hours(w_dtAdmissao,0)
    else
      if "date"(w_dt_alteracoes) < w_dtAdmissao then
        set w_dt_alteracoes=hours(w_dtAdmissao,1)
      else
        set w_dt_alteracoes=hours("date"(w_DtHistorico),1)
      end if
    end if;
    if w_cdEstruturaSalarial <> 0 then
      message 'Estrutura: '+string(w_cdEstruturaSalarial)+' Faixa:'+string(w_cdGrupoFaixaSalarial)+' NrSequencialFaixa:'+string(w_NrSequenciaFaixa) to client;
      if w_i_entidades = 1 then
        if w_cdestruturasalarial = 1 then
          select cdestruturasalarial || cast(substr(cdfaixasalarial, 1, 3) as integer), substr(cdfaixasalarial, 4, 3), substr(cdfaixasalarial, 7, 3)
          into w_i_niveis, w_i_clas_niveis, w_i_referencias
          from tecbth_delivery.gp001_salariofaixa
          where tecbth_delivery.gp001_salariofaixa.cdestruturasalarial = w_cdestruturasalarial
            and tecbth_delivery.gp001_salariofaixa.cdgrupofaixasalarial = w_cdgrupofaixasalarial
            and tecbth_delivery.gp001_salariofaixa.nrsequenciafaixa = w_nrsequenciafaixa;
        else
          if w_cdestruturasalarial = 3 then
            select cdestruturasalarial || cast(substr(cdfaixasalarial, 1, 1) as integer), substr(cdfaixasalarial, 2, 2), substr(cdfaixasalarial, 4, 3)
            into w_i_niveis, w_i_clas_niveis, w_i_referencias
            from tecbth_delivery.gp001_salariofaixa
            where tecbth_delivery.gp001_salariofaixa.cdestruturasalarial = w_cdestruturasalarial
              and tecbth_delivery.gp001_salariofaixa.cdgrupofaixasalarial = w_cdgrupofaixasalarial
              and tecbth_delivery.gp001_salariofaixa.nrsequenciafaixa = w_nrsequenciafaixa;
          end if;
        end if;

/*
   FOI RETIRADO PARA ATENDER A SITUAÇÃO DA CM ARAUCARIA
        select first cast(trim(cdFaixaSalarial) as integer) into w_i_niveis from tecbth_delivery.gp001_salariofaixa where
          CdGrupoFaixaSalarial = w_CdGrupoFaixaSalarial and
          NrSequenciaFaixa = w_NrSequenciaFaixa and
          cdEstruturaSalarial = w_cdEstruturaSalarial
*/
       end if
    else
      set w_i_niveis=null;
      set w_i_clas_niveis=null;
      set w_i_referencias=null
    end if;
    if not exists(select 1 from bethadba.niveis where i_entidades = w_i_entidades and i_niveis = w_i_niveis) then
      set w_i_niveis=null
    end if;
    set w_salario=cast(isnull(w_VlSalarioFaixa,1) as decimal(12,2));
    if(w_i_niveis = 0) or(w_i_niveis is null) then
      set w_i_niveis=null;
      set w_i_clas_niveis=null;
      set w_i_referencias=null;
      set w_salario=cast(isnull(w_VlSalario,0) as decimal(12,2))
    end if;
    set w_i_motivos_altsal=w_cdMotivo;
    if w_i_motivos_altsal = 0 then
      set w_i_motivos_altsal=1
    end if;
    set w_horas_mes="truncate"(cast((w_nrdias-1)*w_NrHorasDia*5 as decimal(5,2)),2);
    if w_horas_mes = 200.10 then
		set w_horas_mes = 200.00
	elseif w_horas_mes = 219.90 then
		set w_horas_mes = 220.00
	elseif w_horas_mes = 80.10 then
		set w_horas_mes = 80
	end if;	
    set w_horas_sem=w_horas_mes/5;
    set w_i_atos=null;
    if w_number = 1 then
      set w_i_motivos_altsal=null
    end if;
    message string(w_i_entidades)+' - '+string(w_i_funcionarios)+' - '+string(w_number)+' - Salario - '+string(w_i_niveis)+' - '+string(w_config) to client;
    if exists(select 1
              from bethadba.funcionarios
              where i_entidades = w_i_entidades
                and i_funcionarios = w_i_funcionarios) then
      if not exists(select 1 from bethadba.hist_salariais
                    where i_entidades = w_i_entidades
                      and i_funcionarios = w_i_funcionarios) then
        insert into bethadba.hist_salariais( i_entidades,i_funcionarios,dt_alteracoes,i_niveis,i_clas_niveis,
          i_referencias,i_motivos_altsal,i_atos,salario,horas_mes,horas_sem) values( w_i_entidades,w_i_funcionarios,
          w_dt_alteracoes,w_i_niveis,w_i_clas_niveis,w_i_referencias,w_i_motivos_altsal,w_i_atos,w_salario,w_horas_mes,
          w_horas_sem) 
      else
        if w_config = 1 then
          if exists(select 1 from bethadba.hist_salariais
                    where i_entidades = w_i_entidades
                      and i_funcionarios = w_i_funcionarios
                      and dt_alteracoes = (select max(dt_alteracoes) from bethadba.hist_salariais
                                           where i_entidades = w_i_entidades
                                             and i_funcionarios = w_i_funcionarios)
                                             and (i_niveis <> w_i_niveis or i_motivos_altsal <> w_i_motivos_altsal or salario <> w_salario)) then
            if exists(select 1 from bethadba.hist_salariais
                      where i_entidades = w_i_entidades
                        and i_funcionarios = w_i_funcionarios
                        and dt_alteracoes = w_dt_alteracoes) then
              update bethadba.hist_salariais set i_niveis = w_i_niveis,i_clas_niveis = w_i_clas_niveis,
                i_referencias = w_i_referencias,i_motivos_altsal = w_i_motivos_altsal,i_atos = w_i_atos,salario = w_salario,
                horas_mes = w_horas_mes,horas_sem = w_horas_sem where
                i_entidades = w_i_entidades and
                i_funcionarios = w_i_funcionarios and
                dt_alteracoes = w_dt_alteracoes
            else
              insert into bethadba.hist_salariais( i_entidades,i_funcionarios,dt_alteracoes,i_niveis,i_clas_niveis,
                i_referencias,i_motivos_altsal,i_atos,salario,horas_mes,horas_sem) values( w_i_entidades,w_i_funcionarios,
                w_dt_alteracoes,w_i_niveis,w_i_clas_niveis,w_i_referencias,w_i_motivos_altsal,w_i_atos,w_salario,w_horas_mes,
                w_horas_sem) 
            end if
          end if
        else
          if exists(select 1 from bethadba.hist_salariais where
              i_entidades = w_i_entidades and
              i_funcionarios = w_i_funcionarios and
              dt_alteracoes = w_dt_alteracoes) then
            select max(hour(dt_alteracoes)) into w_hora_alt from bethadba.hist_salariais where
              i_entidades = w_i_entidades and
              i_funcionarios = w_i_funcionarios and
              "date"(dt_alteracoes) = "date"(w_dt_alteracoes);
            set w_dt_alteracoes=hours(w_dt_alteracoes,w_hora_alt+1)
          end if;
          insert into bethadba.hist_salariais( i_entidades,i_funcionarios,dt_alteracoes,i_niveis,i_clas_niveis,
            i_referencias,i_motivos_altsal,i_atos,salario,horas_mes,horas_sem) values( w_i_entidades,w_i_funcionarios,
            w_dt_alteracoes,w_i_niveis,w_i_clas_niveis,w_i_referencias,w_i_motivos_altsal,w_i_atos,w_salario,w_horas_mes,
            w_horas_sem) 
        end if
      end if;
    end if;
    set w_i_funcionarios_aux=w_i_funcionarios;
  end loop L_item;
  close cur_conver
end;



