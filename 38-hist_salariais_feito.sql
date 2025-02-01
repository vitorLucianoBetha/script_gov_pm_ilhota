ROLLBACK;

call bethadba.dbp_conn_gera(1, 2019, 300);
call bethadba.pg_setoption('wait_for_commit','on');
call bethadba.pg_habilitartriggers('off');
call bethadba.pg_setoption('fire_triggers','off');
COMMIT;

begin
  declare cur_conver dynamic scroll cursor for 
    select 1, Funcionario.CdMatricula, Funcionario.SqContrato, funcionario.dtAdmissao, escala.Nrdias, escala.NrHorasDia, HistoricoSalarial.dthistorico,
           HistoricoSalarial.cdEstruturaSalarial, HistoricoSalarial.CdGrupoFaixaSalarial, HistoricoSalarial.NrSequenciaFaixa, HistoricoSalarial.cdMotivo,
		   HistoricoSalarial.VlSalarioFaixa, HistoricoSalarial.VlSalario, HistoricoSalarial.dtgravacao, 1, HistoricoSalarial.cdcargo, Funcionario.dtRescisao
    from tecbth_delivery.gp001_Funcionario as Funcionario, 
         tecbth_delivery.gp001_HistoricoSalarial as HistoricoSalarial,
         tecbth_delivery.gp001_Escala as Escala 
    where Funcionario.CdMatricula = HistoricoSalarial.CdMatricula 
      and Funcionario.SqContrato = HistoricoSalarial.SqContrato 
      and Funcionario.CdEscalaTrabalho = Escala.CdEscala 
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
  declare w_dtRescisao date;
  // ***** Inicializando variaveis antes da execucao
  set w_cont=0;
  set w_i_funcionarios_aux=null;
  set w_number=0;
--
  open cur_conver with hold;
  L_item: loop
    fetch next cur_conver into w_i_entidades, w_cdMatricula, w_SqContrato, w_dtAdmissao, w_Nrdias, w_NrHorasDia, w_DtHistorico, w_cdEstruturaSalarial, 
          w_cdGrupoFaixaSalarial, w_NrSequenciaFaixa, w_cdMotivo, w_VlSalarioFaixa, w_VlSalario, w_dtGravacao, w_config, w_cdcargo, w_dtRescisao;
    if sqlstate = '02000' then
      leave L_item
    end if;
    set w_cont = w_cont+1;
    // *****  Inicializa Variaveis
    set w_i_funcionarios = null;
    set w_dt_alteracoes = null;
    set w_i_niveis = null;
    set w_i_clas_niveis = null;
    set w_i_referencias = null;
    set w_i_motivos_altsal = null;
    set w_i_atos = null;
    set w_salario = null;
    set w_horas_mes = null;
    set w_horas_sem = null;
    // *****  Converte tabela bethadba.hist_salariais
    set w_i_funcionarios = w_cdmatricula;
    if w_i_funcionarios_aux <> w_i_funcionarios then
      set w_number = 0;
    end if;
    set w_dt_alteracoes = hours(date(w_DtHistorico), 0);
    set w_number = w_number + 1;
    if w_number = 1 then
      set w_dt_alteracoes = hours(w_dtAdmissao, 0);
    else
      if date(w_dt_alteracoes) < w_dtAdmissao then
        set w_dt_alteracoes = dateadd(HOUR, 1, (select max(dt_alteracoes) from bethadba.hist_salariais
                                                where i_entidades = w_i_entidades
                                                  and i_funcionarios = w_i_funcionarios
                                                  and date(dt_alteracoes) = w_dtAdmissao));
      else
        set w_dt_alteracoes = w_DtHistorico;
      end if
    end if;
    if w_cdEstruturaSalarial <> 0 then
      message 'Estrutura: '||string(w_cdEstruturaSalarial)||' Faixa: '||string(w_cdGrupoFaixaSalarial)||' NrSequencialFaixa: '||string(w_NrSequenciaFaixa)||' DtAlt: '||w_DtHistorico to client;
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
      end if
    else
      set w_i_niveis = null;
      set w_i_clas_niveis = null;
      set w_i_referencias = null;
    end if;
    if w_i_niveis is not null then
      if not exists(select 1 from bethadba.niveis where i_entidades = w_i_entidades and i_niveis = w_i_niveis) then
        set w_i_niveis = null;
        set w_i_clas_niveis = null;
        set w_i_referencias = null;
      end if;
    end if;
    set w_salario = "truncate"(isnull(w_VlSalarioFaixa, 0.01), 2);
    if (w_i_niveis = 0) or (w_i_niveis is null) then
      set w_i_niveis = null;
      set w_i_clas_niveis = null;
      set w_i_referencias = null;
      set w_salario = "truncate"(isnull(w_VlSalario,0.01), 2);
    end if;
    set w_i_motivos_altsal = w_cdMotivo;
    if (w_i_motivos_altsal is null) or (w_i_motivos_altsal = 0) then
      set w_i_motivos_altsal = 12;
    end if;
    set w_horas_mes = round(cast((w_nrdias-1)*w_NrHorasDia*5 as decimal(5,2)),0);
    set w_horas_sem = w_horas_mes / 5;
    message string(w_i_entidades)+' - '+string(w_i_funcionarios)+' - '+string(w_number)+' - Salario - '+string(w_i_niveis)+' - '+string(w_config) to client;
    if exists(select 1 from bethadba.funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios) then
      if not exists(select 1 from bethadba.hist_salariais where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios) then
        insert into bethadba.hist_salariais(i_entidades, i_funcionarios, dt_alteracoes, i_niveis, i_clas_niveis, i_referencias, i_motivos_altsal, i_atos, salario, horas_mes, horas_sem)
        values(w_i_entidades, w_i_funcionarios, w_dt_alteracoes, w_i_niveis, w_i_clas_niveis, w_i_referencias, w_i_motivos_altsal, w_i_atos, w_salario, w_horas_mes, w_horas_sem);
      else
        if (w_dtRescisao is null) or (date(w_dt_alteracoes) < w_dtRescisao) then 
          set w_dt_alteracoes = dateadd(HOUR, 1, DATE(w_dt_alteracoes));
          if exists(select 1 from bethadba.hist_funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) or
             exists(select 1 from bethadba.hist_cargos where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) or 
             exists(select 1 from bethadba.hist_salariais where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) then
             set w_dt_alteracoes = dateadd(HOUR, 1, (select max(dt_alteracoes)
                                                     from (select dt_alteracoes from bethadba.hist_salariais
                                                           where i_entidades = w_i_entidades
                                                             and i_funcionarios = w_i_funcionarios
                                                             and date(dt_alteracoes) = date(w_dt_alteracoes)
                                                           union
                                                           select dt_alteracoes from bethadba.hist_funcionarios
                                                           where i_entidades = w_i_entidades
                                                             and i_funcionarios = w_i_funcionarios
                                                             and date(dt_alteracoes) = date(w_dt_alteracoes)
                                                           union
                                                           select dt_alteracoes from bethadba.hist_cargos
                                                           where i_entidades = w_i_entidades
                                                             and i_funcionarios = w_i_funcionarios
                                                             and date(dt_alteracoes) = date(w_dt_alteracoes)) as T));
             message '1-Passou aqui: ' || string(w_dt_alteracoes) to client;
          end if;
        else
          set w_dt_alteracoes = dateadd(HOUR, 1, DATE(w_dtRescisao));
          if exists(select 1 from bethadba.hist_funcionarios where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) or
             exists(select 1 from bethadba.hist_cargos where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) or 
             exists(select 1 from bethadba.hist_salariais where i_entidades = w_i_entidades and i_funcionarios = w_i_funcionarios and dt_alteracoes = w_dt_alteracoes) then
             set w_dt_alteracoes = dateadd(HOUR, 1, (select max(dt_alteracoes)
                                                     from (select dt_alteracoes from bethadba.hist_salariais
                                                           where i_entidades = w_i_entidades
                                                             and i_funcionarios = w_i_funcionarios
                                                             and date(dt_alteracoes) = date(w_dt_alteracoes)
                                                           union
                                                           select dt_alteracoes from bethadba.hist_funcionarios
                                                           where i_entidades = w_i_entidades
                                                             and i_funcionarios = w_i_funcionarios
                                                             and date(dt_alteracoes) = date(w_dt_alteracoes)
                                                           union
                                                           select dt_alteracoes from bethadba.hist_cargos
                                                           where i_entidades = w_i_entidades
                                                             and i_funcionarios = w_i_funcionarios
                                                             and date(dt_alteracoes) = date(w_dt_alteracoes)) as T));
             message '3-Passou aqui: ' || string(w_dt_alteracoes) to client;
          end if;
        end if;
        insert into bethadba.hist_salariais(i_entidades, i_funcionarios, dt_alteracoes, i_niveis, i_clas_niveis, i_referencias, i_motivos_altsal, i_atos, salario, horas_mes, horas_sem)
        values(w_i_entidades, w_i_funcionarios, w_dt_alteracoes, w_i_niveis, w_i_clas_niveis, w_i_referencias, w_i_motivos_altsal, w_i_atos, w_salario, w_horas_mes, w_horas_sem);
      end if;
    else
      message 'FUNCIONARIO NAO ENCONTRADO!!!!!' to client;
    end if;
    set w_i_funcionarios_aux = w_i_funcionarios;
  end loop L_item;
  close cur_conver;
end;

--> Comando para inserir um registro de historico salarial para os funcionários que não possuem na tabela gp001_HistoricoSalarial
insert into bethadba.hist_salariais
select 
    1 as i_entidades,
    Funcionario.CdMatricula as i_funcionarios,
    dtAdmissao as dt_alteracoes,
    NULL as i_niveis,
    NULL as i_clas_niveis,
    NULL as i_referencias,
    if (cdMotivo is null) or (cdMotivo = 0) then 12 else cdMotivo endif as i_motivos_altsal,
    NULL as i_atos,
    if (VlSalario is null) or (VlSalario = 0) then 0.01 else VlSalario endif as salario,
    cast("truncate"(cast((nrdias-1)*NrHorasDia*5 as decimal(5,2)), 2) as integer) as horas_mes,
    horas_mes/5 as horas_sem,
    NULL as observacao,
    NULL as controle_jornada_parc,
    NULL as deduz_iss, 
    NULL as aliq_iss, 
    NULL as qtd_dias_servico,
    NULL as dt_alteracao_esocial,
    dt_alteracoes as dt_chave_esocial
from  tecbth_delivery.gp001_Funcionario as Funcionario,
      tecbth_delivery.gp001_HistoricoSalarial as HistoricoSalarial,
      tecbth_delivery.gp001_Escala as Escala 
where Funcionario.CdMatricula *= HistoricoSalarial.CdMatricula 
  and Funcionario.SqContrato *= HistoricoSalarial.SqContrato 
  and Funcionario.CdEscalaTrabalho = Escala.CdEscala 
  and funcionario.cdMatricula in(select i_funcionarios from bethadba.funcionarios
                                 where not exists(select 1 from bethadba.hist_salariais
                                                  where hist_salariais.i_entidades = funcionarios.i_entidades
                                                    and hist_salariais.i_funcionarios = funcionarios.i_funcionarios));





--> PARA LIMPAR OS REGISTROS ANTES DE IMPORTAR NOVAMENTE:
call bethadba.dbp_conn_gera(1, 2019, 300);
call bethadba.pg_setoption('wait_for_commit','on');
call bethadba.pg_setoption('fire_triggers','off');
commit;
delete from bethadba.hist_salariais where i_entidades = 1 and i_funcionarios = 39012;
call bethadba.pg_setoption('wait_for_commit','off');
call bethadba.pg_setoption('fire_triggers','on');
commit;