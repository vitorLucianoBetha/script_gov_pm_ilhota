if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_horarios') then
	drop procedure cnv_horarios;
end if
;


begin
	// *****  Tabela bethadba.horarios
	declare w_i_horarios smallint;
	declare w_entrada time;
	declare w_saida time;
	declare w_saida_int time;
	declare w_entrada_int time;
	
	ooLoop: for oo as cnv_horarios dynamic scroll cursor for
		select 1 as w_i_entidades,cdHorario as w_CdHorario,dsHorario as w_descricao,HrEntrada as w_HrEntrada,HrInicioIntervalo as w_HrInicioIntervalo,HrFimIntervalo as w_HrFimIntervalo,
			HrSaida as w_HrSaida
		from tecbth_delivery.gp001_horario 
		order by 1,2 asc	  
	do
		
		// *****  Inicializa Variaveis
		set w_i_horarios=null;
		set w_entrada=null;
		set w_saida=null;
		set w_saida_int=null;
		set w_entrada_int=null;
		
		// *****  Converte tabela bethadba.horarios
		select coalesce(max(i_horarios),0)+1 
		into w_i_horarios 
		from bethadba.horarios;
		
		if length(string(w_HrEntrada)) < 4 then
			set w_entrada=convert(time,'0'+TRIM(substr(string(w_HrEntrada),1,1))+':'+TRIM(substr(string(w_HrEntrada),2,2)),9)
		else
			set w_entrada=convert(time,TRIM(substr(string(w_HrEntrada),1,2))+':'+TRIM(substr(string(w_HrEntrada),3,2)),9)
		end if;
		
		if length(string(w_HrInicioIntervalo)) < 4 then
			set w_saida_int=convert(time,'0'+TRIM(substr(string(w_HrInicioIntervalo),1,1))+':'+TRIM(substr(string(w_HrInicioIntervalo),2,2)),9)
		else
			set w_saida_int=convert(time,TRIM(substr(string(w_HrInicioIntervalo),1,2))+':'+TRIM(substr(string(w_HrInicioIntervalo),3,2)),9)
		end if;
		
		if length(string(w_HrFimIntervalo)) < 4 then
			set w_entrada_int=convert(time,'0'+TRIM(substr(string(w_HrFimIntervalo),1,1))+':'+TRIM(substr(string(w_HrFimIntervalo),2,2)),9)
		else
			set w_entrada_int=convert(time,TRIM(substr(string(w_HrFimIntervalo),1,2))+':'+TRIM(substr(string(w_HrFimIntervalo),3,2)),9)
		end if;
		
		if length(string(w_HrSaida)) < 4 then
			set w_saida=convert(time,'0'+TRIM(substr(string(w_HrSaida),1,1))+':'+TRIM(substr(string(w_HrSaida),2,2)),9)
		else
			set w_saida=convert(time,TRIM(substr(string(w_HrSaida),1,2))+':'+TRIM(substr(string(w_HrSaida),3,2)),9)
		end if;
		
		if w_entrada = '00:00' then
			set w_entrada=w_entrada_int;
			set w_entrada_int='00:00'
		end if;
		
		if not exists(select 1 from bethadba.horarios where entrada = w_entrada and	saida = w_saida and	saida_int = w_saida_int and	entrada_int = w_entrada_int) then
			message 'Hor.: '||w_i_horarios||' Des.: '||w_descricao||' Ent.: '||w_entrada||' Sai.: '||w_saida to client;
			insert into bethadba.horarios(i_horarios,descricao,entrada,saida,saida_int,entrada_int) 
			values (w_i_horarios,w_descricao,w_entrada,w_saida,w_saida_int,w_entrada_int);
			
			insert into tecbth_delivery.antes_depois 
			values ('H',w_i_entidades,w_CdHorario,null,null,w_i_horarios,null,null,null,null) 
		else
			select i_horarios 
			into w_i_horarios 
			from bethadba.horarios 
			where entrada = w_entrada 
			and saida = w_saida 
			and saida_int = w_saida_int 
			and entrada_int = w_entrada_int;
			
			insert into tecbth_delivery.antes_depois 
			values ('H',w_i_entidades,w_CdHorario,null,null,w_i_horarios,null,null,null,null);
		end if;
		
	end for;
end
;

/*
INSERT INTO Folharh.bethadba.horarios_ponto
(i_entidades, i_horarios_ponto, descricao, msg_relogio, classificacao, tipo, minima_hora, tolerancia_falta, tolerancia_extra, jornada_diaria, meia_jornada, tolerancia_alocacao, enviar_esocial)
VALUES(1, 1, 'Horário 001                                       ', NULL, '', 'L', '00:10:00.000', NULL, NULL, 10, NULL, 1, 'S');
INSERT INTO Folharh.bethadba.horarios_ponto
(i_entidades, i_horarios_ponto, descricao, msg_relogio, classificacao, tipo, minima_hora, tolerancia_falta, tolerancia_extra, jornada_diaria, meia_jornada, tolerancia_alocacao, enviar_esocial)
VALUES(1, 2, '3,3334                                                                                              ', NULL, '', 'L', '00:10:00.000', NULL, NULL, 10, NULL, 1, 'S');
INSERT INTO Folharh.bethadba.horarios_ponto
(i_entidades, i_horarios_ponto, descricao, msg_relogio, classificacao, tipo, minima_hora, tolerancia_falta, tolerancia_extra, jornada_diaria, meia_jornada, tolerancia_alocacao, enviar_esocial)
VALUES(1, 3, 'Escala 20 Semanal                                                                                   ', NULL, '', 'L', '00:10:00.000', NULL, NULL, 10, NULL, 1, 'S');
INSERT INTO Folharh.bethadba.horarios_ponto
(i_entidades, i_horarios_ponto, descricao, msg_relogio, classificacao, tipo, minima_hora, tolerancia_falta, tolerancia_extra, jornada_diaria, meia_jornada, tolerancia_alocacao, enviar_esocial)
VALUES(1, 4, 'Coleta de Lixo                                                                                      ', NULL, '', 'L', '00:10:00.000', NULL, NULL, 10, NULL, 1, 'S');
INSERT INTO Folharh.bethadba.horarios_ponto
(i_entidades, i_horarios_ponto, descricao, msg_relogio, classificacao, tipo, minima_hora, tolerancia_falta, tolerancia_extra, jornada_diaria, meia_jornada, tolerancia_alocacao, enviar_esocial)
VALUES(1, 5, 'Vigia Noturno 12/36                                                                                 ', NULL, '', 'L', '00:10:00.000', NULL, NULL, 10, NULL, 1, 'S');

INSERT INTO Folharh.bethadba.horarios_ponto
(i_entidades, i_horarios_ponto, descricao, msg_relogio, classificacao, tipo, minima_hora, tolerancia_falta, tolerancia_extra, jornada_diaria, meia_jornada, tolerancia_alocacao, enviar_esocial)
VALUES(1, 6, 'Coleta de Lixo - Noturno                                                                            ', NULL, '', 'L', '00:10:00.000', NULL, NULL, 10, NULL, 1, 'S');

INSERT INTO Folharh.bethadba.horarios_ponto
(i_entidades, i_horarios_ponto, descricao, msg_relogio, classificacao, tipo, minima_hora, tolerancia_falta, tolerancia_extra, jornada_diaria, meia_jornada, tolerancia_alocacao, enviar_esocial)
VALUES(1, 7, 'Normal                                                                                              ', NULL, '', 'L', '00:10:00.000', NULL, NULL, 10, NULL, 1, 'S');

INSERT INTO Folharh.bethadba.horarios_ponto
(i_entidades, i_horarios_ponto, descricao, msg_relogio, classificacao, tipo, minima_hora, tolerancia_falta, tolerancia_extra, jornada_diaria, meia_jornada, tolerancia_alocacao, enviar_esocial)
VALUES(1, 8, 'SETRAN                                                                                              ', NULL, '', 'L', '00:10:00.000', NULL, NULL, 10, NULL, 1, 'S');

INSERT INTO Folharh.bethadba.horarios_ponto
(i_entidades, i_horarios_ponto, descricao, msg_relogio, classificacao, tipo, minima_hora, tolerancia_falta, tolerancia_extra, jornada_diaria, meia_jornada, tolerancia_alocacao, enviar_esocial)
VALUES(1, 9, 'Vigia Diurno 12/36                                                                                  ', NULL, '', 'L', '00:10:00.000', NULL, NULL, 10, NULL, 1, 'S');