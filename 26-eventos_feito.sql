-- Rodar em ordem (Parte 1 até a Parte 9 + eventos_prop_adic)

-- Parte 1 BTHSC-134414
begin 
	declare w_i_eventos integer;
	declare w_tipo_pd char(1);
	declare w_unidade char(15);
	declare w_sai_rais char(1);
	declare w_cont integer;
	set w_cont=0;

	select 1 as entidade,CdVerba 
	into #tempo1 
	from tecbth_delivery.gp001_VerbaTipoProcesso 
	where CdTipoProcesso = 2 
	union
	select 2 as entidade,CdVerba 
	from tecbth_delivery.gp001_VerbaTipoProcesso 
	where CdTipoProcesso = 2;

	ooLoop: for oo as cnv_evento_aux dynamic scroll cursor for
		select 1 as w_i_entidades,cdVerba as w_evento,upper(trim(DsVerba)) as w_nome,TpCategoria as w_TpCategoria,TpReferencia as w_TpReferencia 
		from tecbth_delivery.gp001_Verba 
		order by 2 asc
	do
		set w_cont = w_cont + 1;
		
		set w_tipo_pd=null;
		set w_unidade=null;
		set w_sai_rais=null;

		-- BTHSC-56554 / Ajuste no tipo do evento
		if w_TpCategoria  in ('P', 'V') then
			set w_tipo_pd = 'P'
		else
			set w_tipo_pd = 'D';
		end if;		
		
		if w_TpReferencia = 'H' then
			set w_unidade='Horas'
		elseif w_TpReferencia = 'P' then
			set w_unidade='Percentual'
		elseif w_TpReferencia = 'V' then
			set w_unidade='Valor'
		elseif w_TpReferencia = 'D' then
			set w_unidade='Dias'
		elseif w_TpReferencia = 'U' then
			set w_unidade='Unidade'
		end if;
		
		if exists(select 1 from #tempo1 where entidade = w_i_entidades and cdVerba = w_evento) then
			set w_sai_rais='S'
		else
			set w_sai_rais='N'
		end if;
		
		message 'Ent.: '||w_i_entidades||' Eve.: '||w_evento||' Nom.: '||w_nome||' Tip.: '||w_tipo_pd to client;
		
		insert into tecbth_delivery.evento_aux(evento,i_eventos,nome,tipo_pd,taxa,unidade,sai_rais,compoe_liq,compoe_hmes,digitou_form,classif_evento,retificacao,resc_mov,i_entidades) 
		values(w_evento,w_evento,w_nome,w_tipo_pd,0.0,w_unidade,w_sai_rais,'S','N','N',0,'B','N',w_i_entidades);
		
		if w_cont = 500 then
			set w_cont = 0;
			commit;
			message 'commit' to client;
		end if;		
	end for;
end
;

if exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_evento_atu') then
	drop procedure cnv_evento_atu;
end if
;


;

call tecbth_delivery.cnv_evento_atu()
;

commit
;



if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_eventos') then
	drop procedure cnv_eventos;
end if
;

begin
	
	ooLoop: for oo as cnv_eventos dynamic scroll cursor for
		select distinct i_eventos as w_i_eventos,nome as w_nome,tipo_pd as w_tipo_pd,taxa as w_taxa,unidade as w_unidade,sai_rais as w_sai_rais,compoe_liq as w_compoe_liq,compoe_hmes as w_compoe_hmes,
		       	        digitou_form as w_digitou_form,classif_evento as w_classif_evento,evento as w_cods_conver,i_entidades as w_i_entidades 
		from tecbth_delivery.evento_aux 
	do	
-- BUG BTHSC-8214	
       --if  w_i_eventos >= 400 and w_tipo_pd in('P','D') then
		message 'Eve.: '||w_i_eventos||' Nom.: '||w_nome||' Tip.: '||w_tipo_pd to client;
		insert into bethadba.eventos(i_eventos,nome,tipo_pd,taxa,unidade,sai_rais,compoe_liq,compoe_hmes,digitou_form,classif_evento,cods_conversao,desativado,seq_impressao,
									codigo_tce,caracteristica, deduc_fundo_financ, natureza, i_atos, envia_fly_transparencia, montar_base_fgts_integral_afast, enviar_esocial)on existing skip
		values (w_i_eventos,w_nome,w_tipo_pd,w_taxa,w_unidade,w_sai_rais,w_compoe_liq,w_compoe_hmes,w_digitou_form,w_classif_evento,null,'N',null,w_i_eventos, 'F' ,'N', null, null, 'N', 'N', 'S'); 
      --end if;
	  -- BUG BTHSC-8214

	  insert into bethadba.hist_eventos(i_eventos,i_competencias, nome,tipo_pd,taxa,unidade,sai_rais,compoe_liq,compoe_hmes,digitou_form,classif_evento,cods_conversao,desativado,seq_impressao,
									codigo_tce,caracteristica, deduc_fundo_financ)on existing skip
		values (w_i_eventos,'2024-01-01', w_nome,w_tipo_pd,w_taxa,w_unidade,w_sai_rais,w_compoe_liq,w_compoe_hmes,w_digitou_form,w_classif_evento,null,'N',null,w_i_eventos, 'F','N'); 
	
	end for;
end;

commit;



-- PARTE 2 BTHSC-134414
insert into bethadba.eventos_hist_agrup_esocial (i_eventos, i_competencia_inicio ,codigo_esocial, nome_esocial, tabela_rubrica, rubrica_esocial, incidencia_previdencia, incidencia_irrf, incidencia_fgts, incidencia_contrato_sindical, incidencia_rpps)
on existing skip   SELECT
	 d.i_eventos AS ID_CLOUD,
	 '1990-01-01',
	id_cloud,
	nome as nome_esocial,
	1,
	if v.cdNaturezaRubrica = 0 then null else v.cdNaturezaRubrica endif as rubrica_esocial,
	case v.codIncCP 
	when 18003 then '11' 
	when 18001 then '00'
	when 18004 then '12'
	when 18015 then '51'
	when 18012 then '32'
	when 18006 then '22'
	END AS incidencia_previdencia,
	
	
	case v.codIncIRRF 
	when 19003 then '11'
	when 19046 then '79'
	when 19004 then '12'
	when 19005 then '13'
	when 19014 then '42'
	when 19020 then '52'
	when 19019 then '51'
	when 19013  then '79'
	when 19037 then '79'
	else '00'
	end as incidencia_irrf,
	
	case codIncFgts
	when 20002 then '11'
	when 20001 then '00'
	when 20003 then '12'
	end as incidencia_fgts,
	
	case codIncSind
	when 21001 then '00'
	when 21003 then '31'
	when 21002 then '11'	
	end as incidencia_contrato_sindical,

	case codIncCPRP
	when 22001 then '00'
	when 22002 then '11'
	when 22003 then '12'
	when 22004 then '31'
	when 22005 then '32'
	end as incidencia_rpps

FROM tecbth_delivery.gp001_verba v
LEFT JOIN tecbth_delivery.evento_aux d on d.evento = v.CdVerba
where id_cloud is not null 
commit;

-- PARTE 3 BTHSC-134414
insert into bethadba.itens_lista (i_caracteristicas, i_itens_lista, descricao, dt_expiracao)
values (23905, 'E', 'Eventual', '2999-12-31');
insert into bethadba.itens_lista (i_caracteristicas, i_itens_lista, descricao, dt_expiracao)
values (23905, 'P', 'Percentual', '2999-12-31');

-- PARTE 4 BTHSC-134414

INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20361, 12, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20358, 12, 'N', '2999-12-31');

INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20151, 12, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20166, 1, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20167, 2, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20278, 3, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20279, 4, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20280, 6, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20281, 7, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20282, 5, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20305, 8, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20312, 9, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20329, 10, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20330, 11, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20376, 13, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20377, 14, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20394, 15, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20396, 16, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20402, 17, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20416, 19, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20417, 20, 'N', '2999-12-31');
INSERT INTO FolhaRh.bethadba.eventos_caract_cfg
(i_caracteristicas, ordem, permite_excluir, dt_expiracao)
VALUES(20513, 18, 'N', '2999-12-31');


-- Parte 5 BTHSC-134414

update bethadba.hist_eventos ev 
left join tecbth_delivery.gp001_verba v 
on ev.i_eventos = v.CdVerba 
left join tecbth_delivery.gp001_constanteCalculo cc 
on SIMILAR(V.DsVerba, cc.dsConstante) >= 35
set taxa = isnull(cc.vlConstante, 0);

update bethadba.EVENTOS ev 
left join tecbth_delivery.gp001_verba v 
on ev.i_eventos = v.CdVerba 
left join tecbth_delivery.gp001_constanteCalculo cc 
on SIMILAR(V.DsVerba, cc.dsConstante) >= 35
set taxa = isnull(cc.vlConstante, 0);


-- Parte 6 BTHSC-134414

if  exists (select 1 from sys.sysprocedure where creator = (select user_id from sys.sysuserperms where user_name = current user) and proc_name = 'cnv_eventos_prop_adic') then
	drop procedure cnv_eventos_prop_adic;
end if;

begin
	ooLoop: for oo as cnv_eventos_prop_adic dynamic scroll cursor for
		select distinct i_eventos as w_i_eventos from bethadba.eventos
	do
		message '(Inserindo os adicionais dos eventos) ' || w_i_eventos to client;
	
		
			INSERT INTO Folharh.bethadba.eventos_prop_adic
			(i_caracteristicas, i_eventos, valor_numerico, valor_decimal, valor_data, valor_caracter, valor_hora, valor_texto)
			VALUES(20361, w_i_eventos, NULL, NULL, NULL, 'S', NULL, NULL);


			INSERT INTO Folharh.bethadba.eventos_prop_adic
			(i_caracteristicas, i_eventos, valor_numerico, valor_decimal, valor_data, valor_caracter, valor_hora, valor_texto)
			VALUES(20358, w_i_eventos, NULL, NULL, NULL, '1', NULL, NULL);


			INSERT INTO Folharh.bethadba.eventos_prop_adic
			(i_caracteristicas, i_eventos, valor_numerico, valor_decimal, valor_data, valor_caracter, valor_hora, valor_texto)
			VALUES(23905, w_i_eventos, NULL, NULL, NULL, 'P', NULL, NULL);

	end for;
end;
commit;




-- Parte 7 BTHSC-134414

update bethadba.eventos e
left join tecbth_delivery.gp001_MOVIMENTOATOLEGALTABELA mat 
on e.i_eventos = mat.cdchave
set e.descricao = mat.dsMovimentoAtoLegal
where e.descricao is null

-- Parte 8 BTHSC-134414
update bethadba.eventos e 
left join bethadba.eventos_prop_adic epc 
on e.i_eventos = epc.i_eventos 
set e.caracteristica = 'V'
where epc.valor_caracter ='E'


update bethadba.hist_eventos e 
left join bethadba.eventos_prop_adic epc 
on e.i_eventos = epc.i_eventos 
set e.caracteristica = 'V'
where epc.valor_caracter ='E'

update bethadba.eventos e 
left join bethadba.eventos_prop_adic epc 
on e.i_eventos = epc.i_eventos 
set e.caracteristica = 'F'
where epc.valor_caracter ='P'

update bethadba.hist_eventos e 
left join bethadba.eventos_prop_adic epc 
on e.i_eventos = epc.i_eventos 
set e.caracteristica = 'F'
where epc.valor_caracter ='P'


-- PARTE 8 BTHSC-133910
UPDATE bethadba.eventos SET tipo_pd = 'P', compoe_liq = 'N' WHERE i_eventos IN (19,21,22,27,28,119,120,123,125,126,127,128,137,139,140,142,143,144,147,148,149,174,190,195,196,381,382,480,573,574,581,582,583,584,585,586,587,588,589,590,591,592,600,624,625,626,652,653,654,655,656,657,660,661,691,692,693,694,695,696,697,698,699,700,701,702,703,704,705,706,707,709,710,712,713,717,727,745,746,749,750,751,752,753,754,755,756,757,758,759,760,762,763,764,765,766,767,768,771,772,773,774,775,776,777,778,779,780,781,785,786,787,788,789,790,791,792,793,795,898,899,900,901,902,903,904,906,908,912,913,918,922,923,924,926,927,928,929,930,931,932,933,934,936,956,958,959,960,1002,1003,1004,1009,1014,1015,1016,1017,1018,1019,1020,1021,1022,1023,1024,1025,1026,1027,1028,1035,1039,1042,1046,1055,1056,1057,1058,1059,1060,1061,1062,1063,1064,1065,1066,1067,1068,1069,1070,1071,1072,1073,1077,1078,1079,1080,1081,1082,1083,1084,1085,1086,1087,1088,1089,1090,1093,1094,1095,1096,1097,1098,1099,1100,1101,1102,1103,1104,1105,1106,1107,1108,1117,1118,1131,1132,1144,1145,1155,1157,1160,1166,1167,1168,1169,1170,1171,1172,1173,1175,1185,1186,1187,1191,1192,1193,1194,1195,1198,1200,1202,1206,1207,1212,1213,1214,1215,1218,1219,1220,1221,1222,1223,1224,1225,1227,1228,1229,1230,1231,1232,1233,1234,1235,1246,1247,1248,1249,1250,1251,1254,1255,1256,1257,1260,1261,1262,1263,1264,1265,1266,1276,1277,1278,1279,1280,1281,1282,1283,1284,1285,1287,1288,1289,1290,1291,1292,1293,1294,1295,1296,1297,1301,1303,1305,1306,1307,1311,1313,1314,1315,1316,1317,1318,1319,1320,1321,1322,1323,1324,1325,1326,1329,1330,1331,1333,1338,1340,1348,1349,1350,1351,1352,1353,1360,1361,1362,1363,1365,1366,1367,1371,1372,1373,1374,1375,1376,1382,1383,1384,1385,1386,1387,1388,1389,1390,1391,1392,1393,1394,1395,1396,1397,1398,1399,1400,1403,1404,1405,1407,1412,1418,1419,1420,1421,1422,1423,1424,1425,1426,1427,1428,1429,1430,1431,1432,1433,1434,1435,1436,1437,1438,1439,1440,1441,1442,1443,1444,1445,1446,1447,1451,1452,1468,1470,1471,1474,1475,1476,1477,1478,1479,1480,1481,1482,1483,1484,1485,1494,1495,1496,1497,1498,1499,1500,1501,1502,1504,1505,1508,1525,1526,1528,1991,1992,1993,1994,1995,1996,1997,1998,2000,2001,2002,2100,2101,2102,2103,2104,2200,2201,2202,2203,2250,2300,2301,2302,2303,2304,2305,3001,3002,3019,3029,3035,3045,3051,3056,3057,3074,3120,3121,3122,3123,3124,3125,3126,3127,3131,3132,3133,3159,3162,3164,3174,3176,3177,3178,3180,3181,3182,3190,3205,3236,3242,3244,3248,3253,3275,3286,3287,3288,3369,3370,4000,4001,4002,4003,4004);
UPDATE bethadba.eventos SET tipo_pd = 'D', compoe_liq = 'N' WHERE i_eventos IN (650,651,658,659,662,663,664,667,969,1001,1010,1011,1012,1013,1029,1030,1031,1032,1033,1034,1036,1037,1038,1040,1041,1043,1044,1047,1048,1049,1050,1052,1054,1074,1075,1076,1091,1092,1109,1110,1111,1112,1113,1114,1115,1116,1119,1127,1128,1129,1130,1134,1135,1136,1137,1138,1139,1140,1141,1142,1143,1146,1147,1148,1151,1152,1153,1156,1158,1161,1163,1165,1176,1177,1178,1179,1182,1183,1184,1188,1189,1196,1197,1199,1205,1208,1209,1210,1211,1216,1217,1226,1237,1238,1240,1241,1258,1259,1269,1270,1271,1298,1299,1300,1302,1304,1308,1309,1310,1312,1327,1328,1332,1334,1335,1336,1337,1339,1341,1342,1343,1344,1345,1346,1347,1354,1355,1356,1357,1358,1359,1364,1368,1369,1370,1377,1378,1379,1380,1381,1401,1402,1406,1408,1409,1410,1411,1413,1414,1415,1416,1417,1448,1449,1450,1453,1454,1455,1456,1457,1458,1459,1460,1461,1462,1463,1464,1465,1466,1467,1469,1473,1486,1487,1488,1489,1490,1491,1492,1493,1503,1506,1507,1509,1510,1511,1512,1513,1514,1515,1516,1517,1518,1519,1520,1521,1522,1523,1524,1527,3141,3142,3143,3144,3145,3149,3150,3151,3154,3183,3239,3243,3245,3246,3247,3272,3273,3274,3368);

