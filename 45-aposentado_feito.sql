-- BTHSC-61513 / Ajusta rescisões conforme base do concorrente, sendo possível enviar aposentados.

update bethadba.rescisoes r 
        left join tecbth_delivery.gp001_FUNCIONARIO f 
        on r.i_funcionarios = f.cdMatricula 
        set r.i_motivos_apos = f.CdDesligamento 
        where f.CdDesligamento in (4,33,32)

