begin
	ooLoop: for oo as cnv_modalidades_gfip dynamic scroll cursor for
		select distinct i_entidades as w_i_entidades,i_tipos_proc as w_i_tipos_proc,i_competencias as w_i_competencias,i_processamentos as w_i_processamentos,i_funcionarios as w_i_funcionarios,
						0 as w_modalidade
		from bethadba.bases_calc 
		where not exists(select 1 
						from bethadba.modalidades_gfip as mod 
						where mod.i_entidades = bases_calc.i_entidades 
						and mod.i_funcionarios = bases_calc.i_funcionarios 
						and mod.i_competencias = bases_calc.i_competencias) 
		and i_tipos_bases in(6,7,11,12) 
		order by 1,3,5 asc
	do
		message 'Ent.: '||w_i_entidades||' Fun.: '||w_i_funcionarios||' Tip. Pro.: '||w_i_tipos_proc||' Com.: '||w_i_competencias||' Pro.: '||w_i_processamentos to client;
	
		insert into bethadba.modalidades_gfip(i_entidades,i_tipos_proc,i_competencias,i_processamentos,i_funcionarios,modalidade) on existing skip
		values (w_i_entidades,w_i_tipos_proc,w_i_competencias,w_i_processamentos,w_i_funcionarios,w_modalidade);
		
	end for;
end
;

rollback;