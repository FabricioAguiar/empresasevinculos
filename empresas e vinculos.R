rais <- DBI::dbGetQuery(db,"select [cnpjcei] as cnpj,
[cnae20subclasse] as rais_cnae,
[naturezajuridica],
[qtdvinculosativos],
indsimples
from[rais_id].[Estb]
where [referencia]=2018 and municipio=530010")

#Soma as quantidade de vinculos para os cnpjs que possuam as outras caracteristicas selecionadas semelhantes
rais1<-rais %>%
  srvyr::group_by(cnpj,rais_cnae,naturezajuridica,indsimples) %>% 
  srvyr::summarise(vinculos_rais=sum(qtdvinculosativos))

#Existem 3 cnpjs repetido dentro da base da receita, que apresentam caracteristicas diferentes,incluindo quantidade de vinculos
rais$cnpj[duplicated(rais$cnpj)]

#agrupa o vinculos apenas por cnpj, assim se o mesmo cnpj aparecia mais de uma vez e com caracteristicas diferentes soma-se o total de vinculos
rais2<-rais %>%
  srvyr::group_by(cnpj) %>% 
  srvyr::summarise(vinculos_rais=sum(qtdvinculosativos))

#remove os cnpjs que se repetiram
a<-rais1 %>% distinct(cnpj, .keep_all = TRUE) %>% 
  srvyr::select(cnpj,rais_cnae,naturezajuridica,indsimples)

#junta as quantidade de vinculos com as caracteristicas
b<-left_join(rais2,a,by="cnpj")

#remove os cnpjs que se repetiram
raisfinal<-b %>% distinct(cnpj, .keep_all = TRUE) %>% 
  srvyr::select(cnpj,rais_cnae,naturezajuridica,indsimples,vinculos_rais)

#avalia se ainda há alguma repetição
raisfinal$cnpj[duplicated(raisfinal$cnpj)]


receita <- DBI::dbGetQuery(db,"select [cnpj],
[sit_cadastral],
[dt_sit_cadastral],
cd_natureza_juridica,
[cnae_fiscal] as receita_cnae,
[opcao_simples]
from[raisdes].[cnpj_pub_principal]
where [referencia]=20200302 and uf='DF'")

caged <- DBI::dbGetQuery(db,"select
[cnpjcei] as cnpj,
[competenciamovimentacao],
[cnae20subclasse] as caged_cnae,
sum([saldomov]) saldo
from
[caged_id].[rf_201301_atual]
where [competenciamovimentacao]>=201901 and municipio=530010
group by cnpjcei, competenciamovimentacao, cnae20subclasse")

# agrupa os saldo por cnpj e cnae
caged<-caged %>%
  srvyr::group_by(cnpj,caged_cnae)%>%
  srvyr::summarise(saldo=sum(saldo))

#avalia se há alguma repetição
caged$cnpj[duplicated(caged$cnpj)]

#junta as bases da receita e rais pelo cnpj
dados<-full_join(raisfinal,receita,by="cnpj")

# compatibiliza a opção de simples para as base
try<-dados %>%
  srvyr::mutate(opcao_simples=factor(case_when(
    opcao_simples ==5 ~"1",
    opcao_simples ==7 ~"1",
    TRUE~"0")))

#avalia se após o join há repetiçao de cnpj
try$cnpj[duplicated(try$cnpj)]


regra1<-try %>% 
  dplyr::filter(is.na(indsimples)==T|is.na(rais_cnae)==T|is.na(naturezajuridica)==T|
                  opcao_simples!=indsimples| rais_cnae!=receita_cnae| naturezajuridica!=cd_natureza_juridica
                & dt_sit_cadastral>='2018-12-31') %>%
  dplyr::select(cnpj,opcao_simples,receita_cnae,cd_natureza_juridica,vinculo_rais)

regra2<-try %>% 
  dplyr::filter(opcao_simples==indsimples| rais_cnae==receita_cnae| naturezajuridica==cd_natureza_juridica & dt_sit_cadastral>=20181231) %>%
  ungroup() %>% 
  dplyr::select(cnpj,opcao_simples,receita_cnae,cd_natureza_juridica,n)


regra3<-try %>% 
  srvyr::filter(is.na(opcao_simples)==T | is.na(receita_cnae) ==T | is.na(cd_natureza_juridica)==T
                |opcao_simples!=indsimples| rais_cnae!=receita_cnae| naturezajuridica!=cd_natureza_juridica
                & dt_sit_cadastral<20181231) %>%
  ungroup() %>% 
  select(cnpj,indsimples,naturezajuridica,rais_cnae,naturezajuridica,n)


regra4<-try %>% 
  srvyr::filter(is.na(opcao_simples)==T | is.na(receita_cnae) ==T | is.na(cd_natureza_juridica)==T & dt_sit_cadastral<20181231) %>%
  ungroup() %>% 
  select(cnpj,indsimples,naturezajuridica,rais_cnae,naturezajuridica,n)

regra2<-data_frame(cnpj=regra2$cnpj,opcao_simples=regra2$indsimples,receita_cnae=regra2$rais_cnae,cd_natureza_juridica=regra2$naturezajuridica)   
regra2<-as.data.frame(regra2)

info<-merge(regra1,regra2, all=TRUE)

info$cnpj[duplicated(info$cnpj)]

rule<-try %>% 
  dplyr::filter(is.na(indsimples)==T|is.na(rais_cnae)==T|is.na(naturezajuridica)==T) %>%
  ungroup() %>% 
  dplyr::select(cnpj,opcao_simples,receita_cnae,cd_natureza_juridica,n)







write.table(info,"/u01/u104409/info.csv",
            row.names = F,dec = ".",sep = ";")


try<-try %>% 
  mutate(dt_sit_cadastral=ymd(dt_sit_cadastral))



regra1<-try %>%
  filter(dt_sit_cadastral>'2018-12-31') %>%
  ungroup() %>% 
  dplyr::select(cnpj,opcao_simples,receita_cnae,cd_natureza_juridica,vinculos_rais,dt_sit_cadastral,sit_cadastral)

regra2<-try %>%
  dplyr::filter(dt_sit_cadastral<='2018-12-31' & is.na(rais_cnae)==F) %>%
  ungroup() %>% 
  dplyr::select(cnpj,indsimples,rais_cnae,naturezajuridica,vinculos_rais,dt_sit_cadastral,sit_cadastral)

regra3<-try %>%
  dplyr::filter(dt_sit_cadastral<='2018-12-31' & is.na(rais_cnae)==T) %>%
  ungroup() %>% 
  dplyr::select(cnpj,opcao_simples,receita_cnae,cd_natureza_juridica,vinculos_rais,dt_sit_cadastral,sit_cadastral)

regra4<-try %>%
  dplyr::filter(is.na(dt_sit_cadastral)==T & is.na(rais_cnae)==F) %>%
  ungroup() %>% 
  dplyr::select(cnpj,indsimples,rais_cnae,naturezajuridica,vinculos_rais,dt_sit_cadastral,sit_cadastral)

regra5<-try %>%
  dplyr::filter(is.na(dt_sit_cadastral)==T & is.na(receita_cnae)==F) %>%
  ungroup() %>% 
  dplyr::select(cnpj,opcao_simples,receita_cnae,cd_natureza_juridica,vinculos_rais,dt_sit_cadastral,sit_cadastral)





regra2<-data_frame(cnpj=regra2$cnpj,
                   opcao_simples=regra2$indsimples,
                   receita_cnae=regra2$rais_cnae,
                   cd_natureza_juridica=regra2$naturezajuridica,
                   dt_sit_cadastral=regra2$dt_sit_cadastral,
                   sit_cadastral=regra2$sit_cadastral,
                   vinculos_rais=regra2$vinculos_rais
                   
)   

regra4<-data_frame(cnpj=regra4$cnpj,
                   opcao_simples=regra4$indsimples,
                   receita_cnae=regra4$rais_cnae,
                   cd_natureza_juridica=regra4$naturezajuridica,
                   dt_sit_cadastral=regra4$dt_sit_cadastral,
                   sit_cadastral=regra4$sit_cadastral,
                   vinculos_rais=regra4$vinculos_rais)   

info<-merge(regra1,regra2, all=TRUE)
info<-merge(info,regra3, all=TRUE)
info<-merge(info,regra4, all=TRUE)
info<-merge(info,regra5, all=TRUE)


info$cnpj[duplicated(info$cnpj)]

info1<-info %>% 
  srvyr::filter(cd_natureza_juridica %in% c('2046','2054','2062','2070','2089','2097','2127','2135','2143','2151','2160','2178','2194','2216','2224','2232','2240','2259','2267','2275','2283','2291','2305','2313','2321','2330') 
                & sit_cadastral %in% c(2,NA)) 



cageda<-full_join(info1,caged,by="cnpj")
cagedb<-left_join(info1,caged,by="cnpj")
cagedc<-right_join(info,caged,by="cnpj")

cagedregra1<-cagedb %>%
  filter(caged_cnae!=receita_cnae & is.na(caged_cnae)!=T & dt_sit_cadastral<'2020-01-01') %>%
  ungroup() %>% 
  dplyr::select(cnpj,opcao_simples,cd_natureza_juridica,vinculos_rais,dt_sit_cadastral,sit_cadastral,caged_cnae,saldo)

cagedregra2<-cagedb %>%
  filter(caged_cnae!=receita_cnae & is.na(caged_cnae)!=T & dt_sit_cadastral>='2020-01-01') %>%
  ungroup() %>% 
  dplyr::select(cnpj,opcao_simples,cd_natureza_juridica,vinculos_rais,dt_sit_cadastral,sit_cadastral,receita_cnae,saldo)

cagedregra3<-cagedb %>%
  filter(caged_cnae==receita_cnae | is.na(caged_cnae)==T) %>%
  ungroup() %>% 
  dplyr::select(cnpj,opcao_simples,receita_cnae,cd_natureza_juridica,vinculos_rais,dt_sit_cadastral,sit_cadastral,saldo)

cagedregra4<-cagedb %>%
  filter(caged_cnae!=receita_cnae & is.na(caged_cnae)!=T & is.na(dt_sit_cadastral)) %>%
  ungroup() %>% 
  dplyr::select(cnpj,opcao_simples,cd_natureza_juridica,vinculos_rais,dt_sit_cadastral,sit_cadastral,receita_cnae,saldo)

cagedregra2<-data_frame(cnpj=cagedregra2$cnpj,
                        opcao_simples=cagedregra2$opcao_simples,
                        caged_cnae=cagedregra2$receita_cnae,
                        cd_natureza_juridica=cagedregra2$cd_natureza_juridica,
                        vinculos_rais=cagedregra2$vinculos_rais,
                        dt_sit_cadastral=cagedregra2$dt_sit_cadastral,
                        sit_cadastral=cagedregra2$sit_cadastral,
                        saldo=cagedregra2$saldo)

cagedregra3<-data_frame(cnpj=cagedregra3$cnpj,
                        opcao_simples=cagedregra3$opcao_simples,
                        caged_cnae=cagedregra3$receita_cnae,
                        cd_natureza_juridica=cagedregra3$cd_natureza_juridica,
                        vinculos_rais=cagedregra3$vinculos_rais,
                        dt_sit_cadastral=cagedregra3$dt_sit_cadastral,
                        sit_cadastral=cagedregra3$sit_cadastral,
                        saldo=cagedregra3$saldo)

cagedregra4<-data_frame(cnpj=cagedregra4$cnpj,
                        opcao_simples=cagedregra4$opcao_simples,
                        caged_cnae=cagedregra4$receita_cnae,
                        cd_natureza_juridica=cagedregra4$cd_natureza_juridica,
                        vinculos_rais=cagedregra4$vinculos_rais,
                        dt_sit_cadastral=cagedregra4$dt_sit_cadastral,
                        sit_cadastral=cagedregra4$sit_cadastral,
                        saldo=cagedregra4$saldo)

basefinal<-merge(cagedregra1,cagedregra2, all=TRUE)
basefinal<-merge(basefinal,cagedregra3, all=TRUE)
basefinal<-merge(basefinal,cagedregra4, all=TRUE)



basefinal$cnpj[duplicated(basefinal$cnpj)]

basefinal<-basefinal %>% dplyr::mutate(vinculosdisponiveis=case_when(is.na(vinculos_rais)==T~0,
                                                                     TRUE~1))

basefinal$saldo[is.na(basefinal$saldo)] <- 0
basefinal$vinculos_rais[is.na(basefinal$vinculos_rais)] <- 0



basefinal$projeçao = basefinal$vinculos_rais+basefinal$saldo
basefinal$secao = substr(basefinal$caged_cnae,1,2)

analise_vinculos<-basefinal %>% 
  
  srvyr::group_by(opcao_simples,secao,vinculosdisponiveis)%>%
  srvyr::summarise(n=sum(projeçao))

analise_empresas<-basefinal %>% 
  srvyr::group_by(opcao_simples,secao)%>%
  srvyr::summarise(n=n())


write.table(analise_empresas,"/u01/u104409/analiseempresas.csv",
            row.names = F,dec = ".",sep = ";")


write.table(analise_vinculos,"/u01/u104409/analisevinculos.csv",
            row.names = F,dec = ".",sep = ";")



