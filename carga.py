# coding=utf-8

from requests_html import HTMLSession
from operator import index
from thefuzz import fuzz
import pandas as pd
import numpy as np
import unidecode
import re

def main():
    periodo = ['2020']
    arq_pib = 'PIB dos Municípios - base de dados 2010-2020.xls'
    cab_pib = 0
    col_pib = [0,4,5,7,32,33,34,35,36,37,38,39]

    # Carga dos 27 Estados - fonte: ISO
    estados_df = carga_estados()
    if estados_df.empty:
        print('Erro na carga de Estados a partir da Internet!')
        exit()

    # Carga dos 5570 Municípios - fonte: IBGE
    municip = carga_municipios()

    # Insere a sigla do Estado nos DataFrames de Municípios
    insere_sigla_est_munic(estados_df, municip)
    descreve_df(municip, 'Município')

    # Arrecadação dos MEI dos 5570 Municípios - fonte: RFB
    arrecadacao = carga_mei(periodo)
    descreve_df(arrecadacao, 'Arrecadação')

    # Inadimplência dos MEI dos 5570 Municípios - fonte RFB / Simples Nacional
    inadimplencia = carga_inad(estados_df)  

    # Insere a sigla do Estado no DataFrame de Inadimplência
    inadimplencia = insere_est_inad(estados_df, inadimplencia)
    descreve_df(inadimplencia, 'Inadimplência')

    # PIB dos 5570 Municípios - fonte: IBGE
    pib = carga_pib(arq_pib, cab_pib, col_pib, periodo)
    descreve_df(pib, 'PIB')

    # Ajuste do nome dos Municípios de acordo com a base de Municípios do IBGE
    consist_munic_ibge(municip, pib, 'PIB')
    consist_munic_ibge(municip, arrecadacao, 'Arrecadacao')
    consist_munic_ibge(municip, inadimplencia, 'Inadimplencia')

    # Consistência da posição indexada dos Municípios nos Dataframes de Arrecadação e Inadimplência
    consist_axi(municip, arrecadacao, inadimplencia, pib)

    # Merge dos Dataframes Arrecadação e Inadimplência
    base_axi = consolida_axi(arrecadacao, inadimplencia)
    base_final = consolida_axi(base_axi, pib)
    descreve_df(base_final, 'Base Final Consolidada')

    base_final.to_excel('base_consolidada.xlsx')


def descreve_df(df, tema):
    print('-' * 60)
    print('Dataframe: ', tema)
    print('-' * 60)
    print('Registros: ', df.shape[0], 'Colunas: ', df.shape[1])
    print('-' * 60)
    print('Amostra (5 primeiras linhas):')
    print('-' * 60)
    print(df.head())
    print('-' * 60)
    # Verifica se há duplicatas no DataFrame
    duplicados = df[df.duplicated()]
    if duplicados.empty:
        print('Não há registros duplicados.')
    else:
        print('Registros duplicados: ', duplicados.shape[0])
    print('-' * 60)
    # Verifica colunas com valor ausente e strings vazias
    posicoes_NaN = np.where(pd.isnull(df))
    vazio_NaN = (np.any(posicoes_NaN[0])) and (np.any(posicoes_NaN[1]))

    posicoes_string_vazias = np.where(df.applymap(lambda x: x == ''))
    vazio_str_vz = (np.any(posicoes_string_vazias[0])) and (np.any(posicoes_string_vazias[1]))

    if (vazio_NaN == False) and (vazio_str_vz == False):
        print('Não há valores vazios (NaN ou Strings vazias).')
    else:
        if vazio_NaN:
            print('Alerta - Verifique, há valores NaN!')
        if vazio_str_vz:
            print('Alerta - Verifique, há Strings vazias!')
        exit()
    print('-' * 60)
    print(df.describe())


def carga_estados():
    '''
        Carga dos Estados a partir do sítio da ISO.
        Tratamento da Sigla BR-XX.
        Tratamento de duplicidade.
    '''
    sitio = 'https://www.iso.org/obp/ui/#iso:code:3166:BR'
    session = HTMLSession()

    pagina = session.get(sitio)
    pagina.html.render(sleep=2, timeout=45)

    # Testa se a carga da página web ocorreu com sucesso, caso contrário avisa o usuário e retorna sem dados.
    if pagina.status_code != 200:
        print(f'Erro ao carregar a página da ISO, código {pagina.status_code}')
        estados_vazio = pd.DataFrame()
        return estados_vazio
    
    # Carrega a tabela html, pandas executa a leitura e os campos de Sigla e nome do Estado são renomeados.
    tabela = pagina.html.find('.tablesorter', first=True).html
    estados_cheia = pd.read_html(tabela)
    estados_cheia[0].rename({'3166-2 code':'Sigla', 'Subdivision name':'Estado'}, axis=1, inplace=True)

    # Copia o nome de Estado e a Sigla, converte para string e retira a entrada BR- da Sigla
    estados = estados_cheia[0][['Estado', 'Sigla']].copy()
    estados = estados.convert_dtypes()
    estados["Sigla"] = estados["Sigla"].str.slice(3,5)
    estados["Estado"] = estados["Estado"].str.upper()

    # Descreve o DataFrame estados
    descreve_df(estados, 'Estados')

    # Elimina Estados duplicados, se houver.
    estados = limpa_duplicados(estados)

    # Retorna os Estados com siglas tratadas e sem duplicidade
    return estados


def carga_municipios():
    '''
        Carga dos Municípios a partir de arquivo do sítio do IBGE.
        Renomeio das colunas.
        Tratamento de duplicidade.
        Ajuste da grafia de Municípios ('-' e RN)
    '''        
    # Variáveis que definem o nome do arquivo a ser carregado e as colunas que serão selecionadas.
    arquivo = "RELATORIO_DTB_BRASIL_MUNICIPIO.xls"
    colunas = [1,12]
    cabecalho = 6

    # Executa a leitura do arquivo Excel, renomeia colunas Estado e Nome do Município e as coloca em caixa alta
    municipios = pd.read_excel(arquivo, usecols=colunas, header=cabecalho)
    municipios.rename({'Nome_UF':'Estado', 'Nome_Município':'Municipio'}, axis=1, inplace=True)
    municipios["Estado"] = municipios["Estado"].str.upper()
    municipios["Municipio"] = municipios["Municipio"].str.upper()

    # Elimina Municípios duplicados, se houver.
    municipios = limpa_duplicados(municipios)

    # Ajuste dos Municípios
    ajuste_municipios(municipios)

    # Retorna os Municípios sem duplicidade.
    return(municipios)


def limpa_duplicados(df):
    # verifica se há duplicatas no DataFrame
    duplicados = df[df.duplicated()]
    if not duplicados.empty:
        df.drop_duplicates(inplace=True)
    return df


def carga_mei(periodo):
    ''''
        Carga da Arrecadação de 2020 a partir da planilha do sítio Simples Nacional.
        Definição dos parâmetros a serem carregados da planilha.
        Carga da planilha a partir dos parâmetros para Dataframe mei.
        Cópia dos dados relevantes para novo dataframe arrecadação e conversão para string dos campos texto.
        Consolidação dos impostos municipais, estaduais e federais em uma única coluna.
    '''
    # Definição do DataFrame arrecadação que será retornado.
    arrecadacao = pd.DataFrame()

    # Dados a serem carregados, carga e conversão do tipo objeto para string
    arquivo="arrecadacao-do-mei-por-municipio-2015-a-2020.xlsx"
    planilha="2018-2020"
    cabecalhos=[2,3]
    
    mei = pd.read_excel(arquivo, sheet_name=planilha, header=cabecalhos)
    mei = mei.convert_dtypes()

    # Tratamento para remover a Sigla do Estado do campo Município e convertê-lo para Caixa Alta.
    arrecadacao[['Estado', 'Sigla', 'Municipio']] = mei[['ESTADO', 'UF', 'MUNICÍPIO']]
    arrecadacao["Municipio"] = arrecadacao["Municipio"].str.slice(0, -5)
    arrecadacao["Municipio"] = arrecadacao["Municipio"].str.upper()

    # Ajuste dos nomes dos Municípios
    ajuste_municipios(arrecadacao)
    
    # Consolidação dos impostos
    for ano in periodo:
        arrecadacao['arrec_' + ano] = mei[ano]["ICMS - Simples Nacional - MEI"] + mei[ano]["ISS - Simples Nacional - MEI"] + mei[ano]["INSS - SImples Nacional - MEI"]
    arrecadacao.sort_values(by=['Sigla', 'Municipio'], inplace=True, ignore_index=True)
    return arrecadacao


def carga_pib(arquivo_pib, cabecalho, colunas, periodo):
    ''''
        Carga dos dados do PIB definido pelo período a partir da planilha do sítio do IBGE.
        Parâmetros a serem carregados da planilha são passados na função.
        Cópia dos dados relevantes para novo dataframe pib e conversão para string dos campos objeto.
    '''
    # Definição do DataFrame do PIB que será retornado.
    pib = pd.DataFrame()

    pib_plan = pd.read_excel(arquivo_pib, sheet_name=0, header=cabecalho, usecols=colunas)

    # Elimina os anos diferentes do parâmetro período, elimina a coluna Ano
    for ano in periodo:
        pib_plan.drop(pib_plan[pib_plan['Ano'] != int(ano)].index, inplace = True)
    pib_plan.drop(columns=['Ano'])
    pib_plan.reset_index(drop=True, inplace=True)
    
    # Renomear as colunas e conversão de objetos para tipos de dados
    pib[['Sigla', 'Estado', 'Municipio', 'Valor_ab_agro', 'Valor_ab_indu', 'Valor_ab_serv', 'Valor_ab_publ','Valor_abt', 'Impostos', 'PIB', 'PIB_pc']] = pib_plan[['Sigla da Unidade da Federação', 'Nome da Unidade da Federação', 'Nome do Município', 'Valor adicionado bruto da Agropecuária, \na preços correntes\n(R$ 1.000)', 'Valor adicionado bruto da Indústria,\na preços correntes\n(R$ 1.000)', 'Valor adicionado bruto dos Serviços,\na preços correntes \n- exceto Administração, defesa, educação e saúde públicas e seguridade social\n(R$ 1.000)', 'Valor adicionado bruto da Administração, defesa, educação e saúde públicas e seguridade social, \na preços correntes\n(R$ 1.000)', 'Valor adicionado bruto total, \na preços correntes\n(R$ 1.000)', 'Impostos, líquidos de subsídios, sobre produtos, \na preços correntes\n(R$ 1.000)', 'Produto Interno Bruto, \na preços correntes\n(R$ 1.000)', 'Produto Interno Bruto per capita, \na preços correntes\n(R$ 1,00)']]
    pib = pib.convert_dtypes()
    
    # Colunas Estado e Município convertidas em caixa alta
    pib["Estado"] = pib["Estado"].str.upper()
    pib["Municipio"] = pib["Municipio"].str.upper()

    # Ajuste de valor nas colunas 'Valor_abt', 'Impostos', 'PIB' (x 1000) - retorna a unidade R$ 1,00
    pib['Valor_ab_agro'] = pib['Valor_ab_agro'] * 1000
    pib['Valor_ab_indu'] = pib['Valor_ab_indu'] * 1000
    pib['Valor_ab_serv'] = pib['Valor_ab_serv'] * 1000
    pib['Valor_ab_publ'] = pib['Valor_ab_publ'] * 1000
    pib['Valor_abt'] = pib['Valor_abt'] * 1000
    pib['Impostos'] = pib['Impostos'] * 1000
    pib['PIB'] = pib['PIB'] * 1000

    # Ajuste dos nomes dos Municípios -> Acentuação, traço, de, da(s), do(s) e 2 municípios do RN:
    ajuste_municipios(pib)

    return pib


def carga_inad(estados):
    '''
        Carga das planilhas de Inadimplência.
        Ano de 2020, meses de Janeiro a Dezembro, totalizando 12 planilhas.
        Consistência de quantidade e posição indexada de Estados e Municípios.
    '''
    # Dados a serem carregados
    # arquivo = 'Índice Inadimplência MEI  10.2022.ods'
    # anos = '(.*)(20[1][8-9]|2020)' # Variável a ser usada como Expressão Regular para seleção dos anos.
    arquivo = 'InadimplenciaMEI102022.xlsx'
    anos = '(.*)2020'               # Variável a ser usada como Expressão Regular para seleção dos anos.
    planilhas = pd.DataFrame        # Dicionário de DataFrames carregados dos anos selecionados.
    cabecalho = 1                   # Título das colunas encontra-se na linha 2 de cada planilha.
    colunas = "A:C"                 # Municípios/UF, DAS Pagos xx/yyyy e Optantes xx/yyyy.
    planilhas_selecionadas = []     # Relação dos nomes das planilhas selecionadas para carga (jan/2018 a dez/2020).
    est_brasileiros = 27            # Quantidade de estados brasileiros, uso para consistência da quantidade de estados.
    plan_cons = 'Janeiro_2020'      # Planilha usada como base para consistência

    # Consistência da quantidade de Estados
    qtd_estados = len(estados)
    est_unicos = estados['Sigla'].nunique()

    if not (qtd_estados == est_brasileiros and est_unicos == est_brasileiros):
        print('*** Atenção **** Base de Estados está inconsistente!!')

    # Carga das planilhas de Inadimplência dos anos selecionados por regex
    planilhas_excel = pd.ExcelFile(arquivo)
    for planilha in planilhas_excel.sheet_names:
        m = re.compile('%s' % (anos)).search(planilha)
        if (m):
            planilhas_selecionadas.append(planilha)
    planilhas = pd.read_excel(arquivo, sheet_name=planilhas_selecionadas, header=cabecalho, usecols=colunas)

    # Filtrar somente os Estados das planilhas mensais de Inadimplência da coluna Municípios/UF
    estados_filtrados = {}

    for planilha in planilhas.keys():
        planilha_analisada = planilhas.get(planilha)
        resultado = pd.DataFrame(columns=(planilha_analisada.columns))
        for sigla in estados['Sigla']:
            resultado = pd.concat([resultado, (planilha_analisada[planilha_analisada['Municípios/UF'].str.fullmatch(sigla)])])
        resultado.rename({'Municípios/UF':'Sigla'}, axis=1, inplace=True)
        coluna_selecionada = resultado[['Sigla']]
        est_ordenado = coluna_selecionada.sort_index()
        estados_filtrados[planilha] = est_ordenado

    # Consistência da quantidade de estados das planilhas carregadas
    divergencias = {}   # Armazena as divergências que as planilhas podem apresentar.

    for planilha in estados_filtrados.keys():
        plan_analisada = estados_filtrados.get(planilha)
        qtd_plan = len(plan_analisada)
        est_uni_plan = plan_analisada['Sigla'].nunique()
        if qtd_plan != est_brasileiros or est_uni_plan != est_brasileiros:
            if qtd_plan != est_brasileiros:
                divergencias[planilha] = [planilha, ('Nº de estados não conforme: ' + str(qtd_plan))]
            if est_uni_plan != est_brasileiros:
                divergencias[planilha + 'unique'] = [planilha, ('Nº de estados não únicos: ' + str(est_uni_plan))]
    if bool(divergencias):
        print('*** Atenção **** Base de Inadimplência está inconsistente pela quantidade de estados!!')
        print(divergencias)

    # Consistência da posição indexada de Estados nas planilhas de Inadimplência
    est_consistencia = {}
    for planilha in estados_filtrados.keys():
        if not (estados_filtrados[plan_cons].compare(estados_filtrados[planilha]).empty):
            est_consistencia[planilha] = 'Inconsistente'
    if bool(est_consistencia):
        print('*** Atenção **** Há divergência na posição indexada de Estados nas planilhas de inadimplência!!')
        print(est_consistencia)

    # Insere a coluna Sigla em cada planilha mensal para preenchimento da sigla do respectivo Estado
    for planilha in planilhas.keys():
        planilhas[planilha]['Sigla'] = None
        planilhas[planilha].rename({'Municípios/UF':'Municipio'}, axis=1, inplace=True)
        for indice in planilhas[planilha].index:
            if planilhas[planilha]['Municipio'][indice] in estados['Sigla'].values:
                sigla_preenche = planilhas[planilha]['Municipio'][indice]
            planilhas[planilha].loc[indice, 'Sigla'] = sigla_preenche

    # Eliminando as linhas totalizadoras por Estado, Total Geral e os campos vazios (importados como NaN)
    # Renomeando as colunas DAS mmaa e Optantes mmaa para DAS e Optantes e convertendo-os para o tipo int
    lista_estados = list(estados_filtrados[plan_cons].index.values.tolist())
    lista_estados.append(planilhas[plan_cons].last_valid_index()) # Total Geral

    for planilha in planilhas.keys():
        planilhas[planilha].drop(index=lista_estados, inplace=True)
        planilhas[planilha] = planilhas[planilha].fillna(0) # campos vazios (importados como NaN), inserindo 0
        for coluna in planilhas[planilha].columns:
            m = re.compile('%s' % ('DAS')).search(coluna)
            if (m):
                planilhas[planilha].rename({coluna:'DAS'}, axis=1, inplace=True)
                planilhas[planilha]['DAS'] = planilhas[planilha]['DAS'].astype('int64')
            m = re.compile('%s' % ('Optantes')).search(coluna)
            if (m):
                planilhas[planilha].rename({coluna:'Optantes'}, axis=1, inplace=True)
                planilhas[planilha]['Optantes'] = planilhas[planilha]['Optantes'].astype('int64')

    # Consistência da posição indexada de Municípios nas planilhas de Inadimplência
    cidades_base = planilhas[plan_cons]['Municipio']
    plan_consist_munic = []
    plan_inconsist_munic = {}
    indices_inconsist_munic = []

    for planilha in planilhas.keys():
        cidades_comparar = planilhas[planilha]['Municipio']
        cid_nconf = cidades_base.compare(cidades_comparar)
        if (cid_nconf.empty):
            plan_consist_munic.append(planilha)
        else:
            plan_inconsist_munic[planilha] = cid_nconf
            indices_inconsist_munic.extend(cid_nconf.index.values.tolist())
            print('Inconsistência no índice de Municípios!')
            break

    # Totalização por ano e Município
    anos_fiscais = [2020] # Faixa de anos para consolidação de DAS Pagos, Optantes e cálculo de inadimplência
    df_temp = pd.DataFrame
    totalizacao = pd.DataFrame

    # Carga inicial do Dataframe consolidadas
    df_temp = planilhas[plan_cons][['Municipio', 'Sigla']]
    totalizacao = df_temp.copy()

    for ano in anos_fiscais:
        das_ano = 'DAS' + str(ano)
        optante_ano = 'Optante' + str(ano)
        inad_ano = 'inad_' + str(ano)

        consolida_das = pd.DataFrame    # Captura cada mês e provê o somatório dos DAS pagos no ano
        consolida_opt = pd.DataFrame    # Captura cada mês e provê o somatório dos Optantes pelo Simples no ano
        total_das = pd.DataFrame
        total_opt = pd.DataFrame

        consolida_das = df_temp.copy()
        consolida_opt = df_temp.copy()

        for planilha in planilhas.keys():
            m = re.compile('%s' % (ano)).search(planilha)
            if (m):
                df_plan_temp = pd.DataFrame

                df_plan_temp = planilhas[planilha]['DAS']
                df_plan_temp = df_plan_temp.to_frame(planilha)
                consolida_das = consolida_das.join(df_plan_temp)

                df_plan_temp = planilhas[planilha]['Optantes']
                df_plan_temp = df_plan_temp.to_frame(planilha)
                consolida_opt = consolida_opt.join(df_plan_temp)

        consolida_das[das_ano] = consolida_das.sum(axis=1, numeric_only=True)
        total_das = consolida_das[das_ano]
        total_das = total_das.to_frame(das_ano)

        consolida_opt[optante_ano] = consolida_opt.sum(axis=1, numeric_only=True)
        total_opt = consolida_opt[optante_ano]
        total_opt = total_opt.to_frame(optante_ano)

        totalizacao[inad_ano] = total_das[das_ano] / total_opt[optante_ano]

    # Altera a entrada de Passo de Camaragibe - AC (AL) para Santa Rosa do Purus (AC) - Erro da base original
    totalizacao.loc[(totalizacao['Municipio'] == 'PASSO DE CAMARAGIBE') & (totalizacao['Sigla'] == 'AC'), 'Municipio'] = 'SANTA ROSA DO PURUS'

    # Ajuste de nomes de Municípios e remove acentos
    ajuste_municipios(totalizacao)
    return(totalizacao)


def consist_axi(municipios, arrec, inadi, pib):
    '''
        Função destinada a consistir as posições das cidades nas tabelas de Arrecadação, Inadimplência
        e PIB contra a tabela de Municípios do IBGE.
    '''
    munic_aux = pd.DataFrame
    arrec_aux = pd.DataFrame
    inadi_aux = pd.DataFrame
    pib_aux = pd.DataFrame

    munic_fil = pd.DataFrame
    arrec_fil = pd.DataFrame
    inadi_fil = pd.DataFrame
    pib_fil = pd.DataFrame
    
    arrec_nao_conf = pd.DataFrame
    inadi_nao_conf = pd.DataFrame
    pib_nao_conf = pd.DataFrame
    axi_nao_conf = pd.DataFrame

    # Coloca os Municípios em ordem crescente por Sigla e Município    
    municipios.sort_values(by=['Sigla', 'Municipio'], inplace=True, ignore_index=True)
    arrec.sort_values(by=['Sigla', 'Municipio'], inplace=True, ignore_index=True)
    inadi.sort_values(by=['Sigla', 'Municipio'], inplace=True, ignore_index=True)
    pib.sort_values(by=['Sigla', 'Municipio'], inplace=True, ignore_index=True)

    munic_aux = municipios[['Municipio']]
    arrec_aux = arrec[['Municipio']]
    inadi_aux = inadi[['Municipio']]
    pib_aux = pib[['Municipio']]
    
    munic_fil = munic_aux.copy()
    arrec_fil = arrec_aux.copy()
    inadi_fil = inadi_aux.copy()
    pib_fil = pib_aux.copy()

    # Encontra a diferença entre o dataframe mestre do IBGE e os demais (Arrecadação, Inadimplência e PIB)
    arrec_nao_conf = munic_fil.compare(arrec_fil)
    if not arrec_nao_conf.empty:
        print('Atenção  --- Não Conforme - Municípios da Arrecadação divergentes em posição da base do IBGE!')

    inadi_nao_conf = munic_fil.compare(inadi_fil)
    if not inadi_nao_conf.empty:
        print('Atenção  --- Não Conforme - Municípios da Inadimplência divergentes da base do IBGE!')

    pib_nao_conf = munic_fil.compare(pib_fil)
    if not pib_nao_conf.empty:
        print('Atenção  --- Não Conforme - Municípios do PIB divergentes da base do IBGE!')

    axi_nao_conf = arrec_fil.compare(inadi_fil)
    if not axi_nao_conf.empty:
        print('Atenção  --- Não Conforme - Municípios da Inadimplência divergentes da base de Arrecadação!')

    return ()


def consist_munic_ibge(mun, base_comp, base):
    '''
        DataFrame mun -> IBGE
        DataFrame base para comparação -> geralmente, RFB
        Atualização dos Municípios divergentes no DataFrame comparado
    '''
    diverg = pd.DataFrame
    diverg_aux = pd.DataFrame

    df_temp_mun = pd.DataFrame
    mun_red = pd.DataFrame
    
    df_temp_base_comp = pd.DataFrame
    base_comp_red = pd.DataFrame

    df_temp_mun = mun[['Sigla', 'Estado', 'Municipio']]
    mun_red = df_temp_mun.copy()
    df_temp_base_comp = base_comp[['Sigla', 'Estado', 'Municipio']]
    base_comp_red = df_temp_base_comp.copy()

    diverg = pd.concat([mun_red, base_comp_red], keys=['ibge','compara'], names=['base', 'indice']).drop_duplicates(keep=False)

    if not diverg.empty:
        diverg.reset_index(drop=False, inplace=True)
        diverg.drop_duplicates(keep=False, inplace=True)

        diverg_aux = diverg.copy()
        diverg_aux['Analisada'] = False

        for ind in diverg_aux.index:
            auxiliar = pd.DataFrame
            auxiliar = diverg_aux.copy()
            auxiliar.drop(ind, inplace=True)
            for aux_ind in auxiliar.index:
                if((diverg_aux['Analisada'][ind] == False) & (diverg_aux['base'][ind] == 'ibge')):
                    if ((diverg_aux['Sigla'][ind] == (auxiliar['Sigla'][aux_ind]))):
                        aproxim = fuzz.ratio(diverg_aux['Municipio'][ind], auxiliar['Municipio'][aux_ind])
                        if aproxim >= 83:
                            diverg.drop(ind, inplace=True)
                            diverg.drop(aux_ind, inplace=True)
                            diverg_aux.at[ind, 'Analisada'] = True
                            diverg_aux.at[aux_ind, 'Analisada'] = True
                            mun_ibge = diverg_aux.iloc[ind]['Municipio']
                            indice_destino = diverg_aux.iloc[aux_ind]['indice']
                            base_comp.at[indice_destino, 'Municipio'] = mun_ibge
                        else:
                            aproxim_parcial = fuzz.partial_ratio(diverg_aux['Municipio'][ind], auxiliar['Municipio'][aux_ind])
                            if aproxim_parcial == 100:
                                diverg.drop(ind, inplace=True)
                                diverg.drop(aux_ind, inplace=True)
                                diverg_aux.at[ind, 'Analisada'] = True
                                diverg_aux.at[aux_ind, 'Analisada'] = True
                                mun_ibge = diverg_aux.iloc[ind]['Municipio']
                                indice_destino = diverg_aux.iloc[aux_ind]['indice']
                                base_comp.at[indice_destino, 'Municipio'] = mun_ibge
        if not diverg.empty:
            print('Verifique, ainda há divergências entre os Municípios do IBGE e', base)
            print(diverg)
            exit()
    return()


def ajuste_municipios(tabela):
    # Eliminação dos acentos, traços e substituição de DE DA DO por DE
    tabela['Municipio'] = tabela['Municipio'].apply(remove_acentos)
    tabela['Municipio'].replace(to_replace=r'-', value=r' ', inplace=True, regex=True)
    tabela['Municipio'].replace(to_replace=r' D[AEO]S? ', value=r' DE ', inplace=True, regex=True)

    # Ajuste nome/grafia de Municípios
    tabela.loc[tabela['Municipio'] == 'ASSU', 'Municipio'] = 'ACU'
    tabela.loc[tabela['Municipio'] == 'BOA SAUDE', 'Municipio'] = 'JANUARIO CICCO'


def remove_acentos(a):
    return unidecode.unidecode(a)


def insere_sigla_est_munic(estados, municipios):
    '''
        Para cada município, compara o nome do Estado do DataFrame municipios com o nome do Estado do DataFrame
        estados; se forem iguais, insere a nova coluna sigla no DataFrame Municípios.
        Transforma o DataFrame Estados em Dicionário.
        Uso da função Map do Dataframe para inserção do campo Sigla, nome do estado (index do Dicionário) ...
        igual ao nome do Estado no Dataframe do Município.
    '''
    est_dic = dict(estados.values)
    municipios['Sigla'] = municipios['Estado'].map(est_dic)
    return municipios


def insere_est_inad(estados, inad):
    '''
        Para cada município, compara a sigla do Estado do DataFrame Estados com a sigla do DataFrame
        inad; se forem iguais, insere a nova coluna com o nome do Estado no DataFrame inad.
        Transforma o DataFrame Estados em Dicionário.
        Uso da função Map do Dataframe para inserção do campo Nome do Estado, sigla do estado (index 
        do Dicionário) igual a sigla do Estado no Dataframe do inad.
    '''
    estados_aux = pd.DataFrame
    estados_aux = estados[['Sigla', 'Estado']]
    estados_inv = estados_aux.copy()
    est_dic = dict(estados_inv.values)
    inad['Estado'] = inad['Sigla'].map(est_dic)
    inad_final = inad[['Estado','Sigla','Municipio','inad_2020']].copy()
    inad_final.sort_values(by=['Sigla', 'Municipio'], inplace=True, ignore_index=True)
    return inad_final


def consolida_axi(arrec_cons, inad_cons):
    lista_colunas = ['Estado', 'Sigla', 'Municipio']
    outer_join_df = pd.merge(arrec_cons, inad_cons, on=lista_colunas, how='outer')
    return outer_join_df


if __name__ == '__main__':
    main()
