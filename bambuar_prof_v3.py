import streamlit as st
import pandas as pd
import numpy as np
import os
import base64
from datetime import datetime
import json
import plotly.express as px
from supabase import create_client, Client
import hashlib

# --- Configuração da Página ---
st.set_page_config(page_title="Bambuar V3", layout="wide")

# --- Conexão com Supabase ---
@st.cache_resource
def init_supabase_client():
    try:
        url = st.secrets["supabase"]["url"]
        key = st.secrets["supabase"]["key"]
        return create_client(url, key)
    except Exception as e:
        st.error("Erro ao inicializar a conexão com o Supabase.")
        st.stop()

supabase = init_supabase_client()

# --- Funções de Acesso a Dados V3.1 ---
@st.cache_data(ttl=30)
def get_empresa_info(user_id):
    if not user_id: return None, None
    try:
        perfil = supabase.table('perfis').select('empresa_id, empresas(nome_empresa)').eq('id', user_id).single().execute().data
        if not perfil or not perfil.get('empresas'): return None, None
        return perfil.get('empresa_id'), perfil['empresas'].get('nome_empresa')
    except Exception: return None, None

# No seu código, encontre e substitua esta função inteira:
@st.cache_data(ttl=30)
def load_data(table_name: str, query_params: dict):
    """Carrega dados com base em filtros dinâmicos, incluindo filtros especiais como 'is.null'."""
    try:
        query = supabase.table(table_name).select(query_params.get("select", "*"))
        filters = query_params.get("filters", {})
        
        for key, value in filters.items():
            if isinstance(value, list):
                if not value: return pd.DataFrame()
                query = query.in_(key, value)
            # MUDANÇA AQUI: Reconhece e aplica filtros especiais do Supabase/PostgREST
            elif isinstance(value, str) and value.startswith('is.'):
                # Extrai o valor do filtro, ex: 'null', 'true', 'false'
                filter_value = value.split('.')[1]
                query = query.is_(key, filter_value)
            else:
                query = query.eq(key, value)
                
        response = query.execute()
        return pd.DataFrame(response.data)
    except Exception as e:
        # A st.error aqui pode poluir a interface, um retorno vazio é mais limpo.
        # print(f"Erro ao carregar dados de '{table_name}': {e}") 
        return pd.DataFrame()

def add_data(table_name: str, data_dict: dict, empresa_id: int = None):
    """Adiciona uma nova linha de dados, injetando o empresa_id se fornecido."""
    
    # Esta linha é a chave: ela garante que o ID da empresa seja sempre adicionado.
    if empresa_id:
        data_dict['empresa_id'] = empresa_id
    
    try:
        response = supabase.table(table_name).insert(data_dict).execute()
        # Limpa o cache para que os dados sejam recarregados na próxima vez
        st.cache_data.clear()
        return response
    except Exception as e:
        # Mostra o erro claramente na tela se algo der errado
        st.error(f"Erro ao adicionar dados em '{table_name}': {e}")
        return None

# --- Funções de Autenticação ---
def signup_page():
    st.header("Criar Nova Conta")
    with st.form("signup_form", clear_on_submit=True):
        email = st.text_input("Seu melhor email")
        password = st.text_input("Crie uma senha forte", type="password")
        nome_empresa = st.text_input("Nome da sua Empresa ou Marca")
        if st.form_submit_button("Criar minha conta"):
            if not all([email, password, nome_empresa]):
                st.error("Por favor, preencha todos os campos.")
            else:
                try:
                    supabase.auth.sign_up({"email": email, "password": password, "options": {"data": {'company_name': nome_empresa}}})
                    st.success("Conta criada! Verifique seu e-mail para confirmar o cadastro e depois faça o login.")
                except Exception as e: st.error(f"Erro no cadastro: {e}")

def login_page():
    st.header("Acessar minha conta")
    with st.form("login_form"):
        email = st.text_input("Email")
        password = st.text_input("Senha", type="password")
        if st.form_submit_button("Entrar"):
            try:
                session = supabase.auth.sign_in_with_password({"email": email, "password": password})
                st.session_state['user_session'] = session.model_dump()
                st.rerun()
            except Exception: st.error("Erro no login: Credenciais inválidas.")
            
# ADICIONE ESTA FUNÇÃO NOVA AO SEU CÓDIGO
# SUBSTITUA ESTA FUNÇÃO NO SEU CÓDIGO
def calcula_estoque_final(df_estoque, df_vendas):
    """Calcula o saldo de estoque para o modelo de atributos independentes."""
    if df_estoque.empty:
        return pd.DataFrame()

    def criar_chave(attrs):
        if attrs and isinstance(attrs, str):
            try: attrs = json.loads(attrs)
            except json.JSONDecodeError: return None
        return json.dumps(attrs, sort_keys=True) if isinstance(attrs, dict) else None

    df_estoque['chave_atributos'] = df_estoque['atributos'].apply(criar_chave)
    if not df_vendas.empty:
        df_vendas['chave_atributos'] = df_vendas['atributos'].apply(criar_chave)

    # CORREÇÃO: Mantém o produto_base_id durante o agrupamento
    estoque_agrupado = df_estoque.groupby('chave_atributos').agg(
        quantidade=('quantidade', 'sum'),
        atributos=('atributos', 'first'),
        produto_base_id=('produto_base_id', 'first') # Garante que o ID do produto seja mantido
    ).reset_index()
    
    if not df_vendas.empty:
        vendas_agrupadas = df_vendas.groupby('chave_atributos')['quantidade_vendida'].sum().reset_index()
        df_saldo = pd.merge(estoque_agrupado, vendas_agrupadas, on='chave_atributos', how='left').fillna(0)
    else:
        df_saldo = estoque_agrupado.copy(); df_saldo['quantidade_vendida'] = 0

    df_saldo['saldo'] = df_saldo['quantidade'] - df_saldo['quantidade_vendida']
    
    if not df_saldo.empty:
        df_saldo['atributos'] = df_saldo['atributos'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
        df_atributos_flat = pd.json_normalize(df_saldo['atributos'])
        
        # CORREÇÃO: Adiciona o produto_base_id de volta ao dataframe final
        df_final = pd.concat([df_saldo[['produto_base_id']].reset_index(drop=True), df_atributos_flat.reset_index(drop=True), df_saldo[['quantidade', 'quantidade_vendida', 'saldo']].reset_index(drop=True)], axis=1)
        return df_final
        
    return pd.DataFrame()
    
    
    
# ADICIONE ESTA FUNÇÃO NOVA AO SEU CÓDIGO
# ADICIONE ESTA FUNÇÃO NOVA AO SEU CÓDIGO
def calcula_lucro_v3(df_vendas, df_estoque, df_eventos, df_comissao):
    """Calcula o lucro por venda para a arquitetura V3 com variantes."""
    if df_vendas.empty:
        return pd.Series(dtype='float64')

    df_vendas_lucro = df_vendas.copy()
    COMISSAO_PERCENTUAL = df_comissao['percentual_comissao'].iloc[0] if not df_comissao.empty else 0.10

    # 1. Calcula o Custo do Produto Vendido (CPV/CMV)
    if not df_estoque.empty:
        # Cria um "mapa" de custo médio para cada variante
        custos_medios = df_estoque.groupby('variante_id')['valor_custo'].mean()
        df_vendas_lucro['custo_unitario'] = df_vendas_lucro['variante_id'].map(custos_medios).fillna(0)
        df_vendas_lucro['custo_estoque'] = df_vendas_lucro['custo_unitario'] * df_vendas_lucro['quantidade_vendida']
    else:
        df_vendas_lucro['custo_estoque'] = 0

    # 2. Calcula Custos de Evento Rateado
    df_vendas_lucro['custo_evento_rateado'] = 0
    if not df_eventos.empty and 'evento' in df_vendas_lucro.columns:
        custos_eventos = df_eventos.set_index('nome_evento')[['aluguel', 'estacionamento', 'alimentacao', 'outros_custos']].sum(axis=1)
        vendas_por_evento = df_vendas_lucro.groupby('evento')['quantidade_vendida'].sum()
        
        df_vendas_lucro['custo_total_evento'] = df_vendas_lucro['evento'].map(custos_eventos).fillna(0)
        df_vendas_lucro['total_vendido_evento'] = df_vendas_lucro['evento'].map(vendas_por_evento).fillna(1)
        
        df_vendas_lucro['custo_evento_rateado'] = (df_vendas_lucro['custo_total_evento'] / df_vendas_lucro['total_vendido_evento']) * df_vendas_lucro['quantidade_vendida']

    # 3. Calcula Outros Custos e Receitas
    df_vendas_lucro['receita_bruta'] = df_vendas_lucro['preco_venda'] * df_vendas_lucro['quantidade_vendida']
    df_vendas_lucro['receita_liquida'] = df_vendas_lucro['receita_bruta'] - df_vendas_lucro['desconto'].fillna(0)
    df_vendas_lucro['comissao'] = df_vendas_lucro['receita_bruta'] * COMISSAO_PERCENTUAL
    df_vendas_lucro['taxas_pagamento'] = df_vendas_lucro['taxa_pagamento'].fillna(0)

    # 4. Calcula o Lucro Final
    df_vendas_lucro['lucro'] = (
        df_vendas_lucro['receita_liquida'] - 
        df_vendas_lucro['custo_estoque'] - 
        df_vendas_lucro['custo_evento_rateado'] - 
        df_vendas_lucro['comissao'] - 
        df_vendas_lucro['taxas_pagamento']
    )
    
    return df_vendas_lucro['lucro']
    
# ADICIONE ESTA NOVA FUNÇÃO AO SEU CÓDIGO
# SUBSTITUA a função gerar_visualizacao_hierarquia por esta:
def gerar_tabela_pivotada(df_valores, df_tipos, df_produtos_base):
    """Cria um DataFrame pivotado para exibir a hierarquia de forma horizontal, como no Excel."""
    if df_valores.empty or df_tipos.empty or df_produtos_base.empty:
        return pd.DataFrame()

    # Junta as tabelas para ter todas as informações em um só lugar
    df = pd.merge(df_valores, df_tipos, left_on='atributo_tipo_id', right_on='id', suffixes=('_valor', '_tipo'))
    df = pd.merge(df, df_produtos_base, left_on='produto_base_id', right_on='id', suffixes=('', '_produto'))

    # Identifica os "nós folhas" (os últimos itens da hierarquia, que não são pais de ninguém)
    folhas = df[~df['id_valor'].isin(df['parent_valor_id'].dropna())]
    
    caminhos_formatados = []
    # Para cada "folha", sobe na hierarquia para montar o caminho completo
    for _, folha in folhas.iterrows():
        caminho_atual = {}
        item_atual = folha
        while True:
            caminho_atual[item_atual['nome_atributo']] = item_atual['valor']
            if pd.isna(item_atual['parent_valor_id']):
                caminho_atual['Produto Base'] = item_atual['nome_produto']
                caminho_atual['produto_base_id'] = item_atual['produto_base_id']
                break
            # Encontra o pai na tabela já unida
            pai = df[df['id_valor'] == item_atual['parent_valor_id']]
            if pai.empty:
                break
            item_atual = pai.iloc[0]
        caminhos_formatados.append(caminho_atual)
        
    return pd.DataFrame(caminhos_formatados)
    
# ADICIONE ESTA NOVA FUNÇÃO AO SEU CÓDIGO
# ADICIONE ESTA NOVA FUNÇÃO AO SEU CÓDIGO
def gerar_visualizacao_hierarquia(df_valores, df_tipos, df_produtos_base):
    """Cria um DataFrame formatado para exibir a hierarquia completa dos valores dos atributos."""
    if df_valores.empty or df_tipos.empty or df_produtos_base.empty:
        return pd.DataFrame()

    # Junta valores com seus tipos de atributo para obter o nome do atributo e o id do produto
    df = pd.merge(df_valores, df_tipos, left_on='atributo_tipo_id', right_on='id', suffixes=('_valor', '_tipo'))
    df = pd.merge(df, df_produtos_base, left_on='produto_base_id', right_on='id', suffixes=('', '_produto'))

    # Junta a tabela com ela mesma para encontrar as informações do "pai" de cada valor
    df_pais_info = df[['id_valor', 'valor', 'nome_atributo']].rename(columns={'id_valor': 'parent_valor_id_ref', 'valor': 'Valor Pai', 'nome_atributo': 'Atributo Pai'})
    
    df_final = pd.merge(df, df_pais_info, left_on='parent_valor_id', right_on='parent_valor_id_ref', how='left')

    # Renomeia e seleciona as colunas para uma exibição clara
    df_final = df_final.rename(columns={
        'nome_produto': 'Produto Base',
        'nome_atributo': 'Atributo',
        'valor': 'Valor Cadastrado',
        'valor_pai': 'É filho de (Valor Pai)'
    })
    
    colunas_para_exibir = ['Produto Base', 'Atributo', 'Valor Cadastrado', 'Atributo Pai', 'É filho de (Valor Pai)']
    colunas_existentes = [col for col in colunas_para_exibir if col in df_final.columns]
    
    return df_final[colunas_existentes].fillna('-')
    
# ADICIONE ESTA FUNÇÃO NOVA AO SEU CÓDIGO
# SUBSTITUA a função calcula_lucro_v3 antiga por esta versão correta:
def calcula_lucro_v3(df_vendas, df_estoque, df_eventos, comissao_percentual):
    """Calcula o lucro por venda para a arquitetura V3 com atributos dinâmicos, SEM o conceito de variante_id."""
    if df_vendas.empty:
        return pd.Series(dtype='float64')

    df_vendas_lucro = df_vendas.copy()

    # Cria a "impressão digital" para cada combinação de atributos
    def criar_chave(attrs):
        if attrs and isinstance(attrs, str):
            try: attrs = json.loads(attrs)
            except json.JSONDecodeError: return None
        return json.dumps(attrs, sort_keys=True) if isinstance(attrs, dict) else None

    # Prepara um mapa de custos médios a partir do estoque
    custos_medios = pd.Series()
    if not df_estoque.empty:
        df_estoque_custo = df_estoque.copy()
        df_estoque_custo['chave_atributos'] = df_estoque_custo['atributos'].apply(criar_chave)
        custos_medios = df_estoque_custo.groupby('chave_atributos')['valor_custo'].mean()
    
    # Aplica o mapa de custos às vendas
    df_vendas_lucro['chave_atributos'] = df_vendas_lucro['atributos'].apply(criar_chave)
    df_vendas_lucro['custo_unitario'] = df_vendas_lucro['chave_atributos'].map(custos_medios).fillna(0)
    df_vendas_lucro['custo_estoque'] = df_vendas_lucro['custo_unitario'] * df_vendas_lucro['quantidade_vendida']

    # Calcula os outros custos e receitas
    df_vendas_lucro['receita_bruta'] = df_vendas_lucro['preco_venda'] * df_vendas_lucro['quantidade_vendida']
    df_vendas_lucro['receita_liquida'] = df_vendas_lucro['receita_bruta'] - df_vendas_lucro['desconto'].fillna(0)
    df_vendas_lucro['comissao'] = df_vendas_lucro['receita_bruta'] * comissao_percentual
    df_vendas_lucro['taxas_pagamento'] = df_vendas_lucro['taxa_pagamento'].fillna(0)

    # Rateio de Custo de Evento
    df_vendas_lucro['custo_evento_rateado'] = 0 # Inicia com zero
    if not df_eventos.empty and 'evento' in df_vendas_lucro.columns:
        custos_eventos = df_eventos.set_index('nome_evento')[['aluguel', 'estacionamento', 'alimentacao', 'outros_custos']].sum(axis=1)
        vendas_por_evento = df_vendas_lucro.groupby('evento')['quantidade_vendida'].sum()
        df_vendas_lucro['custo_total_evento'] = df_vendas_lucro['evento'].map(custos_eventos).fillna(0)
        df_vendas_lucro['total_vendido_evento'] = df_vendas_lucro['evento'].map(vendas_por_evento).fillna(1)
        df_vendas_lucro['custo_evento_rateado'] = (df_vendas_lucro['custo_total_evento'] / df_vendas_lucro['total_vendido_evento']) * df_vendas_lucro['quantidade_vendida']

    # Cálculo Final do Lucro
    df_vendas_lucro['lucro'] = (
        df_vendas_lucro['receita_liquida'] - 
        df_vendas_lucro['custo_estoque'] - 
        df_vendas_lucro['custo_evento_rateado'] - 
        df_vendas_lucro['comissao'] - 
        df_vendas_lucro['taxas_pagamento']
    )
    
    return df_vendas_lucro['lucro']
    
# --- Bloco Principal do App ---
def main_app():
    user_id = st.session_state.user_session['user']['id']
    empresa_id, nome_da_empresa = get_empresa_info(user_id)

    if not empresa_id: st.error("Não foi possível identificar sua empresa."); st.stop()
        
    st.title(f"Bambuar V3 | {nome_da_empresa}")
    st.sidebar.header(nome_da_empresa)
    if st.sidebar.button("Sair"):
        for key in list(st.session_state.keys()): del st.session_state[key]
        st.rerun()

    with st.spinner('Carregando dados da sua empresa...'):
        df_produtos_base = load_data('produtos_base', {"filters": {"empresa_id": empresa_id}})
        df_variantes = load_data('produto_variantes', {"filters": {"empresa_id": empresa_id}})
        df_estoque = load_data('estoque', {"filters": {"empresa_id": empresa_id}})
        df_vendas = load_data('vendas', {"filters": {"empresa_id": empresa_id}})
        df_taxas = load_data('taxas_pagamento', {"filters": {"empresa_id": empresa_id}})
                
        
        df_atributo_tipos = pd.DataFrame()
        if not df_produtos_base.empty:
            df_atributo_tipos = load_data('atributo_tipos', {"filters": {"produto_base_id": df_produtos_base['id'].tolist()}})
        
        df_atributo_valores = pd.DataFrame()
        if not df_atributo_tipos.empty:
            df_atributo_valores = load_data('atributo_valores', {"filters": {"atributo_tipo_id": df_atributo_tipos['id'].tolist()}})

    tab_list = ['Dashboard', 'Estoque', 'Estoque - Catálogo', 'Resumo de Vendas', 'Vendas e Eventos', 'DRE', 'Ponto de Equilíbrio', 'DRE Projetada', 'Produtos e Variantes', 'Configurações']
    selected_tab = st.radio("Navegação:", tab_list, horizontal=True, label_visibility="collapsed")

    if selected_tab == 'Produtos e Variantes':
        st.header("⚙️ Configure seu Catálogo de Produtos")
        st.info(
            "Siga os passos:\n"
            "1. Crie o Produto Base.\n"
            "2. Crie os Atributos para ele.\n"
            "3. Adicione os Valores para cada Atributo."
        )
        st.markdown("---")

        # Carrega os dados atualizados para as verificações
        df_vendas = load_data('vendas', {"filters": {"empresa_id": empresa_id}})
        df_estoque = load_data('estoque', {"filters": {"empresa_id": empresa_id}})
        df_produtos_base = load_data('produtos_base', {"filters": {"empresa_id": empresa_id}})
        df_atributo_tipos = load_data('atributo_tipos', {})
        df_atributo_valores = load_data('atributo_valores', {})

        # =========================
        # PASSO 1: CRIAR PRODUTO BASE
        # =========================
        with st.expander("Passo 1: Crie seus Produtos Base", expanded=True):
            col_prod1, col_prod2 = st.columns([1, 2])
            with col_prod1:
                with st.form("form_produto_base", clear_on_submit=True):
                    novo_produto_nome = st.text_input(
                        "Nome do Novo Produto (ex: Luminária, Vaso, Relógio)"
                    )
                    if st.form_submit_button("Adicionar Produto Base"):
                        if nome_limpo := novo_produto_nome.strip():
                            is_duplicate = (
                                not df_produtos_base.empty
                                and nome_limpo.lower() in df_produtos_base['nome_produto'].str.lower().tolist()
                            )
                            if not is_duplicate:
                                add_data('produtos_base', {'nome_produto': nome_limpo}, empresa_id)
                                st.success(f"Produto '{nome_limpo}' criado!")
                                st.rerun()
                            else:
                                st.error("Um produto com este nome já existe.")
            with col_prod2:
                st.write("Produtos existentes:")
                if not df_produtos_base.empty:
                    st.dataframe(
                        df_produtos_base[['nome_produto']],
                        hide_index=True,
                        use_container_width=True
                    )
                else:
                    st.caption("Nenhum produto base cadastrado.")

        # =========================
        # PASSO 2: ATRIBUTOS
        # =========================
        with st.expander("Passo 2: Defina os Atributos de cada Produto"):
            if df_produtos_base.empty:
                st.warning("Crie um Produto Base no Passo 1 para continuar.")
            else:
                produto_id_attr = st.selectbox(
                    "Selecione um produto para gerenciar seus atributos:",
                    options=df_produtos_base['id'],
                    format_func=lambda x: df_produtos_base.loc[df_produtos_base['id'] == x, 'nome_produto'].iloc[0]
                )
                tipos_do_produto = df_atributo_tipos[df_atributo_tipos['produto_base_id'] == produto_id_attr]

                col_attr1, col_attr2 = st.columns([1, 2])
                with col_attr1:
                    with st.form("form_tipo_attr", clear_on_submit=True):
                        novo_nome_attr = st.text_input(
                            "Nome do Novo Atributo (ex: Modelo, Cor)"
                        )
                        if st.form_submit_button("Adicionar Atributo"):
                            if nome_limpo_attr := novo_nome_attr.strip():
                                is_duplicate_attr = (
                                    not tipos_do_produto.empty
                                    and nome_limpo_attr.lower() in tipos_do_produto['nome_atributo'].str.lower().tolist()
                                )
                                if not is_duplicate_attr:
                                    add_data(
                                        'atributo_tipos',
                                        {'produto_base_id': produto_id_attr, 'nome_atributo': nome_limpo_attr}
                                    )
                                    st.success(f"Atributo '{nome_limpo_attr}' adicionado!")
                                    st.rerun()
                                else:
                                    st.error("Este atributo já existe para este produto.")
                with col_attr2:
                    st.write("Atributos existentes:")
                    if not tipos_do_produto.empty:
                        for _, tipo in tipos_do_produto.iterrows():
                            col_nome, col_edit, col_del = st.columns([4, 1, 1])
                            col_nome.markdown(f"**{tipo['nome_atributo']}**")

                            em_estoque = (df_estoque['atributos'].astype(str).str.contains(tipo['nome_atributo'], na=False)).any() if not df_estoque.empty else False
                            em_vendas = (df_vendas['atributos'].astype(str).str.contains(tipo['nome_atributo'], na=False)).any() if not df_vendas.empty else False

                            if col_edit.button("✏️", key=f"edit_attr_{tipo['id']}"):
                                if em_estoque or em_vendas:
                                    st.warning(f"Não é possível editar o atributo '{tipo['nome_atributo']}' pois ele já foi usado.")
                                else:
                                    novo_nome_input = st.text_input(
                                        f"Novo nome para '{tipo['nome_atributo']}':",
                                        value=tipo['nome_atributo'],
                                        key=f"input_attr_{tipo['id']}"
                                    )
                                    if st.button("Salvar alteração", key=f"save_attr_{tipo['id']}"):
                                        supabase.table('atributo_tipos').update(
                                            {'nome_atributo': novo_nome_input}
                                        ).eq('id', tipo['id']).execute()
                                        st.success("Atributo atualizado!")
                                        st.rerun()

                            if col_del.button("🗑️", key=f"del_attr_{tipo['id']}"):
                                if em_estoque or em_vendas:
                                    st.warning(f"Não é possível excluir o atributo '{tipo['nome_atributo']}' pois ele já foi usado.")
                                else:
                                    supabase.table('atributo_tipos').delete().eq('id', tipo['id']).execute()
                                    st.success(f"Atributo '{tipo['nome_atributo']}' excluído!")
                                    st.rerun()
                    else:
                        st.caption("Nenhum atributo definido.")

        # =========================
        # PASSO 3: VALORES
        # =========================
        with st.expander("Passo 3: Cadastre os Valores para cada Atributo"):
            if df_atributo_tipos.empty:
                st.warning("Cadastre Atributos no Passo 2 para continuar.")
            elif df_produtos_base.empty:
                st.warning("Cadastre um Produto Base no Passo 1 antes de continuar.")
            else:
                # Merge apenas quando há produtos_base e atributo_tipos
                tipos_com_produto = pd.merge(
                    df_atributo_tipos,
                    df_produtos_base,
                    left_on='produto_base_id',
                    right_on='id',
                    suffixes=('', '_produto')
                )
                if tipos_com_produto.empty:
                    st.warning("Não há atributos associados a produtos para cadastrar valores.")
                else:
                    tipos_com_produto['display_name'] = (
                        tipos_com_produto['nome_produto'] + ' -> ' + tipos_com_produto['nome_atributo']
                    )

                    tipo_id_val = st.selectbox(
                        "Adicionar valor para o atributo:",
                        options=tipos_com_produto['id'],
                        format_func=lambda x: tipos_com_produto.loc[
                            tipos_com_produto['id'] == x, 'display_name'
                        ].iloc[0]
                    )

                    col_val1, col_val2 = st.columns([1, 2])
                    with col_val1:
                        with st.form("form_valor_attr", clear_on_submit=True):
                            novo_valor = st.text_input(
                                "Novo Valor (ex: Verde, Elefante)"
                            )
                            if st.form_submit_button("Adicionar Valor"):
                                if valor_limpo := novo_valor.strip():
                                    valores_existentes = (
                                        df_atributo_valores[df_atributo_valores['atributo_tipo_id']==tipo_id_val]
                                        if not df_atributo_valores.empty else pd.DataFrame()
                                    )
                                    if not valores_existentes.empty:
                                        ja_existe = valor_limpo.lower() in valores_existentes['valor'].str.lower().tolist()
                                    else:
                                        ja_existe = False

                                    if not ja_existe:
                                        add_data(
                                            'atributo_valores',
                                            {'atributo_tipo_id': tipo_id_val, 'valor': valor_limpo}
                                        )
                                        st.success(f"Valor '{valor_limpo}' adicionado!")
                                        st.rerun()
                                    else:
                                        st.error("Este valor já existe para este atributo.")
                    with col_val2:
                        st.write("Valores existentes para o atributo selecionado:")
                        valores_do_tipo = df_atributo_valores[df_atributo_valores['atributo_tipo_id']==tipo_id_val] if not df_atributo_valores.empty else pd.DataFrame()

                        if not valores_do_tipo.empty:
                            # Ordena os valores alfabeticamente
                            valores_do_tipo = valores_do_tipo.sort_values('valor')
                            for _, val in valores_do_tipo.iterrows():
                                col_nome, col_edit, col_del = st.columns([4, 1, 1])
                                col_nome.markdown(f"{val['valor']}")

                                em_estoque = (df_estoque['atributos'].astype(str).str.contains(val['valor'], na=False)).any() if not df_estoque.empty else False
                                em_vendas = (df_vendas['atributos'].astype(str).str.contains(val['valor'], na=False)).any() if not df_vendas.empty else False

                                if col_edit.button("✏️", key=f"edit_val_{val['id']}"):
                                    if em_estoque or em_vendas:
                                        st.warning(f"Não é possível editar o valor '{val['valor']}' pois ele já foi usado.")
                                    else:
                                        novo_nome_valor = st.text_input(
                                            f"Novo nome para '{val['valor']}':",
                                            value=val['valor'],
                                            key=f"input_val_{val['id']}"
                                        )
                                        if st.button("Salvar valor", key=f"save_val_{val['id']}"):
                                            supabase.table('atributo_valores').update(
                                                {'valor': novo_nome_valor}
                                            ).eq('id', val['id']).execute()
                                            st.success("Valor atualizado!")
                                            st.rerun()

                                if col_del.button("🗑️", key=f"del_val_{val['id']}"):
                                    if em_estoque or em_vendas:
                                        st.warning(f"Não é possível excluir o valor '{val['valor']}' pois ele já foi usado.")
                                    else:
                                        supabase.table('atributo_valores').delete().eq('id', val['id']).execute()
                                        st.success(f"Valor '{val['valor']}' excluído!")
                                        st.rerun()
                        else:
                            st.caption("Nenhum valor cadastrado.")



    # ... (outras abas)
    
   
    elif selected_tab == 'Estoque':
        st.header("📦 Gestão de Estoque")
        st.markdown("---")

        st.subheader("Adicionar Produto ao Estoque")

        if df_produtos_base.empty:
            st.warning("Você precisa primeiro criar um 'Produto Base' na aba 'Configurar Catálogo'.")
        else:
            produto_selecionado_id = st.selectbox(
                "Selecione o Produto Base para adicionar ao estoque:",
                options=df_produtos_base['id'],
                format_func=lambda x: df_produtos_base.loc[df_produtos_base['id'] == x, 'nome_produto'].iloc[0]
            )

            tipos_do_produto = df_atributo_tipos[df_atributo_tipos['produto_base_id'] == produto_selecionado_id] if not df_atributo_tipos.empty else pd.DataFrame()

            if not tipos_do_produto.empty:
                with st.form("form_estoque_final", clear_on_submit=True):
                    st.info("Monte seu produto selecionando uma opção de cada atributo:")
                    atributos_selecionados = {}

                    for _, tipo in tipos_do_produto.iterrows():
                        valores = df_atributo_valores[df_atributo_valores['atributo_tipo_id'] == tipo['id']] if not df_atributo_valores.empty else pd.DataFrame()
                        opcoes = valores['valor'].tolist() if not valores.empty else []
                        selecao = st.selectbox(f"{tipo['nome_atributo']}:", options=sorted(opcoes))
                        atributos_selecionados[tipo['nome_atributo']] = selecao

                    st.markdown("---")
                    st.write("**Detalhes da Entrada:**")
                    quantidade = st.number_input("Quantidade", min_value=1, step=1)
                    valor_custo = st.number_input("Custo Unitário (R$)", min_value=0.00, format="%.2f")
                    data_entrada = st.date_input("Data de Entrada", datetime.today())
                    observacao = st.text_area("Observação (opcional)")

                    # Novo: Upload da imagem
                    st.markdown("---")
                    imagem = st.file_uploader(
                        "Envie uma imagem para este item (opcional):",
                        type=['jpg', 'jpeg', 'png']
                    )

                    if st.form_submit_button("Adicionar ao Estoque"):
                        if any(v is None for v in atributos_selecionados.values()):
                            st.error("Por favor, selecione uma opção para cada atributo.")
                        else:
                            # Adiciona o item ao estoque
                            add_data('estoque', {
                                'produto_base_id': produto_selecionado_id,
                                'atributos': json.dumps(atributos_selecionados),
                                'quantidade': quantidade,
                                'valor_custo': valor_custo,
                                'data_entrada': str(data_entrada),
                                'observacao': observacao
                            }, empresa_id)

                            # Gerando a chave_variante igual à usada no catálogo
                            chave_variante = hashlib.md5(
                                (str(produto_selecionado_id) + json.dumps(
                                    atributos_selecionados, sort_keys=True
                                )).encode('utf-8')
                            ).hexdigest()

                            # Salvando a imagem, se foi enviada
                            if imagem is not None:
                                pasta_imagens = os.path.join("dados", str(empresa_id), "imagens_estoque")
                                os.makedirs(pasta_imagens, exist_ok=True)

                                caminho_arquivo = os.path.join(pasta_imagens, f"{chave_variante}.jpg")
                                with open(caminho_arquivo, "wb") as f:
                                    f.write(imagem.read())

                            st.success("Produto adicionado ao estoque!")
                            st.rerun()
            else:
                st.warning("Defina os atributos para este produto na aba 'Configurar Catálogo'.")

        st.markdown("---")
        st.subheader("Estoque Atual (Saldo)")
        df_saldo_final = calcula_estoque_final(df_estoque, df_vendas)
        if not df_saldo_final.empty:
            st.dataframe(df_saldo_final, hide_index=True, use_container_width=True)
        else:
            st.info("Nenhum item em estoque.")

            
            
            
    elif selected_tab == 'Estoque - Catálogo':
        st.header("🖼️ Catálogo Visual de Estoque")

        @st.cache_data
        def get_image_as_base64(path):
            if not os.path.exists(path):
                return None
            with open(path, "rb") as f:
                return base64.b64encode(f.read()).decode()

        # CSS melhorado para os cards
        st.markdown("""
            <style>
            .card {
                background-color: #262730;
                border: 1px solid #444;
                border-radius: 12px;
                padding: 0.75rem;
                box-shadow: 0 4px 8px rgba(0,0,0,0.2);
                transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                height: 100%;
                color: #e0e0e0;
            }
            .card:hover { transform: scale(1.03); box-shadow: 0 8px 16px rgba(0,0,0,0.3); }
            .card-img-container {
                width: 100%;
                height: 180px;
                display: flex;
                align-items: center;
                justify-content: center;
                overflow: hidden;
                border-radius: 8px;
                background: #1f1f28;
                margin-bottom: 0.5rem;
            }
            .card img {
                max-height: 100%;
                max-width: 100%;
                object-fit: cover;
            }
            .card-body {
                padding-top: 0.5rem;
            }
            .card h5 {
                font-size: 1rem;
                margin-bottom: 0.3rem;
            }
            .card p {
                font-size: 0.85rem;
                margin: 0.1rem 0;
            }
            </style>
        """, unsafe_allow_html=True)

        df_catalogo = calcula_estoque_final(df_estoque, df_vendas)

        if df_catalogo.empty:
            st.warning("Não há produtos no estoque para exibir.")
        else:
            df_disponivel = df_catalogo[df_catalogo['saldo'] >= 1].copy()
            if df_disponivel.empty:
                st.info("Todos os produtos em estoque estão com saldo zerado.")
            else:
                atributos_cols = [col for col in df_disponivel.columns if col not in ['produto_base_id', 'quantidade', 'quantidade_vendida', 'saldo']]
                
                df_disponivel['chave_variante'] = df_disponivel.apply(
                    lambda row: hashlib.md5(
                        (str(row['produto_base_id']) + json.dumps(
                            {col: row[col] for col in atributos_cols}, sort_keys=True
                        )).encode('utf-8')
                    ).hexdigest(),
                    axis=1
                )
                # Ordenar o DataFrame pelo primeiro atributo
                df_disponivel.sort_values(by=atributos_cols[0], inplace=True)
                df_disponivel.reset_index(drop=True, inplace=True)

                # Total de produtos e número de colunas
                num_produtos = len(df_disponivel)
                num_colunas = 4
                col_list = st.columns(num_colunas)

                for i, (_, row) in enumerate(df_disponivel.iterrows()):
                    # Quando for o início de uma nova linha (exceto a primeira), insere uma div espaçadora
                    if i > 0 and i % num_colunas == 0:
                        st.markdown(
                            "<div style='height:1.5rem;'></div>", unsafe_allow_html=True
                        )
                        col_list = st.columns(num_colunas)  # Reinicializa as colunas para a nova linha

                    with col_list[i % num_colunas]:
                        chave_variante = row['chave_variante']
                        nome_arquivo = f"{chave_variante}.jpg"
                        caminho_imagem = os.path.join(f"dados/{empresa_id}/imagens_estoque", nome_arquivo)
                        base64_image = get_image_as_base64(caminho_imagem)

                        image_html = (
                            f'<img src="data:image/jpeg;base64,{base64_image}">' if base64_image
                            else '<div style="height:180px; display:flex; align-items:center; justify-content:center; flex-direction:column; color:grey;">🖼️<br>Sem Imagem</div>'
                        )

                        titulo_card = str(row[atributos_cols[0]]) if atributos_cols else f"Produto {row['produto_base_id']}"
                        detalhes_card = " | ".join(str(row[col]) for col in atributos_cols[1:]) if len(atributos_cols) > 1 else ""

                        html_card = f"""
                        <div class="card">
                            <div class="card-img-container">{image_html}</div>
                            <div class="card-body">
                                <h5>{titulo_card}</h5>
                                <p>{detalhes_card}</p>
                                <p><b>Saldo: {int(row['saldo'])}</b></p>
                            </div>
                        </div>
                        """
                        st.markdown(html_card, unsafe_allow_html=True)

                # Preenche colunas restantes invisíveis, se necessário
                restante = num_colunas - (len(df_disponivel) % num_colunas) if len(df_disponivel) % num_colunas != 0 else 0
                for i in range(restante):
                    with col_list[(len(df_disponivel) + i) % num_colunas]:
                        st.markdown(
                            "<div class='card' style='opacity:0; border:none; box-shadow:none; height:100%'></div>",
                            unsafe_allow_html=True
                        )



                        
    # Dentro da função main_app(), substitua ou adicione este bloco:
    elif selected_tab == 'Dashboard':
        st.header(f"📊 Dashboard: {nome_da_empresa}")

        # Carrega dados
        df_comissao = load_data('comissao', {"filters": {"empresa_id": empresa_id}})
        df_eventos = load_data('eventos', {"filters": {"empresa_id": empresa_id}})
        COMISSAO_PERCENTUAL = df_comissao['percentual_comissao'].iloc[0] if not df_comissao.empty else 0.10

        if df_vendas.empty:
            st.warning("Nenhuma venda registrada para exibir o Dashboard.")
        else:
            # ==================== CÁLCULOS ====================
            df_vendas_dash = df_vendas.copy()

            # Receita e lucro
            df_vendas_dash['receita_bruta'] = df_vendas_dash['preco_venda'] * df_vendas_dash['quantidade_vendida']
            df_vendas_dash['receita_liquida'] = df_vendas_dash['receita_bruta'] - df_vendas_dash['desconto'].fillna(0)
            df_vendas_dash['lucro'] = calcula_lucro_v3(df_vendas_dash, df_estoque, df_eventos, COMISSAO_PERCENTUAL)

            # Totais
            receita_bruta_total = df_vendas_dash['receita_bruta'].sum()
            receita_liquida_total = df_vendas_dash['receita_liquida'].sum()
            lucro_total = df_vendas_dash['lucro'].sum()
            comissao_total = (df_vendas_dash['receita_bruta'] * COMISSAO_PERCENTUAL).sum()
            total_custos_evento = (
                (df_eventos['aluguel'].sum() if not df_eventos.empty else 0) +
                (df_eventos['estacionamento'].sum() if not df_eventos.empty else 0) +
                (df_eventos['alimentacao'].sum() if not df_eventos.empty else 0) +
                (df_eventos['outros_custos'].sum() if not df_eventos.empty else 0)
            )
            total_pedidos = len(df_vendas)
            total_pecas_vendidas = df_vendas['quantidade_vendida'].sum()

            # Métricas
            ticket_medio = receita_bruta_total / total_pecas_vendidas if total_pecas_vendidas > 0 else 0.0
            lucro_por_unidade = lucro_total / total_pecas_vendidas if total_pecas_vendidas > 0 else 0.0
            margem_lucro_percent = (lucro_total / receita_bruta_total) * 100 if receita_bruta_total > 0 else 0.0

            # Estoque atual
            df_saldo_dash = calcula_estoque_final(df_estoque, df_vendas)
            estoque_total = df_saldo_dash['saldo'].sum() if not df_saldo_dash.empty else 0
            valor_estoque_reais = 0.0
            if not df_estoque.empty and not df_saldo_dash.empty:
                custos_medios = df_estoque.groupby('produto_base_id')['valor_custo'].mean()
                df_saldo_dash['valor_custo_medio'] = df_saldo_dash['produto_base_id'].map(custos_medios)
                valor_estoque_reais = (df_saldo_dash['saldo'] * df_saldo_dash['valor_custo_medio'].fillna(0)).sum()

            valor_medio_venda = df_vendas['preco_venda'].mean() if not df_vendas.empty else 0.0
            valor_mercado_estoque = estoque_total * valor_medio_venda

            # ==================== MÉTRICAS ====================
            st.subheader("Métricas Gerais")

            col1, col2, col3 = st.columns(3)
            col1.metric("Receita Bruta Total (R$)", f"{receita_bruta_total:,.2f}")
            col2.metric("Receita Líquida Total (R$)", f"{receita_liquida_total:,.2f}")
            col3.metric("Lucro Total (R$)", f"{lucro_total:,.2f}")

            col4, col5, col6 = st.columns(3)
            col4.metric("Comissões (R$)", f"{comissao_total:,.2f}")
            col5.metric("Custo Total Eventos (R$)", f"{total_custos_evento:,.2f}")
            col6.metric("Estoque Atual (unid)", f"{int(estoque_total)}")

            col7, col8, col9 = st.columns(3)
            col7.metric("Ticket Médio (R$)", f"{ticket_medio:,.2f}")
            col8.metric("Margem de Lucro (%)", f"{margem_lucro_percent:.1f}%")
            col9.metric("Total de Pedidos", f"{total_pedidos}")

            col10, col11, col12 = st.columns(3)
            col10.metric("Lucro por Unidade Vendida (R$)", f"{lucro_por_unidade:,.2f}")
            col11.metric("Valor do Estoque a Custo (R$)", f"{valor_estoque_reais:,.2f}")
            col12.metric("Valor de Mercado do Estoque (R$)", f"{valor_mercado_estoque:,.2f}")

            # ==================== GRÁFICOS ====================
            st.markdown("---")
            st.header("Análises Gráficas")

            # --- Gráfico Vendas por Atributo ---
            st.subheader("Vendas por Atributo")

            # Extrai os atributos JSON para colunas
            df_vendas_analise = df_vendas.copy()
            df_vendas_analise['atributos'] = df_vendas_analise['atributos'].apply(
                lambda x: json.loads(x) if isinstance(x, str) else x
            )
            df_atributos_vendas = pd.json_normalize(df_vendas_analise['atributos'])
            df_vendas_final = pd.concat(
                [df_vendas_analise.drop('atributos', axis=1), df_atributos_vendas],
                axis=1
            )

            # Opções dinâmicas para o selectbox
            nomes_atributos = list(df_atributos_vendas.columns)
            if nomes_atributos:
                atributo_selecionado = st.selectbox(
                    "Analisar vendas por qual atributo?",
                    options=nomes_atributos
                )

                dados_grafico = df_vendas_final.groupby(
                    atributo_selecionado
                )['quantidade_vendida'].sum().reset_index()

                fig_vendas = px.bar(
                    dados_grafico,
                    x=atributo_selecionado,
                    y='quantidade_vendida',
                    title=f"Total de Vendas por {atributo_selecionado}",
                    color='quantidade_vendida',
                    color_continuous_scale='Teal'
                )
                st.plotly_chart(fig_vendas, use_container_width=True)
            else:
                st.info("Não há atributos disponíveis para análise.")


                
    # Adicione ou substitua este bloco elif ao seu main_app()
    elif selected_tab == 'Vendas e Eventos':
        st.header('📅 Gestão de Vendas e Eventos')
        # Carrega os dados
        df_eventos = load_data('eventos', {"filters": {"empresa_id": empresa_id}})
        df_taxas = load_data('taxas_pagamento', {"filters": {"empresa_id": empresa_id}})

        with st.expander("Cadastrar Novo Evento", expanded=False):
            with st.form('form_evento', clear_on_submit=True):
                nome_evento = st.text_input('Nome do evento')
                data_evento = st.date_input('Data do evento', datetime.today())
                aluguel = st.number_input('Aluguel (R$)', min_value=0.0, format="%.2f")
                estacionamento = st.number_input('Estacionamento (R$)', min_value=0.0, format="%.2f")
                alimentacao = st.number_input('Alimentação (R$)', min_value=0.0, format="%.2f")
                outros_custos = st.number_input('Outros custos (R$)', min_value=0.0, format="%.2f")
                observacao_evento = st.text_area('Observação')
                if st.form_submit_button('Registrar Evento'):
                    if nome_evento.strip():
                        add_data(
                            'eventos',
                            {
                                'nome_evento': nome_evento.strip(),
                                'data_evento': str(data_evento),
                                'aluguel': aluguel,
                                'estacionamento': estacionamento,
                                'alimentacao': alimentacao,
                                'outros_custos': outros_custos,
                                'observacao': observacao_evento.strip()
                            },
                            empresa_id
                        )
                        st.success("Evento registrado!")
                        st.rerun()

        st.subheader('Eventos Registrados')
        if not df_eventos.empty:
            st.dataframe(df_eventos, hide_index=True)
        else:
            st.info('Nenhum evento cadastrado ainda.')
        st.markdown('---')

        st.subheader('🛒 Registrar Nova Venda')
        df_vendas = load_data('vendas', {"filters": {"empresa_id": empresa_id}})
        df_estoque = load_data('estoque', {"filters": {"empresa_id": empresa_id}})
        df_saldo_vendas = calcula_estoque_final(df_estoque, df_vendas)

        if df_saldo_vendas.empty or df_saldo_vendas['saldo'].sum() <= 0:
            st.warning("Não há produtos com saldo em estoque para vender.")
        else:
            df_disponivel = df_saldo_vendas[df_saldo_vendas['saldo'] > 0].copy()
            atributos_cols = [col for col in df_disponivel.columns if col not in ['quantidade', 'quantidade_vendida', 'saldo', 'produto_base_id']]
            df_disponivel['display_name'] = df_disponivel[atributos_cols].apply(
                lambda row: ' | '.join(f"{val}" for val in row), axis=1
            )
            opcoes_variantes = df_disponivel.apply(
                lambda row: f"{row['display_name']} (Saldo: {int(row['saldo'])})",
                axis=1
            ).tolist()

            with st.form("form_venda_v3", clear_on_submit=False):
                item_selecionado_str = st.selectbox(
                    "Selecione o Produto a Vender",
                    options=opcoes_variantes
                )
                # Tratamento para eventos
                if df_eventos.empty or 'nome_evento' not in df_eventos.columns:
                    evento_sel = "Nenhum"
                    st.info("Não há eventos cadastrados. A venda será registrada sem evento.")
                else:
                    evento_sel = st.selectbox(
                        'Venda no Evento',
                        options=["Nenhum"] + df_eventos['nome_evento'].unique().tolist()
                    )
                forma_pagamento_sel = st.selectbox(
                    'Forma de Pagamento',
                    options=df_taxas['forma_pagamento'].tolist() if not df_taxas.empty else []
                )
                saldo_selecionado = int(item_selecionado_str.split('(Saldo: ')[1].replace(')', ''))
                quantidade_vendida = st.number_input(
                    "Quantidade Vendida",
                    min_value=1,
                    max_value=saldo_selecionado,
                    step=1
                )
                preco_venda = st.number_input(
                    "Preço de Venda Unitário (R$)",
                    min_value=0.01,
                    format="%.2f"
                )
                desconto = st.number_input(
                    "Desconto Total (R$)",
                    min_value=0.0,
                    format="%.2f"
                )
                data_venda = st.date_input(
                    "Data da Venda",
                    datetime.today()
                )
                observacao_venda = st.text_area(
                    "Observação da Venda"
                )

                if st.form_submit_button("Registrar Venda"):
                    if not forma_pagamento_sel:
                        st.error("Por favor, cadastre uma forma de pagamento antes de continuar.")
                    else:
                        atributos_str_selecionado = item_selecionado_str.split(' (Saldo:')[0]
                        item_vendido_row = df_disponivel[df_disponivel['display_name'] == atributos_str_selecionado].iloc[0]
                        atributos_para_salvar = {col: item_vendido_row[col] for col in atributos_cols}

                        custo_evento = 0
                        if evento_sel != "Nenhum" and not df_eventos.empty:
                            evento_row = df_eventos[df_eventos['nome_evento'] == evento_sel].iloc[0]
                            custo_evento = (
                                (evento_row.get('aluguel', 0) or 0) +
                                (evento_row.get('estacionamento', 0) or 0) +
                                (evento_row.get('alimentacao', 0) or 0) +
                                (evento_row.get('outros_custos', 0) or 0)
                            )
                        taxa_row = df_taxas[df_taxas['forma_pagamento'] == forma_pagamento_sel]
                        taxa_percentual = taxa_row['taxa_percentual'].iloc[0] if not taxa_row.empty else 0
                        taxa_pagamento = (preco_venda * quantidade_vendida) * (taxa_percentual / 100)

                        dados_para_inserir = {
                            'produto_base_id': int(item_vendido_row['produto_base_id']),
                            'atributos': json.dumps(atributos_para_salvar),
                            'quantidade_vendida': quantidade_vendida,
                            'preco_venda': preco_venda,
                            'desconto': desconto,
                            'data_venda': str(data_venda),
                            'evento': evento_sel if evento_sel != "Nenhum" else None,
                            'custo_evento': custo_evento,
                            'forma_pagamento': forma_pagamento_sel,
                            'taxa_pagamento': taxa_pagamento,
                            'percentual_taxa_pagamento': taxa_percentual,
                            'observacao': observacao_venda
                        }

                        response = add_data('vendas', dados_para_inserir, empresa_id)
                        if response and response.data:
                            st.success("Venda registrada com sucesso!")
                        else:
                            st.error("Falha ao registrar a venda. Veja o log no terminal.")

        st.markdown('---')
        st.subheader('Histórico de Vendas Recentes')
        if not df_vendas.empty:
            df_vendas_display = df_vendas.copy()
            df_vendas_display['atributos'] = df_vendas_display['atributos'].apply(
                lambda x: ' | '.join(f"{v}" for k, v in json.loads(x).items()) if isinstance(x, str) else "Produto Removido"
            )
            st.dataframe(
                df_vendas_display.drop(columns=['empresa_id'], errors='ignore').rename(
                    columns={'id': 'id_venda', 'atributos': 'produto'}
                ),
                hide_index=True
            )
        else:
            st.info("Nenhuma venda registrada.")

            
    # Em uma aba como 'Configurações' ou 'Manutenção'
    # No seu código, encontre e substitua este bloco inteiro:
    
    
    elif selected_tab == 'Configurações':
        st.header("⚙️ Configurações da Empresa")

        # Carrega os dados financeiros necessários para esta aba
        df_comissao = load_data('comissao', {"filters": {"empresa_id": empresa_id}})
        df_taxas = load_data('taxas_pagamento', {"filters": {"empresa_id": empresa_id}})
        COMISSAO_PERCENTUAL = df_comissao['percentual_comissao'].iloc[0] if not df_comissao.empty else 0.10

        # Menu interno para as diferentes seções de configuração
        config_selecionada = st.radio(
            "Selecione a área para configurar:",
            ["Taxas de Pagamento", "Comissão"],
            horizontal=True,
            label_visibility="collapsed"
        )

        st.markdown("---")

        if config_selecionada == "Taxas de Pagamento":
            st.subheader("Minhas Taxas de Pagamento")
            st.info("Cadastre aqui as taxas que suas maquininhas ou meios de pagamento cobram. Elas serão usadas para calcular o lucro líquido das vendas.")
            
            if not df_taxas.empty:
                st.write("Taxas Atuais:")
                st.dataframe(df_taxas[['forma_pagamento', 'taxa_percentual']], hide_index=True, use_container_width=True)

            col_form, col_del = st.columns(2)

            with col_form:
                with st.form("form_taxas", clear_on_submit=True):
                    st.write("**Adicionar ou Atualizar Taxa**")
                    fp = st.text_input("Forma de Pagamento (ex: Débito, Crédito 2x)")
                    taxa = st.number_input("Taxa (%)", min_value=0.0, format="%.2f")
                    if st.form_submit_button("Salvar Taxa"):
                        if fp.strip():
                            # Lógica de "Upsert": deleta a antiga (se existir) e insere a nova.
                            supabase.table('taxas_pagamento').delete().match({'forma_pagamento': fp.strip(), 'empresa_id': empresa_id}).execute()
                            add_data('taxas_pagamento', {'forma_pagamento': fp.strip(), 'taxa_percentual': taxa}, empresa_id)
                            st.success(f"Taxa para '{fp.strip()}' salva com sucesso.")
                            st.rerun()
                        else:
                            st.warning("O nome da forma de pagamento não pode ser vazio.")
            
            with col_del:
                if not df_taxas.empty:
                    st.write("**Excluir Taxa Existente**")
                    # Carrega as vendas para verificar o uso das taxas
                    df_vendas = load_data('vendas', {"filters": {"empresa_id": empresa_id}})
                    formas_pagamento_usadas = set(df_vendas['forma_pagamento'].dropna().unique()) if not df_vendas.empty and 'forma_pagamento' in df_vendas.columns else set()
                    
                    taxa_para_deletar = st.selectbox("Selecione uma taxa para deletar", options=["---"] + df_taxas['forma_pagamento'].tolist())
                    if st.button("Deletar Taxa Selecionada", type="primary"):
                        if taxa_para_deletar != "---":
                            if taxa_para_deletar in formas_pagamento_usadas:
                                st.error(f"A taxa '{taxa_para_deletar}' já foi usada em vendas e não pode ser excluída.")
                            else:
                                supabase.table('taxas_pagamento').delete().match({'forma_pagamento': taxa_para_deletar, 'empresa_id': empresa_id}).execute()
                                st.success(f"Taxa '{taxa_para_deletar}' deletada.")
                                st.rerun()
        
        elif config_selecionada == "Comissão":
            st.subheader("Percentual de Comissão")
            st.info("Este é o percentual padrão de comissão sobre o preço de venda que será usado nos cálculos de lucro do seu negócio.")
            
            comissao_atual = COMISSAO_PERCENTUAL * 100
            comissao_nova = st.number_input(
                "Percentual de Comissão (%)", 
                value=comissao_atual, 
                min_value=0.0, 
                max_value=100.0,
                step=0.5,
                format="%.2f"
            )
            
            if st.button("Salvar Comissão"):
                comissao_decimal = comissao_nova / 100
                if not df_comissao.empty:
                    # Atualiza a comissão existente para esta empresa
                    id_comissao = df_comissao['id'].iloc[0]
                    supabase.table('comissao').update({'percentual_comissao': comissao_decimal}).eq('id', int(id_comissao)).execute()
                else:
                    # Caso o onboarding tenha falhado, cria a comissão pela primeira vez
                    add_data('comissao', {'percentual_comissao': comissao_decimal}, empresa_id)
                
                st.success("Percentual de comissão atualizado com sucesso!")
                st.rerun()
                            
    # Adicione este bloco elif ao seu main_app()
    elif selected_tab == 'DRE':
        st.header("🧾 Demonstração de Resultados do Exercício (DRE)")

        # Carrega os dados financeiros necessários para esta aba
        df_comissao = load_data('comissao', {"filters": {"empresa_id": empresa_id}})
        df_eventos = load_data('eventos', {"filters": {"empresa_id": empresa_id}})
        df_custos_fixos = load_data('custos_fixos', {"filters": {"empresa_id": empresa_id}})
        COMISSAO_PERCENTUAL = df_comissao['percentual_comissao'].iloc[0] if not df_comissao.empty else 0.10

        if df_vendas.empty:
            st.warning('Não há vendas registradas para gerar uma DRE.')
        else:
            df_vendas_dre = df_vendas.copy()
            df_vendas_dre['data_venda'] = pd.to_datetime(df_vendas_dre['data_venda'], errors='coerce')
            df_vendas_dre.dropna(subset=['data_venda'], inplace=True)
            
            # --- Filtros de Período e Evento ---
            data_min = df_vendas_dre['data_venda'].min().date()
            data_max = df_vendas_dre['data_venda'].max().date()
            
            col1, col2 = st.columns(2)
            with col1:
                periodo_inicio = st.date_input("Data de Início", data_min, min_value=data_min, max_value=data_max, key="dre_inicio")
            with col2:
                periodo_fim = st.date_input("Data de Fim", data_max, min_value=data_min, max_value=data_max, key="dre_fim")

            if periodo_inicio > periodo_fim:
                st.error("A data de início não pode ser posterior à data de fim.")
            else:
                df_filtered = df_vendas_dre[
                    (df_vendas_dre['data_venda'].dt.date >= periodo_inicio) & 
                    (df_vendas_dre['data_venda'].dt.date <= periodo_fim)
                ]
                
                if not df_filtered.empty and 'evento' in df_filtered.columns:
                    eventos_disponiveis = ["Todos"] + df_filtered['evento'].dropna().unique().tolist()
                    evento_selecionado = st.selectbox("Filtrar por evento (opcional)", options=eventos_disponiveis, key="dre_evento")
                    if evento_selecionado != "Todos":
                        df_filtered = df_filtered[df_filtered['evento'] == evento_selecionado]

                if not df_filtered.empty:
                    df_dre = df_filtered.copy()
                    
                    # --- Lógica V3 para Calcular o Custo do Estoque ---
                    # --- Lógica V3 para Calcular o Custo do Estoque ---
                    if not df_estoque.empty:
                        custos_medios = df_estoque.groupby('produto_base_id')['valor_custo'].mean()
                        df_dre['custo_unitario'] = df_dre['produto_base_id'].map(custos_medios).fillna(0)
                        df_dre['custo_estoque'] = df_dre['custo_unitario'] * df_dre['quantidade_vendida']
                    else:
                        df_dre['custo_estoque'] = 0

                    
                    # --- Demais Cálculos da DRE ---
                    df_dre['comissao'] = (df_dre['preco_venda'] * df_dre['quantidade_vendida']) * COMISSAO_PERCENTUAL
                    df_dre['receita_bruta'] = df_dre['preco_venda'] * df_dre['quantidade_vendida']
                    df_dre['receita_liquida'] = df_dre['receita_bruta'] - df_dre['desconto'].fillna(0)
                    df_dre['taxas_pagamento'] = df_dre['taxa_pagamento'].fillna(0)

                    vendas_por_evento_filtrado = df_dre.groupby('evento')['quantidade_vendida'].sum().to_dict()
                    custo_rateado_dre = []
                    for _, venda in df_dre.iterrows():
                        total_vendido_evento = vendas_por_evento_filtrado.get(venda['evento'], 1)
                        custo_unitario_evento = (venda.get('custo_evento', 0) or 0) / total_vendido_evento if total_vendido_evento > 0 else 0
                        custo_rateado_dre.append(custo_unitario_evento * venda['quantidade_vendida'])
                    df_dre['custo_evento_rateado'] = custo_rateado_dre
                    
                    # Soma dos custos fixos totais da empresa (não filtrado por período, por padrão)
                    custos_fixos_total = df_custos_fixos['valor'].sum() if not df_custos_fixos.empty else 0
                    
                    # --- Montagem da Tabela Final da DRE ---
                    st.subheader(f"DRE para o Período e Filtro Selecionado")
                    receita_bruta_total = df_dre['receita_bruta'].sum()
                    descontos_total = df_dre['desconto'].sum()
                    custo_estoque_total = df_dre['custo_estoque'].sum()
                    custo_evento_total = df_dre['custo_evento_rateado'].sum()
                    comissao_total = df_dre['comissao'].sum()
                    taxas_total = df_dre['taxas_pagamento'].sum()
                    
                    lucro_bruto = receita_bruta_total - descontos_total - custo_estoque_total
                    resultado_antes_impostos = lucro_bruto - comissao_total - taxas_total - custo_evento_total - custos_fixos_total

                    dre_data = {
                        'Descrição': [
                            '(+) Receita Bruta de Vendas', 
                            '(-) Descontos Concedidos', 
                            '(=) Receita Líquida', 
                            '(-) Custo dos Produtos Vendidos (CPV/CMV)', 
                            '(=) Lucro Bruto',
                            '(-) Despesas Variáveis',
                            '    (-) Comissões', 
                            '    (-) Taxas de Pagamento', 
                            '    (-) Custos de Evento (Rateado)',
                            '(-) Despesas Fixas',
                            '(=) Lucro Líquido (Resultado do Exercício)'
                        ],
                        'Valor (R$)': [
                            receita_bruta_total, 
                            -descontos_total, 
                            receita_bruta_total - descontos_total,
                            -custo_estoque_total,
                            lucro_bruto,
                            '',
                            -comissao_total,
                            -taxas_total,
                            -custo_evento_total,
                            -custos_fixos_total,
                            resultado_antes_impostos
                        ]
                    }
                    df_dre_final = pd.DataFrame(dre_data)
                    # MUDANÇA AQUI: Adicionado o parâmetro height
                    st.dataframe(
                        df_dre_final.style.format({'Valor (R$)': lambda x: f"R$ {x:,.2f}" if isinstance(x, (int, float)) else ""}), 
                        height=420,  # Altura em pixels, ajuste conforme necessário
                        hide_index=True, 
                        use_container_width=True
                    )
                else:
                    st.info("Nenhuma venda encontrada para os filtros selecionados.")
                    
                    
    # Adicione este bloco elif ao seu main_app()
    elif selected_tab == 'Ponto de Equilíbrio':
        st.header("⚖️ Calculadora de Ponto de Equilíbrio")
        st.subheader("Simulação Manual para um Evento Futuro")

        # Carrega os dados financeiros necessários para esta aba
        df_taxas = load_data('taxas_pagamento', {"filters": {"empresa_id": empresa_id}})
        df_comissao = load_data('comissao', {"filters": {"empresa_id": empresa_id}})
        COMISSAO_PERCENTUAL = df_comissao['percentual_comissao'].iloc[0] if not df_comissao.empty else 0.10
        
        # --- Custos Fixos do Evento (Input do Usuário) ---
        st.markdown("**Custos Fixos do Evento (R$)**")
        aluguel = st.number_input("Aluguel", min_value=0.0, value=0.0, step=100.0, key="pe_aluguel")
        estacionamento = st.number_input("Estacionamento", min_value=0.0, value=0.0, step=50.0, key="pe_estacionamento")
        alimentacao = st.number_input("Alimentação", min_value=0.0, value=0.0, step=50.0, key="pe_alimentacao")
        outros = st.number_input("Outros", min_value=0.0, value=0.0, step=50.0, key="pe_outros")
        custo_fixo_total_evento = aluguel + estacionamento + alimentacao + outros
        
        st.markdown("---")
        
        # --- Variáveis por Unidade (Input do Usuário com Padrões do Tenant) ---
        st.markdown("**Variáveis por Unidade Vendida (R$)**")
        preco_venda_manual = st.number_input("Preço de Venda Unitário", min_value=0.01, value=100.0, step=10.0, key="pe_preco_venda")
        
        custo_medio_estoque_real = df_estoque['valor_custo'].mean() if not df_estoque.empty else 30.0
        preco_custo_manual = st.number_input("Custo de Estoque Unitário", min_value=0.01, value=float(custo_medio_estoque_real), step=5.0, key="pe_custo_estoque")

        # --- Lógica para Taxas de Pagamento ---
        percentual_taxa_manual = 0.0
        if not df_taxas.empty:
            opcoes_pagamento = df_taxas['forma_pagamento'].tolist()
            opcoes_pagamento.append("Mix")
            meio_pagamento_manual = st.selectbox("Simular Meio de Pagamento:", options=opcoes_pagamento, key="pe_pagamento_manual")

            if meio_pagamento_manual == "Mix":
                percentual_taxa_manual = df_taxas['taxa_percentual'].mean()
            else:
                percentual_taxa_manual = df_taxas.loc[df_taxas['forma_pagamento'] == meio_pagamento_manual, 'taxa_percentual'].iloc[0]
        else:
            st.warning("Nenhuma taxa de pagamento configurada. Usando 0% para a simulação.")

        # --- Cálculo da Margem de Contribuição ---
        comissao_manual = preco_venda_manual * COMISSAO_PERCENTUAL
        taxa_pagamento_manual = preco_venda_manual * (percentual_taxa_manual / 100)
        margem_contribuicao_manual = preco_venda_manual - preco_custo_manual - comissao_manual - taxa_pagamento_manual
        
        st.markdown("---")

        # --- Exibição dos Resultados ---
        st.subheader("Resultados da Simulação")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Custo Fixo Total do Evento", f"R$ {custo_fixo_total_evento:,.2f}")
            st.metric("Margem de Contribuição por Unidade", f"R$ {margem_contribuicao_manual:,.2f}")
        
        with col2:
            if margem_contribuicao_manual > 0:
                ponto_equilibrio_qtd_manual = custo_fixo_total_evento / margem_contribuicao_manual
                ponto_equilibrio_receita_manual = ponto_equilibrio_qtd_manual * preco_venda_manual
                st.metric(label="Ponto de Equilíbrio (Peças)", value=f"{np.ceil(ponto_equilibrio_qtd_manual):,.0f} unidades")
                st.metric(label="Ponto de Equilíbrio (Receita)", value=f"R$ {ponto_equilibrio_receita_manual:,.2f}")
            else:
                st.error("Margem de Contribuição negativa ou zero.")
                st.warning("Não é possível atingir o ponto de equilíbrio com os valores atuais.")
            
            
    # Adicione este outro bloco elif ao seu main_app()
    elif selected_tab == 'DRE Projetada':
        st.header("💡 DRE Projetada por Evento")
        
        # Carrega os dados financeiros necessários para esta aba
        df_taxas = load_data('taxas_pagamento', {"filters": {"empresa_id": empresa_id}})
        df_comissao = load_data('comissao', {"filters": {"empresa_id": empresa_id}})
        COMISSAO_PERCENTUAL = df_comissao['percentual_comissao'].iloc[0] if not df_comissao.empty else 0.10

        st.markdown("### Receita Estimada")
        preco_venda_unit_dre = st.number_input("Preço de Venda Unitário (R$)", min_value=0.0, value=100.0, step=10.0, key="preco_venda_unit_dre")
        quantidade_vendida_dre = st.number_input("Quantidade Vendida Estimada", min_value=0, value=50, step=10, key="quantidade_vendida_dre")
        receita_bruta = preco_venda_unit_dre * quantidade_vendida_dre

        st.markdown("### Custos Variáveis")
        custo_medio_estoque_dre = df_estoque['valor_custo'].mean() if not df_estoque.empty else 30.0
        preco_custo_unit_dre = st.number_input("Custo Unitário de Estoque (R$)", min_value=0.0, value=float(custo_medio_estoque_dre), step=5.0, key="preco_custo_unit_dre")
        custo_estoque_total = preco_custo_unit_dre * quantidade_vendida_dre
        comissao_projetada = receita_bruta * COMISSAO_PERCENTUAL

        percentual_taxa_dre = 0.0
        if not df_taxas.empty:
            opcoes_pagamento_dre = df_taxas['forma_pagamento'].tolist()
            opcoes_pagamento_dre.append("Mix")
            meio_pagamento_dre = st.selectbox("Simular Meio de Pagamento para Taxa", options=opcoes_pagamento_dre, key="dre_meio_pagamento")
            
            if meio_pagamento_dre == "Mix":
                percentual_taxa_dre = df_taxas['taxa_percentual'].mean()
            else:
                percentual_taxa_dre = df_taxas.loc[df_taxas['forma_pagamento'] == meio_pagamento_dre, 'taxa_percentual'].iloc[0]
        else:
            st.warning("Nenhuma taxa de pagamento configurada. Usando 0% para a simulação.")

        taxa_pagamento_projetada = receita_bruta * (percentual_taxa_dre / 100)
        st.info(f"Taxa de pagamento calculada com base em {percentual_taxa_dre:.2f}%")

        st.markdown("### Custos Fixos do Evento")
        aluguel_dre = st.number_input("Aluguel (R$)", min_value=0.0, value=1000.0, step=50.0, key="aluguel_dre")
        estacionamento_dre = st.number_input("Estacionamento (R$)", min_value=0.0, value=50.0, step=10.0, key="estacionamento_dre")
        alimentacao_dre = st.number_input("Alimentação (R$)", min_value=0.0, value=100.0, step=10.0, key="alimentacao_dre")
        outros_dre = st.number_input("Outros Custos (R$)", min_value=0.0, value=50.0, step=20.0, key="outros_dre")
        custo_evento_total = aluguel_dre + estacionamento_dre + alimentacao_dre + outros_dre

        st.markdown("---")
        st.subheader("DRE Projetada (Tabela)")
        
        lucro_bruto = receita_bruta - custo_estoque_total
        lucro_operacional = lucro_bruto - comissao_projetada - taxa_pagamento_projetada - custo_evento_total
        
        dre_data = [
            ["(+) Receita Bruta", receita_bruta],
            ["(-) Custo do Produto Vendido (CMV)", -custo_estoque_total],
            ["(=) Lucro Bruto", lucro_bruto],
            ["(-) Despesas Operacionais", ""],
            ["     (-) Comissão", -comissao_projetada],
            ["     (-) Taxas de Pagamento", -taxa_pagamento_projetada],
            ["     (-) Custos Fixos do Evento", -custo_evento_total],
            ["(=) Lucro Líquido do Evento", lucro_operacional]
        ]
        df_dre_final = pd.DataFrame(dre_data, columns=["Descrição", "Valor (R$)"])
        st.table(df_dre_final.style.format({"Valor (R$)": lambda x: f"R$ {x:,.2f}" if isinstance(x, (int, float)) else ""}))
        
        
        # Adicione este bloco elif ao seu main_app()
    elif selected_tab == 'Resumo de Vendas':
        st.header('📋 Resumo de Vendas')

        # Carrega todos os dados necessários
        df_vendas = load_data('vendas', {"filters": {"empresa_id": empresa_id}})
        df_estoque = load_data('estoque', {"filters": {"empresa_id": empresa_id}})
        df_comissao = load_data('comissao', {"filters": {"empresa_id": empresa_id}})
        df_eventos = load_data('eventos', {"filters": {"empresa_id": empresa_id}})
        COMISSAO_PERCENTUAL = df_comissao['percentual_comissao'].iloc[0] if not df_comissao.empty else 0.10

        if df_vendas.empty:
            st.warning('Não há vendas registradas para gerar um resumo.')
        else:
            vendas_completa = df_vendas.copy()

            # Preenche campos nulos
            vendas_completa['forma_pagamento'] = vendas_completa['forma_pagamento'].fillna('não informado')
            vendas_completa['taxa_pagamento'] = vendas_completa['taxa_pagamento'].fillna(0.0)

            # Calcula receita e comissões
            vendas_completa['receita_bruta'] = vendas_completa['preco_venda'] * vendas_completa['quantidade_vendida']
            vendas_completa['comissao'] = vendas_completa['receita_bruta'] * COMISSAO_PERCENTUAL
            vendas_completa['receita_liquida'] = vendas_completa['receita_bruta'] - vendas_completa['desconto'].fillna(0)

            # Calcula custos médios por produto_base_id (estoque)
            if not df_estoque.empty:
                custos_medios = df_estoque.groupby('produto_base_id')['valor_custo'].mean()
                vendas_completa['custo_unitario'] = vendas_completa['produto_base_id'].map(custos_medios).fillna(0)
            else:
                vendas_completa['custo_unitario'] = 0

            vendas_completa['custo_estoque'] = vendas_completa['custo_unitario'] * vendas_completa['quantidade_vendida']

            # Custo por evento rateado
            vendas_por_evento = vendas_completa.groupby('evento')['quantidade_vendida'].sum().to_dict()
            custo_rateado = []
            for _, venda in vendas_completa.iterrows():
                total_vendido_evento = vendas_por_evento.get(venda['evento'], 1) or 1
                custo_evento = venda.get('custo_evento', 0) or 0
                custo_rateado.append((custo_evento / total_vendido_evento) * venda['quantidade_vendida'])

            vendas_completa['custo_evento_rateado'] = custo_rateado
            vendas_completa['lucro_final'] = (
                vendas_completa['receita_liquida'] -
                vendas_completa['custo_estoque'] -
                vendas_completa['comissao'] -
                vendas_completa['custo_evento_rateado'] -
                vendas_completa['taxa_pagamento']
            )

            # Extrai atributos JSON
            vendas_completa['atributos'] = vendas_completa['atributos'].apply(
                lambda x: json.loads(x) if isinstance(x, str) else x
            )
            df_atributos_flat = pd.json_normalize(vendas_completa['atributos'])

            vendas_display = pd.concat([vendas_completa.reset_index(drop=True), df_atributos_flat], axis=1)

            nomes_atributos = df_atributos_flat.columns.tolist()
            colunas_agrupamento = nomes_atributos + ['evento'] if 'evento' in vendas_display.columns else nomes_atributos

            resumo = vendas_display.groupby(colunas_agrupamento).agg(
                quantidade_vendida=('quantidade_vendida', 'sum'),
                receita_bruta=('receita_bruta', 'sum'),
                lucro_final=('lucro_final', 'sum')
            ).reset_index()

            st.subheader('Resumo Agregado por Produto (Atributos) e Evento')
            st.dataframe(
                resumo.style.format(precision=2).background_gradient(subset=['lucro_final'], cmap='Greens'),
                hide_index=True, use_container_width=True
            )

            st.subheader('Resumo Consolidado por Evento')
            resumo_evento = vendas_completa.groupby('evento').agg(
                quantidade_vendida=('quantidade_vendida', 'sum'),
                receita_bruta=('receita_bruta', 'sum'),
                receita_liquida=('receita_liquida', 'sum'),
                custo_estoque=('custo_estoque', 'sum'),
                comissao=('comissao', 'sum'),
                custo_evento_rateado=('custo_evento_rateado', 'sum'),
                taxa_pagamento=('taxa_pagamento', 'sum'),
                lucro_final=('lucro_final', 'sum')
            ).reset_index()

            st.dataframe(
                resumo_evento.style.format(precision=2).background_gradient(subset=['lucro_final'], cmap='Blues'),
                hide_index=True, use_container_width=True
            )

    
    
    
# --- Ponto de Entrada Principal ---
if 'user_session' not in st.session_state or st.session_state.user_session is None:
    st.title("Bem-vindo ao Bambuar V3")
    login_tab, signup_tab = st.tabs(["Login", "Criar Conta"])
    with login_tab: login_page()
    with signup_tab: signup_page()
else:
    main_app()