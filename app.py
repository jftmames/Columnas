import streamlit as st
import pandas as pd
import time

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Column-Oriented Lab", layout="wide")

st.title("📊 AnalyticsEngine: Almacén NoSQL Orientado a Columnas")
st.markdown("""
    Este simulador replica el comportamiento de **Apache Cassandra** o **Google BigTable**. 
    En lugar de leer filas, leemos columnas específicas para maximizar la velocidad analítica.
""")

# --- 1. INICIALIZACIÓN DEL ALMACÉN (COLUMN FAMILIES) ---
# Estructura: { 'Familia': { 'Columna': [Datos] } }
if 'column_store' not in st.session_state:
    st.session_state.column_store = {
        "Datos_Usuario": {
            "ID_Cliente": ["C-001", "C-002", "C-003"],
            "Nombre": ["Alicia", "Bob", "Carlos"]
        },
        "Datos_Geograficos": {
            "Ciudad": ["Madrid", "Barcelona", "Madrid"],
            "IP": ["192.168.1.1", "10.0.0.5", "172.16.0.20"]
        },
        "Datos_Metricas": {
            "Gasto_Publicitario": [150.0, 300.5, 50.0],
            "Clics": [12, 45, 8]
        }
    }

# --- 2. BARRA LATERAL: INSERCIÓN DE DATOS ---
with st.sidebar:
    st.header("📥 Ingesta de Datos (Write)")
    with st.form("insert_form"):
        f_id = st.text_input("ID Cliente", value="C-004")
        f_nom = st.text_input("Nombre", value="Diana")
        f_ciu = st.selectbox("Ciudad", ["Madrid", "Barcelona", "Valencia", "Sevilla"])
        f_gas = st.number_input("Gasto ($)", min_value=0.0, value=100.0)
        f_cli = st.number_input("Clics", min_value=0, value=5)
        
        if st.form_submit_button("Insertar Registro"):
            # En bases de columnas, insertamos en cada 'sub-almacén'
            st.session_state.column_store["Datos_Usuario"]["ID_Cliente"].append(f_id)
            st.session_state.column_store["Datos_Usuario"]["Nombre"].append(f_nom)
            st.session_state.column_store["Datos_Geograficos"]["Ciudad"].append(f_ciu)
            st.session_state.column_store["Datos_Geograficos"]["IP"].append("0.0.0.0") # Simulado
            st.session_state.column_store["Datos_Metricas"]["Gasto_Publicitario"].append(f_gas)
            st.session_state.column_store["Datos_Metricas"]["Clics"].append(f_cli)
            st.success("Registro añadido a todas las familias de columnas.")
            st.rerun()

# --- 3. CUERPO PRINCIPAL: CONSULTA SELECTIVA ---
tab_consulta, tab_analisis = st.tabs(["🔍 Consulta de Columnas", "📈 Analítica Agregada"])

with tab_consulta:
    st.subheader("Selección de Columnas (Ahorro de I/O)")
    st.write("Selecciona solo los atributos que necesitas procesar:")
    
    # Recopilar todas las columnas disponibles
    familias = st.session_state.column_store.keys()
    columnas_disponibles = []
    for f in familias:
        for c in st.session_state.column_store[f].keys():
            columnas_disponibles.append(f"{f} -> {c}")
    
    seleccion = st.multiselect("Columnas a leer del disco:", columnas_disponibles, default=["Datos_Usuario -> ID_Cliente", "Datos_Metricas -> Gasto_Publicitario"])

    if st.button("Ejecutar Lectura"):
        if seleccion:
            # Simulamos el acceso solo a las columnas necesarias
            start_time = time.time()
            res_df = pd.DataFrame()
            
            for s in seleccion:
                fam, col = s.split(" -> ")
                res_df[col] = st.session_state.column_store[fam][col]
            
            proc_time = (time.time() - start_time) * 1000 # ms
            
            st.dataframe(res_df, use_container_width=True)
            st.metric("Datos procesados", f"{len(seleccion)} columnas", f"{proc_time:.4f} ms")
            st.info(f"Nota: Se han ignorado {len(columnas_disponibles) - len(seleccion)} columnas, ahorrando ancho de banda.")
        else:
            st.warning("Selecciona al menos una columna.")

with tab_analisis:
    st.subheader("Cómputo Directo sobre Columnas")
    
    col1, col2 = st.columns(2)
    
    # Analítica 1: Gasto por Ciudad (Cruza solo 2 columnas)
    df_geo = pd.DataFrame(st.session_state.column_store["Datos_Geograficos"])
    df_met = pd.DataFrame(st.session_state.column_store["Datos_Metricas"])
    df_analisis = pd.concat([df_geo["Ciudad"], df_met["Gasto_Publicitario"]], axis=1)
    
    with col1:
        st.write("**Inversión por Ciudad:**")
        st.bar_chart(df_analisis.groupby("Ciudad").sum())
        
    with col2:
        total_global = df_met["Gasto_Publicitario"].sum()
        st.metric("Gasto Total Global", f"{total_global:,.2f} $")
        st.write("Esta operación se realiza leyendo **únicamente** la columna 'Gasto_Publicitario'.")

# --- 4. EXPLICACIÓN TEÓRICA ---
st.divider()

with st.expander("💡 ¿Por qué esto es Big Data?"):
    st.write("""
    1. **Estructura:** Los datos de la misma columna están físicamente juntos en el disco.
    2. **Compresión:** Como los datos de una columna son del mismo tipo (ej: puros números), se comprimen mucho mejor que una fila SQL.
    3. **Casos de Uso:** Ideal para Data Warehousing y Analytics, donde no necesitas ver el nombre del cliente para calcular la suma de ventas anuales.
    """)
