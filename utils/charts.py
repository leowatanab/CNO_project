import altair as alt
import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import geopandas as gpd
import unicodedata

def chart_por_estado(df: pd.DataFrame):
    state_counts = (
        df["Estado"]
        .value_counts()
        .rename("Quantidade")
        .reset_index()
        .rename(columns={"index": "Estado"})
    )

    bars = (
        alt.Chart(state_counts)
        .mark_bar()
        .encode(
            y=alt.Y("Estado:N", sort="-x", title="Estado"),
            x=alt.X("Quantidade:Q", title="Quantidade de obras"),
            tooltip=["Estado", "Quantidade"]
        )
    )

    labels = (
        alt.Chart(state_counts)
        .mark_text(dx=5, align="left", baseline="middle")
        .encode(
            y=alt.Y("Estado:N", sort="-x"),
            x="Quantidade:Q",
            text="Quantidade:Q"
        )
    )

    st.altair_chart(bars + labels, use_container_width=True)

def normalizar(texto):
    if texto is None:
        return None
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    return texto.strip().upper()


def mapa_coropletico_obras(df):

    UF_PARA_ESTADO = {
        "AC": "ACRE", "AL": "ALAGOAS", "AP": "AMAPA", "AM": "AMAZONAS",
        "BA": "BAHIA", "CE": "CEARA", "DF": "DISTRITO FEDERAL",
        "ES": "ESPIRITO SANTO", "GO": "GOIAS", "MA": "MARANHAO",
        "MT": "MATO GROSSO", "MS": "MATO GROSSO DO SUL", "MG": "MINAS GERAIS",
        "PA": "PARA", "PB": "PARAIBA", "PR": "PARANA", "PE": "PERNAMBUCO",
        "PI": "PIAUI", "RJ": "RIO DE JANEIRO", "RN": "RIO GRANDE DO NORTE",
        "RS": "RIO GRANDE DO SUL", "RO": "RONDONIA", "RR": "RORAIMA",
        "SC": "SANTA CATARINA", "SP": "SAO PAULO", "SE": "SERGIPE",
        "TO": "TOCANTINS",
    }

    # ======================
    # DADOS
    # ======================
    df_aux = df.copy()
    df_aux["Estado"] = (
        df_aux["Estado"]
        .str.upper()
        .map(UF_PARA_ESTADO)
        .apply(normalizar)
    )

    obras_estado = (
        df_aux["Estado"]
        .dropna()
        .value_counts()
        .rename("Qtd_Obras")
        .reset_index()
        .rename(columns={"index": "Estado"})
    )

    # ======================
    # MAPA
    # ======================
    gdf = gpd.read_file(
        "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
    )
    gdf["Estado"] = gdf["name"].apply(normalizar)

    gdf = gdf.merge(obras_estado, on="Estado", how="left")
    gdf["Qtd_Obras"] = gdf["Qtd_Obras"].fillna(0)

    fig, ax = plt.subplots(figsize=(10, 10))

    gdf.plot(
        column="Qtd_Obras",
        cmap="OrRd",
        linewidth=0.8,
        edgecolor="black",
        legend=True,
        ax=ax
    )

    # ======================
    # 🔢 RÓTULOS NO MAPA
    # ======================
    for _, row in gdf.iterrows():
        if row["Qtd_Obras"] > 0:
            x, y = row.geometry.centroid.coords[0]
            ax.text(
                x, y,
                f"{int(row['Qtd_Obras'])}",
                ha="center",
                va="center",
                fontsize=9,
                color="black",
                weight="bold"
            )

    ax.set_title("Quantidade de Obras por Estado")
    ax.axis("off")

    st.pyplot(fig)