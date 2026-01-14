import os
import duckdb
import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.express as px

# =====================================================
# CONEXÃO MOTHERDUCK
# =====================================================
def query_table(query: str) -> pd.DataFrame:
    md_token = os.getenv("MOTHERDUCK_TOKEN")
    if not md_token:
        raise ValueError("Token do MotherDuck não encontrado")

    con = duckdb.connect(f"md:?motherduck_token={md_token}")
    try:
        return con.sql(query).df()
    finally:
        con.close()


# =====================================================
# QUERY
# =====================================================
QUERY = """
SELECT
    CNO,
    Nome,
    Estado,
    "Nome do município",
    Categoria,
    Destinação,
    Metragem,
    "Código de localização"
FROM cno.cno_base
WHERE "Data de registro" > '2025-07-01'
  AND Categoria = 'Obra Nova'
  AND Destinação = 'Residencial multifamiliar'
  AND Situação = '02'
"""


# =====================================================
# CARGA INICIAL
# =====================================================
df = query_table(QUERY)

# Plus code → lat/lon
from openlocationcode import openlocationcode as olc

def pluscode_to_latlon(code):
    try:
        if not code or not olc.isFull(code):
            return None, None
        area = olc.decode(code)
        lat = (area.latitudeLo + area.latitudeHi) / 2
        lon = (area.longitudeLo + area.longitudeHi) / 2
        if not (-33.75 <= lat <= 5.27 and -73.99 <= lon <= -34.79):
            return None, None
        return lat, lon
    except:
        return None, None

df[["lat", "lon"]] = df["Código de localização"].apply(
    lambda x: pd.Series(pluscode_to_latlon(x))
)

df = df.dropna(subset=["lat", "lon"])


# =====================================================
# DASH APP
# =====================================================
app = Dash(__name__)
server = app.server  # 👈 necessário para deploy

estados = sorted(df["Estado"].dropna().unique())

app.layout = html.Div(
    style={"maxWidth": "1400px", "margin": "auto"},
    children=[
        html.H1("📊 Análise CNO"),

        dcc.Dropdown(
            id="estado",
            options=[{"label": "Todos", "value": "Todos"}] +
                    [{"label": e, "value": e} for e in estados],
            value="Todos",
            clearable=False
        ),

        html.Br(),

        dcc.Graph(id="mapa"),

        html.Br(),

        dcc.Graph(id="barra_estado")
    ]
)


# =====================================================
# CALLBACKS
# =====================================================
@app.callback(
    Output("mapa", "figure"),
    Output("barra_estado", "figure"),
    Input("estado", "value")
)
def atualizar(estado):
    dff = df if estado == "Todos" else df[df["Estado"] == estado]

    mapa = px.scatter_map(
        dff,
        lat="lat",
        lon="lon",
        hover_name="Nome do município",
        hover_data={
            "Estado": True,
            "Metragem": True,
            "lat": False,
            "lon": False
        },
        zoom=4,
        height=600
    )

    mapa.update_layout(
        mapbox_style="carto-positron",
        margin=dict(l=0, r=0, t=0, b=0)
    )

    barra = px.bar(
        dff["Estado"].value_counts().reset_index(),
        x="index",
        y="Estado",
        labels={"index": "Estado", "Estado": "Qtd Obras"}
    )

    return mapa, barra


if __name__ == "__main__":
    app.run(debug=True)
