import altair as alt
import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

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

def heatmap_por_estado(df):
    estado_counts = (
        df["Estado"]
        .dropna()
        .value_counts()
        .sort_values(ascending=False)  # 🔥 ordena
    )

    estados = estado_counts.index.tolist()
    valores = estado_counts.values.reshape(1, -1)

    fig, ax = plt.subplots(figsize=(16, 2.5))

    im = ax.imshow(valores, aspect="auto")

    # Anotações
    for i, v in enumerate(estado_counts.values):
        ax.text(i, 0, f"{v:,}", ha="center", va="center", fontsize=9)

    ax.set_xticks(range(len(estados)))
    ax.set_xticklabels(estados)
    ax.set_yticks([])

    ax.set_title("Mapa de Calor — Quantidade de Obras por Estado")

    plt.colorbar(im, ax=ax, label="Quantidade de obras")

    st.pyplot(fig)