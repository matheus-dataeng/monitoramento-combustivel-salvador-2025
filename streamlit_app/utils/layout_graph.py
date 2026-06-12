def default_layout(graph):
    graph.update_layout(
        height=600,
        title_font_size=25,
        xaxis_title_font=dict(size=17),
        yaxis_title_font=dict(size=17),
        xaxis=dict(tickfont=dict(size=17)),
        yaxis=dict(tickfont=dict(size=17))
    )

    return graph

def color_map():
    COLOR_MAP = {
    "Gasolina": "#1f77b4",
    "Gasolina Aditivada": "#ff7f0e",
    "Etanol": "#2ca02c",
    "Diesel S10": "#d62728"
}
    
    return COLOR_MAP