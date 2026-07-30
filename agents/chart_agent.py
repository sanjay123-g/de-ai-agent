"""
agents/chart_agent.py

Generates a Plotly chart from query_agent results. Chart type is
picked deterministically based on data shape (not LLM-guessed styling) -
consistent visual quality regardless of question complexity.
"""
import plotly.graph_objects as go
from agents.query_agent import answer_question


def make_chart(question: str, output_path: str = "chart.html"):
    result = answer_question(question)
    if result.get("error"):
        raise ValueError(f"Query failed: {result['error']}")

    columns = result["columns"]
    rows = result["rows"]

    if len(columns) < 2:
        raise ValueError("Need at least 2 columns (label + value) to chart")

    # Use the first column as labels, and the LAST numeric column as
    # values -- query results can include extra text columns (e.g.
    # tournament) between the label and the actual metric, so we can't
    # assume index 1 is always the value.
    value_idx = None
    for i in range(len(columns) - 1, 0, -1):
        if isinstance(rows[0][i], (int, float)):
            value_idx = i
            break
    if value_idx is None:
        raise ValueError("No numeric column found to chart")

    labels = [str(r[0]) for r in rows]
    values = [r[value_idx] for r in rows]
    value_col_name = columns[value_idx]

    fig = go.Figure(data=[go.Bar(x=labels, y=values, marker_color="#f97316")])
    fig.update_layout(
        title=question,
        xaxis_title=columns[0],
        yaxis_title=value_col_name,
        template="plotly_white",
        font=dict(family="Segoe UI, Helvetica, Arial", size=13),
    )
    fig.write_html(output_path)
    return output_path


if __name__ == "__main__":
    path = make_chart("Show the top 10 teams by points")
    print(f"Chart saved to {path}")
