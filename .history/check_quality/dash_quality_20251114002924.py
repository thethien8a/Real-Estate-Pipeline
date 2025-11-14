import os
import pandas as pd
from supabase import create_client
import plotly.graph_objects as go
import plotly.express as px

# Load env from GitHub Actions
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_qc_data():
    response = client.schema("bronze").table("qc_daily_overview").select("*").execute()
    df = pd.DataFrame(response.data)
    df["crawl_date"] = pd.to_datetime(df["crawl_date"])
    df = df.sort_values("crawl_date")
    return df


def build_dashboard(df, output_path="dashboard_quality.html"):
    # Columns
    missing_cols = [c for c in df.columns if c.startswith("missing_")]
    suspicious_cols = [
        c for c in df.columns if c.startswith("suspicious_") 
        or c.startswith("invalid_")
    ]

    # ------------------------
    # Missing chart
    # ------------------------
    fig_missing = go.Figure()

    for col in missing_cols:
        fig_missing.add_trace(go.Scatter(
            x=df["crawl_date"],
            y=df[col],
            mode='lines+markers',
            name=col
        ))

    fig_missing.update_layout(
        title="Missing Data Over Time",
        xaxis_title="Crawl Date",
        yaxis_title="Count",
        xaxis=dict(rangeslider=dict(visible=True))
    )

    # Dropdown filter for missing data
    fig_missing.update_layout(
        updatemenus=[
            dict(
                active=0,
                buttons=[
                    dict(
                        label=str(d),
                        method="update",
                        args=[
                            {"visible": [cd == d for cd in df["crawl_date"] for _ in missing_cols]},
                            {"title": f"Missing Data on {d}"}
                        ],
                    )
                    for d in df["crawl_date"].dt.date.unique()
                ] + [
                    dict(
                        label="Show All",
                        method="update",
                        args=[{"visible": [True] * len(missing_cols)}]
                    )
                ],
                x=0.1,
                y=1.15,
                xanchor="left",
                yanchor="top"
            )
        ]
    )

    # ------------------------
    # Suspicious chart
    # ------------------------
    fig_suspicious = go.Figure()

    for col in suspicious_cols:
        fig_suspicious.add_trace(go.Scatter(
            x=df["crawl_date"],
            y=df[col],
            mode='lines+markers',
            name=col
        ))

    fig_suspicious.update_layout(
        title="Suspicious / Invalid Data Over Time",
        xaxis_title="Crawl Date",
        yaxis_title="Count",
        xaxis=dict(rangeslider=dict(visible=True))
    )

    # Dropdown filter for suspicious data
    fig_suspicious.update_layout(
        updatemenus=[
            dict(
                active=0,
                buttons=[
                    dict(
                        label=str(d),
                        method="update",
                        args=[
                            {"visible": [cd == d for cd in df["crawl_date"] for _ in suspicious_cols]},
                            {"title": f"Suspicious Data on {d}"}
                        ],
                    )
                    for d in df["crawl_date"].dt.date.unique()
                ] + [
                    dict(
                        label="Show All",
                        method="update",
                        args=[{"visible": [True] * len(suspicious_cols)}]
                    )
                ],
                x=0.1,
                y=1.15,
                xanchor="left",
                yanchor="top"
            )
        ]
    )

    # ------------------------
    # Combine into HTML
    # ------------------------
    html = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>Quality Check Dashboard</title>
    </head>
    <body>
        <h1>Quality Check Dashboard</h1>

        <h2>Missing Data</h2>
        {fig_missing.to_html(full_html=False, include_plotlyjs='cdn')}

        <h2>Suspicious Data</h2>
        {fig_suspicious.to_html(full_html=False, include_plotlyjs='cdn')}
    </body>
    </html>
    """

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Dashboard generated: {output_path}")


if __name__ == "__main__":
    df = fetch_qc_data()
    build_dashboard(df)
