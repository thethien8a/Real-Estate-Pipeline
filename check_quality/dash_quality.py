import os
import pandas as pd
from supabase import create_client
import plotly.graph_objects as go
import plotly.express as px
from dotenv import load_dotenv

# Load env
load_dotenv()

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

    # Tính tổng records
    total_all_records = df["total_records"].sum()

    # ------------------------
    # Missing chart
    # ------------------------
    fig_missing = go.Figure()

    for col in missing_cols:
        fig_missing.add_trace(go.Scatter(
            x=df["crawl_date"],
            y=df[col],
            mode='lines+markers',
            name=col,
            connectgaps=True
        ))

    fig_missing.update_layout(
        title="Missing Data Over Time",
        xaxis_title="Crawl Date",
        yaxis_title="Count",
        hovermode='x unified'
    )

    # Tạo dropdown buttons
    date_buttons_missing = []
    dates = sorted(df["crawl_date"].dt.date.unique())
    
    for date in dates:
        date_df = df[df["crawl_date"].dt.date == date]
        total_records = date_df["total_records"].values[0] if len(date_df) > 0 else 0
        
        # Chuẩn bị data cho từng trace
        x_data = [date_df["crawl_date"].tolist() for _ in missing_cols]
        y_data = [date_df[col].tolist() for col in missing_cols]
        
        date_buttons_missing.append(
            dict(
                label=f"{date} ({total_records:,} records)",
                method="update",
                args=[
                    {
                        "x": x_data,
                        "y": y_data,
                        "mode": ["markers"] * len(missing_cols)
                    },
                    {
                        "title": f"Missing Data on {date} - Total Records: {total_records:,}"
                    }
                ]
            )
        )
    
    # Button "Show All"
    x_data_all = [df["crawl_date"].tolist() for _ in missing_cols]
    y_data_all = [df[col].tolist() for col in missing_cols]
    
    date_buttons_missing.append(
        dict(
            label="Show All",
            method="update",
            args=[
                {
                    "x": x_data_all,
                    "y": y_data_all,
                    "mode": ["lines+markers"] * len(missing_cols)
                },
                {
                    "title": "Missing Data Over Time"
                }
            ]
        )
    )

    fig_missing.update_layout(
        updatemenus=[
            dict(
                active=len(date_buttons_missing) - 1,
                buttons=date_buttons_missing,
                direction="down",
                showactive=True,
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
            name=col,
            connectgaps=True
        ))

    fig_suspicious.update_layout(
        title="Suspicious / Invalid Data Over Time",
        xaxis_title="Crawl Date",
        yaxis_title="Count",
        hovermode='x unified'
    )

    # Tạo dropdown buttons
    date_buttons_suspicious = []
    
    for date in dates:
        date_df = df[df["crawl_date"].dt.date == date]
        total_records = date_df["total_records"].values[0] if len(date_df) > 0 else 0
        
        # Chuẩn bị data cho từng trace
        x_data = [date_df["crawl_date"].tolist() for _ in suspicious_cols]
        y_data = [date_df[col].tolist() for col in suspicious_cols]
        
        date_buttons_suspicious.append(
            dict(
                label=f"{date} ({total_records:,} records)",
                method="update",
                args=[
                    {
                        "x": x_data,
                        "y": y_data,
                        "mode": ["markers"] * len(suspicious_cols)
                    },
                    {
                        "title": f"Suspicious Data on {date} - Total Records: {total_records:,}"
                    }
                ]
            )
        )
    
    # Button "Show All"
    x_data_all = [df["crawl_date"].tolist() for _ in suspicious_cols]
    y_data_all = [df[col].tolist() for col in suspicious_cols]
    
    date_buttons_suspicious.append(
        dict(
            label="Show All",
            method="update",
            args=[
                {
                    "x": x_data_all,
                    "y": y_data_all,
                    "mode": ["lines+markers"] * len(suspicious_cols)
                },
                {
                    "title": "Suspicious / Invalid Data Over Time"
                }
            ]
        )
    )

    fig_suspicious.update_layout(
        updatemenus=[
            dict(
                active=len(date_buttons_suspicious) - 1,
                buttons=date_buttons_suspicious,
                direction="down",
                showactive=True,
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
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }}
            h1 {{
                color: #333;
                text-align: center;
            }}
            h2 {{
                color: #555;
                margin-top: 40px;
            }}
            .stats-box {{
                position: fixed;
                top: 20px;
                right: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 20px 25px;
                border-radius: 12px;
                box-shadow: 0 4px 15px rgba(0,0,0,0.2);
                font-size: 14px;
                z-index: 1000;
                min-width: 200px;
            }}
            .stats-box h3 {{
                margin: 0 0 10px 0;
                font-size: 16px;
                font-weight: 600;
                border-bottom: 2px solid rgba(255,255,255,0.3);
                padding-bottom: 8px;
            }}
            .stats-box .stat-item {{
                margin: 8px 0;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }}
            .stats-box .stat-label {{
                font-weight: 500;
                opacity: 0.9;
            }}
            .stats-box .stat-value {{
                font-weight: 700;
                font-size: 18px;
                background: rgba(255,255,255,0.2);
                padding: 4px 12px;
                border-radius: 6px;
            }}
        </style>
    </head>
    <body>
        <div class="stats-box">
            <h3>📊 Tổng quan</h3>
            <div class="stat-item">
                <span class="stat-label">Tổng số tin:</span>
                <span class="stat-value">{total_all_records:,}</span>
            </div>
            <div class="stat-item">
                <span class="stat-label">Số ngày:</span>
                <span class="stat-value">{len(dates)}</span>
            </div>
        </div>

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