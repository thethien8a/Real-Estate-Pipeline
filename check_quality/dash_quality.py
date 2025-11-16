import os
import pandas as pd
from supabase import create_client
import plotly.graph_objects as go
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


def _format_column_label(column_name: str) -> str:
    return column_name.replace("_", " ").title()


def _render_metric_chart(df: pd.DataFrame, column: str) -> str:
    fig = go.Figure()
    
    # Add gradient fill
    fig.add_trace(
        go.Scatter(
            x=df["crawl_date"],
            y=df[column],
            mode="lines+markers",
            name=_format_column_label(column),
            line=dict(color="#3b82f6", width=3),
            marker=dict(size=8, color="#3b82f6", line=dict(width=2, color="white")),
            fill='tozeroy',
            fillcolor='rgba(59, 130, 246, 0.1)',
            connectgaps=True,
        )
    )

    fig.update_layout(
        title=dict(
            text=_format_column_label(column),
            font=dict(size=16, color="#1e293b", family="Inter, system-ui, sans-serif", weight=600)
        ),
        xaxis_title="Ngày Crawl",
        yaxis_title="Số lượng",
        hovermode="x unified",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=300,
        margin=dict(l=50, r=20, t=60, b=40),
        font=dict(family="Inter, system-ui, sans-serif", size=12, color="#64748b"),
        xaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor="rgba(148, 163, 184, 0.1)",
            showline=True,
            linewidth=1,
            linecolor="rgba(148, 163, 184, 0.2)",
        ),
        yaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor="rgba(148, 163, 184, 0.1)",
            showline=True,
            linewidth=1,
            linecolor="rgba(148, 163, 184, 0.2)",
        ),
    )

    return fig.to_html(full_html=False, include_plotlyjs=False, config={"displayModeBar": False})


def _build_chart_grid(df: pd.DataFrame, columns: list[str]) -> str:
    if not columns:
        return '<div class="empty-state">📊 Không có dữ liệu để hiển thị</div>'

    cards = []
    for col in columns:
        chart_html = _render_metric_chart(df, col)
        cards.append(f'<div class="chart-card">{chart_html}</div>')
    return "\n".join(cards)


def _calculate_metrics(df: pd.DataFrame) -> dict:
    """Calculate key metrics for the dashboard"""
    total_records = df["total_records"].sum()
    avg_daily_records = df["total_records"].mean()
    
    suspicious_cols = [c for c in df.columns if c.startswith("suspicious_") or c.startswith("invalid_")]
    total_suspicious = sum(df[col].sum() for col in suspicious_cols) if suspicious_cols else 0
    
    return {
        "total_records": int(total_records),
        "avg_daily_records": int(avg_daily_records),
        "total_suspicious": int(total_suspicious),
        "num_days": len(df["crawl_date"].unique())
    }


def build_dashboard(df, output_path="dashboard_quality.html"):
    # Columns
    missing_cols = [c for c in df.columns if c.startswith("missing_")]
    suspicious_cols = [
        c for c in df.columns if c.startswith("suspicious_") 
        or c.startswith("invalid_")
    ]

    # Calculate metrics
    metrics = _calculate_metrics(df)

    missing_chart_grid = _build_chart_grid(df, missing_cols)
    suspicious_chart_grid = _build_chart_grid(df, suspicious_cols)

    # Combine into HTML
    html = f"""
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Quality Check Dashboard - BDS</title>
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            :root {{
                --primary: #3b82f6;
                --primary-dark: #2563eb;
                --success: #10b981;
                --warning: #f59e0b;
                --danger: #ef4444;
                --bg-primary: #ffffff;
                --bg-secondary: #f8fafc;
                --text-primary: #0f172a;
                --text-secondary: #64748b;
                --border-color: #e2e8f0;
                --shadow-sm: 0 1px 3px rgba(0, 0, 0, 0.05);
                --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
                --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.05);
                --shadow-xl: 0 20px 25px -5px rgba(0, 0, 0, 0.05);
            }}
            
            body {{
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                padding: 2rem;
                color: var(--text-primary);
                -webkit-font-smoothing: antialiased;
                -moz-osx-font-smoothing: grayscale;
            }}
            
            .container {{
                max-width: 1400px;
                margin: 0 auto;
            }}
            
            .header {{
                background: var(--bg-primary);
                border-radius: 20px;
                padding: 2.5rem;
                margin-bottom: 2rem;
                box-shadow: var(--shadow-xl);
                border: 1px solid var(--border-color);
            }}
            
            .header h1 {{
                font-size: 2.5rem;
                font-weight: 800;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
                margin-bottom: 0.5rem;
            }}
            
            .header p {{
                color: var(--text-secondary);
                font-size: 1.1rem;
                font-weight: 500;
            }}
            
            .metrics-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 1.5rem;
                margin-bottom: 2rem;
            }}
            
            .metric-card {{
                background: var(--bg-primary);
                border-radius: 16px;
                padding: 1.5rem;
                box-shadow: var(--shadow-lg);
                border: 1px solid var(--border-color);
                transition: all 0.3s ease;
                position: relative;
                overflow: hidden;
            }}
            
            .metric-card::before {{
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                height: 4px;
                background: linear-gradient(90deg, var(--gradient-start), var(--gradient-end));
            }}
            
            .metric-card:hover {{
                transform: translateY(-4px);
                box-shadow: var(--shadow-xl);
            }}
            
            .metric-card.primary {{
                --gradient-start: #3b82f6;
                --gradient-end: #2563eb;
            }}
            
            .metric-card.success {{
                --gradient-start: #10b981;
                --gradient-end: #059669;
            }}
            
            .metric-card.warning {{
                --gradient-start: #f59e0b;
                --gradient-end: #d97706;
            }}
            
            .metric-card.danger {{
                --gradient-start: #ef4444;
                --gradient-end: #dc2626;
            }}
            
            .metric-header {{
                display: flex;
                align-items: center;
                gap: 0.75rem;
                margin-bottom: 1rem;
            }}
            
            .metric-icon {{
                width: 48px;
                height: 48px;
                border-radius: 12px;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 1.5rem;
                background: var(--icon-bg);
            }}
            
            .metric-card.primary .metric-icon {{
                --icon-bg: rgba(59, 130, 246, 0.1);
            }}
            
            .metric-card.success .metric-icon {{
                --icon-bg: rgba(16, 185, 129, 0.1);
            }}
            
            .metric-card.warning .metric-icon {{
                --icon-bg: rgba(245, 158, 11, 0.1);
            }}
            
            .metric-card.danger .metric-icon {{
                --icon-bg: rgba(239, 68, 68, 0.1);
            }}
            
            .metric-label {{
                font-size: 0.875rem;
                font-weight: 600;
                color: var(--text-secondary);
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }}
            
            .metric-value {{
                font-size: 2.25rem;
                font-weight: 800;
                color: var(--text-primary);
                line-height: 1;
            }}
            
            .metric-subtitle {{
                font-size: 0.875rem;
                color: var(--text-secondary);
                margin-top: 0.5rem;
            }}
            
            .section {{
                background: var(--bg-primary);
                border-radius: 20px;
                padding: 2rem;
                margin-bottom: 2rem;
                box-shadow: var(--shadow-xl);
                border: 1px solid var(--border-color);
            }}
            
            .section-header {{
                display: flex;
                align-items: center;
                gap: 1rem;
                margin-bottom: 1.5rem;
                padding-bottom: 1rem;
                border-bottom: 2px solid var(--border-color);
            }}
            
            .section-icon {{
                width: 40px;
                height: 40px;
                border-radius: 10px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 1.25rem;
            }}
            
            .section h2 {{
                font-size: 1.75rem;
                font-weight: 700;
                color: var(--text-primary);
                margin: 0;
            }}
            
            .section-description {{
                color: var(--text-secondary);
                font-size: 1rem;
                margin-top: 0.5rem;
                line-height: 1.6;
            }}
            
            .chart-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
                gap: 1.5rem;
                margin-top: 1.5rem;
            }}
            
            .chart-card {{
                background: var(--bg-secondary);
                border-radius: 16px;
                padding: 1.5rem;
                box-shadow: var(--shadow-md);
                border: 1px solid var(--border-color);
                transition: all 0.3s ease;
                min-height: 350px;
            }}
            
            .chart-card:hover {{
                box-shadow: var(--shadow-lg);
                transform: translateY(-2px);
            }}
            
            .empty-state {{
                background: var(--bg-secondary);
                border-radius: 16px;
                padding: 4rem 2rem;
                text-align: center;
                color: var(--text-secondary);
                border: 2px dashed var(--border-color);
                font-size: 1.125rem;
                font-weight: 500;
            }}
            
            footer {{
                text-align: center;
                padding: 2rem;
                color: rgba(255, 255, 255, 0.9);
                font-size: 0.875rem;
                font-weight: 500;
            }}
            
            @media (max-width: 768px) {{
                body {{
                    padding: 1rem;
                }}
                
                .header h1 {{
                    font-size: 1.75rem;
                }}
                
                .metrics-grid {{
                    grid-template-columns: 1fr;
                }}
                
                .chart-grid {{
                    grid-template-columns: 1fr;
                }}
                
                .metric-value {{
                    font-size: 1.75rem;
                }}
            }}
            
            @keyframes fadeInUp {{
                from {{
                    opacity: 0;
                    transform: translateY(20px);
                }}
                to {{
                    opacity: 1;
                    transform: translateY(0);
                }}
            }}
            
            .header, .metric-card, .section {{
                animation: fadeInUp 0.6s ease-out;
            }}
            
            .metric-card:nth-child(1) {{ animation-delay: 0.1s; }}
            .metric-card:nth-child(2) {{ animation-delay: 0.2s; }}
            .metric-card:nth-child(3) {{ animation-delay: 0.3s; }}
            .metric-card:nth-child(4) {{ animation-delay: 0.4s; }}
            .metric-card:nth-child(5) {{ animation-delay: 0.5s; }}
            .metric-card:nth-child(6) {{ animation-delay: 0.6s; }}
        </style>
        <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
    </head>
    <body>
        <div class="container">
            <header class="header">
                <h1>📊 Quality Check Dashboard</h1>
                <p>Giám sát chất lượng dữ liệu BDS theo thời gian thực</p>
            </header>

            <div class="metrics-grid">
                <div class="metric-card primary">
                    <div class="metric-header">
                        <div class="metric-icon">📈</div>
                        <div class="metric-label">Tổng số tin</div>
                    </div>
                    <div class="metric-value">{metrics['total_records']:,}</div>
                    <div class="metric-subtitle">Trung bình {metrics['avg_daily_records']:,} tin/ngày</div>
                </div>

                <div class="metric-card danger">
                    <div class="metric-header">
                        <div class="metric-icon">🚨</div>
                        <div class="metric-label">Dữ liệu nghi ngờ</div>
                    </div>
                    <div class="metric-value">{metrics['total_suspicious']:,}</div>
                    <div class="metric-subtitle">Cần kiểm tra</div>
                </div>

                <div class="metric-card primary">
                    <div class="metric-header">
                        <div class="metric-icon">📅</div>
                        <div class="metric-label">Số ngày crawl</div>
                    </div>
                    <div class="metric-value">{metrics['num_days']}</div>
                    <div class="metric-subtitle">Dữ liệu theo dõi</div>
                </div>
            </div>

            <section class="section">
                <div class="section-header">
                    <div class="section-icon">❌</div>
                    <div>
                        <h2>Missing Data</h2>
                        <p class="section-description">Theo dõi các trường dữ liệu bị thiếu qua từng ngày crawl</p>
                    </div>
                </div>
                <div class="chart-grid">
                    {missing_chart_grid}
                </div>
            </section>

            <section class="section">
                <div class="section-header">
                    <div class="section-icon">🔍</div>
                    <div>
                        <h2>Suspicious Data</h2>
                        <p class="section-description">Theo dõi các trường dữ liệu nghi ngờ hoặc không hợp lệ</p>
                    </div>
                </div>
                <div class="chart-grid">
                    {suspicious_chart_grid}
                </div>
            </section>

            <footer>
                <p>🏠 BDS Quality Dashboard • Cập nhật lần cuối: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}</p>
            </footer>
        </div>
    </body>
    </html>
    """

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ Dashboard generated successfully: {output_path}")


if __name__ == "__main__":
    df = fetch_qc_data()
    build_dashboard(df)