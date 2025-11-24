import os
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
from dotenv import load_dotenv
from supabase import create_client

# Load env vars up-front so CLI usage is seamless
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY in environment")

client = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_error_view() -> pd.DataFrame:
    """Load aggregated error stats from Supabase view."""
    response = (
        client.schema("silver")
        .table("view_error_table")
        .select("*")
        .order("report_date")
        .execute()
    )
    df = pd.DataFrame(response.data)
    if df.empty:
        return df

    df["report_date"] = pd.to_datetime(df["report_date"])
    if "latest_error_time" in df.columns:
        df["latest_error_time"] = pd.to_datetime(
            df["latest_error_time"], errors="coerce"
        )
    return df.sort_values("report_date")


def _calculate_metrics(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "total_days": 0,
            "total_errors": 0,
            "avg_daily_errors": 0,
            "pending": 0,
            "resolved": 0,
            "failed": 0,
            "resolution_rate": 0.0,
            "latest_report": "N/A",
            "latest_error_time": "N/A",
        }

    latest_row = df.iloc[-1]
    totals = {
        "pending": int(latest_row.get("total_pending", 0) or 0),
        "resolved": int(latest_row.get("total_resolved", 0) or 0),
        "failed": int(latest_row.get("total_failed", 0) or 0),
    }
    total_errors_today = int(latest_row.get("total_errors", 0) or 0)
    resolution_rate = (
        (totals["resolved"] / total_errors_today) * 100 if total_errors_today else 0.0
    )

    latest_error_time = latest_row.get("latest_error_time")
    if pd.notna(latest_error_time):
        latest_error_time = pd.to_datetime(latest_error_time).strftime(
            "%d/%m/%Y %H:%M"
        )
    else:
        latest_error_time = "N/A"

    return {
        "total_days": int(df["report_date"].nunique()),
        "total_errors": int(df["total_errors"].sum()),
        "avg_daily_errors": round(float(df["total_errors"].mean()), 2),
        "pending": totals["pending"],
        "resolved": totals["resolved"],
        "failed": totals["failed"],
        "resolution_rate": round(resolution_rate, 1),
        "latest_report": latest_row["report_date"].strftime("%d/%m/%Y"),
        "latest_error_time": latest_error_time,
        "latest_total_errors": total_errors_today,
    }


def _render_errors_trend_chart(df: pd.DataFrame) -> str:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["report_date"],
            y=df["total_errors"],
            mode="lines+markers",
            name="Total Errors",
            line=dict(color="#3b82f6", width=3),
            marker=dict(size=8, color="#3b82f6", line=dict(width=2, color="white")),
            fill="tozeroy",
            fillcolor="rgba(59, 130, 246, 0.08)",
        )
    )
    fig.update_layout(
        title="Total Errors Per Day",
        xaxis_title="Report Date",
        yaxis_title="Errors",
        hovermode="x unified",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=350,
        margin=dict(l=40, r=20, t=60, b=40),
        font=dict(family="Inter, system-ui, sans-serif", size=12, color="#334155"),
        xaxis=dict(showgrid=True, gridcolor="rgba(148, 163, 184, 0.15)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(148, 163, 184, 0.15)"),
    )
    return fig.to_html(full_html=False, include_plotlyjs=False, config={"displayModeBar": False})


def _render_recent_table(df: pd.DataFrame, limit: int = 10) -> str:
    if df.empty:
        return "<div class=\"empty-state\">Chưa có dữ liệu lỗi nào được ghi nhận.</div>"

    table_df = df.sort_values("report_date", ascending=False).head(limit).copy()
    table_df["report_date"] = table_df["report_date"].dt.strftime("%d/%m/%Y")
    if "latest_error_time" in table_df.columns:
        table_df["latest_error_time"] = table_df["latest_error_time"].apply(
            lambda ts: pd.to_datetime(ts).strftime("%d/%m/%Y %H:%M")
            if pd.notna(ts)
            else "N/A"
        )
    formatted = table_df.rename(
        columns={
            "report_date": "Report Date",
            "total_errors": "Total Errors",
            "total_pending": "Pending",
            "total_resolved": "Resolved",
            "total_failed": "Failed",
            "latest_error_time": "Latest Error Time",
        }
    )
    return formatted.to_html(
        index=False,
        classes="data-table",
        border=0,
        justify="center",
        escape=False,
    )


def _build_empty_dashboard(output_path: str):
    html = """
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="utf-8" />
        <title>Silver Error Dashboard - BDS</title>
        <style>
            body {
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                background: #0f172a;
                color: white;
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
            }
            .empty-state {
                padding: 2rem 3rem;
                background: rgba(255, 255, 255, 0.08);
                border-radius: 16px;
                max-width: 480px;
                text-align: center;
                line-height: 1.6;
            }
        </style>
    </head>
    <body>
        <div class="empty-state">
            <h1>Không có dữ liệu</h1>
            <p>Bảng silver.view_error_table chưa có dữ liệu để dựng dashboard.</p>
        </div>
    </body>
    </html>
    """
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)


def build_dashboard(df: pd.DataFrame, output_path: str = "dashboard_silver.html"):
    if df.empty:
        _build_empty_dashboard(output_path)
        print(f"⚠️ No data found. Generated empty state at {output_path}")
        return

    metrics = _calculate_metrics(df)
    errors_chart = _render_errors_trend_chart(df)
    table_html = _render_recent_table(df)
    rendered_at = datetime.now().strftime("%d/%m/%Y %H:%M")

    html = f"""
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Silver Error Dashboard - BDS</title>
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
        <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
        <style>
            :root {{
                --primary: #4c1d95;
                --bg-primary: #ffffff;
                --bg-secondary: #f8fafc;
                --text-primary: #0f172a;
                --text-secondary: #475569;
                --border-color: #e2e8f0;
            }}
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            body {{
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                background: linear-gradient(135deg, #0f172a 0%, #312e81 100%);
                min-height: 100vh;
                padding: 2rem;
                color: var(--text-primary);
            }}
            .container {{
                max-width: 1400px;
                margin: 0 auto;
            }}
            .card {{
                background: var(--bg-primary);
                border-radius: 20px;
                padding: 2rem;
                margin-bottom: 2rem;
                box-shadow: 0 20px 25px -5px rgba(15, 23, 42, 0.15);
                border: 1px solid var(--border-color);
            }}
            .header h1 {{
                font-size: 2.25rem;
                font-weight: 800;
                color: #312e81;
                margin-bottom: 0.5rem;
            }}
            .metrics-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                gap: 1.5rem;
            }}
            .metric {{
                background: var(--bg-secondary);
                border-radius: 16px;
                padding: 1.5rem;
                border: 1px solid var(--border-color);
            }}
            .metric-label {{
                font-size: 0.9rem;
                font-weight: 600;
                color: var(--text-secondary);
                text-transform: uppercase;
                letter-spacing: 0.08em;
            }}
            .metric-value {{
                font-size: 2rem;
                font-weight: 800;
                color: var(--text-primary);
                margin-top: 0.35rem;
            }}
            .section-title {{
                font-size: 1.5rem;
                font-weight: 700;
                margin-bottom: 1rem;
                color: var(--text-primary);
            }}
            .chart {{
                background: var(--bg-secondary);
                border-radius: 16px;
                padding: 1rem;
                border: 1px solid var(--border-color);
            }}
            .data-table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 0.95rem;
            }}
            .data-table th, .data-table td {{
                padding: 0.85rem 1rem;
                text-align: left;
                border-bottom: 1px solid var(--border-color);
            }}
            .data-table th {{
                text-transform: uppercase;
                font-size: 0.8rem;
                letter-spacing: 0.08em;
                color: var(--text-secondary);
            }}
            footer {{
                text-align: center;
                color: rgba(255, 255, 255, 0.8);
                margin-top: 2rem;
            }}
            @media (max-width: 768px) {{
                body {{ padding: 1rem; }}
                .metrics-grid {{ grid-template-columns: 1fr; }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <section class="card header">
                <h1>Silver Error Dashboard</h1>
                <p>Theo dõi backlog lỗi sau bước làm sạch</p>
            </section>

            <section class="card">
                <div class="metrics-grid">
                    <div class="metric">
                        <div class="metric-label">Tổng lỗi ghi nhận</div>
                        <div class="metric-value">{metrics['total_errors']:,}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Lỗi trung bình/ngày</div>
                        <div class="metric-value">{metrics['avg_daily_errors']:,}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Pending hiện tại</div>
                        <div class="metric-value">{metrics['pending']:,}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Resolved hiện tại</div>
                        <div class="metric-value">{metrics['resolved']:,}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Failed hiện tại</div>
                        <div class="metric-value">{metrics['failed']:,}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Tỉ lệ xử lý</div>
                        <div class="metric-value">{metrics['resolution_rate']}%</div>
                    </div>
                </div>
            </section>

            <section class="card">
                <h2 class="section-title">Diễn biến tổng lỗi</h2>
                <div class="chart">
                    {errors_chart}
                </div>
            </section>

            <section class="card">
                <h2 class="section-title">Chi tiết gần nhất</h2>
                {table_html}
            </section>

            <footer>
                Cập nhật lần cuối: {rendered_at} • Ngày báo cáo mới nhất: {metrics['latest_report']} • Lỗi mới nhất lúc: {metrics['latest_error_time']}
            </footer>
        </div>
    </body>
    </html>
    """

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ Dashboard generated successfully: {output_path}")


if __name__ == "__main__":
    df = fetch_error_view()
    build_dashboard(df)

