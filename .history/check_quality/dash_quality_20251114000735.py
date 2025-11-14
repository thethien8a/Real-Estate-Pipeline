import pandas as pd
import plotly.express as px
from supabase import create_client
import os

# -------------------------
# 1. KẾT NỐI SUPABASE
# -------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------------------------
# 2. LẤY DỮ LIỆU TỪ VIEW QC
# -------------------------
response = (
    client
    .schema("bronze")
    .table("qc_daily_overview")
    .select("*")
    .execute()
)

df = pd.DataFrame(response.data)

# Chuyển kiểu ngày
df["crawl_date"] = pd.to_datetime(df["crawl_date"])
df = df.sort_values("crawl_date")

# -------------------------
# 3. XÁC ĐỊNH CÁC CỘT QC
# -------------------------
missing_cols = [c for c in df.columns if c.startswith("missing_")]
suspicious_cols = [
    c for c in df.columns 
    if c.startswith("suspicious_") or c.startswith("invalid_")
]

# -------------------------
# 4. VẼ CHART (Plotly)
# -------------------------

# ⚠️ Missing Chart
fig_missing = px.line(
    df,
    x="crawl_date",
    y=missing_cols,
    title="Missing Fields Overview",
    markers=True
)
fig_missing.update_xaxes(rangeslider_visible=True)

# ⚠️ Suspicious Chart
fig_suspicious = px.line(
    df,
    x="crawl_date",
    y=suspicious_cols,
    title="Suspicious / Invalid Fields Overview",
    markers=True
)
fig_suspicious.update_xaxes(rangeslider_visible=True)

# -------------------------
# 5. TẠO HTML DASHBOARD
# -------------------------
html_content = f"""
<html>
<head>
    <title>QC Dashboard</title>
</head>
<body>
    <h1 style="text-align:center;">Daily QC Data Quality Dashboard</h1>

    <h2>Missing Fields</h2>
    {fig_missing.to_html(full_html=False, include_plotlyjs='cdn')}

    <h2>Suspicious / Invalid Fields</h2>
    {fig_suspicious.to_html(full_html=False, include_plotlyjs='cdn')}
</body>
</html>
"""

# -------------------------
# 6. XUẤT HTML RA /public
# -------------------------
output_path = "public/qc_dashboard.html"
os.makedirs("public", exist_ok=True)

with open(output_path, "w", encoding="utf-8") as f:
    f.write(html_content)

print("Dashboard generated → public/qc_dashboard.html")
