import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="台股法人操盤系統", page_icon="📈", layout="wide")

SHEET_ID = "1GjcN6DSFWwJG14bPyMW8aNUkE70Auz6BQFPJ9EGzR38"
STOCK_SHEET = "stock_Sheet"
GAS_URL = "https://script.google.com/macros/s/AKfycbwbNmQfyI0zvyXlSuGOemID25o0EDtoAyl7hoZ3zfYtgpSES0w69e8GrIRzfSzUMVuy/exec"

# 欄位定義
COL_NAMES = [
    "日期", "股票代號", "股票名稱", "關鍵分點", "買超張數",
    "現價", "MA5均價", "建議買價", "預估目標價", "預估獲利%",
    "發動天數", "推薦等級", "操盤建議", "價差%", "法人強度"
]


@st.cache_resource
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scopes
    )
    return gspread.authorize(creds)


def get_worksheet(name):
    client = get_gspread_client()
    return client.open_by_key(SHEET_ID).worksheet(name)


@st.cache_data(ttl=300)
def load_stock_data():
    try:
        ws = get_worksheet(STOCK_SHEET)
        data = ws.get_all_values()
        if len(data) <= 1:
            return pd.DataFrame()
        headers = data[0]
        df = pd.DataFrame(data[1:], columns=headers)
        # 只取需要的欄位
        existing = [c for c in COL_NAMES if c in df.columns]
        if existing:
            df = df[existing]
        return df
    except Exception as e:
        st.error("讀取失敗：" + str(e))
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_existing_dates():
    try:
        ws = get_worksheet(STOCK_SHEET)
        vals = ws.col_values(1)
        return set(v for v in vals[1:] if v)
    except:
        return set()


def trigger_gas(action="backfill"):
    try:
        resp = requests.get(
            GAS_URL + "?action=" + action,
            timeout=60,
            allow_redirects=True
        )
        return True, resp.text
    except requests.exceptions.Timeout:
        return False, "逾時（Apps Script 仍在背景執行）"
    except Exception as e:
        return False, str(e)


def to_numeric_safe(series):
    return pd.to_numeric(series, errors='coerce').fillna(0)


# ====================== 介面 ======================

st.markdown("""
<style>
.hdr {
    background: linear-gradient(135deg, #0f0f23, #1a1a3e);
    padding: 20px 30px; border-radius: 12px;
    margin-bottom: 20px; border: 1px solid #00d4ff33;
}
.hdr h1 { color: #00d4ff; margin: 0; font-size: 1.6rem; }
.hdr p { color: #8899aa; margin: 4px 0 0; font-size: .85rem; }
.recommend-box {
    background: #1a1a2e; border-radius: 10px;
    padding: 15px; margin: 8px 0;
    border-left: 4px solid #00d4ff;
}
</style>
<div class="hdr">
    <h1>📈 台股法人操盤系統</h1>
    <p>三大法人籌碼追蹤 · 智能推薦等級 · 每日自動更新</p>
</div>
""", unsafe_allow_html=True)

# ── 側邊欄 ──
with st.sidebar:
    st.title("操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢推薦", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")

    try:
        edates = get_existing_dates()
        valid = sorted([d for d in edates if d and len(d) > 5], reverse=True)
        if valid:
            st.success("最新：" + valid[0])
            st.info("共 " + str(len(valid)) + " 個交易日")
        else:
            st.warning("尚無資料")
    except Exception as e:
        st.error("連線失敗：" + str(e))

    st.markdown("---")

    if st.button("自動補抓缺失資料", type="primary", use_container_width=True):
        with st.spinner("通知 Apps Script 補抓中..."):
            ok, msg = trigger_gas("backfill")
            st.info("指令已送出！Apps Script 正在背景執行\n約 2-3 分鐘後按「重新整理」")

    if st.button("重新整理資料", use_container_width=True):
        load_stock_data.clear()
        get_existing_dates.clear()
        st.success("已重新載入！")
        time.sleep(1)
        st.rerun()

    st.markdown("---")
    st.markdown("""
**📖 推薦等級說明**
- ⭐⭐⭐ 強烈推薦：發動1-2天+大量買超
- ⭐⭐ 值得關注：發動初期或穩定買超
- ⭐ 謹慎追蹤：已發動多天，追高需謹慎

**💡 操盤心法**
法人第1天進場是最佳時機，第2天確認後可加碼，第3天以後要等回測再進場。
    """)


# ── 主畫面 ──
st.header(mode)

df_raw = load_stock_data()

if df_raw.empty:
    st.warning("尚無資料，請點左側「自動補抓缺失資料」。")
    st.stop()

latest_date = df_raw["日期"].max() if "日期" in df_raw.columns else ""
today_df = df_raw[df_raw["日期"] == latest_date].copy() if latest_date else pd.DataFrame()

# 數值欄位轉換
for col in ["買超張數", "現價", "MA5均價", "建議買價", "預估目標價", "發動天數"]:
    if col in today_df.columns:
        today_df[col] = to_numeric_safe(today_df[col])

for col in ["預估獲利%", "價差%"]:
    if col in today_df.columns:
        today_df[col] = pd.to_numeric(today_df[col], errors='coerce')
        today_df[col] = today_df[col].apply(
            lambda x: "{:.1f}%".format(x * 100) if pd.notna(x) and x != 0 else "-"
        )

# ── 今日強勢推薦 ──
if mode == "今日強勢推薦":

    # 統計卡片
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("最新日期", latest_date)
    c2.metric("今日標的數", len(today_df))

    if "推薦等級" in today_df.columns:
        top = today_df[today_df["推薦等級"].str.contains("⭐⭐⭐", na=False)]
        c3.metric("強烈推薦", len(top))
        mid = today_df[today_df["推薦等級"].str.contains("⭐⭐ 值", na=False)]
        c4.metric("值得關注", len(mid))

    st.markdown("---")

    # 強烈推薦區塊
    if "推薦等級" in today_df.columns:
        top3 = today_df[today_df["推薦等級"].str.contains("⭐⭐⭐", na=False)].sort_values(
            by="買超張數", ascending=False
        )

        if not top3.empty:
            st.subheader("🔥 今日強烈推薦（發動初期 + 大量買超）")
            display_cols = [c for c in [
                "股票代號", "股票名稱", "買超張數", "現價",
                "MA5均價", "建議買價", "預估目標價", "預估獲利%",
                "發動天數", "推薦等級", "操盤建議", "法人強度"
            ] if c in top3.columns]
            st.dataframe(top3[display_cols], use_container_width=True, hide_index=True)

    st.markdown("---")

    # 值得關注
    if "推薦等級" in today_df.columns:
        mid2 = today_df[today_df["推薦等級"].str.contains("⭐⭐", na=False)].sort_values(
            by="買超張數", ascending=False
        )
        if not mid2.empty:
            st.subheader("👀 值得關注")
            display_cols = [c for c in [
                "股票代號", "股票名稱", "買超張數", "現價",
                "建議買價", "預估目標價", "預估獲利%",
                "發動天數", "推薦等級", "操盤建議"
            ] if c in mid2.columns]
            st.dataframe(mid2[display_cols], use_container_width=True, hide_index=True)

    # 全部今日資料
    with st.expander("📋 今日全部標的"):
        st.dataframe(today_df, use_container_width=True, hide_index=True)

    with st.expander("📂 歷史資料"):
        st.dataframe(df_raw, use_container_width=True, hide_index=True)


# ── 籌碼週期分析 ──
elif mode == "籌碼週期分析":

    for col in ["買超張數", "發動天數"]:
        if col in today_df.columns:
            today_df[col] = to_numeric_safe(today_df[col])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("基準日", latest_date)
    if "發動天數" in today_df.columns:
        c2.metric("法人鎖碼 ≥3天", len(today_df[today_df["發動天數"] >= 3]))
        c3.metric("發動第1天", len(today_df[today_df["發動天數"] == 1]))
        c4.metric("發動第2天", len(today_df[today_df["發動天數"] == 2]))

    st.markdown("---")

    tab1, tab2, tab3, tab4 = st.tabs(["⭐⭐⭐ 強烈推薦", "🚀 第1天發動", "✅ 第2天確認", "🔒 法人鎖碼"])

    display_cols = [c for c in [
        "股票代號", "股票名稱", "買超張數", "現價",
        "MA5均價", "建議買價", "預估目標價", "預估獲利%",
        "發動天數", "推薦等級", "操盤建議", "法人強度"
    ] if c in today_df.columns]

    with tab1:
        if "推薦等級" in today_df.columns:
            d = today_df[today_df["推薦等級"].str.contains("⭐⭐⭐", na=False)].sort_values(
                by="買超張數", ascending=False
            )
            if d.empty:
                st.info("今日無強烈推薦標的")
            else:
                st.caption("買超量大 + 發動初期，是最佳進場時機！")
                st.dataframe(d[display_cols], use_container_width=True, hide_index=True)

    with tab2:
        if "發動天數" in today_df.columns:
            d = today_df[today_df["發動天數"] == 1].sort_values(by="買超張數", ascending=False)
            if d.empty:
                st.info("今日無第1天發動標的")
            else:
                st.caption("法人今天剛開始進場，股價還沒反應，是最佳觀察時機")
                st.dataframe(d[display_cols], use_container_width=True, hide_index=True)

    with tab3:
        if "發動天數" in today_df.columns:
            d = today_df[today_df["發動天數"] == 2].sort_values(by="買超張數", ascending=False)
            if d.empty:
                st.info("今日無第2天確認標的")
            else:
                st.caption("連續第2天買超，趨勢確認！可以考慮進場")
                st.dataframe(d[display_cols], use_container_width=True, hide_index=True)

    with tab4:
        if "發動天數" in today_df.columns:
            d = today_df[today_df["發動天數"] >= 3].sort_values(
                by=["發動天數", "買超張數"], ascending=[False, False]
            )
            if d.empty:
                st.info("今日無法人鎖碼標的")
            else:
                st.caption("連續買超 ≥3 天，籌碼鎖定，等回測 MA5 再進場比較安全")
                st.dataframe(d[display_cols], use_container_width=True, hide_index=True)


# ── 資料庫管理 ──
elif mode == "資料庫管理":
    st.subheader("資料庫管理")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("[📊 開啟 Google Sheet](https://docs.google.com/spreadsheets/d/" + SHEET_ID + ")")
        if st.button("測試連線", use_container_width=True):
            try:
                ws = get_worksheet(STOCK_SHEET)
                st.success("連線成功：" + ws.title)
            except Exception as e:
                st.error("連線失敗：" + str(e))

    with col2:
        target_input = st.date_input(
            "補抓指定日期",
            value=datetime.now().date() - timedelta(days=1)
        )
        if st.button("補抓此日期", use_container_width=True):
            date_str = target_input.strftime('%Y%m%d')
            with st.spinner("通知 Apps Script..."):
                ok, msg = trigger_gas("single&date=" + date_str)
                st.info("指令已送出，等 1 分鐘後按「重新整理資料」")

    st.markdown("---")
    st.subheader("欄位說明")
    st.markdown("""
| 欄位 | 說明 |
|------|------|
| 現價 | GOOGLEFINANCE 即時報價 |
| MA5均價 | 近10日平均收盤價（支撐參考）|
| 建議買價 | MA5 和當日最低價取較低者 |
| 預估目標價 | 現價 × 1.06（保守6%獲利目標）|
| 預估獲利% | 目標價和現價的差距百分比 |
| 發動天數 | 連續出現幾天 |
| 推薦等級 | ⭐⭐⭐強烈 / ⭐⭐關注 / ⭐謹慎 |
| 操盤建議 | 一句話告訴你現在該怎麼做 |
| 法人強度 | 買超量的強弱分級 |
    """)

    st.markdown("---")
    st.subheader("最新 50 筆資料")
    if not df_raw.empty:
        st.dataframe(df_raw.tail(50), use_container_width=True, hide_index=True)
    else:
        st.info("尚無資料")
