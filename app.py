import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')
import gspread
from google.oauth2.service_account import Credentials

# 1. 網頁基本設定 (預設寬螢幕模式)
st.set_page_config(page_title="台股法人操盤系統", page_icon="📈", layout="wide")

SHEET_ID = "1GjcN6DSFWwJG14bPyMW8aNUkE70Auz6BQFPJ9EGzR38"
STOCK_SHEET = "stock_Sheet"
GAS_URL = "https://script.google.com/macros/s/AKfycbwbNmQfyI0zvyXlSuGOemID25o0EDtoAyl7hoZ3zfYtgpSES0w69e8GrIRzfSzUMVuy/exec"

# 2. Google Sheets 連線快取機制
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

# 3. 資料讀取與精準欄位對齊
@st.cache_data(ttl=300)
def load_stock_data():
    try:
        ws = get_worksheet(STOCK_SHEET)
        data = ws.get_all_values()
        if len(data) <= 1:
            return pd.DataFrame()
        
        headers = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=headers)
        
        # 🎯 精準對齊全新的 13 個 GAS 黃金欄位名稱，徹底排除錯字
        needed = [
            "日期", "股票代號", "股票名稱", "法人買超(張)", "目前現價", 
            "5日均價(MA5)", "建議買價", "預估目標價", "價差%", 
            "連續發動天數", "推薦等級", "法人強度", "操盤建議"
        ]
        
        existing = [c for c in needed if c in df.columns]
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
    """呼叫 Apps Script Web App 觸發下載"""
    try:
        resp = requests.get(
            GAS_URL + "?action=" + action,
            timeout=60,
            allow_redirects=True
        )
        return resp.status_code == 200, resp.text
    except requests.exceptions.Timeout:
        return False, "請求逾時（Apps Script 仍在背景執行中）"
    except Exception as e:
        return False, str(e)


# ====================== 🚀 介面優化：精簡化頂部版面 ======================

st.markdown("""
<style>
.hdr {
    background: #0f0f23;
    padding: 8px 15px; 
    border-radius: 6px;
    margin-bottom: 10px; 
    border-left: 5px solid #00d4ff;
}
.hdr h3 { color: #00d4ff; margin: 0; display: inline-block; font-size: 1.2rem; }
.hdr span { color: #8899aa; margin-left: 15px; font-size: .85rem; }
/* 移除 Streamlit 預設的上邊距，讓表格升到最高 */
.block-container { padding-top: 1.5rem !important; padding-bottom: 1rem !important; }
</style>
<div class="hdr">
    <h3>台股法人操盤系統</h3>
    <span>三大法人籌碼追蹤 · 自動化監控儀表板</span>
</div>
""", unsafe_allow_html=True)


# ====================== 🛠️ 側邊欄：功能與數據統計 ======================
with st.sidebar:
    st.subheader("📊 系統狀態與統計")
    
    # 讀取資料供狀態欄與主畫面使用
    df = load_stock_data()
    
    try:
        edates = get_existing_dates()
        valid = sorted([d for d in edates if d and len(d) > 5], reverse=True)
        if valid and not df.empty:
            latest_date = df["日期"].max()
            today_df = df[df["日期"] == latest_date].copy()
            
            # 將原本佔據大版面的數據卡片，精簡縮小到側邊欄
            st.metric("最新交易日期", latest_date)
            st.metric("今日篩選標的數", f"{len(today_df)} 檔")
            st.metric("資料庫總記錄筆數", f"{len(df)} 筆")
        else:
            st.warning("尚無資料")
            latest_date = None
            today_df = pd.DataFrame()
    except Exception as e:
        st.error("連線失敗：" + str(e))
        latest_date = None
        today_df = pd.DataFrame()

    st.markdown("---")
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")

    st.subheader("🔄 資料更新")
    if st.button("自動補抓缺失資料", type="primary", use_container_width=True):
        with st.spinner("正在通知 Apps Script 補抓資料..."):
            ok, msg = trigger_gas("backfill")
            if ok:
                st.success("補抓指令已送出！")
            else:
                st.warning("指令已送出（逾時不代表失敗）")

    if st.button("重新整理資料", use_container_width=True):
        load_stock_data.clear()
        get_existing_dates.clear()
        st.success("快取已清除！")
        time.sleep(0.5)
        st.rerun()

    st.caption("每天下午 5 點自動更新，或手動點擊上方按鈕")


# ====================== ── 主畫面：聚焦詳細資料 ── ======================

if df.empty:
    st.warning("尚無資料，請點左側「自動補抓缺失資料」按鈕。")
else:
    # 預先處理數值轉換，避免排序噴錯
    df["買超_n"] = pd.to_numeric(df["法人買超(張)"], errors='coerce').fillna(0)
    df["天數_n"] = pd.to_numeric(df["連續發動天數"], errors='coerce').fillna(0)
    
    if latest_date:
        today_df = df[df["日期"] == latest_date].copy()

    # ── 功能一：今日強勢戰報 ──
    if mode == "今日強勢戰報":
        st.subheader(f"📅 {latest_date} 詳細標的清單 (買超 >= 500張)", anchor=False)
        
        # 依連續天數、買超張數降序排序
        if not today_df.empty:
            today_df = today_df.sort_values(
                by=["天數_n", "買超_n"], ascending=[False, False]
            ).drop(columns=["買超_n", "天數_n"])
        
        # 🎯 核心：直接展現超大滿版詳細資料表格
        st.dataframe(today_df, use_container_width=True, hide_index=True, height=550)

        with st.expander("🔍 查看歷史完整資料庫明細"):
            st.dataframe(df.sort_values(by=["日期", "天數_n"], ascending=[False, False]).drop(columns=["買超_n", "天數_n"]), use_container_width=True, hide_index=True)

    # ── 功能二：籌碼週期分析 ──
    elif mode == "籌碼週期分析":
        st.subheader(f"🎯 籌碼多週期篩選明細 ({latest_date})", anchor=False)
        
        # 小指標區
        c1, c2, c3 = st.columns(3)
        c1.markdown(f"**🔥 法人鎖碼 (>=3天)：** `{len(today_df[today_df['天數_n'] >= 3])}` 檔")
        c2.markdown(f"**💪 大主力買超 (>=1000張)：** `{len(today_df[today_df['買超_n'] >= 1000])}` 檔")
        c3.markdown(f"**🚀 首次發動新標的：** `{len(today_df[today_df['天數_n'] == 1])}` 檔")
        
        tab1, tab2, tab3 = st.tabs(["🔒 法人鎖碼明細", "⚡ 雙強初現明細", "🚀 首次發動明細"])

        with tab1:
            d1 = today_df[today_df["天數_n"] >= 3].sort_values(by=["天數_n", "買超_n"], ascending=[False, False]).drop(columns=["買超_n", "天數_n"])
            if d1.empty: st.info("今日無法人持續鎖碼標的")
            else: st.dataframe(d1, use_container_width=True, hide_index=True, height=450)

        with tab2:
            d2 = today_df[(today_df["買超_n"] >= 1000) & (today_df["天數_n"] <= 2)].sort_values(by="買超_n", ascending=False).drop(columns=["買超_n", "天數_n"])
            if d2.empty: st.info("今日無雙強初現標的")
            else: st.dataframe(d2, use_container_width=True, hide_index=True, height=450)

        with tab3:
            d3 = today_df[today_df["天數_n"] == 1].sort_values(by="買超_n", ascending=False).drop(columns=["買超_n", "天數_n"])
            if d3.empty: st.info("今日無新發動標的")
            else: st.dataframe(d3, use_container_width=True, hide_index=True, height=450)

        st.markdown("---")
        st.subheader("📈 全市場連續發動天數強勢榜 TOP 30", anchor=False)
        rank_df = today_df.sort_values(by=["天數_n", "買超_n"], ascending=[False, False]).drop(columns=["買超_n", "天數_n"]).head(30)
        st.dataframe(rank_df, use_container_width=True, hide_index=True)

    # ── 功能三：資料庫管理 ──
    elif mode == "資料庫管理":
        st.subheader("⚙️ 後端資料庫串接管理", anchor=False)
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"🔗 [點此直接開啟 Google 試算表原檔](https://docs.google.com/spreadsheets/d/{SHEET_ID})")
            if st.button("測試 Google Sheets API 連線狀態", use_container_width=True):
                try:
                    ws = get_worksheet(STOCK_SHEET)
                    st.success("連線成功！當前指向工作表： " + ws.title)
                except Exception as e:
                    st.error("連線失敗：" + str(e))

        with col2:
            target_input = st.date_input("手動指定日期強制補抓", value=datetime.now().date() - timedelta(days=1))
            if st.button("執行指定日期補抓", use_container_width=True):
                date_str = target_input.strftime('%Y%m%d')
                with st.spinner(f"通知 Apps Script 補抓 {date_str} 中..."):
                    ok, msg = trigger_gas(f"single&date={date_str}")
                    if ok: st.success("指令已成功送出！請靜候 1 分鐘後重新整理。")
                    else: st.error("補抓失敗：" + msg)

        st.markdown("---")
        st.subheader("📋 雲端資料庫最新 50 筆原始紀錄流水帳", anchor=False)
        st.dataframe(df.tail(50), use_container_width=True, hide_index=True)
