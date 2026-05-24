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

COL_NAMES = ["日期", "股票代號", "股票名稱", "關鍵分點", "買超張數",
             "5日均價", "目前現價", "價差%", "出現天數", "超盤建議"]


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
        rows = data[1:]
        df = pd.DataFrame(rows, columns=headers)
        needed = ["日期", "股票代號", "股票名稱", "關鍵分點", "買超張數",
                  "5日均僳", "目前現僳", "僳差%", "出現天數", "超盤建議"]
        existing = [c for c in needed if c in df.columns]
        if existing:
            df = df[existing]
        return df
    except Exception as e:
        st.error("讀取失敗：" + str(e))
        return pd.DataFrame()
        return pd.DataFrame(data[1:], columns=data[0])
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


# ====================== 介面 ======================

st.markdown("""
<style>
.hdr {
    background: linear-gradient(135deg, #0f0f23, #1a1a3e);
    padding: 20px 30px; border-radius: 12px;
    margin-bottom: 20px; border: 1px solid #00d4ff33;
}
.hdr h1 { color: #00d4ff; margin: 0; }
.hdr p { color: #8899aa; margin: 4px 0 0; font-size: .9rem; }
</style>
<div class="hdr">
    <h1>台股法人操盤系統</h1>
    <p>三大法人籌碼追蹤 · Google Apps Script 自動抓取 · Google Sheets 持久化儲存</p>
</div>
""", unsafe_allow_html=True)


with st.sidebar:
    st.title("操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析", "資料庫管理"], index=0)
    st.markdown("---")

    try:
        edates = get_existing_dates()
        valid = sorted([d for d in edates if d and len(d) > 5], reverse=True)
        if valid:
            st.success("最新：" + valid[0])
            st.info("共 " + str(len(valid)) + " 筆日期記錄")
        else:
            st.warning("尚無資料")
    except Exception as e:
        st.error("連線失敗：" + str(e))

    st.markdown("---")

    st.subheader("資料更新")

    if st.button("自動補抓缺失資料", type="primary", use_container_width=True):
        with st.spinner("正在通知 Apps Script 補抓資料...\n（約需 1-3 分鐘，請稍候）"):
            ok, msg = trigger_gas("backfill")
            if ok:
                st.success("補抓指令已送出！")
                st.info("Apps Script 正在背景執行，請等 2-3 分鐘後按「重新整理資料」")
            else:
                st.warning("指令已送出（逾時不代表失敗）")
                st.info("Apps Script 可能仍在背景執行，請等 2-3 分鐘後按「重新整理資料」")

    if st.button("重新整理資料", use_container_width=True):
        load_stock_data.clear()
        get_existing_dates.clear()
        st.success("已清除快取，重新載入！")
        time.sleep(1)
        st.rerun()

    st.markdown("---")
    st.caption("每天下午 5 點自動抓取，或手動點上方按鈕更新")


# ── 主畫面 ──
st.header(mode)

if mode == "今日強勢戰報":
    df = load_stock_data()
    if df.empty:
        st.warning("尚無資料，請點左側「自動補抓缺失資料」。")
    else:
        latest_date = df["日期"].max()
        today_df = df[df["日期"] == latest_date].copy()

        c1, c2, c3 = st.columns(3)
        c1.metric("最新日期", latest_date)
        c2.metric("總記錄筆數", len(df))
        c3.metric("今日標的數", len(today_df))

        st.markdown("---")
        st.subheader(latest_date + " 強勢標的（買超 >= 500 張）")

        try:
            today_df["買超_n"] = pd.to_numeric(today_df["買超張數"], errors='coerce').fillna(0)
            today_df["天數_n"] = pd.to_numeric(today_df["出現天數"], errors='coerce').fillna(0)
            today_df = today_df.sort_values(
                by=["天數_n", "買超_n"], ascending=[False, False]
            ).drop(columns=["買超_n", "天數_n"])
        except:
            pass

        st.dataframe(today_df, use_container_width=True, hide_index=True)

        with st.expander("查看全部歷史資料"):
            st.dataframe(df, use_container_width=True, hide_index=True)

elif mode == "籌碼週期分析":
    df = load_stock_data()
    if df.empty:
        st.warning("尚無資料，請點左側「自動補抓缺失資料」。")
    else:
        df["買超_n"] = pd.to_numeric(df["買超張數"], errors='coerce').fillna(0)
        df["天數_n"] = pd.to_numeric(df["出現天數"], errors='coerce').fillna(0)

        latest_date = df["日期"].max()
        today_df = df[df["日期"] == latest_date].copy()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("基準日", latest_date)
        c2.metric("法人鎖碼（>=3天）", len(today_df[today_df["天數_n"] >= 3]))
        c3.metric("大買超（>=1000張）", len(today_df[today_df["買超_n"] >= 1000]))
        c4.metric("首次發動", len(today_df[today_df["天數_n"] == 1]))

        st.markdown("---")

        tab1, tab2, tab3 = st.tabs(["法人鎖碼", "雙強初現", "首次發動"])

        with tab1:
            d1 = today_df[today_df["天數_n"] >= 3].sort_values(
                by=["天數_n", "買超_n"], ascending=[False, False]
            ).drop(columns=["買超_n", "天數_n"])
            if d1.empty:
                st.info("今日無法人鎖碼標的")
            else:
                st.caption("連續出現 >= 3 天，籌碼持續鎖定中")
                st.dataframe(d1, use_container_width=True, hide_index=True)

        with tab2:
            d2 = today_df[
                (today_df["買超_n"] >= 1000) & (today_df["天數_n"] <= 2)
            ].sort_values(by="買超_n", ascending=False).drop(columns=["買超_n", "天數_n"])
            if d2.empty:
                st.info("今日無雙強初現標的")
            else:
                st.caption("買超 >= 1000 張 且 出現天數 <= 2 天，剛剛起漲！")
                st.dataframe(d2, use_container_width=True, hide_index=True)

        with tab3:
            d3 = today_df[today_df["天數_n"] == 1].sort_values(
                by="買超_n", ascending=False
            ).drop(columns=["買超_n", "天數_n"])
            if d3.empty:
                st.info("今日無首次發動標的")
            else:
                st.caption("今日首次出現，法人剛開始進場")
                st.dataframe(d3, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("各股連續天數排行 TOP 30")
        rank_df = today_df.sort_values(
            by=["天數_n", "買超_n"], ascending=[False, False]
        ).drop(columns=["買超_n", "天數_n"]).head(30)
        st.dataframe(rank_df, use_container_width=True, hide_index=True)

elif mode == "資料庫管理":
    st.subheader("資料庫管理")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("[開啟 Google Sheet](https://docs.google.com/spreadsheets/d/" + SHEET_ID + ")")
        if st.button("測試 Google Sheets 連線", use_container_width=True):
            try:
                ws = get_worksheet(STOCK_SHEET)
                st.success("連線成功：" + ws.title)
            except Exception as e:
                st.error("連線失敗：" + str(e))

    with col2:
        st.info("指定日期補抓")
        target_input = st.date_input(
            "選擇日期",
            value=datetime.now().date() - timedelta(days=1)
        )
        if st.button("補抓指定日期", use_container_width=True):
            date_str = target_input.strftime('%Y%m%d')
            with st.spinner("通知 Apps Script 補抓 " + str(target_input) + "..."):
                ok, msg = trigger_gas("single&date=" + date_str)
                if ok:
                    st.success("指令已送出！等 1 分鐘後按「重新整理資料」")
                else:
                    st.warning("指令已送出（逾時不代表失敗），等 1 分鐘後重新整理")

    st.markdown("---")
    st.subheader("Sheet 資料預覽（最新 50 筆）")
    df_p = load_stock_data()
    if not df_p.empty:
        st.dataframe(df_p.tail(50), use_container_width=True, hide_index=True)
    else:
        st.info("尚無資料")
