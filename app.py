import streamlit as st
import pandas as pd
import requests
import random
import time
from datetime import datetime, timedelta
import io
import warnings
warnings.filterwarnings('ignore')
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="台股法人操盤系統", page_icon="📈", layout="wide")

SHEET_ID = st.secrets["SHEET_ID"]
STOCK_SHEET = "stock_Sheet"

TW_HOLIDAYS_2026 = {
    datetime(2026, 1, 1).date(), datetime(2026, 1, 26).date(),
    datetime(2026, 1, 27).date(), datetime(2026, 1, 28).date(),
    datetime(2026, 1, 29).date(), datetime(2026, 1, 30).date(),
    datetime(2026, 2, 28).date(), datetime(2026, 4, 3).date(),
    datetime(2026, 4, 4).date(), datetime(2026, 5, 1).date(),
    datetime(2026, 6, 19).date(), datetime(2026, 9, 27).date(),
    datetime(2026, 10, 9).date(), datetime(2026, 10, 10).date(),
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# ====================== Google Sheets ======================

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

def load_stock_data():
    try:
        ws = get_worksheet(STOCK_SHEET)
        data = ws.get_all_values()
        if len(data) <= 1:
            return pd.DataFrame()
        return pd.DataFrame(data[1:], columns=data[0])
    except Exception as e:
        st.error("讀取失敗：" + str(e))
        return pd.DataFrame()

def get_existing_dates():
    try:
        ws = get_worksheet(STOCK_SHEET)
        vals = ws.col_values(1)
        return set(v for v in vals[1:] if v)
    except:
        return set()

def append_rows_to_sheet(sheet_rows):
    try:
        ws = get_worksheet(STOCK_SHEET)
        existing = ws.get_all_values()
        if len(existing) == 0:
            ws.append_row(["日期", "股票代號", "股票名稱", "關鍵分點", "買超張數",
                           "5日均價", "目前現價", "價差%", "出現天數", "超盤建議"])
        ws.append_rows(sheet_rows, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        st.error("寫入失敗：" + str(e))
        return False

# ====================== 證交所下載 ======================

def is_trading_day(d):
    if hasattr(d, 'date'):
        d = d.date()
    return d.weekday() < 5 and d not in TW_HOLIDAYS_2026

def clean_num(s):
    s = str(s).strip().replace(',', '').replace('+', '')
    try:
        return float(s)
    except:
        return 0.0

def download_twse(target_date):
    date_str = target_date.strftime('%Y%m%d')
    url = ("https://www.twse.com.tw/fund/T86"
           "?response=csv&date=" + date_str + "&selectType=ALLBUT0999")
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://www.twse.com.tw/zh/page/trading/fund/T86.html",
        "Cache-Control": "no-cache",
    }
    session = requests.Session()
    try:
        session.get(
            "https://www.twse.com.tw/zh/page/trading/fund/T86.html",
            headers=headers, timeout=10, verify=False
        )
        time.sleep(random.uniform(1.5, 2.5))
    except:
        pass
    try:
        resp = session.get(url, headers=headers, timeout=20, verify=False)
        if resp.status_code != 200:
            return None, "HTTP " + str(resp.status_code)
        try:
            text = resp.content.decode('big5', errors='replace')
        except:
            text = resp.content.decode('utf-8', errors='replace')
        if '查詢無資料' in text or len(text.strip()) < 100:
            return None, "查詢無資料"
        if '<html' in text.lower():
            return None, "返回HTML非CSV"
        lines = text.strip().split('\n')
        header_idx = None
        for i, line in enumerate(lines):
            if '證券代號' in line and '證券名稱' in line:
                header_idx = i
                break
        if header_idx is None:
            return None, "找不到標題行"
        data_lines = []
        for line in lines[header_idx + 1:]:
            line = line.strip()
            if not line or '合計' in line:
                continue
            if line[0].isdigit() or line.startswith('"'):
                data_lines.append(line)
        if not data_lines:
            return None, "無有效資料列"
        csv_str = lines[header_idx] + '\n' + '\n'.join(data_lines)
        df = pd.read_csv(io.StringIO(csv_str), dtype=str,
                         na_values=['--', '-', ''], keep_default_na=False)
        df.columns = [c.strip().replace('"', '') for c in df.columns]
        net_col = next((c for c in df.columns if '三大法人' in c), None)
        code_col = next((c for c in df.columns if '代號' in c or '代碼' in c), None)
        name_col = next((c for c in df.columns if '名稱' in c), None)
        if not all([net_col, code_col, name_col]):
            return None, "欄位不完整：" + str(list(df.columns))
        result = pd.DataFrame({
            '代號': df[code_col].astype(str).str.strip().str.replace('"', ''),
            '名稱': df[name_col].astype(str).str.strip().str.replace('"', ''),
            '買賣超股數': df[net_col].apply(clean_num),
        })
        result = result[result['代號'].str.match(r'^\d{4,6}$')]
        result = result[result['買賣超股數'] > 0]
        if result.empty:
            return None, "無買超資料"
        return result, "OK"
    except requests.exceptions.Timeout:
        return None, "請求逾時"
    except Exception as e:
        return None, str(e)

def build_sheet_rows(df_raw, date_str, existing_df, start_row):
    rows = []
    for _, row in df_raw.iterrows():
        code = str(row['代號'])
        buy_qty = round(float(row['買賣超股數']) / 1000, 1)
        if buy_qty < 500:
            continue
        if not existing_df.empty and '股票代號' in existing_df.columns:
            cnt = len(existing_df[
                existing_df['股票代號'].astype(str).str.strip().str.lstrip("'") == code
            ])
            appear = cnt + 1
        else:
            appear = 1
        if buy_qty > 1000 and appear <= 2:
            suggest = "🔥 雙強初現"
        elif appear >= 3:
            suggest = "🔒 法人鎖碼"
        elif appear == 1:
            suggest = "🚀 首次發動"
        else:
            suggest = "⏳ 籌碼鎖定"
        rn = start_row + len(rows)
        rows.append([
            date_str,
            "'" + code,
            str(row['名稱']),
            "三大法人",
            buy_qty,
            '=IFERROR(AVERAGE(INDEX(GOOGLEFINANCE("TPE:"&B' + str(rn) + ',"price",TODAY()-10,TODAY()),,2)),G' + str(rn) + ')',
            '=IFERROR(GOOGLEFINANCE("TPE:"&B' + str(rn) + ',"price"),"")',
            '=IF(AND(N(G' + str(rn) + ')>0,N(F' + str(rn) + ')>0),(G' + str(rn) + '-F' + str(rn) + ')/F' + str(rn) + ',"")',
            appear,
            suggest,
        ])
    return rows

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
    <h1>📈 台股法人操盤系統</h1>
    <p>三大法人籌碼追蹤 · 均線防護策略 · Google Sheets 持久化儲存</p>
</div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢戰報", "資料庫管理"], index=0)
    st.markdown("---")

    try:
        edates = get_existing_dates()
        valid = sorted([d for d in edates if d and len(d) > 5], reverse=True)
        if valid:
            st.success("📁 最新：" + valid[0])
            st.info("📊 共 " + str(len(valid)) + " 筆日期記錄")
        else:
            st.warning("尚無資料")
    except Exception as e:
        st.error("連線失敗：" + str(e))

    st.markdown("---")

    if st.button("🔄 自動更新（斷點續傳）", type="primary", use_container_width=True):
        existing_dates = get_existing_dates()
        start_date = datetime(2026, 4, 27).date()
        today = datetime.now().date()
        missing = []
        d = start_date
        while d <= today:
            ds = d.strftime('%Y/%m/%d')
            if is_trading_day(d) and ds not in existing_dates:
                missing.append(d)
            d += timedelta(days=1)

        if not missing:
            st.success("✅ 資料已是最新！")
        else:
            st.info("需補抓 " + str(len(missing)) + " 個交易日")
            prog = st.progress(0)
            status_box = st.empty()
            updated = 0
            existing_df = load_stock_data()

            for i, target in enumerate(missing[:20]):
                date_str = target.strftime('%Y/%m/%d')
                status_box.info("⏳ 抓取 " + str(target) + "...")
                df_raw, msg = download_twse(target)

                if df_raw is not None:
                    try:
                        ws = get_worksheet(STOCK_SHEET)
                        start_row = len(ws.get_all_values()) + 1
                    except:
                        start_row = 2
                    sheet_rows = build_sheet_rows(df_raw, date_str, existing_df, start_row)
                    if sheet_rows:
                        if append_rows_to_sheet(sheet_rows):
                            updated += 1
                            status_box.success("✅ " + str(target) + " 成功（" + str(len(sheet_rows)) + " 檔）")
                            existing_df = load_stock_data()
                        else:
                            status_box.error("❌ " + str(target) + " 寫入失敗")
                    else:
                        status_box.warning("⚠️ " + str(target) + " 無符合條件標的")
                else:
                    if '查詢無資料' in msg:
                        status_box.warning("🏖️ " + str(target) + " 休市")
                    else:
                        status_box.error("❌ " + str(target) + "：" + msg)

                prog.progress((i + 1) / len(missing[:20]))
                if i < len(missing) - 1:
                    time.sleep(random.uniform(5.5, 8.5))

            st.success("✅ 完成！更新 " + str(updated) + " 天")
            time.sleep(1)
            st.rerun()

# ── 主畫面 ──
st.header("📊 " + mode)

COL_NAMES = ["日期", "股票代號", "股票名稱", "關鍵分點", "買超張數",
             "5日均價", "目前現價", "價差%", "出現天數", "超盤建議"]

if mode == "今日強勢戰報":
    df = load_stock_data()
    if df.empty:
        st.warning("⚠️ 尚無資料，請點左側「自動更新」下載資料。")
    else:
        df.columns = COL_NAMES[:len(df.columns)]
        latest_date = df["日期"].max()
        today_df = df[df["日期"] == latest_date].copy()

        c1, c2, c3 = st.columns(3)
        c1.metric("📅 最新日期", latest_date)
        c2.metric("📋 總記錄筆數", len(df))
        c3.metric("📈 今日標的數", len(today_df))

        st.markdown("---")
        st.subheader("🔥 " + latest_date + " 強勢標的（買超 ≥ 500 張）")

        try:
            today_df["買超_n"] = pd.to_numeric(today_df["買超張數"], errors='coerce').fillna(0)
            today_df["天數_n"] = pd.to_numeric(today_df["出現天數"], errors='coerce').fillna(0)
            today_df = today_df.sort_values(
                by=["天數_n", "買超_n"], ascending=[False, False]
            ).drop(columns=["買超_n", "天數_n"])
        except:
            pass

        st.dataframe(today_df, use_container_width=True, hide_index=True)

        with st.expander("📂 查看全部歷史資料"):
            st.dataframe(df, use_container_width=True, hide_index=True)

elif mode == "資料庫管理":
    st.subheader("🔧 資料庫管理")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("[📊 開啟 Google Sheet](https://docs.google.com/spreadsheets/d/" + SHEET_ID + ")")
        if st.button("🧪 測試 Google Sheets 連線", use_container_width=True):
            try:
                ws = get_worksheet(STOCK_SHEET)
                st.success("✅ 連線成功：" + ws.title)
            except Exception as e:
                st.error("❌ 連線失敗：" + str(e))

    with col2:
        target_input = st.date_input(
            "手動補抓指定日期",
            value=datetime.now().date() - timedelta(days=1)
        )
        if st.button("🚨 強制補抓此日期", use_container_width=True):
            with st.spinner("抓取 " + str(target_input) + "..."):
                df_raw, msg = download_twse(target_input)
                if df_raw is not None:
                    existing_df = load_stock_data()
                    date_str = target_input.strftime('%Y/%m/%d')
                    try:
                        ws = get_worksheet(STOCK_SHEET)
                        start_row = len(ws.get_all_values()) + 1
                    except:
                        start_row = 2
                    sheet_rows = build_sheet_rows(df_raw, date_str, existing_df, start_row)
                    if sheet_rows:
                        if append_rows_to_sheet(sheet_rows):
                            st.success("✅ 成功！" + str(len(sheet_rows)) + " 檔寫入完成")
                            st.rerun()
                        else:
                            st.error("寫入 Sheet 失敗")
                    else:
                        st.warning("無符合條件標的（買超 < 500 張）")
                else:
                    st.error("❌ " + msg)

    st.markdown("---")
    st.subheader("📋 Sheet 資料預覽（最新 50 筆）")
    df_p = load_stock_data()
    if not df_p.empty:
        df_p.columns = COL_NAMES[:len(df_p.columns)]
        st.dataframe(df_p.tail(50), use_container_width=True, hide_index=True)
    else:
        st.info("尚無資料")
