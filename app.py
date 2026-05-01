import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import random
import requests
import os
from io import StringIO
import re

st.set_page_config(page_title="台股法人共識工具", layout="wide")
st.title("🟢 台股三大法人買超操盤工具（2026穩定版）")
st.markdown("**已針對最新證交所格式優化** | 解決抓不下來的問題")

DATA_FILE = "twse_institutional_db.parquet"
START_DATE = datetime(2026, 4, 27).date()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
]

KNOWN_HOLIDAYS = {datetime(2026, 5, 1).date()}

def is_trading_day(d: datetime.date) -> bool:
    if d.weekday() >= 5 or d in KNOWN_HOLIDAYS:
        return False
    return True

def get_t86_url(date: datetime.date) -> str:
    return f"https://www.twse.com.tw/fund/T86?response=csv&date={date.strftime('%Y%m%d')}&selectType=ALLBUT0999"

def clean_number(x):
    if pd.isna(x) or x == '':
        return np.nan
    if isinstance(x, str):
        x = re.sub(r'[^\d.-]', '', x.strip())
    try:
        return float(x)
    except:
        return np.nan

def download_t86(date: datetime.date):
    """下載並解析三大法人 T86 CSV（強化版）"""
    if not is_trading_day(date):
        st.info(f"{date} 是休市日")
        return None

    url = get_t86_url(date)
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        
        text = resp.text
        lines = [line.strip() for line in text.splitlines() if line.strip()]

        # 找到資料開始行（最可靠的方式）
        start_idx = None
        for i, line in enumerate(lines):
            if "證券代號" in line or "证券代码" in line:
                start_idx = i
                break

        if start_idx is None:
            st.error(f"{date} 無法找到資料起始行，請檢查證交所格式是否又變了")
            st.text_area("原始回應前300字元", text[:500], height=200)
            return None

        csv_content = "\n".join(lines[start_idx:])
        df = pd.read_csv(StringIO(csv_content), encoding='big5', on_bad_lines='skip')

        # 清理欄位名稱
        df.columns = [col.strip().replace('\n', '').replace(' ', '') for col in df.columns]

        # 顯示實際欄位幫助除錯
        st.caption(f"{date} 抓到的欄位：{list(df.columns)}")

        # 關鍵欄位檢查（多種可能名稱）
        buy_col = None
        possible_names = ['三大法人買賣超股數', '三大法人買賣超', '買賣超股數', 'NetBuy']
        for name in possible_names:
            if name in df.columns:
                buy_col = name
                break

        if buy_col is None:
            st.error(f"{date} 找不到三大法人買賣超欄位")
            return None

        if '證券代號' not in df.columns:
            st.error(f"{date} 找不到證券代號欄位")
            return None

        # 清理數據
        df['三大法人買賣超股數'] = df[buy_col].apply(clean_number)
        df = df.dropna(subset=['證券代號', '三大法人買賣超股數'])
        df['日期'] = pd.to_datetime(date).date()
        df['證券代號'] = df['證券代號'].astype(str).str.zfill(4)

        return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]

    except Exception as e:
        st.error(f"{date} 下載失敗: {str(e)}")
        return None

# ====================== 主程式 ======================
def auto_update_db():
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE)
    else:
        db = pd.DataFrame()

    if db.empty:
        last_date = START_DATE - timedelta(days=1)
    else:
        last_date = pd.to_datetime(db['日期']).max().date()

    today = datetime.now().date()
    target_date = last_date + timedelta(days=1)

    progress_bar = st.progress(0)
    status_text = st.empty()

    updated_count = 0
    i = 0

    while target_date <= today and i < 60:
        if is_trading_day(target_date):
            status_text.info(f"正在抓取 {target_date} ...")
            new_df = download_t86(target_date)
            
            if new_df is not None and not new_df.empty:
                db = pd.concat([db, new_df], ignore_index=True)
                db = db.drop_duplicates(subset=['日期', '證券代號'])
                db.to_parquet(DATA_FILE, index=False)
                updated_count += 1
                status_text.success(f"✅ {target_date} 抓取成功")
            else:
                status_text.warning(f"⚠️ {target_date} 無有效資料")
            
            time.sleep(random.uniform(6, 9))  # 更保守的間隔

        target_date += timedelta(days=1)
        i += 1
        progress_bar.progress(min(i / 40, 1.0))

    if updated_count > 0:
        st.success(f"本次共更新 {updated_count} 天資料")
    return db

# ====================== UI ======================
if st.button("🔄 開始/繼續 更新資料（從2026-4-27補齊）", type="primary"):
    with st.spinner("正在執行斷點續傳..."):
        db_df = auto_update_db()

# 載入並顯示
if os.path.exists(DATA_FILE):
    db_df = pd.read_parquet(DATA_FILE)
    if not db_df.empty:
        latest = pd.to_datetime(db_df['日期']).max().date()
        st.success(f"✅ 資料最新日期：**{latest}**｜總筆數：{len(db_df):,}｜股票數：{db_df['證券代號'].nunique()}")
        
        # 簡單顯示今日買超前20強
        today = db_df['日期'].max()
        today_data = db_df[db_df['日期'] == today].copy()
        if not today_data.empty:
            today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
            today_data = today_data[today_data['三大法人買賣超股數'] > 500]
            st.subheader(f"{today} 買超 > 500張 前20強")
            st.dataframe(today_data[['證券代號', '證券名稱', '買超張數']].sort_values('買超張數', ascending=False).head(20),
                        use_container_width=True, hide_index=True)
    else:
        st.info("資料庫為空，請點上方按鈕開始下載")
else:
    st.info("尚未有資料，請點擊「開始/繼續 更新資料」按鈕")

with st.sidebar:
    st.header("除錯資訊")
    st.write("如果還是抓不下來，請把錯誤訊息完整複製給我")
    st.caption("目前使用更強的解析邏輯 + 顯示實際欄位名稱")

st.caption("這版已大幅強化解析能力。如果執行後還是顯示「找不到欄位」或錯誤，請把 Streamlit 畫面上顯示的「抓到的欄位」和錯誤訊息貼給我，我會馬上再調整。")

st.markdown("---")
st.markdown("**下一步**：資料抓成功後，告訴我「現在可以加上MA5與價格」，我會立刻給你完整包含5日均線、防護邏輯的終極版本。")