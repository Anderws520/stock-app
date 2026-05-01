import streamlit as st
import pandas as pd
import requests
import random
import time
from datetime import datetime, timedelta
from io import StringIO
import re
import os

st.set_page_config(page_title="台股法人操盤工具", layout="wide")
st.title("🟢 台股三大法人買超操盤工具（2026最新版）")
st.markdown("**已修正欄位解析問題**")

DATA_FILE = "twse_institutional_db.parquet"
START_DATE = datetime(2026, 4, 27).date()

USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"]

def is_trading_day(d):
    if d.weekday() >= 5 or d == datetime(2026, 5, 1).date():
        return False
    return True

def get_url(date):
    return f"https://www.twse.com.tw/fund/T86?response=csv&date={date.strftime('%Y%m%d')}&selectType=ALLBUT0999"

def clean_number(x):
    if isinstance(x, str):
        x = re.sub(r'[^\d.-]', '', x)
    try:
        return float(x)
    except:
        return 0

def download_t86(date):
    if not is_trading_day(date):
        return None
    url = get_url(date)
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    
    try:
        resp = requests.get(url, headers=headers, timeout=30, verify=False)
        resp.raise_for_status()
        
        text = resp.text
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        
        # 找到含有「證券代號」的那一行開始
        start_idx = next((i for i, line in enumerate(lines) if "證券代號" in line), None)
        if start_idx is None:
            st.error("找不到證券代號")
            return None
            
        csv_text = "\n".join(lines[start_idx:])
        df = pd.read_csv(StringIO(csv_text), encoding='big5', on_bad_lines='skip')
        
        # 清理欄位名稱
        df.columns = [str(col).strip().replace('\n', '').replace(' ', '') for col in df.columns]
        
        # 關鍵：找到「三大法人買賣超股數」這一欄（通常是最後幾欄之一）
        buy_col = None
        for col in df.columns:
            if "三大法人買賣超股數" in col:
                buy_col = col
                break
        
        if buy_col is None:
            st.error("找不到三大法人買賣超股數欄位")
            st.write("可用欄位：", list(df.columns))
            return None
        
        df['三大法人買賣超股數'] = df[buy_col].apply(clean_number)
        df = df.dropna(subset=['證券代號']).copy()
        
        df['日期'] = pd.to_datetime(date).date()
        df['證券代號'] = df['證券代號'].astype(str).str.strip().str.zfill(4)
        
        return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]
        
    except Exception as e:
        st.error(f"下載失敗: {str(e)[:100]}")
        return None

# ====================== UI ======================
if st.button("🔄 開始/繼續 更新資料（從2026-4-27開始）", type="primary"):
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE)
    else:
        db = pd.DataFrame()
    
    last_date = START_DATE - timedelta(days=1) if db.empty else pd.to_datetime(db['日期']).max().date()
    today = datetime.now().date()
    target = last_date + timedelta(days=1)
    
    progress = st.progress(0)
    status = st.empty()
    count = 0
    
    while target <= today and count < 60:
        if is_trading_day(target):
            status.info(f"正在抓取 {target} ...")
            new_df = download_t86(target)
            if new_df is not None and not new_df.empty:
                db = pd.concat([db, new_df], ignore_index=True)
                db = db.drop_duplicates(subset=['日期', '證券代號'])
                db.to_parquet(DATA_FILE, index=False)
                status.success(f"✅ {target} 成功存檔")
            time.sleep(random.uniform(6, 9))
        target += timedelta(days=1)
        count += 1
        progress.progress(min(count/30, 1.0))
    
    st.success("更新完成！")

# 顯示結果
if os.path.exists(DATA_FILE):
    db = pd.read_parquet(DATA_FILE)
    if not db.empty:
        latest = pd.to_datetime(db['日期']).max().date()
        st.success(f"資料最新日期：**{latest}** | 總筆數：{len(db):,}")
        
        today_data = db[db['日期'] == latest].copy()
        today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
        strong = today_data[today_data['三大法人買賣超股數'] > 500_000].sort_values('三大法人買賣超股數', ascending=False)
        
        st.subheader(f"{latest} 買超較強股票（>50萬股）")
        st.dataframe(strong[['證券代號', '證券名稱', '買超張數']].head(30), 
                    use_container_width=True, hide_index=True)
    else:
        st.info("資料庫尚無資料")
else:
    st.info("請點上方按鈕開始下載")

st.caption("已修正2026年最新 CSV 欄位格式。如果還有問題請截圖給我。")