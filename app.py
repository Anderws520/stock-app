import streamlit as st
import pandas as pd
import requests
import random
import time
from datetime import datetime, timedelta
from io import StringIO
import os

st.set_page_config(page_title="台股法人工具", layout="wide")
st.title("🟢 台股三大法人買超工具")
st.markdown("**已加強反爬蟲防護**")

DATA_FILE = "twse_db.parquet"

def is_trading_day(d):
    if d.weekday() >= 5: return False
    if d == datetime(2026, 5, 1).date(): return False
    return True

def download_t86(date):
    url = f"https://www.twse.com.tw/fund/T86?response=csv&date={date.strftime('%Y%m%d')}&selectType=ALLBUT0999"
    
    headers = {
        "User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
        ]),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Referer": "https://www.twse.com.tw/zh/page/trading/fund/T86.html",
        "Origin": "https://www.twse.com.tw",
        "Connection": "keep-alive"
    }
    
    try:
        # 允許重導向 + 增加延遲
        resp = requests.get(url, headers=headers, timeout=25, verify=False, allow_redirects=True)
        
        st.caption(f"{date} 狀態碼: {resp.status_code}")
        
        if resp.status_code != 200:
            st.error(f"HTTP錯誤: {resp.status_code}")
            return None
        
        text = resp.text
        if len(text) < 2000:
            st.warning(f"{date} 回應內容異常（太短）")
            return None
        
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        start_idx = next((i for i, line in enumerate(lines) if "證券代號" in line), None)
        if start_idx is None:
            st.error("找不到資料標頭")
            return None
        
        df = pd.read_csv(StringIO("\n".join(lines[start_idx:])), encoding='big5', on_bad_lines='skip')
        df.columns = [str(col).strip().replace('\n','').replace(' ','') for col in df.columns]
        
        buy_col = next((col for col in df.columns if "三大法人買賣超股數" in col), None)
        if buy_col is None:
            st.error("找不到買賣超欄位")
            st.write("可用欄位:", list(df.columns)[:10])
            return None
        
        df['三大法人買賣超股數'] = df[buy_col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce').fillna(0)
        df = df.dropna(subset=['證券代號']).copy()
        df['日期'] = pd.to_datetime(date).date()
        df['證券代號'] = df['證券代號'].astype(str).str.zfill(4)
        
        st.success(f"✅ {date} 成功抓到 {len(df)} 筆資料")
        return df[['日期', '證券代號', '證券名稱', '三大法人買賣超股數']]
        
    except Exception as e:
        st.error(f"錯誤: {str(e)}")
        return None

# ====================== 更新功能 ======================
if st.button("🔄 開始/繼續 更新資料（斷點續傳）", type="primary"):
    with st.spinner("正在更新資料..."):
        if os.path.exists(DATA_FILE):
            db = pd.read_parquet(DATA_FILE)
        else:
            db = pd.DataFrame(columns=['日期', '證券代號', '證券名稱', '三大法人買賣超股數'])
        
        if db.empty:
            last_date = datetime(2026, 4, 27).date()
        else:
            last_date = pd.to_datetime(db['日期']).max().date()
        
        today = datetime.now().date()
        target = last_date + timedelta(days=1)
        
        progress = st.progress(0)
        status = st.empty()
        count = 0
        
        while target <= today and count < 30:
            if is_trading_day(target):
                status.info(f"正在抓取 {target}")
                new_df = download_t86(target)
                if new_df is not None and not new_df.empty:
                    db = pd.concat([db, new_df], ignore_index=True)
                    db = db.drop_duplicates(subset=['日期', '證券代號'])
                    db.to_parquet(DATA_FILE, index=False)
            progress.progress(min(count / 25, 1.0))
            target += timedelta(days=1)
            count += 1
            time.sleep(8)   # 增加等待時間
        
        st.success("更新完成！")

# ====================== 顯示 ======================
if os.path.exists(DATA_FILE):
    db = pd.read_parquet(DATA_FILE)
    latest = pd.to_datetime(db['日期']).max().date()
    st.success(f"✅ 最新日期：{latest} | 總筆數：{len(db):,}")
    
    today_data = db[db['日期'] == latest].copy()
    today_data['買超張數'] = (today_data['三大法人買賣超股數'] / 1000).round(1)
    
    st.subheader(f"{latest} 買超前30強")
    st.dataframe(today_data.sort_values('買超張數', ascending=False).head(30)[['證券代號', '證券名稱', '買超張數']], 
                use_container_width=True, hide_index=True)
else:
    st.info("請點擊上方按鈕開始下載")

st.caption("已加強 User-Agent + Referer + 等待時間")