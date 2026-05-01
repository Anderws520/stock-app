import streamlit as st
import pandas as pd
import requests
import io
import os
import time
from datetime import datetime, timedelta

# --- 核心設定 ---
DB_FILE = "stock_database.parquet"
# 證交所三大法人買賣超日報 CSV 網址格式
TWSE_URL = "https://www.twse.com.tw/fund/T86?response=csv&date={}&selectType=ALLBUT0999"

st.set_page_config(page_title="買點定位系統 4.0", layout="wide")
st.title("🛡️ 買點定位系統 (官方數據直連版)")

def load_db():
    if os.path.exists(DB_FILE):
        try:
            df = pd.read_parquet(DB_FILE)
            if not df.empty and '日期' in df.columns:
                return df
        except:
            pass
    return pd.DataFrame()

def save_db(df):
    try:
        # 強制轉字串存檔，解決 Arrow 類型衝突
        df.astype(str).to_parquet(DB_FILE, index=False)
        return True
    except:
        return False

# --- 官方 CSV 下載邏輯 ---
def download_twse_csv(date_str):
    """直接從證交所下載 CSV 並解析"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    url = TWSE_URL.format(date_str.replace('-', ''))
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200 and len(response.text) > 500:
            # 證交所 CSV 前幾行是標題，需跳過；最後幾行是說明，也要過濾
            df = pd.read_csv(io.StringIO(response.text), skiprows=1)
            df = df.dropna(subset=['證券代號']) # 移除結尾說明行
            df.columns = [c.replace(' ', '').strip() for c in df.columns]
            return df
        return None
    except:
        return None

# --- 自動同步流程 ---
db = load_db()

# 根據要求：從 4/27 開始補
if db.empty:
    curr = datetime(2026, 4, 27)
else:
    last_date = db['日期'].max()
    curr = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)

today = datetime.now()

if curr <= today:
    with st.status("📥 正在從證交所官網下載 CSV 補資料...", expanded=True) as status:
        while curr <= today:
            d_str = curr.strftime("%Y-%m-%d")
            # 跳過週末與勞動節
            if curr.weekday() >= 5 or d_str == "2026-05-01":
                st.write(f"🏮 {d_str} 休市，自動跳過")
            else:
                st.write(f"⏳ 正在請求 {d_str} 的官方數據...")
                time.sleep(5) # 官方網頁防爬機制較嚴，間隔拉長至 5 秒
                
                new_data = download_twse_csv(d_str)
                
                if new_data is not None:
                    new_data['日期'] = d_str
                    db = pd.concat([db, new_data], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                    save_db(db)
                    st.write(f"✅ {d_str} 下載並補入成功")
                else:
                    st.write(f"❌ {d_str} 請求失敗 (可能尚未開盤或連線繁忙)")
                    break # 失敗則停止，避免被證交所暫時封鎖 IP
            curr += timedelta(days=1)
        status.update(label="數據更新完成", state="complete")

# --- 報表顯示 ---
if not db.empty:
    latest_dt = db['日期'].max()
    df_show = db[db['日期'] == latest_dt].copy()
    
    # 識別買賣超與價格欄位
    buy_col = next((c for c in db.columns if '三大法人買賣超股數' in c), None)
    price_col = '收盤價' # 官方 CSV 欄位固定
    
    if buy_col and price_col in df_show.columns:
        report = []
        for _, row in df_show.iterrows():
            try:
                # 處理 CSV 中的逗號數值
                val = float(str(row[buy_col]).replace(',', ''))
                if val <= 100000: continue # 濾掉買不到 100 張的
                
                report.append({
                    '代號': row['證券代號'],
                    '名稱': row['證券名稱'],
                    '買超張數': int(round(val/1000, 0)),
                    '現價': row[price_col],
                    '日期': latest_dt
                })
            except: continue
            
        st.subheader(f"📊 官方數據分析報表 ({latest_dt})")
        st.dataframe(pd.DataFrame(report).sort_values('買超張數', ascending=False), use_container_width=True, hide_index=True)
    else:
        st.error("❌ 官方資料解析失敗，請確認 4/27 之後是否有開盤數據")
else:
    st.info("💡 系統正嘗試連接證交所下載 4/27 起的 CSV 檔案...")
