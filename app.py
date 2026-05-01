import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import time

# --- 核心設定 ---
DB_FILE = "stock_database.parquet"
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.set_page_config(page_title="買點定位系統 2.8", layout="wide")
st.title("🛡️ 買點定位系統 (晨間數據補完版)")

def load_db():
    if os.path.exists(DB_FILE):
        try:
            df = pd.read_parquet(DB_FILE)
            # 必須包含日期與收盤價才算完整
            if not df.empty and '日期' in df.columns and any('收盤價' in c for c in df.columns):
                return df
            os.remove(DB_FILE) # 不完整就砍掉重練
        except:
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
    return pd.DataFrame()

# --- 早上補課邏輯 ---
db = load_db()
# 從 4/25 開始補，這段時間數據最穩
start_date = db['日期'].max() if not db.empty else "2026-04-25"
curr = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=1)
today = datetime.now()

with st.status("🌅 早上好！正在檢查並補齊本週數據...", expanded=True) as status:
    while curr <= today:
        d_str = curr.strftime("%Y-%m-%d")
        if curr.weekday() >= 5 or d_str == "2026-05-01": # 週末與今日勞動節不抓
            st.write(f"☕ {d_str} 休市，喝杯咖啡跳過")
        else:
            try:
                time.sleep(3) # 晨間請求間隔
                r = requests.get(f"{GAS_URL}?date={d_str.replace('-', '')}", timeout=25)
                data = r.json()
                if data.get('stat') == 'OK' and data.get('data'):
                    new_df = pd.DataFrame(data['data'], columns=data['fields'])
                    new_df.columns = [c.strip() for c in new_df.columns]
                    new_df['日期'] = d_str
                    db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                    db.astype(str).to_parquet(DB_FILE, index=False) # 強制儲存
                    st.write(f"✅ {d_str} 數據補齊成功")
                else:
                    st.write(f"⚠️ {d_str} 證交所尚未提供資料")
            except:
                st.write(f"⏳ {d_str} 連線繁忙，請稍後...")
        curr += timedelta(days=1)
    status.update(label="數據補完完畢", state="complete")

# --- 顯示報表 ---
if not db.empty:
    latest = db['日期'].max()
    df_now = db[db['日期'] == latest].copy()
    buy_col = next((c for c in db.columns if '買賣超' in c or '合計' in c), None)
    price_col = next((c for c in db.columns if '收盤價' in c), None)
    
    if buy_col and price_col:
        res = []
        for _, row in df_now.iterrows():
            sid, name = row['證券代號'], row['證券名稱']
            vol = int(round(float(str(row[buy_col]).replace(',',''))/1000, 0))
            if vol <= 100: continue # 略過交易量太小的
            
            p_now = float(str(row[price_col]).replace(',',''))
            res.append({
                '分析日期': latest, '代號': sid, '名稱': name,
                '買超張數': vol, '現價': p_now,
                '操盤建議': "多頭強勢" if vol > 500 else "觀察中"
            })
        st.dataframe(pd.DataFrame(res).sort_values('買超張數', ascending=False), hide_index=True)
else:
    st.warning("目前還在排隊抓資料，請點擊網頁右上角的 'Rerun' 重新整理看看。")
