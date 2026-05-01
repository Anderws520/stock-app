import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import time

# --- 核心設定 ---
DB_FILE = "stock_database.parquet"
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.set_page_config(page_title="買點定位系統 2.6", layout="wide")
st.title("🛡️ 買點定位系統 (全自動備援版)")

def to_num(v):
    try:
        s = str(v).replace(',', '').replace(' ', '').strip()
        if s in ['--', '', 'None', 'nan']: return 0.0
        return float(s)
    except: return 0.0

def load_db():
    if os.path.exists(DB_FILE):
        try:
            df = pd.read_parquet(DB_FILE)
            # 檢查關鍵欄位是否存在，若不存在則視為損壞檔
            if not df.empty and any('收盤價' in c for c in df.columns):
                return df
            else:
                os.remove(DB_FILE) # 暴力刪除損壞檔
        except:
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
    return pd.DataFrame()

def save_db(df):
    try:
        # 強制轉換所有內容為字串，徹底封殺 ArrowTypeError
        df.astype(str).to_parquet(DB_FILE, index=False)
        return True
    except: return False

# --- 全自動同步邏輯 ---
db = load_db()
# 如果是空的，預設從 4/25 開始補，避免跑太久被封鎖
last_date_str = db['日期'].max() if not db.empty else "2026-04-25"
curr_dt = datetime.strptime(last_date_str, "%Y-%m-%d") + timedelta(days=1)
today_dt = datetime.now()

if curr_dt <= today_dt:
    with st.status("🔍 正在全自動補齊缺失數據...", expanded=True) as status:
        while curr_dt <= today_dt:
            d_str = curr_dt.strftime("%Y-%m-%d")
            if curr_dt.weekday() >= 5: # 週末自動跳過
                curr_dt += timedelta(days=1)
                continue
            
            try:
                time.sleep(2.5) # 防封鎖延遲
                r = requests.get(f"{GAS_URL}?date={d_str.replace('-', '')}", timeout=20)
                data = r.json()
                
                if data.get('stat') == 'OK' and data.get('data'):
                    new_df = pd.DataFrame(data['data'], columns=data['fields'])
                    new_df.columns = [c.strip() for c in new_df.columns]
                    # 再次確認欄位
                    if any('收盤價' in c for c in new_df.columns):
                        new_df['日期'] = d_str
                        db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                        save_db(db)
                        st.write(f"✅ {d_str} 數據補齊成功")
                else:
                    st.write(f"🏮 {d_str} 休市或無資料")
            except:
                st.write(f"⏳ {d_str} 連線繁忙，稍後重試")
                break
            curr_dt += timedelta(days=1)
        status.update(label="數據檢查完畢", state="complete")

# --- 最終報表呈現 ---
if not db.empty:
    latest = db['日期'].max()
    df_now = db[db['日期'] == latest].copy()
    
    # 智慧欄位比對
    buy_col = next((c for c in db.columns if '買賣超股數' in c or '合計' in c), None)
    price_col = next((c for c in db.columns if '收盤價' in c), None)
    
    if buy_col and price_col:
        res = []
        for _, row in df_now.iterrows():
            sid = row['證券代號']
            shares = to_num(row[buy_col])
            if shares == 0: continue # 只看有動作的
            
            # 計算 5 日均價
            hist = db[db['證券代號'] == sid].sort_values('日期', ascending=False).head(5)
            p_list = [to_num(p) for p in hist[price_col].tolist() if to_num(p) > 0]
            ma5 = round(sum(p_list)/len(p_list), 2) if p_list else 0
            
            curr_p = to_num(row[price_col])
            diff_pct = (curr_p - ma5) / ma5 if ma5 > 0 else 0
            
            res.append({
                '日期': latest, '股票代號': sid, '股票名稱': row['證券名稱'],
                '買超張數': int(round(shares/1000, 0)), '5日均價': ma5, '目前現價': curr_p,
                '價差%': f"{diff_pct:.2%}", '連續出現': len(hist),
                '操盤建議': "多頭" if curr_p > ma5 else "盤整"
            })
        
        st.subheader(f"📊 最新數據日期：{latest}")
        st.dataframe(pd.DataFrame(res).sort_values('買超張數', ascending=False), use_container_width=True, hide_index=True)
    else:
        st.warning("⚠️ 資料庫格式仍不完整，正在背景重新抓取中...")
else:
    st.info("💡 系統正在第一次建立資料庫，請稍候 30 秒後重新整理網頁。")
