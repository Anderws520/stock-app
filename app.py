import streamlit as st
import pandas as pd
import requests
import io
import os
import time
from datetime import datetime, timedelta

# --- 核心設定 ---
DB_FILE = "stock_database.parquet"
GAS_URL = "https://script.google.com/macros/s/AKfycbxmoO3M1vsgwUStzDvDY5uRebEo_EGu79-FWSCLzSJYsB5Kz33h2WE8CuhBGEBAsjO7/exec"

st.set_page_config(page_title="買點定位系統 6.0", layout="wide")
st.title("🛡️ 買點定位系統 (嚴格同步版)")

def load_db():
    """載入並檢查資料庫是否健康"""
    if os.path.exists(DB_FILE):
        try:
            df = pd.read_parquet(DB_FILE)
            # 必須包含日期且有實質內容才算成功
            if not df.empty and '日期' in df.columns and '收盤價' in "".join(df.columns):
                return df
        except: pass
    return pd.DataFrame()

def save_db(df):
    """確保只有健康的資料才會被寫入檔案"""
    if df.empty or '收盤價' not in "".join(df.columns): return False
    try:
        df.astype(str).to_parquet(DB_FILE, index=False)
        return True
    except: return False

# --- 核心同步邏輯 ---
db = load_db()

# 鎖定 4/27 起點，解決 4/26 週日與 5/1 休市的混亂
if db.empty:
    curr = datetime(2026, 4, 27)
else:
    last_date = db['日期'].max()
    curr = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)

today = datetime.now()

if curr <= today:
    with st.status("📥 資料補齊中，請保持畫面開啟...", expanded=True) as status:
        while curr <= today:
            d_str = curr.strftime("%Y-%m-%d")
            # 排除週末與勞動節
            if curr.weekday() >= 5 or d_str == "2026-05-01":
                st.write(f"🏮 {d_str} 休市，自動跳過")
            else:
                st.write(f"⏳ 嘗試抓取 {d_str}...")
                time.sleep(5) # 延長間隔至 5 秒，降低被當機率
                
                success = False
                try:
                    r = requests.get(f"{GAS_URL}?date={d_str.replace('-', '')}", timeout=25)
                    data = r.json()
                    # 只有當 stat 是 OK 且真的有 data 才算數
                    if data.get('stat') == 'OK' and data.get('data'):
                        new_df = pd.DataFrame(data['data'], columns=data['fields'])
                        new_df.columns = [c.strip() for c in new_df.columns]
                        new_df['日期'] = d_str
                        
                        # 再次驗證欄位是否正確
                        if '收盤價' in "".join(new_df.columns):
                            db = pd.concat([db, new_df], ignore_index=True).drop_duplicates(subset=['日期', '證券代號'])
                            save_db(db)
                            st.write(f"✅ {d_str} 數據補齊成功")
                            success = True
                except: pass
                
                if not success:
                    st.write(f"❌ {d_str} 抓取失敗 (GAS 被擋或休市)，將在下次啟動時重試")
                    break # 失敗就停，不往後跑，避免存入空資料導致後續報錯
            curr += timedelta(days=1)
        status.update(label="數據檢查流程結束", state="complete")

# --- 報表顯示 (防崩潰保護) ---
if not db.empty:
    try:
        latest = db['日期'].max()
        df_now = db[db['日期'] == latest].copy()
        
        # 智慧欄位對齊
        buy_col = next((c for c in db.columns if '買賣超股數' in c or '合計' in c), None)
        price_col = next((c for c in db.columns if '收盤價' in c), None)
        
        if buy_col and price_col:
            res = []
            for _, row in df_now.iterrows():
                try:
                    vol = float(str(row[buy_col]).replace(',', ''))
                    if vol <= 0: continue
                    res.append({
                        '代號': row['證券代號'], '名稱': row['證券名稱'],
                        '買超張數': int(round(vol/1000, 0)),
                        '現價': row[price_col], '日期': latest
                    })
                except: continue
            
            st.subheader(f"📅 最新報表：{latest}")
            st.dataframe(pd.DataFrame(res).sort_values('買超張數', ascending=False), use_container_width=True, hide_index=True)
        else:
            st.error("⚠️ 資料庫雖然有檔案，但缺少關鍵欄位，請稍候讓系統重新同步。")
    except Exception as e:
        st.error(f"報表生成出錯：{e}")
else:
    st.info("💡 目前資料庫尚未建立 4/27 以後的正確數據，請點擊上方工具列 Rerun 嘗試補齊。")
