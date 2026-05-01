import streamlit as st
import pandas as pd
import numpy as np
import os
import yfinance as yf
from datetime import datetime, timedelta

# ====================== 1. 核心系統設定 ======================
DATA_FILE = "twse_institutional_db.parquet"

with st.sidebar:
    st.title("🛠️ 操盤工具箱")
    mode = st.selectbox("功能分頁切換", ["今日強勢戰報", "籌碼週期分析"])

# ====================== 2. 分頁邏輯 B: 籌碼週期分析 (新增賣價與次數) ======================
if mode == "籌碼週期分析":
    st.title("🔍 1/1 至今：最佳進場與預期賣點分析")
    
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE).sort_values(['證券代號', '日期'])
        db['買超張數'] = (db['三大法人買賣超股數'] / 1000).round(1)
        db['買超正'] = db['買超張數'] > 100
        db['連買計數'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        if st.button("📊 啟動全年度週期規律掃描"):
            active_stocks = db[db['連買計數'] >= 3]['證券代號'].unique()
            results = []
            
            with st.status("正在計算波段目標價與賣點...") as status:
                codes = active_stocks[:40].tolist()
                tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                price_data = yf.download(tickers, period="10d", interval="1d", group_by='ticker', progress=False)
                
                for code in codes:
                    s_data = db[db['證券代號'] == code].copy()
                    name = s_data['證券名稱'].iloc[0]
                    # 歷史發動點紀錄
                    entry_points = s_data[s_data['連買計數'] == 1]['日期'].tolist()
                    
                    # 獲取價格資訊
                    curr_price, ma5 = np.nan, np.nan
                    for suffix in [".TW", ".TWO"]:
                        t = f"{code}{suffix}"
                        if t in price_data.columns.levels[0]:
                            p_df = price_data[t].dropna()
                            if not p_df.empty:
                                curr_price = round(float(p_df['Close'].iloc[-1]), 2)
                                ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                break
                    
                    if not np.isnan(curr_price):
                        last_count = s_data.iloc[-1]['連買計數']
                        
                        # --- 最佳賣出價格邏輯 ---
                        # 以 MA5 加上 8% 作為初步壓力位（乖離率警戒）
                        target_sell_price = round(ma5 * 1.08, 2)
                        
                        results.append({
                            "代號": code,
                            "名稱": name,
                            "歷史發動次數": f"{len(entry_points)} 次",
                            "今日狀態": "🟢 剛發動" if last_count == 1 else f"⚪ 連買 {int(last_count)} 天",
                            "最佳購買日期": "🔥 就在今天" if last_count == 1 else "⏳ 等待回測",
                            "最佳購買價位": curr_price if last_count == 1 else ma5,
                            "預計賣出價格": target_sell_price,
                            "最近發動紀錄": " → ".join([d.strftime('%m/%d') for d in entry_points[-3:]])
                        })
                status.update(label="全功能掃描完成！", state="complete")
            
            res_df = pd.DataFrame(results)
            st.dataframe(res_df, use_container_width=True, hide_index=True)
            st.info("💡 **賣出邏輯說明**：預計賣出價格是以 5 日均線（MA5）為基準，向上計算約 8% 的乖離壓力位。當股價接近此價位且法人買盤縮小時，建議分批獲利結單。")

    else:
        st.warning("請先完成補帳程序。")
