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

# ====================== 2. 分頁邏輯 B: 籌碼週期分析 (新增價位分析) ======================
if mode == "籌碼週期分析":
    st.title("🔍 1/1 至今：最佳進場日期與價位分析")
    
    if os.path.exists(DATA_FILE):
        db = pd.read_parquet(DATA_FILE).sort_values(['證券代號', '日期'])
        db['買超張數'] = (db['三大法人買賣超股數'] / 1000).round(1)
        db['買超正'] = db['買超張數'] > 100
        db['連買計數'] = db.groupby('證券代號')['買超正'].transform(lambda x: x * (x.groupby((x != x.shift()).cumsum()).cumcount() + 1))
        
        if st.button("📊 啟動全年度量價週期掃描"):
            cycle_stocks = db[db['連買計數'] >= 3]['證券代號'].unique()
            results = []
            
            with st.status("正在計算歷史發動點與支撐價位...") as status:
                # 為了計算價位，我們需要抓取最新的市場價格
                codes = cycle_stocks[:40].tolist() # 掃描前 40 檔
                tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
                price_data = yf.download(tickers, period="10d", interval="1d", group_by='ticker', progress=False)
                
                for code in codes:
                    s_data = db[db['證券代號'] == code].copy()
                    name = s_data['證券名稱'].iloc[0]
                    entry_points = s_data[s_data['連買計數'] == 1]['日期'].tolist()
                    
                    # 獲取現價與 MA5
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
                        
                        # --- 最佳購買價位邏輯 ---
                        # 如果剛發動，建議價就是現價附近
                        # 如果已發動多日，建議價是 MA5 (回測支撐)
                        if last_count == 1:
                            best_price = curr_price
                            strategy = f"🚀 現價 {curr_price} 直接進場"
                        else:
                            best_price = ma5
                            strategy = f"🎣 建議掛單: {ma5} (MA5)"
                        
                        results.append({
                            "代號": code,
                            "名稱": name,
                            "目前現價": curr_price,
                            "5日均價(支撐)": ma5,
                            "今日狀態": "🟢 今日發動" if last_count == 1 else f"⚪ 連買 {int(last_count)} 天",
                            "最佳購買價位": best_price,
                            "操作建議": strategy
                        })
                status.update(label="價位分析完成！", state="complete")
            
            res_df = pd.DataFrame(results)
            st.dataframe(res_df, use_container_width=True, hide_index=True)
            st.info("💡 **操盤手筆記**：\n- 當法人連買超過 3 天，股價通常已經遠離 MA5。此時「最佳購買價位」會設在 MA5 附近，等它拉回再買，勝率最高。")

    else:
        st.warning("請先完成補帳。")
