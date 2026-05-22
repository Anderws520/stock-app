import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import re

# ====================== 1. 核心系統設定 ======================
st.set_page_config(page_title="台股法人戰術分析系統", layout="wide", initial_sidebar_state="collapsed")

st.title("📈 台股法人籌碼戰術分析系統")
st.caption("⚡ 已全面移除非法爬蟲機制，改由交易員主動提供籌碼大數據，100% 避開伺服器阻擋。")

# ====================== 2. 側邊欄：數據輸入與切換 ======================
with st.sidebar:
    st.title("⚒️ 操盤工具箱")
    mode = st.radio("功能切換", ["今日強勢戰報", "籌碼週期分析"], index=0)
    st.markdown("---")
    
    st.header("📥 籌碼數據輸入")
    st.markdown("請將今日看盤軟體或三大法人買超個股，依格式輸入於下方（支援多行貼上）：")
    
    # 提供預設範例，方便使用者一打開就能看到效果
    default_input = "2330 台積電 1500\n2317 鴻海 800\n2454 聯發科 -300\n2603 長榮 2500"
    raw_input = st.text_area("格式：[代號] [名稱] [買超張數/股數]", value=default_input, height=250)

# ====================== 3. 解析輸入數據 ======================
def parse_user_input(text):
    records = []
    lines = text.strip().split('\n')
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # 使用正規表達式切分 數字(代號)、文字(名稱)、數字(數量)
        tokens = line.split()
        if len(tokens) >= 2:
            # 尋找代號（純數字）
            code = None
            name = "未知"
            volume = 0.0
            
            # 簡單的解析邏輯
            code_match = re.search(r'\d+', tokens[0])
            if code_match:
                code = code_match.group()
                if len(tokens) == 2:
                    # 只有代號跟數量 (例如: 2330 1500)
                    try: volume = float(tokens[1])
                    except: pass
                elif len(tokens) >= 3:
                    # 代號、名稱、數量 (例如: 2330 台積電 1500)
                    name = tokens[1]
                    try: volume = float(tokens[2])
                    except: pass
            else:
                # 也許代號在後面，嘗試從整行抓第一個四碼以上的數字
                all_nums = re.findall(r'-?\d+', line)
                if len(all_nums) >= 1:
                    code = all_nums[0]
                    # 數量取最後一個數字
                    try: volume = float(all_nums[-1])
                    except: pass
            
            if code:
                # 處理張數與股數的模糊判定（通常法人買超張數很少單日超過 10 萬張，若數字極大視為股數，除以 1000）
                if abs(volume) > 100000:
                    volume = volume / 1000.0
                records.append({
                    "證券代號": code,
                    "證券名稱": name,
                    "買超張數": round(volume, 1),
                    "買超正": volume > 0
                })
    return pd.DataFrame(records)

# ====================== 4. 戰報顯示主面板 ======================
if raw_input.strip():
    df_input = parse_user_input(raw_input)
    
    if not df_input.empty:
        codes = df_input['證券代號'].tolist()
        # 同時打包上市與上櫃的 yfinance 格式進行查詢
        tickers = [f"{s}.TW" for s in codes] + [f"{s}.TWO" for s in codes]
        
        if mode == "今日強勢戰報":
            st.subheader("🚀 今日法人強勢戰報")
            # 過濾出買超大於 0 的標的進行即時分析
            df_positive = df_input[df_input['買超張數'] >= 50].sort_values('買超張數', ascending=False).head(50)
            
            if not df_positive.empty:
                with st.spinner("⚡ 正在同步 Yahoo Finance 即時國際盤價線..."):
                    price_data = yf.download(df_positive['證券代號'].tolist(), period="5d", interval="1d", group_by='ticker', progress=False)
                    
                    res_today = []
                    for _, row in df_positive.iterrows():
                        s = row['證券代號']
                        for suffix in [".TW", ".TWO"]:
                            t = f"{s}{suffix}"
                            # 如果這個後綴存在於 yfinance 回傳的資料中
                            if t in price_data.columns.levels[0] if isinstance(price_data.columns, pd.MultiIndex) else t in price_data.columns:
                                p_df = price_data[t].dropna() if isinstance(price_data.columns, pd.MultiIndex) else price_data.dropna()
                                if not p_df.empty:
                                    curr = round(float(p_df['Close'].iloc[-1]), 2)
                                    ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                    diff_pct = round(((curr - ma5) / ma5 * 100), 2)
                                    
                                    res_today.append({
                                        "代號": s,
                                        "名稱": row['證券名稱'],
                                        "法人買超(張)": row['買超張數'],
                                        "當前現價": curr,
                                        "5日均線價": ma5,
                                        "乖離價差%": f"{diff_pct}%",
                                        "操盤策略建議": "🚀 籌碼剛發動" if diff_pct < 3 else "⏳ 順勢緊跟法人"
                                    })
                                    break
                    
                    if res_today:
                        st.dataframe(pd.DataFrame(res_today), use_container_width=True, hide_index=True)
                    else:
                        st.warning("無法取得即時市場報價，請確認代號是否正確。")
            else:
                st.warning("⚠️ 目前輸入的資料中，沒有單日買超大於 50 張的陽線標的。")

        elif mode == "籌碼週期分析":
            st.subheader("🔄 法人籌碼波段週期分析")
            
            with st.spinner("🔍 正在進行多核心波段回測分析..."):
                price_data_c = yf.download(df_input['證券代號'].tolist(), period="20d", interval="1d", group_by='ticker', progress=False)
                
                res_cycle = []
                for _, row in df_input.iterrows():
                    c = row['證券代號']
                    for suf in [".TW", ".TWO"]:
                        t = f"{c}{suf}"
                        if t in price_data_c.columns.levels[0] if isinstance(price_data_c.columns, pd.MultiIndex) else t in price_data_c.columns:
                            p_df = price_data_c[t].dropna() if isinstance(price_data_c.columns, pd.MultiIndex) else price_data_c.dropna()
                            if not p_df.empty:
                                curr = round(float(p_df['Close'].iloc[-1]), 2)
                                ma5 = round(float(p_df['Close'].tail(5).mean()), 2)
                                # 計算近 10 日的平均真實波幅 (High - Low)
                                avg_r = (p_df['High'] - p_df['Low']).tail(10).mean()
                                
                                # Grok 經典核心演算法：計算支撐買點與預期擴展賣點
                                buy_pt = round(min(ma5, p_df['Low'].tail(3).min()), 2)
                                sell_pt = round(curr + (avg_r * 1.6), 2)
                                expected_gain = round(sell_pt - curr, 2)
                                
                                res_cycle.append({
                                    "代號": c,
                                    "名稱": row['證券名稱'],
                                    "當前現價": curr,
                                    "建議潛伏買點": buy_pt,
                                    "預期滿足賣點": sell_pt,
                                    "期望波段價差": expected_gain,
                                    "戰術黃金買日": "🔥 處於低軌估值" if curr <= ma5 else "⏳ 等待拉回均線",
                                })
                                break
                                
                if res_cycle:
                    df_cycle = pd.DataFrame(res_cycle).sort_values('期望波段價差', ascending=False)
                    st.dataframe(df_cycle, use_container_width=True, hide_index=True)
                else:
                    st.warning("無法取得週期歷史數據。")
    else:
        st.error("❌ 無法解析輸入的文字，請確認是否包含股票代號。")
else:
    st.info("💡 請在左側工具箱的輸入框內貼上或輸入籌碼數據。")
