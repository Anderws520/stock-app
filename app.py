# ===== Python Streamlit 端 欄位綁定對齊修正 =====
import streamlit as st
import pandas as pd

# 假設你用 st.connection 或 gspread 讀進來的資料叫做 df
# df = conn.read(...)

# 1. 確保 DataFrame 欄位名稱防呆（移除前後空白）
df.columns = [str(c).strip() for c in df.columns]

# 2. 核心計算與排序修正 (精準對齊 GAS 輸出的「法人買超(張)」與「推薦等級」)
try:
    # 範例：篩選出值得關注或強烈推薦的標的
    filtered_df = df[df["推薦等級"].str.contains("推薦|關注", na=False)]
    
    # 修正關鍵：原本的 "買超_n" 欄位賦值，必須去接 Google 試算表目前的 "法人買超(張)"
    df["買超_n"] = pd.to_numeric(df["法人買超(張)"], errors='coerce')
    filtered_df["買超_n"] = pd.to_numeric(filtered_df["法人買超(張)"], errors='coerce')
    
    # 排序修正：由大到小排出前三名
    top3 = filtered_df.sort_values(by="買超_n", ascending=False).head(3)
    
    # 在 Streamlit 上漂亮呈現
    st.write("🏆 今日法人精選 Top 3 標的：")
    st.dataframe(top3[["股票代號", "股票名稱", "法人買超(張)", "目前現價", "操盤建議"]])

except KeyError as e:
    st.error(f"欄位對齊失敗，找不到欄位: {e}")
    st.info("請檢查 Google 試算表第一行是否已成功更新為標準黃金欄位。")
