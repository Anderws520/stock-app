import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")
st.title("📊 三大法人籌碼監控儀表板")

# =========================================================================
# 🟢 終極網址轉換：將您提供的原始網址，自動轉換為標準的 CSV 匯出串流網址
# =========================================================================
ORIGINAL_URL = "https://docs.google.com/spreadsheets/d/1GjcN6DSFWwJG14bPyMW8aNUkE70Auz6BQFPJ9EGzR38/edit?gid=287857197#gid=287857197"
CSV_URL = ORIGINAL_URL.replace("/edit?", "/export?format=csv&").split("#")[0]

# --- 1. 資料讀取區塊 ---
try:
    df = pd.read_csv(CSV_URL)
except Exception as e:
    st.error(f"❌ 讀取雲端試算表失敗，請確認該試算表是否已開啟「知道連結的任何人都能檢視」權限。")
    st.error(f"詳細錯誤訊息: {e}")
    st.stop()

# --- 2. 資料清洗與對齊處理 ---
if df is None or df.empty:
    st.warning("⚠️ 目前讀取到的資料庫是空的，請確認雲端試算表內是否有資料。")
else:
    # 移除欄位名稱前後可能殘留的隱形空白
    df.columns = [str(c).strip() for c in df.columns]
    
    try:
        # 📌 欄位精準對齊一：處理「法人買超(張)」數值轉換（供 Top 3 排序使用）
        if "法人買超(張)" in df.columns:
            df["買超張數_n"] = pd.to_numeric(df["法人買超(張)"], errors='coerce').fillna(0)
            df["顯示_法人買超(張)"] = df["買超張數_n"].apply(lambda x: f"{x:,.0f}" if x != 0 else "0")
        elif "買超張數" in df.columns:
            df["買超張數_n"] = pd.to_numeric(df["買超張數"], errors='coerce').fillna(0)
            df["顯示_法人買超(張)"] = df["買超張數_n"].apply(lambda x: f"{x:,.0f}" if x != 0 else "0")
        else:
            df["買超張數_n"] = 0
            df["顯示_法人買超(張)"] = "0"

        # 📌 欄位精準對齊二：自動修正「價差%」原本顯示 0 的問題
        # 利用「目前現價」與「5日均價(MA5)」在後台即時運算：(現價 - 5日線) / 5日線
        ma5_col = "5日均價(MA5)" if "5日均價(MA5)" in df.columns else "5日均價"
        
        if "目前現價" in df.columns and ma5_col in df.columns:
            price_now = pd.to_numeric(df["目前現價"], errors='coerce').fillna(0)
            price_ma5 = pd.to_numeric(df[ma5_col], errors='coerce').fillna(0)
            
            df["價差%_calculated"] = (price_now - price_ma5) / price_ma5
            df["價差%_calculated"] = df["價差%_calculated"].fillna(0)
            df["價差%"] = df["價差%_calculated"].apply(lambda x: f"{x*100:.2f}%" if x != 0 else "0.00%")
        else:
            df["價差%"] = "0.00%"

        # 📌 欄位精準對齊三：清理股票代號格式
        if "股票代號" in df.columns:
            df["股票代號"] = df["股票代號"].astype(str).str.replace("'", "").str.strip()

        # 📌 欄位精準對齊四：動態鎖定最新交易日
        latest_date = "監控清單"
        if "日期" in df.columns and not df.empty:
            df["日期"] = df["日期"].astype(str).str.strip()
            latest_date = df["日期"].iloc[0]
            today_df = df[df["日期"] == latest_date].copy()
        else:
            today_df = df.copy()

        # 📌 欄位精準對齊五：打造不閃退的「爆買 Top 3」排序
        # 以隱藏的數值欄位 `買超張數_n` 進行降冪排序，完美根除 KeyError
        top3 = today_df.sort_values(by="買超張數_n", ascending=False).head(3).copy()
        
        # 將要呈現給使用者的「法人買超(張)」替換成加了千分位、漂亮的整數格式
        if not top3.empty:
            top3["法人買超(張)"] = top3["顯示_法人買超(張)"]
        today_df["法人買超(張)"] = today_df["顯示_法人買超(張)"]

        # --- 3. 前端儀表板渲染呈現 ---
        st.markdown(f"### 📅 當前監控交易日：{latest_date}")
        
        # ----- 🏆 TOP 3 區塊 -----
        st.markdown("### 🏆 今日法人大戶爆買精選 Top 3")
        top3_target_cols = ["股票代號", "股票名稱", "關鍵分點", "法人買超(張)", "目前現價", ma5_col, "價差%", "推薦等級"]
        top3_display_cols = [c for c in top3_target_cols if c in top3.columns]
        
        if not top3.empty:
            # hide_index=True 可以拔掉最左邊難看的 0, 1, 2 序號
            st.dataframe(top3[top3_display_cols], use_container_width=True, hide_index=True)
        else:
            st.info("💡 今日暫無符合標準的 Top 3 標的。")
            
        st.write("---")
        
        # ----- 📋 全標的一覽 -----
        st.markdown(f"### 📋 {latest_date} 全標的監控清單")
        all_target_cols = ["日期", "股票代號", "股票名稱", "關鍵分點", "法人買超(張)", ma5_col, "目前現價", "價差%", "出現天數", "連續出現天數", "集保人數變動", "最佳購買日期", "推薦等級", "超盤建議"]
        all_display_cols = [c for c in all_target_cols if c in today_df.columns]
        
        if not all_display_cols:
            all_display_cols = today_df.columns.tolist()
            
        st.dataframe(today_df[all_display_cols], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"💥 資料運算或格式對齊時發生非預期錯誤: {e}")
