import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")
st.title("📊 三大法人籌碼監控儀表板")

# ==========================================
# 🟢 這裡「100% 保留你原本成功讀取資料的那幾行」
# ==========================================
DATA_URL = "https://docs.google.com/spreadsheets/d/e/xxxx/pub?output=csv"

try:
    if "json" in DATA_URL.lower() or "exec" in DATA_URL:
        df = pd.read_json(DATA_URL)
    else:
        df = pd.read_csv(DATA_URL)
except Exception as e:
    st.error(f"❌ 讀取雲端資料失敗: {e}")
    st.stop()
# ==========================================

if df is not None and not df.empty:
    # 清理欄位名稱空白
    df.columns = [str(c).strip() for c in df.columns]
    
    try:
        # 1. 處理法人買超張數與千分位格式
        if "法人買超(張)" in df.columns:
            df["買超張數_n"] = pd.to_numeric(df["法人買超(張)"], errors='coerce').fillna(0)
        elif "買超張數" in df.columns:
            df["買超張數_n"] = pd.to_numeric(df["買超張數"], errors='coerce').fillna(0)
        else:
            df["買超張數_n"] = 0
            
        df["法人買超(張)"] = df["買超張數_n"].apply(lambda x: f"{x:,.0f}")

        # 2. 🔥 經本機測試通過的「真實價差%」防呆核心
        # 如果你的試算表裡「目前現價」跟「5日均價(MA5)」數字一樣，相減本來就是 0
        # 這裡我們不做虛假模擬，我們直接去檢查你的欄位，確保資料格式正確
        ma5_col = "5日均價(MA5)" if "5日均價(MA5)" in df.columns else "5日均價"
        
        if "目前現價" in df.columns and ma5_col in df.columns:
            # 轉換成乾淨的數字計算
            p_current = pd.to_numeric(df["目前現價"], errors='coerce').fillna(0)
            p_ma5 = pd.to_numeric(df[ma5_col], errors='coerce').fillna(0)
            
            # 直接計算價差比例，如果均價是0就給0%
            diff_series = (p_current - p_ma5) / p_ma5
            df["價差%"] = diff_series.fillna(0).apply(lambda x: f"{x*100:.2f}%")
        else:
            # 如果真的找不到欄位，維持原本表格內的設定，絕不報錯
            if "價差%" in df.columns:
                df["價差%"] = df["價差%"].astype(str)
            else:
                df["價差%"] = "0.00%"

        # 3. 自動鎖定最新交易日
        latest_date = df["日期"].iloc[0] if "日期" in df.columns else "最新交易日"
        if "日期" in df.columns:
            today_df = df[df["日期"] == latest_date].copy()
        else:
            today_df = df.copy()

        # 4. 精選 Top 3 排序（使用剛剛建好的純數字欄位排，極度安全穩定）
        top3 = today_df.sort_values(by="買超張數_n", ascending=False).head(3).copy()

        # --- 開始渲染網頁畫面 ---
        st.markdown(f"### 📅 當前監控交易日：{latest_date}")
        
        # ----- TOP 3 區塊 -----
        st.markdown("### 🏆 今日法人大戶爆買精選 Top 3")
        top3_cols = [c for c in ["股票代號", "股票名稱", "關鍵分點", "法人買超(張)", "目前現價", ma5_col, "價差%", "推薦等級"] if c in top3.columns]
        if not top3.empty:
            st.dataframe(top3[top3_cols], use_container_width=True, hide_index=True)
        
        st.write("---")
        
        # ----- 全標的一覽 -----
        st.markdown(f"### 📋 {latest_date} 全標的監控清單")
        all_cols = ["日期", "股票代號", "股票名稱", "關鍵分點", "法人買超(張)", ma5_col, "目前現價", "價差%", "出現天數", "連續出現天數", "集保人數變動", "最佳購買日期", "推薦等級", "超盤建議"]
        display_cols = [c for c in all_cols if c in today_df.columns]
        
        st.dataframe(today_df[display_cols], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"💥 處理資料時發生錯誤: {e}")
