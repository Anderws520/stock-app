import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")
st.title("📊 三大法人籌碼監控儀表板")

# ==========================================
# 🟢 這裡請「100% 保留你原本讀取資料的那幾行」
# 也就是你最當初能成功秀出表格、沒噴連線錯誤的那段程式碼！
# ==========================================
# 範例（請用你自己本來成功讀取 df 的代碼覆蓋這段）：
# df = pd.read_csv("你原本的網址") 
# ==========================================

if 'df' in locals() and df is not None and not df.empty:
    # 1. 清理欄位隱形空白
    df.columns = [str(c).strip() for c in df.columns]
    
    try:
        # 2. 依據你的截圖 (1000018361.jpg)，欄位名稱叫「法人買超(張)」
        # 進行安全的數值轉換，並建立排序用的暫存欄位
        if "法人買超(張)" in df.columns:
            df["買超張數_n"] = pd.to_numeric(df["法人買超(張)"], errors='coerce').fillna(0)
        elif "買超張數" in df.columns:
            df["買超張數_n"] = pd.to_numeric(df["買超張數"], errors='coerce').fillna(0)
        else:
            df["買超張數_n"] = 0

        # 3. 處理「價差%」：如果讀出來是 0，就保持 0.00%，如果有數值就轉成百分比
        if "價差%" in df.columns:
            df["價差%_n"] = pd.to_numeric(df["價差%"], errors='coerce').fillna(0)
            df["價差%_display"] = df["價差%_n"].apply(lambda x: f"{x*100:.2f}%" if x != 0 else "0.00%")
            df["價差%"] = df["價差%_display"]

        # 4. 確保股票代號不會變成浮點數
        if "股票代號" in df.columns:
            df["股票代號"] = df["股票代號"].astype(str).str.replace("'", "").str.strip()

        # 5. 自動抓最新日期並篩選
        latest_date = "未知的日期"
        if "日期" in df.columns and not df.empty:
            df["日期"] = df["日期"].astype(str).str.strip()
            latest_date = df["日期"].iloc[0]
            today_df = df[df["日期"] == latest_date].copy()
        else:
            today_df = df.copy()

        # 6. 正確進行 Top 3 排序（解決 KeyError 的關鍵）
        # 依據我們剛剛建立的數值型態欄位 "買超張數_n" 排序，絕對不會再報錯！
        top3 = today_df.sort_values(by="買超張數_n", ascending=False).head(3)

        # --- 渲染網頁畫面 ---
        st.markdown(f"### 📅 當前監控交易日：{latest_date}")
        
        # ----- TOP 3 區塊 -----
        st.markdown("### 🏆 今日法人大戶爆買精選 Top 3")
        
        # 動態抓取你想顯示在 Top 3 的現有欄位，防呆不報錯
        top3_cols = [c for c in ["股票代號", "股票名稱", "關鍵分點", "法人買超(張)", "目前現價", "5日均價(MA5)", "價差%", "推薦等級"] if c in top3.columns]
        if not top3.empty:
            st.dataframe(top3[top3_cols], use_container_width=True, hide_index=True)
        else:
            st.info("💡 今日暫無符合標準的 Top 3 標的。")
        
        st.write("---")
        
        # ----- 全標的一覽 -----
        st.markdown(f"### 📋 {latest_date} 全標的監控清單")
        # 直接秀出全欄位，並用 hide_index=True 拔掉左邊礙眼的 0,1,2 序號
        st.dataframe(today_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"💥 資料運算時發生錯誤: {e}")
