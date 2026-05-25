import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")
st.title("📊 三大法人籌碼監控儀表板")

# ==========================================================
# 🟢 核心讀取區：請把下方這段換成你原本「唯一成功秀出黑表格」的讀取程式碼
# ==========================================================
try:
    # 這裡只是防呆示範，請用你原本寫在 code 裡會通的那幾行（例如 st.connection 或 pd.read_csv）直接覆蓋
    # 範例：
    # conn = st.connection("gsheets")
    # df_raw = conn.read(worksheet="stock_Sheet")
    
    # 暫時沿用 df_raw 作為變數名稱
    df_raw = st.connection("gsheets").read(worksheet="stock_Sheet")
    
except Exception as e:
    st.error(f"❌ 讀取資料來源失敗，請確認連線設定。錯誤訊息: {e}")
    st.stop()
# ==========================================================


# --- 資料清理與核心邏輯運算 ---
if df_raw is None or df_raw.empty:
    st.warning("⚠️ 目前讀取到的資料庫為空，請確認雲端試算表內有資料。")
else:
    # 複製一份資料進行清洗，避免更動原始資料
    df = df_raw.copy()
    
    # 清理所有欄位名稱的隱形空白（全面使用 df.columns 確保不噴 NameError）
    df.columns = [str(c).strip() for c in df.columns]
    
    try:
        # 1. 關鍵對齊：解決 Top 3 排序報錯 (KeyError)
        # 判斷你的欄位到底是「法人買超(張)」還是「買超張數」，並統一轉換為數字計算
        if "法人買超(張)" in df.columns:
            df["買超張數_n"] = pd.to_numeric(df["法人買超(張)"], errors='coerce').fillna(0)
            main_buy_col = "法人買超(張)"
        elif "買超張數" in df.columns:
            df["買超張數_n"] = pd.to_numeric(df["買超張數"], errors='coerce').fillna(0)
            main_buy_col = "買超張數"
        else:
            df["買超張數_n"] = 0
            main_buy_col = None

        # 2. 處理「5日均價」欄位別名對齊
        if "5日均價(MA5)" in df.columns:
            main_ma5_col = "5日均價(MA5)"
        elif "5日均價" in df.columns:
            main_ma5_col = "5日均價"
        else:
            main_ma5_col = None

        # 3. 處理「價差%」：若是 0 或小數點，自動美化成 XX.XX% 格式
        if "價差%" in df.columns:
            df["價差%_n"] = pd.to_numeric(df["價差%"], errors='coerce').fillna(0)
            # 如果欄位裡全都是 0，就顯示 0.00%；若有數值（如 0.06），則轉成 6.00%
            df["價差%_display"] = df["價差%_n"].apply(lambda x: f"{x*100:.2f}%" if x != 0 else "0.00%")
            df["價差%"] = df["價差%_display"]

        # 4. 股票代號防呆（防止文字格式前後有單引號或空白）
        if "股票代號" in df.columns:
            df["股票代號"] = df["股票代號"].astype(str).str.replace("'", "").str.strip()

        # 5. 自動日期篩選：永遠只抓最新一天的交易資料
        if "日期" in df.columns:
            df["日期"] = df["日期"].astype(str).str.strip()
            # 抓取第一列的日期作為最新交易日
            latest_date = df["日期"].iloc[0] if not df.empty else "未知的日期"
            today_df = df[df["日期"] == latest_date].copy()
        else:
            latest_date = "未分類日期"
            today_df = df.copy()

        # 6. 計算 Top 3 爆買標的（使用我們建立的隱藏數值欄位排序，保證不 KeyError）
        top3 = today_df.sort_values(by="買超張數_n", ascending=False).head(3)

        # ==========================================================
        # 🔵 網頁前端畫面渲染區
        # ==========================================================
        st.markdown(f"### 📅 當前監控交易日：{latest_date}")
        
        # ----- 🏆 TOP 3 精選區塊 -----
        st.markdown("### 🏆 今日法人大戶爆買精選 Top 3")
        
        # 定義 Top 3 想要秀出的理想欄位順序
        ideal_top3_cols = ["股票代號", "股票名稱", "關鍵分點", main_buy_col, "目前現價", main_ma5_col, "價差%", "推薦等級"]
        # 防呆過濾：只抽出現實真的存在的欄位，避免展示時出錯
        actual_top3_cols = [c for c in ideal_top3_cols if c and c in top3.columns]
        
        if not top3.empty and main_buy_col:
            st.dataframe(
                top3[actual_top3_cols],
                use_container_width=True,
                hide_index=True  # 拔掉左側 0, 1, 2 序號，更像看盤軟體
            )
        else:
            st.info("💡 今日暫無符合標準的 Top 3 標的。")
            
        st.write("---")
        
        # ----- 📋 全標的一覽區塊 -----
        st.markdown(f"### 📋 {latest_date} 全標的監控清單 (買超 ≥ 500張)")
        
        if not today_df.empty:
            # 移除用於後台運算的暫存隱藏欄位，不展示給使用者看
            display_today_df = today_df.drop(columns=[col for col in ["買超張數_n", "價差%_n", "價差%_display"] if col in today_df.columns])
            
            st.dataframe(
                display_today_df,
                use_container_width=True,
                hide_index=True  # 拔掉左側 0, 1, 2 序號
            )
        else:
            st.info("💡 今日無符合買超大於 500 張的監控標的。")

    except Exception as e:
        st.error(f"💥 籌碼資料運算或欄位對齊時發生錯誤: {e}")
