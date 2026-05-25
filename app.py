# ===== Python Streamlit 帳號直連 + 完美對齊還原版 =====
import streamlit as st
import pandas as pd
from streamlit_gsheets import GSheetsConnection

st.set_page_config(layout="wide")
st.title("📊 三大法人籌碼監控儀表板")

# --- 1. 使用你綁定好的 Google 帳號直接撈取資料 ---
try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    # 這裡完全自動吃你後台綁定的帳號權限
    df_raw = conn.read(worksheet="stock_Sheet")
except Exception as e:
    st.error(f"❌ 透過 Google 帳號讀取試算表失敗，請檢查後台連線設定。錯誤訊息: {e}")
    st.stop()

# --- 2. 資料結構極致防呆與清理 ---
if df_raw is None or df_raw.empty:
    st.warning("⚠️ 讀取到的分頁目前沒有資料，請確認 stock_Sheet 內有內容。")
else:
    # 清理欄位名稱（去除任何隱形空白）
    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    
    # 🔥 關鍵修正：丟棄因為篩選器產生的第二行空資料 (NaN)
    # 只要「股票代號」或「股票名稱」是空的，就代表不是真正的股票資料，直接過濾掉
    df = df_raw.dropna(subset=["股票代號", "股票名稱"], how="any").copy()
    
    # --- 3. 欄位存在檢查 ---
    required_cols = ["日期", "股票代號", "股票名稱", "關鍵分點", "買超張數", "5日均價", "目前現價", "價差%", "連續出現天數", "集保人數變動", "最佳購買日期", "超盤建議"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        st.error(f"❌ 試算表欄位不匹配！目前缺少欄位: {missing_cols}")
        st.info("💡 目前抓到的表頭有：\n" + ", ".join(df.columns))
    else:
        try:
            # 資料型態轉換，防止排序或格式化崩潰
            df["買超張數_n"] = pd.to_numeric(df["買超張數"], errors='coerce').fillna(0)
            df["股票代號"] = df["股票代號"].astype(str).str.replace("'", "").str.strip()
            
            # 價差% 完美轉換：試算表原始值是 0.06，轉成 6.00% 展示
            df["價差%_n"] = pd.to_numeric(df["價差%"], errors='coerce').fillna(0)
            df["價差%_display"] = df["價差%_n"].apply(lambda x: f"{x*100:.2f}%" if x != 0 else "0.00%")
            
            # 確保日期格式乾淨並抓取最新日期
            df["日期"] = df["日期"].astype(str).str.strip()
            latest_date = df["日期"].iloc[0] if not df.empty else "未知的日期"
            
            # 篩選最新交易日資料
            today_df = df[df["日期"] == latest_date].copy()
            today_df["價差%"] = today_df["價差%_display"] # 取代為漂亮的百分比格式
            
            # 精選 Top 3 邏輯（按買超張數排序）
            top3 = today_df.sort_values(by="買超張數_n", ascending=False).head(3)
            
            # --- 4. 渲染 Streamlit 畫面 ---
            st.markdown(f"### 📅 當前監控交易日：{latest_date}")
            
            # ----- TOP 3 區塊 -----
            st.markdown("### 🏆 今日法人大戶爆買精選 Top 3")
            if not top3.empty:
                st.dataframe(
                    top3[["股票代號", "股票名稱", "關鍵分點", "買超張數", "目前現價", "5日均價", "價差%", "連續出現天數", "超盤建議"]],
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("💡 今日暫無符合標準的 Top 3 標的。")
            
            st.write("---")
            
            # ----- 全標的一覽 -----
            st.markdown(f"### 📋 {latest_date} 全標的監控清單")
            st.dataframe(
                today_df[["日期", "股票代號", "股票名稱", "關鍵分點", "買超張數", "5日均價", "目前現價", "價差%", "連續出現天數", "集保人數變動", "最佳購買日期", "超盤建議"]],
                use_container_width=True,
                hide_index=True
            )
            
        except Exception as e:
            st.error(f"💥 運算資料時發生非預期錯誤: {e}")
