# ===== Python Streamlit 帳號直連 + 雙層標頭完美對齊版 =====
import streamlit as st
import pandas as pd
from streamlit_gsheets import GSheetsConnection

st.set_page_config(layout="wide")
st.title("📊 三大法人籌碼監控儀表板")

# --- 1. 使用你原本就綁定好的 Google 帳號直連讀取 ---
try:
    # 這裡會直接吃你後台設定好的帳號憑證，完全不需要手動貼 ID 網址！
    conn = st.connection("gsheets", type=GSheetsConnection)
    raw_df = conn.read(worksheet="stock_Sheet")
except Exception as e:
    st.error(f"❌ 透過 Google 帳號讀取試算表失敗，請檢查後台連線設定。錯誤訊息: {e}")
    st.stop()

# --- 2. 核心修正：處理雙層標頭（跳過第一行舊標頭，將第二行設為欄位） ---
if raw_df is None or raw_df.empty:
    st.warning("⚠️ 讀取到的分頁目前沒有資料，請確認 stock_Sheet 內有內容。")
else:
    try:
        # 修正錯位：直接將第 1 行（Python 索引 0）當作真正的欄位名稱
        # 並把後面的資料切下來當作真正的內容
        corrected_df = raw_df.copy()
        
        # 抓取你試算表第二行的中文字（日期、股票代號、股票名稱...）
        new_columns = [str(c).strip() for c in corrected_df.iloc[0].tolist()]
        corrected_df.columns = new_columns
        
        # 切掉第一行（因為那一行已經被拿來當欄位名稱了）
        df = corrected_df.iloc[1:].reset_index(drop=True)
        
        # 欄位名稱再次極致清理，防止隱形空白
        df.columns = [str(c).strip() for c in df.columns]
        
    except Exception as e:
        st.error(f"💥 處理雙層標頭對齊時發生錯誤: {e}")
        st.stop()

    # --- 3. 欄位安全檢查與呈現 ---
    required_cols = ["日期", "股票代號", "股票名稱", "關鍵分點", "買超張數", "5日均價", "目前現價", "價差%", "連續出現天數", "集保人數變動", "最佳購買日期", "超盤建議"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        st.error(f"❌ 試算表欄位不匹配！目前缺少欄位: {missing_cols}")
        st.info("💡 目前從第二行自動對齊到的表頭有：\n" + ", ".join(df.columns))
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
