def get_prices_with_yfinance(stock_codes):
    """利用 yfinance 快速批次抓取收盤價與 MA5"""
    prices_info = {}
    if not stock_codes:
        return prices_info
        
    # 準備所有可能的代碼 (上市 .TW, 上櫃 .TWO)
    tickers_tw = [f"{s}.TW" for s in stock_codes]
    tickers_two = [f"{s}.TWO" for s in stock_codes]
    all_tickers = tickers_tw + tickers_two

    try:
        # 一次下載過去 10 天的數據，效率最高
        data = yf.download(all_tickers, period="10d", interval="1d", group_by='ticker', progress=False)
        
        for stock in stock_codes:
            # 優先嘗試 .TW，再嘗試 .TWO
            for suffix in [".TW", ".TWO"]:
                t_str = f"{stock}{suffix}"
                if t_str in data.columns.levels[0]:
                    s_data = data[t_str].dropna()
                    if not s_data.empty:
                        curr_p = round(float(s_data['Close'].iloc[-1]), 2)
                        ma5_p = round(float(s_data['Close'].tail(5).mean()), 2)
                        prices_info[stock] = {
                            '目前現價': curr_p,
                            '5日均價': ma5_p
                        }
                        break # 抓到就跳過該股其他後綴
    except Exception as e:
        st.error(f"yfinance 抓取出錯: {e}")
        
    return prices_info
