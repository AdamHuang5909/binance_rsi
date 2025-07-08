from binance.client import Client
from binance import BinanceSocketManager, AsyncClient
from binance.exceptions import BinanceAPIException
import sys
import pandas as pd
import pandas_ta as ta
import logging
import argparse
import asyncio
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    PushMessageRequest,
    TextMessage
)
from linebot.v3.exceptions import InvalidSignatureError

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.FileHandler("rsi_signals.log"),  # 記錄到檔案
        logging.StreamHandler()  # 同時輸出到控制台
    ]
)


# LINE Bot 設定
LINE_CHANNEL_ACCESS_TOKEN = "wJtu5RvwVtVwBtb+x1rdqUnfzPOn+T7NvsaQ/SzsIcIrLxbo+QAVEqw/sX3mavZzD06sV6c/rFB2W/HD2AFNH8OiDU4pXOUy4DuPZmMRudexrRUNhXX5Ts6P2m8ELHoRzSAqw2I+JqrnOSIYwSzgLAdB04t89/1O/w1cDnyilFU="  # 替換為您的 Channel Access Token
LINE_RECIPIENT_ID = "U48cff451fa7f113aa609daff52158f33"  # 替換為 User ID 或 Group ID
line_bot_configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
line_bot_api = MessagingApi(line_bot_configuration)

def calculate_rsi(df, rsi_period=14):
    """計算 RSI 並返回 DataFrame"""
    df["rsi"] = ta.rsi(df["close"], length=rsi_period)
    return df

def check_signals(df, overbought_threshold=90, oversold_threshold=10):
    """檢查 RSI 訊號"""
    latest_rsi = df["rsi"].iloc[-1]
    timestamp = df["timestamp"].iloc[-1]
    message = None
    if latest_rsi > overbought_threshold:
        message = f"超買訊號！時間: {timestamp}, RSI: {latest_rsi:.2f} (> {overbought_threshold})";
        logging.info(message)
    elif latest_rsi < oversold_threshold:
        message = f"超賣訊號！時間: {timestamp}, RSI: {latest_rsi:.2f} (< {oversold_threshold})"
        logging.info(message)
    else:
        # logging.info(f"時間: {timestamp}, RSI: {latest_rsi:.2f} (無訊號)")
        message = f"時間: {timestamp}, RSI: {latest_rsi:.2f} (無訊號)"
        logging.info(message)

    if message :
        # 發送 LINE 通知
        messages = [TextMessage(text=message)]

        with ApiClient(line_bot_configuration) as api_client:
            api_instance = MessagingApi(api_client)
            push_message_request = PushMessageRequest(to=LINE_RECIPIENT_ID, messages=messages)

            try:
                # 發送訊息
                api_instance.push_message(push_message_request)
                logging.info("Push message sent successfully!")
            except InvalidSignatureError as e:
                print(f"Error sending push message: {e}")
                # 檢查是否有更詳細的錯誤資訊
                if hasattr(e, 'status') and hasattr(e, 'body'):
                    print(f"HTTP Status: {e.status}, Response body: {e.body}")
                if hasattr(e, 'headers'):
                    print(f"Request ID: {e.headers.get('x-line-request-id')}")
            except Exception as e:
                print(f"Unexpected error: {e}")

def fetch_historical_rsi(symbol, interval, limit, rsi_period, overbought_threshold, oversold_threshold):
    """獲取歷史數據並計算 RSI"""
    client = Client()
    try:
        klines = client.get_historical_klines(symbol=symbol, interval=interval, limit=limit)
    except BinanceAPIException as e:
        logging.error(f"獲取 {symbol} 數據失敗: {e}")
        return None

    data = [{
        "timestamp": pd.to_datetime(kline[0], unit="ms"),
        "close": float(kline[4])
    } for kline in klines]

    df = pd.DataFrame(data)
    df = calculate_rsi(df, rsi_period)

    if df["rsi"].isna().all():
        logging.error("RSI 計算失敗，數據不足")
        return None

    check_signals(df, overbought_threshold, oversold_threshold)
    return df

async def monitor_realtime_rsi(symbol, interval, rsi_period, overbought_threshold, oversold_threshold, limit=200):
    """使用 WebSocket 監控實時 RSI（異步實現）"""
    # 初始化 AsyncClient
    client = await AsyncClient.create()
    # 獲取歷史 K 線數據作為初始數據

    try:
        klines = await client.get_klines(symbol=symbol, interval=interval, limit=limit)
    except BinanceAPIException as e:
        logging.error(f"獲取 {symbol} 歷史數據失敗: {e}")
        await client.close_connection()
        return

    # 將歷史數據轉為 DataFrame
    closes = [{
        "timestamp": pd.to_datetime(kline[0], unit="ms"),
        "close": float(kline[4])
    } for kline in klines]
    df = pd.DataFrame(closes)

    # 計算初始 RSI
    df = calculate_rsi(df, rsi_period)
    if not df["rsi"].isna().all():
        check_signals(df, overbought_threshold, oversold_threshold)

    # 啟動 WebSocket
    bm = BinanceSocketManager(client)

    async with bm.kline_socket(symbol=symbol, interval=interval) as stream:
        logging.info(f"開始監控 {symbol} 的 RSI（間隔: {interval}, 週期: {rsi_period}）")
        while True:
            try:
                msg = await stream.recv()
                if msg["k"]["x"]:  # 當 K 線完成時
                    close = float(msg["k"]["c"])
                    timestamp = pd.to_datetime(msg["k"]["T"], unit="ms")
                    closes.append({"timestamp": timestamp, "close": close})

                    if len(closes) > limit:
                        closes.pop(0)  # 保持數據量不超過 limit

                    df = pd.DataFrame(closes)
                    df = calculate_rsi(df, rsi_period)

                    if not df["rsi"].isna().all():
                        check_signals(df, overbought_threshold, oversold_threshold)
            except Exception as e:
                logging.error(f"WebSocket 錯誤: {e}")
                break
    # 關閉客戶端連線
    await client.close_connection()

def main():
    """主程式：處理命令列參數並啟動監控"""
    parser = argparse.ArgumentParser(description="監控 Binance 交易對的 RSI 並發送訊號")
    parser.add_argument("--symbol", default="BTCUSDT", help="交易對（例如 BTCUSDT, ETHUSDT）")
    parser.add_argument("--interval", default="1h", help="K 線間隔（例如 1m, 1h, 4h, 1d）")
    parser.add_argument("--rsi_period", type=int, default=14, help="RSI 計算週期（例如 14）")
    parser.add_argument("--overbought", type=float, default=90, help="超買閾值（例如 90）")
    parser.add_argument("--oversold", type=float, default=10, help="超賣閾值（例如 10）")
    parser.add_argument("--limit", type=int, default=200, help="歷史數據點數（例如 200）")
    parser.add_argument("--realtime", action="store_true", help="啟用實時監控（否則僅計算歷史數據）")

    args = parser.parse_args()

    # 將間隔轉為 Binance API 格式
    interval_map = {
        "1m": Client.KLINE_INTERVAL_1MINUTE,
        "5m": Client.KLINE_INTERVAL_5MINUTE,
        "15m": Client.KLINE_INTERVAL_15MINUTE,
        "1h": Client.KLINE_INTERVAL_1HOUR,
        "4h": Client.KLINE_INTERVAL_4HOUR,
        "1d": Client.KLINE_INTERVAL_1DAY
    }
    interval = interval_map.get(args.interval, Client.KLINE_INTERVAL_1HOUR)

    if not args.realtime:
        # 計算歷史 RSI
        df = fetch_historical_rsi(
            symbol=args.symbol,
            interval=interval,
            limit=args.limit,
            rsi_period=args.rsi_period,
            overbought_threshold=args.overbought,
            oversold_threshold=args.oversold
        )
        if df is not None:
            df.to_csv(f"{args.symbol}_rsi.csv", index=False)
            logging.info(f"RSI 數據已保存至 {args.symbol}_rsi.csv")
    else:
        # 修復 Windows 事件循環問題
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        # 啟動實時監控（異步執行）
        asyncio.run(monitor_realtime_rsi(
            symbol=args.symbol,
            interval=interval,
            rsi_period=args.rsi_period,
            overbought_threshold=args.overbought,
            oversold_threshold=args.oversold,
            limit=args.limit
        ))

if __name__ == "__main__":
    main()