import MetaTrader5 as mt5
import pandas as pd
import logging
from config import config

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def execute_market_order(symbol, volume, direction, stop_loss=0.0, take_profit=0.0,
                         slippage=20, order_comment='', order_magic=1, fill_policy=mt5.ORDER_FILLING_IOC):
    """
    Executes a market order.

    :param symbol: Trading symbol
    :param volume: Volume of the order
    :param direction: 'buy' or 'sell'
    :param stop_loss: Stop loss value
    :param take_profit: Take profit value
    :param slippage: Allowed slippage
    :param order_comment: Comment for the order
    :param order_magic: Magic number for the order
    :param fill_policy: Fill policy
    :return: Order result
    """
    try:
        tick_data = mt5.symbol_info_tick(symbol)
        direction_to_type = {'buy': 0, 'sell': 1}
        direction_to_price = {'buy': tick_data.ask, 'sell': tick_data.bid}

        order_details = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": volume,
            "type": direction_to_type[direction],
            "price": direction_to_price[direction],
            "sl": stop_loss,
            "tp": take_profit,
            "deviation": slippage,
            "magic": order_magic,
            "comment": order_comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": fill_policy,
        }

        result = mt5.order_send(order_details)
        logging.info(f"Market order executed: {result}")
        return result
    except Exception as e:
        logging.error(f"Error executing market order: {e}")
        return None


def liquidate_position(pos, slippage=20, order_magic=1, order_comment='', fill_policy=mt5.ORDER_FILLING_IOC):
    """
    Closes a specific position.

    :param pos: Position to be closed
    :param slippage: Allowed slippage
    :param order_magic: Magic number for the order
    :param order_comment: Comment for the order
    :param fill_policy: Fill policy
    :return: Order result
    """
    try:
        opposite_order_type = {0: mt5.ORDER_TYPE_SELL, 1: mt5.ORDER_TYPE_BUY}
        price_data = {0: mt5.symbol_info_tick(pos['symbol']).bid, 1: mt5.symbol_info_tick(pos['symbol']).ask}

        close_order = {
            "action": mt5.TRADE_ACTION_DEAL,
            "position": pos['ticket'],
            "symbol": pos['symbol'],
            "volume": pos['volume'],
            "type": opposite_order_type[pos['type']],
            "price": price_data[pos['type']],
            "deviation": slippage,
            "magic": order_magic,
            "comment": order_comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": fill_policy,
        }

        logging.info(f"Closing position: {close_order}")
        result = mt5.order_send(close_order)
        logging.info(f"Position closed: {result}")
        return result
    except Exception as e:
        logging.error(f"Error closing position: {e}")
        return None


def close_all_trades(order_direction, fill_policy=mt5.ORDER_FILLING_IOC):
    """
    Closes all open trades.

    :param order_direction: 'buy', 'sell', or 'all'
    :param fill_policy: Fill policy
    """
    direction_to_type = {'buy': 0, 'sell': 1}

    if mt5.positions_total() > 0:
        all_positions = mt5.positions_get()
        positions_frame = pd.DataFrame(all_positions, columns=all_positions[0]._asdict().keys())

        if order_direction != 'all':
            positions_frame = positions_frame[positions_frame['type'] == direction_to_type[order_direction]]

        for _, pos in positions_frame.iterrows():
            result = liquidate_position(pos, fill_policy=fill_policy)
            logging.info(f"Order result: {result}")


def retrieve_open_positions():
    """
    Retrieves all open positions.

    :return: DataFrame of open positions
    """
    if mt5.positions_total():
        all_positions = mt5.positions_get()
        positions_frame = pd.DataFrame(all_positions, columns=all_positions[0]._asdict().keys())
        return positions_frame
    else:
        return pd.DataFrame()


def main():
    # Initialize connection to MetaTrader 5
    if not mt5.initialize():
        logging.error("MetaTrader 5 initialization failed")
        return

    # Login to MetaTrader 5
    if not mt5.login(config['account_number'], password=config['password'], server=config['server']):
        logging.error("MetaTrader 5 login failed")
        mt5.shutdown()
        return

    # Example usage of the functions
    symbol = config['symbol']
    volume = config['volume']
    direction = config['direction']

    logging.info("Placing market order")
    execute_market_order(symbol, volume, direction)

    logging.info("Retrieving open positions")
    positions = retrieve_open_positions()
    logging.info(f"Open positions: {positions}")

    # Close all trades
    logging.info("Closing all trades")
    close_all_trades('all')

    # Shutdown MetaTrader 5
    mt5.shutdown()

if __name__ == "__main__":
    main()
