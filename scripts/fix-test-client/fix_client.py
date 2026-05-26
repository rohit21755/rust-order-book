#!/usr/bin/env python3
"""
Minimal FIX 4.2 initiator using `quickfix` Python bindings.
Sends one NewOrderSingle (D) + one OrderCancelRequest (F); logs ExecutionReports.

Install:
    pip install quickfix

Run:
    python fix_client.py --cfg initiator.cfg --symbol BTC-USDT --side BUY --qty 1 --price 100
"""
import argparse
import sys
import time
import uuid

try:
    import quickfix as fix
    import quickfix42 as fix42
except ImportError:
    sys.stderr.write("Install: pip install quickfix\n")
    sys.exit(1)


class Application(fix.Application):
    def __init__(self):
        super().__init__()
        self.session_id = None
        self.execs = []

    def onCreate(self, sessionID):
        print(f"[fix] session created {sessionID}")

    def onLogon(self, sessionID):
        self.session_id = sessionID
        print(f"[fix] logon {sessionID}")

    def onLogout(self, sessionID):
        print(f"[fix] logout {sessionID}")

    def toAdmin(self, msg, sessionID): pass
    def fromAdmin(self, msg, sessionID): pass
    def toApp(self, msg, sessionID): pass

    def fromApp(self, msg, sessionID):
        msg_type = fix.MsgType()
        msg.getHeader().getField(msg_type)
        if msg_type.getValue() == fix.MsgType_ExecutionReport:
            self.execs.append(msg.toString())
            print(f"[fix] EXEC: {msg.toString().replace(chr(1), '|')}")


def send_new_order(app, symbol, side, qty, price):
    cl_ord_id = f"cli-{uuid.uuid4()}"
    o = fix42.NewOrderSingle(
        fix.ClOrdID(cl_ord_id),
        fix.HandlInst('1'),
        fix.Symbol(symbol),
        fix.Side(fix.Side_BUY if side.upper() == 'BUY' else fix.Side_SELL),
        fix.TransactTime(),
        fix.OrdType(fix.OrdType_LIMIT if price else fix.OrdType_MARKET),
    )
    o.setField(fix.OrderQty(float(qty)))
    if price:
        o.setField(fix.Price(float(price)))
    fix.Session.sendToTarget(o, app.session_id)
    print(f"[fix] sent NewOrderSingle clOrdID={cl_ord_id}")
    return cl_ord_id


def send_cancel(app, orig_cl_ord_id, symbol, side, qty):
    cancel = fix42.OrderCancelRequest(
        fix.OrigClOrdID(orig_cl_ord_id),
        fix.ClOrdID(f"cancel-{uuid.uuid4()}"),
        fix.Symbol(symbol),
        fix.Side(fix.Side_BUY if side.upper() == 'BUY' else fix.Side_SELL),
        fix.TransactTime(),
    )
    cancel.setField(fix.OrderQty(float(qty)))
    fix.Session.sendToTarget(cancel, app.session_id)
    print("[fix] sent OrderCancelRequest")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cfg', default='initiator.cfg')
    ap.add_argument('--symbol', default='BTC-USDT')
    ap.add_argument('--side', default='BUY')
    ap.add_argument('--qty', default='1')
    ap.add_argument('--price', default='100')
    ap.add_argument('--cancel-after', type=float, default=2.0)
    args = ap.parse_args()

    settings = fix.SessionSettings(args.cfg)
    app = Application()
    store = fix.FileStoreFactory(settings)
    logf  = fix.FileLogFactory(settings)
    factory = fix.DefaultMessageFactory()
    initiator = fix.SocketInitiator(app, store, settings, logf, factory)
    initiator.start()

    # wait for logon
    for _ in range(50):
        if app.session_id: break
        time.sleep(0.1)
    if not app.session_id:
        print("logon failed"); initiator.stop(); sys.exit(1)

    clid = send_new_order(app, args.symbol, args.side, args.qty, args.price)
    time.sleep(args.cancel_after)
    send_cancel(app, clid, args.symbol, args.side, args.qty)
    time.sleep(2)
    initiator.stop()


if __name__ == '__main__':
    main()
