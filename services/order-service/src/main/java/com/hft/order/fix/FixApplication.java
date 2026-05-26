package com.hft.order.fix;

import com.hft.order.cqrs.OrderCommandHandler;
import com.hft.order.cqrs.commands.CancelOrderCommand;
import com.hft.order.cqrs.commands.SubmitOrderCommand;
import com.hft.order.domain.OrderSide;
import com.hft.order.domain.OrderType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import quickfix.Application;
import quickfix.DataDictionary;
import quickfix.DoNotSend;
import quickfix.FieldNotFound;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.Message;
import quickfix.MessageUtils;
import quickfix.RejectLogon;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.UnsupportedMessageType;
import quickfix.field.AvgPx;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.ExecID;
import quickfix.field.ExecType;
import quickfix.field.LeavesQty;
import quickfix.field.MsgType;
import quickfix.field.OrdStatus;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.Price;
import quickfix.field.SenderCompID;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TargetCompID;
import quickfix.fix42.ExecutionReport;
import quickfix.fix42.NewOrderSingle;
import quickfix.fix42.OrderCancelRequest;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * QuickFIX/J Application bridging FIX 4.2 → internal Command bus.
 *
 * Supported inbound:
 *   D (NewOrderSingle)      → SubmitOrderCommand
 *   F (OrderCancelRequest)  → CancelOrderCommand
 * Outbound:
 *   8 (ExecutionReport)
 *
 * <p>Every inbound + outbound message is mirrored to {@link FixAuditLogger} for the audit trail.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FixApplication implements Application {

    private final OrderCommandHandler commandHandler;
    private final FixAuditLogger audit;

    @Override
    public void onCreate(SessionID sessionId) {
        log.info("FIX session created: {}", sessionId);
    }

    @Override
    public void onLogon(SessionID sessionId) {
        log.info("FIX session logon: {}", sessionId);
    }

    @Override
    public void onLogout(SessionID sessionId) {
        log.info("FIX session logout: {}", sessionId);
    }

    @Override public void toAdmin(Message message, SessionID sessionId) {
        audit.record("OUT_ADMIN", sessionId.toString(), message.toString());
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionId)
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
        audit.record("IN_ADMIN", sessionId.toString(), message.toString());
    }

    @Override public void toApp(Message message, SessionID sessionId) throws DoNotSend {
        audit.record("OUT_APP", sessionId.toString(), message.toString());
    }

    @Override
    public void fromApp(Message message, SessionID sessionId)
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        audit.record("IN_APP", sessionId.toString(), message.toString());
        String msgType = message.getHeader().getString(MsgType.FIELD);
        try {
            switch (msgType) {
                case MsgType.ORDER_SINGLE -> handleNewOrder((NewOrderSingle) message, sessionId);
                case MsgType.ORDER_CANCEL_REQUEST -> handleCancel((OrderCancelRequest) message, sessionId);
                default -> {
                    log.warn("FIX: unsupported msgType={}", msgType);
                    throw new UnsupportedMessageType();
                }
            }
        } catch (UnsupportedMessageType e) {
            throw e;
        } catch (Exception e) {
            log.error("FIX fromApp failed", e);
        }
    }

    private void handleNewOrder(NewOrderSingle msg, SessionID sessionId) throws FieldNotFound {
        String clOrdId = msg.getString(ClOrdID.FIELD);
        String symbol = msg.getString(Symbol.FIELD);
        char fixSide = msg.getChar(Side.FIELD);
        OrderSide side = (fixSide == Side.BUY) ? OrderSide.BUY : OrderSide.SELL;
        BigDecimal qty = BigDecimal.valueOf(msg.getDouble(OrderQty.FIELD));
        BigDecimal price = msg.isSetField(Price.FIELD)
                ? BigDecimal.valueOf(msg.getDouble(Price.FIELD)) : null;
        UUID aggregateId = UUID.randomUUID();

        // Map session SenderCompID → user UUID via a directory in real systems;
        // for the simulation we hash it deterministically.
        UUID userId = UUID.nameUUIDFromBytes(sessionId.getTargetCompID().getBytes());

        SubmitOrderCommand cmd = new SubmitOrderCommand(
                aggregateId, userId, symbol, side,
                price != null ? OrderType.LIMIT : OrderType.MARKET,
                price, null, qty, clOrdId);

        commandHandler.handle(cmd).subscribe(
                resp -> sendExecutionReport(sessionId, resp.id().toString(), clOrdId,
                        symbol, fixSide, qty, price, ExecType.NEW, OrdStatus.NEW,
                        BigDecimal.ZERO, qty, BigDecimal.ZERO),
                err -> {
                    log.warn("FIX submit failed: {}", err.toString());
                    sendExecutionReport(sessionId, aggregateId.toString(), clOrdId,
                            symbol, fixSide, qty, price, ExecType.REJECTED, OrdStatus.REJECTED,
                            BigDecimal.ZERO, qty, BigDecimal.ZERO);
                });
    }

    private void handleCancel(OrderCancelRequest msg, SessionID sessionId) throws FieldNotFound {
        String origClOrdId = msg.getString(quickfix.field.OrigClOrdID.FIELD);
        // OrderID field is mandatory but may be NONE for "no exchange id yet" — use ClOrdID as
        // a hash key when needed. For now require a UUID-format OrderID.
        String orderIdStr = msg.isSetField(OrderID.FIELD) ? msg.getString(OrderID.FIELD) : origClOrdId;
        UUID orderId;
        try { orderId = UUID.fromString(orderIdStr); }
        catch (Exception e) { log.warn("FIX cancel: bad OrderID {}", orderIdStr); return; }

        UUID userId = UUID.nameUUIDFromBytes(sessionId.getTargetCompID().getBytes());
        CancelOrderCommand cmd = new CancelOrderCommand(orderId, userId, "FIX cancel request");
        commandHandler.handle(cmd).subscribe(
                resp -> sendExecutionReport(sessionId, orderId.toString(), origClOrdId,
                        msg.isSetField(Symbol.FIELD) ? msg.getString(Symbol.FIELD) : "",
                        Side.BUY /* placeholder */, BigDecimal.ZERO, null,
                        ExecType.CANCELED, OrdStatus.CANCELED,
                        BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO),
                err -> log.warn("FIX cancel failed: {}", err.toString()));
    }

    private void sendExecutionReport(SessionID sessionId, String orderId, String clOrdId,
                                     String symbol, char side, BigDecimal qty, BigDecimal price,
                                     char execType, char ordStatus,
                                     BigDecimal cumQty, BigDecimal leavesQty, BigDecimal avgPx) {
        ExecutionReport er = new ExecutionReport(
                new OrderID(orderId),
                new ExecID(UUID.randomUUID().toString()),
                new ExecType(execType),
                new OrdStatus(ordStatus),
                new Side(side),
                new LeavesQty(leavesQty.doubleValue()),
                new CumQty(cumQty.doubleValue()),
                new AvgPx(avgPx.doubleValue()));
        er.set(new ClOrdID(clOrdId == null ? orderId : clOrdId));
        er.set(new Symbol(symbol));
        if (qty != null) er.set(new OrderQty(qty.doubleValue()));
        if (price != null) er.set(new Price(price.doubleValue()));
        try {
            // Echo back to the connected counterpart.
            er.getHeader().setString(SenderCompID.FIELD, sessionId.getSenderCompID());
            er.getHeader().setString(TargetCompID.FIELD, sessionId.getTargetCompID());
            Session.sendToTarget(er, sessionId);
        } catch (Exception e) {
            log.error("FIX send ExecutionReport failed", e);
        }
    }

    /**
     * Reserved: hook for QuickFIX/J's data dictionary if we ever need custom parsing.
     */
    @SuppressWarnings("unused")
    void _dictionaryHook(DataDictionary dd, Message m) {
        MessageUtils.getStringField(m.toString(), 35); // no-op
    }
}
