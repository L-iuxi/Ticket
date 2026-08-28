package order

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"Ticket/app/event/event-rpc/eventclient"
	"Ticket/app/inventory/inventory-rpc/inventoryclient"
	"Ticket/app/order/order-api/internal/common"
	"Ticket/app/order/order-api/internal/svc"
	"Ticket/app/order/order-api/internal/types"
	"Ticket/app/order/order-rpc/orderclient"
	"Ticket/common/mq"
	"Ticket/common/xerr"
	"Ticket/internal/redis"

	"github.com/google/uuid"
	"github.com/zeromicro/go-zero/core/logx"
)

type BuyTicketLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewBuyTicketLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BuyTicketLogic {
	return &BuyTicketLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *BuyTicketLogic) BuyTicket(req *types.BuyTicketReq) (*types.BuyTicketResp, error) {
	userID := common.GetUserID(l.ctx)
	if userID == 0 {
		return nil, xerr.NewErrCode(xerr.USER_NOT_LOGIN)
	}

	if req.RequestId == "" {
		return &types.BuyTicketResp{Status: "fail", Message: "缺少请求ID"}, nil
	}

	/*1. 限流+令牌桶+分布式锁 — pipeline 合并 3 次 Redis 往返为 1 次
	 */
	ticketTypeId := req.TicketTypeID

	limitKey := fmt.Sprintf("limit:buy:user:%d", userID)

	bucketKey := fmt.Sprintf("bucket:ticket:%d", ticketTypeId)

	lockKey := fmt.Sprintf("lock:order:%d:%d:%d:%d", userID, req.EventID, req.ShowID, req.TicketTypeID)
	lockValue := uuid.NewString()

	pipe := l.svcCtx.Redis.Pipeline()

	rateLimitCmd := pipe.Eval(l.ctx, redis.RateLimitLua, []string{limitKey}, int(time.Second.Seconds()), 5)

	tokenBucketCmd := pipe.Eval(l.ctx, redis.TokenBucketLua, []string{bucketKey}, 200, 100, time.Now().Unix())

	lockCmd := pipe.SetNX(l.ctx, lockKey, lockValue, 30*time.Second)

	if _, err := pipe.Exec(l.ctx); err != nil {
		return nil, err
	}

	if rateLimitVal, _ := rateLimitCmd.Int(); rateLimitVal != 1 {
		return nil, xerr.NewErrMsg("请求过于频繁请稍后重试")
	}
	if tokenVal, _ := tokenBucketCmd.Int(); tokenVal != 1 {
		return nil, xerr.NewErrMsg("请求过于频繁请稍后重试")
	}
	if !lockCmd.Val() {
		return &types.BuyTicketResp{Status: "fail", Message: "订单正在创建，请勿重复提交"}, nil
	}
	defer func() {
		if unlockErr := l.svcCtx.Lock.Unlock(l.ctx, lockKey, lockValue); unlockErr != nil {
			logx.Errorf("释放锁失败: %v", unlockErr)
		}
	}()

	/*3. 幂等检查
	*检查请求是否已经存在，建相比较以上加了一个requestid，保证两次相同请求不会产生充分结果
	 */
	idemKey := redis.IdempotentKey(userID, req.EventID, req.ShowID, req.TicketTypeID, req.RequestId)
	val, err := l.svcCtx.Idempotent.Get(l.ctx, idemKey)
	if err != nil {
		return nil, fmt.Errorf("幂等检查失败: %w", err)
	}
	if strings.HasPrefix(val, "ok:") {
		return &types.BuyTicketResp{
			OrderNo: strings.TrimPrefix(val, "ok:"),
			Status:  "unpaid",
			Message: "订单已存在",
		}, nil
	}
	if val == "pending" {
		return &types.BuyTicketResp{Status: "fail", Message: "请求处理中，请稍后"}, nil
	}
	if val == "failed" {
		return &types.BuyTicketResp{Status: "fail", Message: "上次请求失败，请更换RequestId重试"}, nil
	}

	if err := l.svcCtx.Idempotent.Start(l.ctx, idemKey, 5*time.Minute); err != nil {
		if err == redis.ErrDuplicateRequest {
			return &types.BuyTicketResp{Status: "fail", Message: "请勿重复提交"}, nil
		}
		return nil, fmt.Errorf("幂等标记失败: %w", err)
	}

	/*4. 查票种 + 校验活动 + 限购

	 */
	ticketTypeResp, err := l.svcCtx.EventRpc.GetTicketType(l.ctx, &eventclient.GetTicketTypeReq{
		TicketTypeId: req.TicketTypeID,
	})
	if err != nil {
		l.svcCtx.Idempotent.Failed(l.ctx, idemKey, time.Minute)
		return nil, fmt.Errorf("查询票种失败: %w", err)
	}
	if ticketTypeResp == nil || ticketTypeResp.TicketType == nil {
		l.svcCtx.Idempotent.Failed(l.ctx, idemKey, time.Minute)
		return &types.BuyTicketResp{Status: "fail", Message: "票种不存在"}, nil
	}

	totalPrice := ticketTypeResp.TicketType.Price * float64(req.Quantity)

	eventResp, err := l.svcCtx.EventRpc.GetEvent(l.ctx, &eventclient.GetEventReq{EventId: req.EventID})
	if err != nil || eventResp.Event == nil {
		l.svcCtx.Idempotent.Failed(l.ctx, idemKey, time.Minute)
		return &types.BuyTicketResp{Status: "fail", Message: "活动不存在"}, nil
	}
	if eventResp.Event.Status != "selling" {
		l.svcCtx.Idempotent.Failed(l.ctx, idemKey, time.Minute)
		return &types.BuyTicketResp{Status: "fail", Message: "活动未开售"}, nil
	}

	// 限购检查
	orderList, err := l.svcCtx.OrderRpc.GetOrderList(l.ctx, &orderclient.GetOrderListReq{
		UserId: uint32(userID),
	})
	if err != nil {
		l.svcCtx.Idempotent.Failed(l.ctx, idemKey, time.Minute)
		return nil, fmt.Errorf("查询订单列表失败: %w", err)
	}
	bought := int32(0)
	for _, o := range orderList.Orders {
		if o.TicketTypeId == uint32(req.TicketTypeID) &&
			o.Status != "failed" && o.Status != "cancelled" {
			bought += o.Quantity
		}
	}
	if bought+req.Quantity > ticketTypeResp.TicketType.MaxPerUser {
		l.svcCtx.Idempotent.Failed(l.ctx, idemKey, time.Minute)
		return &types.BuyTicketResp{Status: "fail", Message: "超出限购数量"}, nil
	}

	// 5. 扣库存（Redis Lua 原子）

	deductResp, err := l.svcCtx.InventoryRpc.DeductStock(l.ctx, &inventoryclient.DeductStockReq{
		TicketTypeId: req.TicketTypeID,
		Quantity:     req.Quantity,
	})
	if err != nil {
		l.svcCtx.Idempotent.Failed(l.ctx, idemKey, time.Minute)
		return nil, fmt.Errorf("库存扣减失败: %w", err)
	}
	if !deductResp.Success {
		l.svcCtx.Idempotent.Failed(l.ctx, idemKey, time.Minute)
		return &types.BuyTicketResp{Status: "fail", Message: deductResp.Message}, nil
	}

	// 6. 发 MQ 异步建订单

	//提前创建订单号供用户调用支付
	orderNo := uuid.NewString()

	msg := mq.CreateOrderMessage{
		OrderNo:      orderNo,
		UserID:       uint32(userID),
		EventID:      req.EventID,
		ShowID:       uint32(req.ShowID),
		TicketTypeID: uint32(req.TicketTypeID),
		Quantity:     req.Quantity,
		TotalPrice:   totalPrice,
		RequestID:    req.RequestId,
		IdemKey:      idemKey,
	}

	body, err := json.Marshal(msg)
	if err != nil {
		l.svcCtx.Idempotent.Failed(l.ctx, idemKey, time.Minute)
		l.releaseStock(req.TicketTypeID, req.Quantity)
		return nil, err
	}

	if err := l.svcCtx.MQ.SendMsg(l.ctx, body, mq.RoutingOrderCreate); err != nil {
		l.svcCtx.Idempotent.Failed(l.ctx, idemKey, time.Minute)
		l.releaseStock(req.TicketTypeID, req.Quantity)
		return nil, fmt.Errorf("发送MQ消息失败: %w", err)
	}

	// 存 orderNo → idemKey 映射，供查订单时 Redis 回退查询
	creatingKey := "order:creating:" + orderNo
	if err := l.svcCtx.Redis.Set(l.ctx, creatingKey, idemKey, 5*time.Minute); err != nil {
		l.Errorf("存储订单创建映射失败: orderNo=%s, err=%v", orderNo, err)
	}

	return &types.BuyTicketResp{
		OrderNo:    orderNo,
		Status:     "pending",
		TotalPrice: totalPrice,
		Message:    "下单成功，订单正在创建，请稍后查询",
	}, nil
}

// releaseStock 回滚库存，三次直接 Redis INCRBY 重试，全部失败则写入补偿记录
func (l *BuyTicketLogic) releaseStock(ticketTypeID uint64, quantity int32) {
	stockKey := "stock:ticket:" + fmt.Sprintf("%d", ticketTypeID)

	for i := 0; i < 3; i++ {
		_, err := l.svcCtx.Redis.IncrBy(l.ctx, stockKey, int64(quantity))
		if err == nil {
			l.Infof("回滚库存成功: ticketTypeId=%d, quantity=%d, retry=%d", ticketTypeID, quantity, i)
			return
		}
		l.Errorf("回滚库存失败(第%d次): ticketTypeId=%d, quantity=%d, err=%v", i+1, ticketTypeID, quantity, err)
		time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
	}

	// 三次重试全部失败 → 写入补偿记录，人工处理
	reason := "MQ发送成功后回滚库存三次直接INCRBY重试均失败"
	if err := l.svcCtx.Redis.RecordCompensation(l.ctx, ticketTypeID, quantity, reason, ""); err != nil {
		l.Errorf("[CRITICAL] 写入补偿记录失败: ticketTypeId=%d, quantity=%d, err=%v", ticketTypeID, quantity, err)
		return
	}
	l.Errorf("[COMPENSATE] 库存回滚失败已记录补偿任务: ticketTypeId=%d, quantity=%d, 请管理员处理", ticketTypeID, quantity)
}
