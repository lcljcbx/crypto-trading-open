"""
网格执行引擎实现

功能：
- 与交易所适配器交互，执行订单操作
- 通过 WebSocket 实时监控订单成交
- 支持批量下单、订单健康检查、价格监控

核心机制：
1. 订单追踪：使用 client_id 映射订单（Lighter支持双ID系统）
2. 成交触发：WebSocket 推送 FILLED 状态时立即触发反手单
3. 健康检查：定时验证订单和持仓状态，自动修复异常
4. 智能监控：WebSocket 优先，REST API 备用

Lighter 交易所特殊处理：
- 订单有两个ID：client_id（下单时）和 order_index（WebSocket推送时）
- 使用 _pending_orders_by_client_id 缓存实现快速匹配
- 批量下单时串行执行，避免 nonce 冲突
- 全局下单锁确保所有下单操作（反手单、止盈单、健康检查补单）串行

健康检查机制：
1. 等待5秒内无订单成交（避免高频交易时检查）
2. 3次快照验证（间隔0.5秒，确保数据一致且稳定）
3. 通过验证后执行订单和持仓修复
"""

import asyncio
import time
from typing import List, Optional, Callable, Dict, Set, Tuple
from decimal import Decimal
from datetime import datetime

from ....logging import get_logger
from ....adapters.exchanges import ExchangeInterface, OrderSide as ExchangeOrderSide, OrderType
from ..interfaces.grid_engine import IGridEngine
from ..models import GridConfig, GridOrder, GridOrderSide, GridOrderStatus


class GridEngineImpl(IGridEngine):
    """
    网格执行引擎实现

    复用现有组件：
    - 交易所适配器（ExchangeInterface）
    - 订单管理
    - WebSocket订阅
    """

    # 🔥 类变量：价格获取失败的全局计数器（在所有实例间共享）
    _global_price_failures = 0  # 连续价格获取失败次数
    _global_price_successes = 0  # 连续价格获取成功次数（用于确认网络恢复）
    _last_price_failure_time = 0  # 上次价格获取失败时间
    _price_failure_reset_timeout = 30  # 超过30秒没有新失败则重置计数器
    _price_max_failures = 5  # 🔥 最大连续失败次数（3次后判定为网络故障）
    _price_success_required_to_reset = 3  # 🆕 连续成功N次后才重置失败计数器（确认网络稳定）

    def __init__(self, exchange_adapter: ExchangeInterface):
        """
        初始化执行引擎

        Args:
            exchange_adapter: 交易所适配器（通过DI注入）
        """
        self.logger = get_logger(__name__)
        self.exchange = exchange_adapter
        self.config: GridConfig = None
        self.coordinator = None  # 🔥 协调器引用（用于访问剥头皮管理器等）

        # 订单回调
        self._order_callbacks: List[Callable] = []

        # 订单追踪
        # order_id -> GridOrder
        self._pending_orders: Dict[str, GridOrder] = {}
        self._expected_cancellations: set = set()  # 🔥 记录主动取消的订单ID（剥头皮模式、本金保护等）

        # 🔥 价格监控
        self._current_price: Optional[Decimal] = None
        self._last_price_update_time: float = 0
        self._price_ws_enabled = False  # WebSocket价格订阅是否启用

        # 🔥 订单健康检查
        self._expected_total_orders: int = 0  # 预期的总订单数（初始化时设定）
        self._health_check_task = None
        self._last_health_check_time: float = 0
        self._last_order_fill_time: float = 0  # 🆕 最后一次订单成交时间（用于避免高频交易时检查）
        self._last_health_repair_count: int = 0  # 最后一次健康检查补充的订单数
        self._last_health_repair_time: float = 0  # 最后一次补充订单的时间
        self._health_checker = None  # 🆕 健康检查器（延迟初始化）

        # 🔥 WebSocket持仓缓存警告频率控制
        self._last_position_warning_time: float = 0  # 上次警告时间
        self._position_warning_interval: float = 60  # 警告间隔（秒）

        # 🔥 价格缓存过期警告频率控制
        self._last_price_warning_time: float = 0  # 上次价格警告时间
        self._price_warning_interval: float = 60  # 价格警告间隔（秒）

        # 🔥 Lighter交易所：全局下单锁（确保所有下单操作串行）
        # 之前只在 grid_coordinator 的反手单中使用锁，但止盈订单、健康检查补单等也需要串行
        self._lighter_order_lock = asyncio.Lock()  # 全局下单锁

        # 🔥 REST价格更新锁和时间戳（避免并发请求API）
        self._rest_price_update_lock = asyncio.Lock()  # REST更新锁
        self._last_rest_price_update_time: float = 0  # 上次REST更新时间
        self._rest_price_update_interval: float = 30  # REST更新间隔（秒）
        self._rest_price_updater_task = None  # 后台更新任务

        # 🔥 新方案：通过 client_id 映射原始订单（替代价格验证方案）
        # 存储：client_id → 原始 GridOrder 对象
        # 用途：WebSocket 推送时通过 client_id 找到原始订单，使用原始价格挂反手单
        self._pending_orders_by_client_id: Dict[str, GridOrder] = {}

        # 运行状态
        self._running = False

        # 获取交易所ID，避免直接打印整个对象（可能导致循环引用）
        exchange_id = getattr(exchange_adapter.config,
                              'exchange_id', 'unknown')
        self.logger.info(f"网格执行引擎初始化: {exchange_id}")

    async def initialize(self, config: GridConfig):
        """
        初始化执行引擎

        Args:
            config: 网格配置
        """
        self.config = config
        
        # 🔥 设置运行状态（确保后台任务能正常运行）
        self._running = True

        # 确保交易所连接
        if not self.exchange.is_connected():
            await self.exchange.connect()
            self.logger.info(f"连接到交易所: {config.exchange}")

        # 订阅用户数据流（接收订单更新）- 优先使用WebSocket
        self._ws_monitoring_enabled = False
        self._polling_task = None
        self._last_ws_check_time = 0  # 上次检查WebSocket的时间
        self._ws_check_interval = 30  # WebSocket检查间隔（秒）
        self._last_ws_message_time = time.time()  # 上次收到WebSocket消息的时间
        # 🔥 WebSocket 心跳超时阈值（秒）- 仅用于 Backpack/Hyperliquid
        # Lighter 不使用心跳超时检测，只依赖连接状态检测
        # Backpack/Hyperliquid 会在每次消息时更新心跳，可以用此阈值检测异常
        self._ws_timeout_threshold = 600  # 10分钟超时阈值

        try:
            self.logger.info("🔄 正在订阅WebSocket用户数据流...")
            await self.exchange.subscribe_user_data(self._on_order_update)
            self._ws_monitoring_enabled = True
            self.logger.info("✅ 订单更新流订阅成功 (WebSocket)")
            self.logger.info("📡 使用WebSocket实时监控订单成交")
        except Exception as e:
            self.logger.error(f"❌ 订单更新流订阅失败: {e}")
            self.logger.error(f"❌ 错误类型: {type(e).__name__}")
            import traceback
            self.logger.error(f"❌ 错误堆栈:\n{traceback.format_exc()}")
            self.logger.warning("⚠️ WebSocket暂时不可用，启用REST轮询作为临时备用")

        # 🔥 启动智能订单监控：WebSocket优先，REST备用
        self._start_smart_monitor()

        # 🔥 启动智能价格监控：WebSocket优先，REST备用
        await self._start_price_monitor()

        # 🔥 启动后台REST价格更新任务（30秒间隔兜底）
        self._start_rest_price_updater()

        # 🔥 启动时立即获取一次价格（填充缓存，避免初始化失败）
        self.logger.info("📊 正在获取初始价格...")
        await self._update_price_from_rest()

        # 🔥 设置预期订单总数（网格数量）
        self._expected_total_orders = config.grid_count

        # 🆕 初始化健康检查器（使用新模块）
        from .order_health_checker import OrderHealthChecker

        # 🔥 从 coordinator 获取 reserve_manager（如果存在）
        reserve_manager = None
        if self.coordinator and hasattr(self.coordinator, 'reserve_manager'):
            reserve_manager = self.coordinator.reserve_manager

        self._health_checker = OrderHealthChecker(
            config, self, reserve_manager)
        self.logger.info("✅ 订单健康检查器已初始化（新模块）")

        if reserve_manager:
            self.logger.info("✅ 健康检查器已配置现货预留管理")

        # 🔥 注意：订单健康检查任务在 start() 中启动（确保 _running=True）

        self.logger.info(
            f"✅ 执行引擎初始化完成: {config.exchange}/{config.symbol}"
        )

    async def place_order(self, order: GridOrder, batch_mode: bool = False, source: str = "未知") -> GridOrder:
        """
        下单

        Args:
            order: 网格订单
            batch_mode: 批量模式（仅限Lighter，避免频繁查询order_index）
            source: 下单来源标识（反手单/健康检查/初始化/止盈单）

        Returns:
            更新后的订单（包含交易所订单ID）
        """
        # 🔥 Lighter交易所：全局下单锁（确保所有下单操作串行）
        # 包括：反手单、止盈订单、健康检查补单等
        exchange_id = str(self.config.exchange).lower(
        ) if self.config.exchange else ''
        if exchange_id == 'lighter':
            async with self._lighter_order_lock:
                return await self._place_order_internal(order, batch_mode, source)
        else:
            return await self._place_order_internal(order, batch_mode, source)

    async def _place_order_internal(self, order: GridOrder, batch_mode: bool = False, source: str = "未知") -> GridOrder:
        """
        内部下单实现（实际下单逻辑）

        Args:
            order: 网格订单
            batch_mode: 批量模式（仅限Lighter，避免频繁查询order_index）
            source: 下单来源标识（反手单/健康检查/初始化/止盈单）

        Returns:
            更新后的订单（包含交易所订单ID）
        """
        try:
            # 转换订单方向
            exchange_side = self._convert_order_side(order.side)

            # 使用交易所适配器下单（纯限价单）
            # 注意：不能在 params 中传递 Backpack API 不支持的参数（如 grid_id），
            # 否则会导致签名验证失败！Backpack 支持 clientId 参数

            # 🔥 准备保证金模式参数（Lighter交易所必需）
            # margin_mode: "isolated"=逐仓(1), "cross"=全仓(0)
            margin_mode_value = 1 if self.config.margin_mode.lower() == "isolated" else 0

            # 🔥 检查是否为Lighter交易所（只有Lighter支持batch_mode参数）
            exchange_id = str(self.config.exchange).lower() if self.config.exchange else ''
            is_lighter = exchange_id == 'lighter'

            # 🔥 构建create_order参数（只有Lighter才传递batch_mode）
            create_order_kwargs = {
                "symbol": self.config.symbol,
                "side": exchange_side,
                "order_type": OrderType.LIMIT,  # 只使用限价单
                "amount": order.amount,
                "price": order.price,
                "params": {"margin_mode": margin_mode_value} if is_lighter else None
            }
            # 只有Lighter交易所才传递batch_mode参数
            if is_lighter:
                create_order_kwargs["batch_mode"] = batch_mode

            exchange_order = await self.exchange.create_order(**create_order_kwargs)

            # 🔥 检查返回值是否为None（API调用失败）- 带重试机制
            if exchange_order is None:
                self.logger.warning(
                    f"⚠️ 下单返回None，1秒后重试: Grid {order.grid_id}, {order.side.value} {order.amount}@{order.price}"
                )
                await asyncio.sleep(1)  # 等待1秒

                # 重试一次
                # 🔥 构建重试参数（只有Lighter才传递batch_mode和margin_mode）
                retry_kwargs = {
                    "symbol": self.config.symbol,
                    "side": exchange_side,
                    "order_type": OrderType.LIMIT,
                    "amount": order.amount,
                    "price": order.price,
                    "params": {"margin_mode": margin_mode_value} if is_lighter else None
                }
                # 只有Lighter交易所才传递batch_mode参数
                if is_lighter:
                    retry_kwargs["batch_mode"] = batch_mode
                
                exchange_order = await self.exchange.create_order(**retry_kwargs)

                # 如果重试后仍然为None，则抛出异常
                if exchange_order is None:
                    error_msg = f"下单失败: 交易所返回None（已重试1次） (Grid {order.grid_id}, {order.side.value} {order.amount}@{order.price})"
                    self.logger.error(error_msg)
                    raise Exception(error_msg)
                else:
                    self.logger.info(f"✅ 重试成功: Grid {order.grid_id}")

            # 更新订单ID
            order.order_id = exchange_order.id or exchange_order.order_id
            order.status = GridOrderStatus.PENDING

            # 🔥 更新 client_id（如果交易所返回了）
            if hasattr(exchange_order, 'client_id') and exchange_order.client_id:
                order.client_id = str(exchange_order.client_id)

            # 如果订单ID为临时ID（"pending"），尝试从符号查询获取实际ID
            if order.order_id == "pending" or not order.order_id:
                # Backpack API 有时只返回状态，需要查询获取实际订单ID
                # 暂时使用价格+数量作为唯一标识
                temp_id = f"grid_{order.grid_id}_{int(order.price)}_{int(order.amount*1000000)}"
                order.order_id = temp_id
                self.logger.warning(
                    f"订单ID为临时值，使用组合ID: {temp_id} "
                    f"(Grid {order.grid_id}, {order.side.value} {order.amount}@{order.price})"
                )

            # 添加到追踪列表
            self._pending_orders[order.order_id] = order

            # 🔥 新方案：如果有 client_id，存入 client_id 缓存
            # 用于 WebSocket 推送时通过 client_id 查找原始订单
            if order.client_id:
                self._pending_orders_by_client_id[order.client_id] = order
                self.logger.debug(
                    f"📝 订单已缓存: client_id={order.client_id}, "
                    f"price={order.price}, grid={order.grid_id}"
                )

            # 🔥 根据来源使用不同的标识符
            if source == "反手单":
                prefix = "🔄 [反手]"
            elif source == "健康检查":
                prefix = "🏥 [补单]"
            elif source == "批量初始化":
                prefix = "🎯 [初始]"
            elif source == "止盈单":
                prefix = "💰 [止盈]"
            else:
                prefix = "📝 [下单]"

            self.logger.info(
                f"{prefix} {order.side.value.upper()} {order.amount}@{order.price} "
                f"(Grid {order.grid_id}, OrderID: {order.order_id})"
            )

            return order

        except Exception as e:
            self.logger.error(f"下单失败: {e}")
            order.mark_failed()
            raise

    async def place_market_order(self, side: GridOrderSide, amount: Decimal) -> None:
        """
        下市价单（用于平仓）

        Args:
            side: 订单方向（BUY/SELL）
            amount: 订单数量
        """
        try:
            # 转换订单方向
            exchange_side = self._convert_order_side(side)

            self.logger.info(f"📊 下市价单: {side.value} {amount}")

            # 使用交易所适配器下市价单
            exchange_order = await self.exchange.create_order(
                symbol=self.config.symbol,
                side=exchange_side,
                order_type=OrderType.MARKET,
                amount=amount,
                price=None,  # 市价单不需要价格
                params=None
            )

            # 🔥 检查返回值是否为None（API调用失败）
            if exchange_order is None:
                error_msg = f"市价单失败: 交易所返回None ({side.value} {amount})"
                self.logger.error(error_msg)
                raise Exception(error_msg)

            self.logger.info(
                f"✅ 市价单成功: {side.value} {amount}, "
                f"OrderID: {exchange_order.id or exchange_order.order_id}"
            )

        except Exception as e:
            self.logger.error(f"❌ 市价单失败: {e}")
            raise

    async def place_batch_orders(self, orders: List[GridOrder], max_retries: int = 2) -> List[GridOrder]:
        """
        批量下单 - 优化版，支持大批量订单和失败重试

        Args:
            orders: 订单列表
            max_retries: 最大重试次数（默认2次）

        Returns:
            更新后的订单列表
        """
        total_orders = len(orders)
        self.logger.info(f"开始批量下单: {total_orders}个订单")

        # 分批下单，避免一次性并发过多（每批50个）
        batch_size = 50
        successful_orders = []
        failed_orders = []  # 记录失败的订单

        for i in range(0, total_orders, batch_size):
            batch = orders[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_orders + batch_size - 1) // batch_size

            self.logger.info(
                f"处理第{batch_num}/{total_batches}批订单 "
                f"({len(batch)}个订单)"
            )

            # 🔥 Lighter交易所特殊处理：串行下单（避免nonce冲突）
            # 其他交易所：并发下单（保持原有性能）
            exchange_id = str(self.config.exchange).lower(
            ) if self.config.exchange else ''
            if exchange_id == 'lighter':
                self.logger.info("🔥 Lighter交易所：使用串行批量下单模式（避免nonce冲突）")
                results = []
                for order in batch:
                    try:
                        # 🔥 批量下单时使用 batch_mode=True，不立即查询 order_index
                        result = await self.place_order(order, batch_mode=True, source="批量初始化")
                        results.append(result)
                    except Exception as e:
                        results.append(e)
                        self.logger.error(f"订单下单异常: {e}")
            else:
                # 并发下单当前批次（其他交易所）
                tasks = [self.place_order(order) for order in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

            # 统计当前批次结果
            batch_success = 0
            for idx, result in enumerate(results):
                if isinstance(result, GridOrder):
                    successful_orders.append(result)
                    batch_success += 1
                else:
                    # 记录失败的订单
                    failed_orders.append((batch[idx], str(result)))
                    self.logger.error(f"订单下单失败: {result}")

            self.logger.info(
                f"第{batch_num}批完成: 成功{batch_success}/{len(batch)}个，"
                f"总进度: {len(successful_orders)}/{total_orders}"
            )

            # 短暂延迟，避免触发交易所限频
            if i + batch_size < total_orders:
                await asyncio.sleep(0.5)

        # ✅ 重试失败的订单
        if failed_orders and max_retries > 0:
            self.logger.warning(
                f"⚠️ 检测到{len(failed_orders)}个失败订单，开始重试..."
            )

            for retry_attempt in range(1, max_retries + 1):
                if not failed_orders:
                    break

                self.logger.info(
                    f"🔄 第{retry_attempt}次重试: {len(failed_orders)}个订单"
                )

                # 等待一段时间再重试，避免立即重试
                await asyncio.sleep(1.0)

                retry_orders = [order for order, _ in failed_orders]
                failed_orders = []  # 清空失败列表

                # 🔥 Lighter交易所：串行重试（避免nonce冲突）
                # 其他交易所：并发重试
                exchange_id = str(self.config.exchange).lower(
                ) if self.config.exchange else ''
                if exchange_id == 'lighter':
                    results = []
                    for order in retry_orders:
                        try:
                            # 🔥 重试也使用批量模式
                            result = await self.place_order(order, batch_mode=True, source="批量初始化重试")
                            results.append(result)
                        except Exception as e:
                            results.append(e)
                else:
                    # 重试失败的订单（并发）
                    tasks = [self.place_order(order) for order in retry_orders]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                retry_success = 0
                for idx, result in enumerate(results):
                    if isinstance(result, GridOrder):
                        successful_orders.append(result)
                        retry_success += 1
                    else:
                        # 仍然失败，记录下来
                        failed_orders.append((retry_orders[idx], str(result)))

                self.logger.info(
                    f"重试结果: 成功{retry_success}/{len(retry_orders)}个，"
                    f"剩余失败{len(failed_orders)}个"
                )

                # 如果还有失败的订单，短暂延迟后继续重试
                if failed_orders and retry_attempt < max_retries:
                    await asyncio.sleep(1.0)

        # 最终统计
        final_failed_count = len(failed_orders)
        success_rate = (len(successful_orders) / total_orders *
                        100) if total_orders > 0 else 0

        if final_failed_count > 0:
            self.logger.warning(
                f"⚠️ 批量下单完成: 成功{len(successful_orders)}/{total_orders}个 "
                f"({success_rate:.1f}%), 最终失败{final_failed_count}个"
            )

            # 记录失败订单的详细信息
            for order, error in failed_orders:
                self.logger.error(
                    f"订单最终失败: Grid {order.grid_id}, "
                    f"{order.side.value} {order.amount}@{order.price}, "
                    f"错误: {error}"
                )
        else:
            self.logger.info(
                f"✅ 批量下单完成: 成功{len(successful_orders)}/{total_orders}个 "
                f"({success_rate:.1f}%)"
            )

        # 🔥 批量下单完成后，主动查询一次所有订单状态
        # 目的：检测那些在提交时立即成交的订单
        self.logger.info("🔍 正在同步订单状态，检测立即成交的订单...")
        await asyncio.sleep(3)  # 等待3秒，让交易所处理完所有订单并更新状态
        await self._sync_order_status_after_batch()

        return successful_orders

    def _remove_order_from_pending(self, order_id: str) -> int:
        """
        从 _pending_orders 中移除订单（支持双键删除）

        🔥 批量模式说明（2025-11）：
        - 批量下单时，Lighter订单会暂时有两个键：client_id + order_index
        - 需要找到并删除所有指向同一订单对象的键
        - 使用对象ID (id(order)) 进行匹配

        Args:
            order_id: 订单ID（可能是 client_id 或 order_index）

        Returns:
            删除的键数量（0表示订单不存在，1-2表示删除的键数量）
        """
        if order_id not in self._pending_orders:
            return 0

        # 获取订单对象
        order_obj = self._pending_orders[order_id]
        order_obj_id = id(order_obj)

        # 找到所有指向同一订单对象的键
        keys_to_remove = [
            key for key, order in self._pending_orders.items()
            if id(order) == order_obj_id
        ]

        # 删除所有找到的键
        for key in keys_to_remove:
            del self._pending_orders[key]

        return len(keys_to_remove)

    async def cancel_order(self, order_id: str) -> bool:
        """
        取消订单（主动取消，不会重新挂单）

        Args:
            order_id: 订单ID

        Returns:
            是否成功
        """
        try:
            # 🔥 关键修复：记录主动取消的订单ID
            # 当WebSocket收到取消事件时，会检查这个集合，避免重新挂单
            self._expected_cancellations.add(order_id)

            await self.exchange.cancel_order(order_id, self.config.symbol)

            # 标记为已取消并从追踪列表移除（自动处理 Lighter 双键）
            if order_id in self._pending_orders:
                order = self._pending_orders[order_id]
                order.mark_cancelled()
                self._remove_order_from_pending(order_id)

            self.logger.info(f"✅ 主动取消订单成功: {order_id}")
            return True

        except Exception as e:
            self.logger.error(f"取消订单失败 {order_id}: {e}")
            return False

    async def cancel_all_orders(self) -> int:
        """
        取消所有订单（主动批量取消，不会重新挂单）

        Returns:
            取消的订单数量
        """
        try:
            # 🔥 如果引擎未初始化，直接返回
            if self.config is None:
                self.logger.debug("引擎未初始化，跳过取消订单")
                return 0

            # 🔥 关键修复：记录所有待取消的订单ID
            # 在调用取消前先记录，避免WebSocket事件先到达
            pending_order_ids = list(self._pending_orders.keys())
            for order_id in pending_order_ids:
                self._expected_cancellations.add(order_id)

            cancelled_orders = await self.exchange.cancel_all_orders(self.config.symbol)
            count = len(cancelled_orders)

            # 清空追踪列表
            for order_id in pending_order_ids:
                if order_id in self._pending_orders:
                    order = self._pending_orders[order_id]
                    order.mark_cancelled()
                    del self._pending_orders[order_id]

            self.logger.info(f"✅ 主动批量取消所有订单: {count}个")
            return count

        except Exception as e:
            self.logger.error(f"取消所有订单失败: {e}")
            return 0

    async def get_order_status(self, order_id: str) -> Optional[GridOrder]:
        """
        查询订单状态

        Args:
            order_id: 订单ID

        Returns:
            订单信息
        """
        try:
            # 从交易所查询
            exchange_order = await self.exchange.get_order(order_id, self.config.symbol)

            # 更新本地订单信息
            if order_id in self._pending_orders:
                grid_order = self._pending_orders[order_id]

                # 如果已成交
                if exchange_order.status.value == "filled":
                    grid_order.mark_filled(
                        filled_price=exchange_order.price,
                        filled_amount=exchange_order.filled
                    )

                return grid_order

            return None

        except Exception as e:
            self.logger.error(f"查询订单状态失败 {order_id}: {e}")
            return None

    async def get_current_price(self) -> Decimal:
        """
        获取当前市场价格（从缓存读取）

        🔥 新架构：统一缓存机制
        - 所有调用都从缓存读取，不主动请求API
        - WS推送实时更新缓存
        - 后台REST任务定期更新缓存（30秒间隔兜底）
        - 并发安全，避免频繁API请求触发429限流

        Returns:
            当前价格（从缓存）
        
        Raises:
            ValueError: 缓存为空且无兜底价格时
        """
        current_time = time.time()

        # 🔥 纯缓存读取：如果缓存有效，直接返回
        if self._current_price is not None:
            price_age = current_time - self._last_price_update_time
            # 如果价格在60秒内更新过，认为有效
            if price_age < 60:
                await self._handle_price_success()
                return self._current_price
            
            # 价格超过60秒未更新，记录警告但仍返回（避免中断交易）
            # 🔥 警告频率控制：避免刷屏（每60秒最多1次）
            if current_time - self._last_price_warning_time >= self._price_warning_interval:
                self.logger.warning(
                    f"⚠️ 价格缓存已过期 {price_age:.0f}秒，仍使用旧价格。"
                    f"检查WS连接和REST更新任务是否正常。"
                )
                self._last_price_warning_time = current_time
            return self._current_price

        # 🔥 缓存为空：使用配置区间中间价作为兜底（首次启动时）
        if self.config and self.config.lower_price and self.config.upper_price:
            fallback_price = (self.config.lower_price + self.config.upper_price) / Decimal('2')
            self.logger.warning(
                f"⚠️ 价格缓存为空，使用配置区间中间价作为兜底: {fallback_price}"
            )
            self._current_price = fallback_price
            self._last_price_update_time = current_time
            return fallback_price

        # 🔥 无任何可用价格
        raise ValueError(
            "价格缓存为空且无配置区间，请检查WS连接和REST更新任务。"
            "系统启动时应等待WS首次价格推送或REST更新完成。"
        )

    async def _handle_price_success(self):
        """
        处理价格获取成功

        🔥 网络恢复检测：
        - 累积连续成功次数
        - 连续成功N次后重置失败计数器
        - 确保网络真正稳定后才判定为恢复
        """
        current_time = time.time()

        # 🔥 检查是否需要重置全局计数器（超时重置机制）
        if (GridEngineImpl._last_price_failure_time > 0 and
                current_time - GridEngineImpl._last_price_failure_time > GridEngineImpl._price_failure_reset_timeout):
            if GridEngineImpl._global_price_failures > 0:
                self.logger.info(
                    f"⏰ 距离上次价格获取失败已超过{GridEngineImpl._price_failure_reset_timeout}秒，"
                    f"重置价格失败计数器（之前失败{GridEngineImpl._global_price_failures}次）"
                )
                GridEngineImpl._global_price_failures = 0
                GridEngineImpl._global_price_successes = 0

        # 如果之前有失败记录，累积成功次数
        if GridEngineImpl._global_price_failures > 0:
            GridEngineImpl._global_price_successes += 1

            # 只有连续成功N次后才重置失败计数器（确保网络真正稳定）
            if GridEngineImpl._global_price_successes >= GridEngineImpl._price_success_required_to_reset:
                self.logger.info(
                    f"✅ 价格获取网络恢复，重置失败计数器"
                    f"（之前失败{GridEngineImpl._global_price_failures}次，"
                    f"连续成功{GridEngineImpl._global_price_successes}次）"
                )
                GridEngineImpl._global_price_failures = 0
                GridEngineImpl._global_price_successes = 0
                GridEngineImpl._last_price_failure_time = 0

                # 🔥 尝试恢复系统（如果系统因网络故障暂停）
                # 🔥 修改：任意一个恢复即可（价格获取或REST），无需等待两者都恢复
                # 原因：REST请求频率很低（60-180秒），导致恢复检测时间过长
                if self.coordinator and self.coordinator._paused and self.coordinator._paused_reason == 'network':
                    self.logger.info(
                        "✅ 价格获取已恢复，正在自动恢复系统..."
                    )
                    await self.coordinator.resume(auto=True)
            else:
                self.logger.debug(
                    f"⚠️ 价格获取部分恢复，连续成功{GridEngineImpl._global_price_successes}/"
                    f"{GridEngineImpl._price_success_required_to_reset}次"
                    f"（需要更多成功才能重置失败计数器）"
                )

    async def _handle_price_failure(self):
        """
        处理价格获取失败

        🔥 网络故障检测：
        - 使用全局失败计数器（不受重新初始化影响）
        - 连续失败3次 → 判定为网络故障，暂停系统
        - 超过30秒无新失败 → 重置计数器
        """
        current_time = time.time()

        # 🔥 检查是否需要重置全局计数器（超时重置机制）
        if (GridEngineImpl._last_price_failure_time > 0 and
                current_time - GridEngineImpl._last_price_failure_time > GridEngineImpl._price_failure_reset_timeout):
            if GridEngineImpl._global_price_failures > 0:
                self.logger.info(
                    f"⏰ 距离上次价格获取失败已超过{GridEngineImpl._price_failure_reset_timeout}秒，"
                    f"重置价格失败计数器（之前失败{GridEngineImpl._global_price_failures}次）"
                )
                GridEngineImpl._global_price_failures = 0

        # 🔥 更新全局失败计数器
        GridEngineImpl._global_price_failures += 1
        GridEngineImpl._global_price_successes = 0  # 🆕 失败时清零成功计数器
        GridEngineImpl._last_price_failure_time = current_time

        # 🔥 检查是否达到故障阈值
        if GridEngineImpl._global_price_failures >= GridEngineImpl._price_max_failures:
            self.logger.error(
                f"🚨 价格获取网络故障！连续失败{GridEngineImpl._global_price_failures}次 "
                f"(阈值={GridEngineImpl._price_max_failures}次)"
            )

            # 🔥 暂停系统
            if self.coordinator and not self.coordinator._paused:
                self.logger.error(
                    f"⏸️ 因价格获取网络故障暂停系统（失败{GridEngineImpl._global_price_failures}次）"
                )
                await self.coordinator.pause(reason='network')
        else:
            self.logger.warning(
                f"⚠️ 价格获取失败 [{GridEngineImpl._global_price_failures}/{GridEngineImpl._price_max_failures}]"
            )

    def get_pending_orders(self) -> List[GridOrder]:
        """
        获取当前所有挂单列表

        🔥 批量模式说明（2025-11）：
        - 批量下单时，订单会暂时有两个键：client_id + order_index
        - 需要去重，避免统计重复
        - 使用对象ID (id(order)) 进行去重

        Returns:
            挂单列表（去重后）
        """
        # 使用对象ID去重，避免同一订单被计数两次
        seen_objects = set()
        unique_orders = []

        for order in self._pending_orders.values():
            order_obj_id = id(order)
            if order_obj_id not in seen_objects:
                seen_objects.add(order_obj_id)
                unique_orders.append(order)

        return unique_orders

    def clear_all_caches(self):
        """
        清空所有订单缓存

        ⚠️ 仅在网格重置时调用！
        用途：
        - 取消所有订单后，清空本地缓存
        - 避免旧订单缓存影响新一轮网格

        清空内容：
        - _pending_orders: order_id → GridOrder
        - _pending_orders_by_client_id: client_id → GridOrder
        """
        cleared_count = len(self._pending_orders)
        cleared_client_count = len(self._pending_orders_by_client_id)

        self._pending_orders.clear()
        self._pending_orders_by_client_id.clear()

        self.logger.info(
            f"🧹 订单缓存已清空: "
            f"order_id缓存={cleared_count}个, "
            f"client_id缓存={cleared_client_count}个"
        )

    def subscribe_order_updates(self, callback: Callable):
        """
        订阅订单更新

        Args:
            callback: 回调函数，接收订单更新
        """
        self._order_callbacks.append(callback)
        self.logger.debug(f"添加订单更新回调: {callback}")

    def get_monitoring_mode(self) -> str:
        """
        获取当前监控方式

        Returns:
            监控方式：'WebSocket' 或 'REST轮询'
        """
        if self._ws_monitoring_enabled:
            return "WebSocket"
        else:
            return "REST轮询"

    async def get_real_time_position(self, symbol: str) -> Dict[str, Decimal]:
        """
        从WebSocket缓存获取实时持仓信息（完全不使用REST API）

        Args:
            symbol: 交易对符号

        Returns:
            持仓信息字典：{
                'size': 持仓数量（正数=多头，负数=空头，0=无持仓）,
                'entry_price': 平均入场价格,
                'unrealized_pnl': 未实现盈亏,
                'has_cache': 是否有缓存（区分"无缓存"和"真的没持仓"）
            }
        """
        try:
            # 🔥 只使用WebSocket缓存（不用REST API）
            if hasattr(self.exchange, '_position_cache'):
                cached_position = self.exchange._position_cache.get(symbol)
                if cached_position:
                    cache_age = (datetime.now() -
                                 cached_position['timestamp']).total_seconds()

                    self.logger.debug(
                        f"📊 使用WebSocket持仓缓存: {symbol} "
                        f"数量={cached_position['size']}, "
                        f"成本=${cached_position['entry_price']}, "
                        f"缓存年龄={cache_age:.1f}秒"
                    )

                    return {
                        'size': cached_position['size'],
                        'entry_price': cached_position['entry_price'],
                        'unrealized_pnl': cached_position['unrealized_pnl'],
                        'has_cache': True  # 🔥 标记：有缓存数据
                    }

            # 🔥 WebSocket缓存不可用（可能还没收到更新）
            # 🔥 频率控制：每60秒最多打印一次警告
            current_time = time.time()
            if current_time - self._last_position_warning_time >= self._position_warning_interval:
                self.logger.debug(
                    f"📊 WebSocket持仓缓存暂无数据: {symbol} "
                    f"(使用PositionTracker数据)"
                )
                self._last_position_warning_time = current_time
            return {
                'size': Decimal('0'),
                'entry_price': Decimal('0'),
                'unrealized_pnl': Decimal('0'),
                'has_cache': False  # 🔥 标记：无缓存数据
            }

        except Exception as e:
            self.logger.error(f"获取WebSocket持仓缓存失败: {e}")
            return {
                'size': Decimal('0'),
                'entry_price': Decimal('0'),
                'unrealized_pnl': Decimal('0'),
                'has_cache': False  # 🔥 标记：无缓存数据
            }

    def _start_smart_monitor(self):
        """启动智能监控：WebSocket优先，REST临时备用"""
        if self._polling_task is None or self._polling_task.done():
            self._polling_task = asyncio.create_task(
                self._smart_monitor_loop())
            if self._ws_monitoring_enabled:
                self.logger.info("✅ 智能监控已启动：WebSocket (主)")
            else:
                self.logger.info("✅ 智能监控已启动：REST轮询 (临时备用)")

    async def _smart_monitor_loop(self):
        """智能监控循环：优先WebSocket，必要时使用REST"""
        self.logger.info("📡 智能监控循环已启动")

        while True:
            try:
                # 🔥 策略1：如果WebSocket正常，只做定期检查（不轮询订单）
                if self._ws_monitoring_enabled:
                    await asyncio.sleep(30)  # 30秒检查一次WebSocket状态

                    current_time = time.time()
                    time_since_last_message = current_time - self._last_ws_message_time

                    # 🔥 优先检查WebSocket连接状态（而不是消息时间）
                    ws_connected = True
                    if hasattr(self.exchange, '_ws_connected'):
                        ws_connected = self.exchange._ws_connected

                    if not ws_connected:
                        self.logger.error("❌ WebSocket连接断开，切换到REST轮询模式")
                        self.logger.info(
                            f"📊 最后收到消息时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self._last_ws_message_time))}")
                        self.logger.info(
                            f"📊 当前挂单数量: {len(self.get_pending_orders())}")
                        self._ws_monitoring_enabled = False
                        self._last_ws_check_time = current_time
                        continue

                    # 🔥 检查WebSocket心跳状态（仅对支持主动心跳的交易所）
                    exchange_name = self.config.exchange.lower() if hasattr(
                        self.config, 'exchange') else 'unknown'

                    # 🔥 Lighter 不会主动推送心跳消息，只依赖连接状态检测
                    # Backpack/Hyperliquid 会在每次消息时更新心跳，可以用超时检测
                    if exchange_name == 'lighter':
                        # 对于 Lighter：没有订单成交时不会有消息，这是正常现象
                        # 只要连接状态正常，就继续使用 WebSocket
                        self.logger.info(
                            f"💓 WebSocket健康: 连接正常, "
                            f"消息 {time_since_last_message:.0f}秒前"
                        )

                        # 💡 如果长时间没有消息，提示这是正常现象
                        if time_since_last_message > 600:  # 10分钟
                            self.logger.info(
                                f"💡 提示: {time_since_last_message:.0f}秒未收到订单更新 "
                                f"(无订单成交时的正常现象)"
                            )
                    else:
                        # 对于 Backpack/Hyperliquid：检查心跳超时
                        heartbeat_age = 0
                        if hasattr(self.exchange, '_last_heartbeat'):
                            last_heartbeat = self.exchange._last_heartbeat
                            # 处理可能的datetime对象
                            if isinstance(last_heartbeat, datetime):
                                last_heartbeat = last_heartbeat.timestamp()
                            heartbeat_age = current_time - last_heartbeat

                            # 对于这些交易所，心跳超时是真正的问题
                            if heartbeat_age > self._ws_timeout_threshold:
                                self.logger.error(
                                    f"❌ WebSocket心跳超时（{heartbeat_age:.0f}秒未更新），"
                                    f"切换到REST轮询模式"
                                )
                                self.logger.info(
                                    f"📊 最后心跳时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_heartbeat))}")
                                self.logger.info(
                                    f"📊 最后消息时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self._last_ws_message_time))}")
                                self.logger.info(
                                    f"📊 当前挂单数量: {len(self.get_pending_orders())}")
                                self._ws_monitoring_enabled = False
                                self._last_ws_check_time = current_time
                                continue

                        # 打印健康状态
                        self.logger.info(
                            f"💓 WebSocket健康: 连接正常, 心跳 {heartbeat_age:.0f}秒前, "
                            f"消息 {time_since_last_message:.0f}秒前"
                        )

                    continue

                # 🔥 策略2：WebSocket不可用时，使用REST轮询
                await asyncio.sleep(3)  # 3秒轮询一次

                if self._pending_orders:
                    await self._check_pending_orders()

                # 🔥 策略3：定期尝试恢复WebSocket
                current_time = time.time()
                if current_time - self._last_ws_check_time >= self._ws_check_interval:
                    self._last_ws_check_time = current_time
                    await self._try_restore_websocket()

            except asyncio.CancelledError:
                self.logger.info("智能监控已停止")
                break
            except Exception as e:
                self.logger.error(f"智能监控出错: {e}")
                await asyncio.sleep(5)

    async def _try_restore_websocket(self):
        """尝试恢复WebSocket监控"""
        if self._ws_monitoring_enabled:
            return  # 已经在使用WebSocket

        try:
            self.logger.info("🔄 尝试恢复WebSocket监控...")

            # 尝试重新订阅用户数据流
            await self.exchange.subscribe_user_data(self._on_order_update)

            # 订阅成功，切换回WebSocket模式
            self._ws_monitoring_enabled = True
            # 重置WebSocket消息时间戳
            self._last_ws_message_time = time.time()
            self.logger.info("✅ WebSocket监控已恢复！切换回WebSocket模式")
            self.logger.info("📡 使用WebSocket实时监控订单成交")

        except Exception as e:
            self.logger.warning(f"⚠️ WebSocket恢复失败: {type(e).__name__}: {e}")
            self.logger.debug(f"详细错误: {e}，继续使用REST轮询")
            import traceback
            self.logger.debug(f"错误堆栈:\n{traceback.format_exc()}")

    async def _sync_order_status_after_batch(self):
        """
        批量下单后同步订单状态

        🔥 新逻辑（2025-11-03）：
        - 只建立 client_id → order_index 的映射
        - 不触发成交回调（完全依赖 WebSocket 推送）
        - 避免重复触发反手单
        """
        try:
            if not self._pending_orders:
                self.logger.debug("没有挂单需要同步")
                return

            # 获取所有挂单
            open_orders = await self.exchange.get_open_orders(self.config.symbol)

            if not open_orders:
                self.logger.warning("⚠️ 未获取到任何挂单，等待 WebSocket 推送处理成交")
                return

            # 创建挂单ID集合
            open_order_ids = {order.id for order in open_orders if order.id}
            open_client_ids = {
                order.client_id for order in open_orders if order.client_id}

            self.logger.debug(f"🔍 挂单 order_index 集合: {len(open_order_ids)}个")
            self.logger.debug(f"🔍 挂单 client_id 集合: {len(open_client_ids)}个")

            # 🔥 只建立 order_index 映射，不触发成交回调
            mapped_count = 0
            pending_order_ids = list(self._pending_orders.keys())

            for order_id in pending_order_ids:
                # 如果 order_id 在 open_client_ids 中，建立映射
                if order_id in open_client_ids:
                    # 找到对应的 order_index
                    for ex_order in open_orders:
                        if ex_order.client_id == order_id:
                            order_index = ex_order.id

                            # 建立映射（如果还没有）
                            if order_index and order_index not in self._pending_orders:
                                grid_order = self._pending_orders[order_id]
                                self._pending_orders[order_index] = grid_order
                                mapped_count += 1

                                self.logger.debug(
                                    f"✅ 映射订单ID: client_id={order_id} → "
                                    f"order_index={order_index} "
                                    f"(Grid {grid_order.grid_id})"
                                )
                            break

            # 使用 get_pending_orders() 获取去重后的订单数量
            pending_count = len(self.get_pending_orders())
            self.logger.info(
                f"✅ 同步完成: 建立 {mapped_count} 个ID映射，"
                f"当前挂单 {pending_count} 个（等待 WebSocket 推送处理成交）"
            )

        except Exception as e:
            self.logger.error(f"同步订单状态失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    async def _check_pending_orders(self):
        """检查挂单状态（通过REST API）"""
        try:
            # 获取当前所有挂单
            open_orders = await self.exchange.get_open_orders(self.config.symbol)

            # 创建订单ID集合（用于快速查找）
            open_order_ids = {
                order.id or order.order_id for order in open_orders if order.id or order.order_id}

            # 检查我们跟踪的订单
            filled_orders = []
            for order_id, grid_order in list(self._pending_orders.items()):
                # 如果订单不在挂单列表中，说明已成交或取消
                if order_id not in open_order_ids:
                    # 假设是成交了（网格系统不会主动取消订单）
                    filled_orders.append((order_id, grid_order))

            # 处理成交的订单
            for order_id, grid_order in filled_orders:
                self.logger.info(
                    f"📊 REST轮询检测到订单成交: {grid_order.side.value} "
                    f"{grid_order.amount}@{grid_order.price} (Grid {grid_order.grid_id})"
                )

                # 标记为已成交
                grid_order.mark_filled(grid_order.price, grid_order.amount)

                # 从挂单列表移除
                del self._pending_orders[order_id]

                # 通知回调
                for callback in self._order_callbacks:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(grid_order)
                        else:
                            callback(grid_order)
                    except Exception as e:
                        self.logger.error(f"订单回调执行失败: {e}")

            if filled_orders:
                self.logger.info(f"✅ REST轮询处理了 {len(filled_orders)} 个成交订单")

        except Exception as e:
            self.logger.error(f"检查挂单状态失败: {e}")

    async def _on_order_update(self, update_data: dict):
        """
        处理订单更新（来自WebSocket）

        Args:
            update_data: 交易所推送的订单更新数据

        Backpack格式:
        {
            "e": "orderFilled",     // 事件类型
            "i": "11815754679",     // 订单ID
            "X": "Filled",          // 订单状态
            "p": "215.10",          // 价格
            "z": "0.10"             // 已成交数量
        }
        """
        try:
            # 🔍 简化日志：仅记录关键信息到日志文件
            self.logger.debug(
                f"📨 收到WebSocket订单更新，类型={type(update_data).__name__}")

            # 🔥 更新WebSocket消息时间戳（表示WebSocket正常工作）
            self._last_ws_message_time = time.time()

            self.logger.debug(f"📨 完整订单更新数据: {update_data}")
            self.logger.debug(
                f"📊 WebSocket消息时间戳已更新: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self._last_ws_message_time))}")

            # 🔥 检测数据格式：Hyperliquid OrderData对象 vs Backpack字典
            from ....adapters.exchanges.models import OrderData as ExchangeOrderData

            # === Hyperliquid/Lighter: OrderData对象 ===
            if isinstance(update_data, ExchangeOrderData):
                order_id = str(update_data.id)
                status = update_data.status.value.upper() if update_data.status else ""

                self.logger.debug(
                    f"收到OrderData: id={order_id}, status={status}, "
                    f"side={update_data.side.value}, filled={update_data.filled}/{update_data.amount}")

                # 🔥 新逻辑：直接根据WebSocket推送判断订单完全成交
                if status in ["FILLED", "CLOSED"]:
                    # 提取成交信息
                    filled_amount = update_data.filled  # 已成交数量
                    total_amount = update_data.amount   # 订单总数量
                    filled_price = update_data.average or update_data.price
                    side = update_data.side  # BUY 或 SELL

                    # ✅ 判断是否完全成交
                    if filled_amount >= total_amount:
                        # 🆕 更新最后订单成交时间（用于健康检查延迟）
                        self._last_order_fill_time = time.time()

                        # 完全成交，立即触发反手单
                        self.logger.info(
                            f"✅ [成交] {side.value.upper()} {filled_amount}@{filled_price} "
                            f"(OrderID: {order_id})"
                        )

                        # 🔥 新方案：优先通过 client_id 查找原始订单
                        client_id = str(
                            update_data.client_id) if update_data.client_id else None
                        grid_order = None

                        if client_id:
                            grid_order = self._pending_orders_by_client_id.get(
                                client_id)
                            if grid_order:
                                self.logger.info(
                                    f"   🔍 原始订单: Grid {grid_order.grid_id}, "
                                    f"价格={grid_order.price}, 方向={grid_order.side.value}"
                                )
                            else:
                                self.logger.warning(
                                    f"   ⚠️ client_id={client_id} 不在缓存中（可能是历史订单或健康检查市价单）"
                                )

                        # 如果通过 client_id 找不到，尝试通过 order_id 查找（兼容旧数据）
                        if not grid_order:
                            grid_order = self._pending_orders.get(order_id)
                            if grid_order:
                                self.logger.info(
                                    f"   ✅ 通过 order_id 找到订单: Grid {grid_order.grid_id}"
                                )

                        if grid_order:
                            # 找到了原始订单，标记为已成交
                            grid_order.mark_filled(filled_price, filled_amount)

                            # 从两个缓存中删除
                            if order_id in self._pending_orders:
                                del self._pending_orders[order_id]
                            if client_id and client_id in self._pending_orders_by_client_id:
                                del self._pending_orders_by_client_id[client_id]

                            # 🔥 触发回调，让 grid_coordinator 使用原始价格挂反手单
                            for callback in self._order_callbacks:
                                try:
                                    if asyncio.iscoroutinefunction(callback):
                                        await callback(grid_order)
                                    else:
                                        callback(grid_order)
                                except Exception as e:
                                    self.logger.error(f"订单回调执行失败: {e}")
                        else:
                            # ❌ 找不到原始订单：忽略（可能是历史订单或健康检查市价单）
                            self.logger.warning(
                                f"⚠️ 未找到原始订单 (order_id={order_id}, client_id={client_id or 'N/A'})，"
                                f"忽略此成交推送（可能是历史订单或健康检查市价单）"
                            )
                            # 不触发反手单，让健康检查处理
                    else:
                        # ⏳ 部分成交，继续等待
                        remaining = total_amount - filled_amount
                        self.logger.info(
                            f"⏳ 部分成交: {side.value} 已成交{filled_amount}/{total_amount}, "
                            f"剩余{remaining}, 继续等待..."
                        )

                    return

                elif status in ["CANCELLED", "CANCELED"]:
                    self.logger.debug(f"订单被取消: order_id={order_id}")

                    # 从 _pending_orders 中删除
                    if order_id in self._pending_orders:
                        del self._pending_orders[order_id]

                    client_id = str(
                        update_data.client_id) if update_data.client_id else None
                    if client_id and client_id in self._pending_orders:
                        del self._pending_orders[client_id]

                    # 检查是否是预期的取消
                    is_expected_cancellation = order_id in self._expected_cancellations
                    if is_expected_cancellation:
                        self._expected_cancellations.remove(order_id)
                        self.logger.info(f"ℹ️ 订单已主动取消: {order_id}")
                    else:
                        self.logger.warning(f"⚠️ 订单被手动取消: {order_id}")

                    return

                else:
                    # 🔥 其他状态（如 OPEN, PENDING）：订单挂单成功的通知，无需处理
                    self.logger.debug(f"订单状态更新: {status}, OrderID={order_id}")
                    return

            # === Hyperliquid: 列表格式（订单列表更新）===
            if isinstance(update_data, list):
                self.logger.debug(f"收到Hyperliquid订单列表，包含{len(update_data)}个订单")

                # 🔥 遍历处理每个订单（实现实时WebSocket监控）
                processed_count = 0
                for order_item in update_data:
                    if isinstance(order_item, dict):
                        # 提取订单信息
                        order_id = str(order_item.get('id', ''))
                        status = order_item.get('status', '').lower()

                        # 检查是否是我们的订单
                        if order_id not in self._pending_orders:
                            continue

                        grid_order = self._pending_orders[order_id]

                        # 处理订单成交
                        if status in ['closed', 'filled']:

                            filled_price = Decimal(
                                str(order_item.get('price', grid_order.price)))
                            filled_amount = Decimal(
                                str(order_item.get('filled', grid_order.amount)))

                            # 标记成交并移除
                            grid_order.mark_filled(filled_price, filled_amount)
                            del self._pending_orders[order_id]

                            self.logger.info(
                                f"✅ WebSocket订单成交: {grid_order.side.value} {filled_amount}@{filled_price} "
                                f"(Grid {grid_order.grid_id}, OrderID: {order_id})"
                            )

                            # 🔥 触发回调（反向挂单）
                            for callback in self._order_callbacks:
                                try:
                                    if asyncio.iscoroutinefunction(callback):
                                        await callback(grid_order)
                                    else:
                                        callback(grid_order)
                                except Exception as e:
                                    self.logger.error(f"订单回调执行失败: {e}")

                            processed_count += 1

                if processed_count > 0:
                    self.logger.debug(f"处理了{processed_count}个订单成交")

                return

            # === Backpack: 字典格式 ===
            if not isinstance(update_data, dict):
                self.logger.warning(f"未知的订单更新格式: {type(update_data)}")
                return

            self.logger.debug("使用Backpack格式处理")
            data = update_data.get('data', update_data)

            # 如果data仍然不是字典，跳过
            if not isinstance(data, dict):
                self.logger.debug(f"data字段不是字典格式，跳过: {type(data)}")
                return

            # 从data字段中提取订单信息（Backpack格式）
            order_id = data.get('i')  # Backpack使用'i'表示订单ID
            status = data.get('X')     # Backpack使用'X'表示状态
            event_type = data.get('e')  # 事件类型

            if not order_id:
                self.logger.debug(f"订单更新缺少订单ID: {update_data}")
                return

            # 检查是否是我们的订单
            if order_id not in self._pending_orders:
                self.logger.debug(f"收到非监控订单的更新: {order_id}")
                return

            grid_order = self._pending_orders[order_id]

            self.logger.info(
                f"📨 订单更新: ID={order_id}, "
                f"事件={event_type}, 状态={status}, "
                f"Grid={grid_order.grid_id}"
            )

            # ✅ 修复：Backpack使用"Filled"表示已成交
            if status == 'Filled' or event_type == 'orderFilled':
                # 获取成交价格和数量 - 从data字段中提取
                filled_price = Decimal(str(data.get('p', grid_order.price)))
                filled_amount = Decimal(
                    str(data.get('z', grid_order.amount)))  # 'z'是已成交数量

                grid_order.mark_filled(filled_price, filled_amount)

                # 从挂单列表移除
                del self._pending_orders[order_id]

                self.logger.info(
                    f"✅ 订单成交: {grid_order.side.value} {filled_amount}@{filled_price} "
                    f"(Grid {grid_order.grid_id})"
                )

                # 通知所有回调
                for callback in self._order_callbacks:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(grid_order)
                        else:
                            callback(grid_order)
                    except Exception as e:
                        self.logger.error(f"订单回调执行失败: {e}")

            # 🔥 处理订单取消事件
            elif status == 'Cancelled' or event_type == 'orderCancelled':
                # 从挂单列表移除
                if order_id in self._pending_orders:
                    del self._pending_orders[order_id]

                # 🔥 关键修复：区分主动取消和被动取消
                is_expected_cancellation = order_id in self._expected_cancellations

                if is_expected_cancellation:
                    # 主动取消（剥头皮模式、本金保护等），不重新挂单
                    self._expected_cancellations.remove(order_id)
                    self.logger.info(
                        f"ℹ️ 订单已主动取消，不重新挂单: {grid_order.side.value} {grid_order.amount}@{grid_order.price} "
                        f"(Grid {grid_order.grid_id}, OrderID: {order_id})"
                    )
                else:
                    # 被动取消（用户手动取消），需要重新挂单恢复网格
                    self.logger.warning(
                        f"⚠️ 订单被手动取消，正在恢复网格: {grid_order.side.value} {grid_order.amount}@{grid_order.price} "
                        f"(Grid {grid_order.grid_id}, OrderID: {order_id})"
                    )

                    # 创建新订单（使用相同的网格参数）
                    new_order = GridOrder(
                        order_id="",  # 新订单ID将在提交后获得
                        grid_id=grid_order.grid_id,
                        side=grid_order.side,
                        price=grid_order.price,
                        amount=grid_order.amount,
                        status=GridOrderStatus.PENDING,
                        created_at=datetime.now()
                    )

                    try:
                        # 提交新订单
                        placed_order = await self.place_order(new_order, source="健康检查补单")
                        if placed_order:
                            self.logger.info(
                                f"✅ 网格恢复成功: {placed_order.side.value} {placed_order.amount}@{placed_order.price} "
                                f"(Grid {placed_order.grid_id}, 新OrderID: {placed_order.order_id})"
                            )
                        else:
                            self.logger.error(
                                f"❌ 网格恢复失败: Grid {grid_order.grid_id}, "
                                f"{grid_order.side.value} {grid_order.amount}@{grid_order.price}"
                            )
                    except Exception as e:
                        self.logger.error(
                            f"❌ 重新挂单失败: Grid {grid_order.grid_id}, 错误: {e}"
                        )

        except Exception as e:
            import traceback
            self.logger.error(f"处理订单更新失败: {e}")
            self.logger.error(traceback.format_exc())

    def _convert_order_side(self, grid_side: GridOrderSide) -> ExchangeOrderSide:
        """
        转换订单方向

        Args:
            grid_side: 网格订单方向

        Returns:
            交易所订单方向
        """
        if grid_side == GridOrderSide.BUY:
            return ExchangeOrderSide.BUY
        else:
            return ExchangeOrderSide.SELL

    async def start(self):
        """启动执行引擎"""
        self._running = True
        self.logger.info("网格执行引擎已启动")

        # 🔥 启动订单健康检查（在 _running=True 之后）
        self._start_order_health_check()

    async def stop(self):
        """停止执行引擎"""
        self._running = False

        # 🔥 取消健康检查任务
        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                self.logger.info("健康检查任务已取消")

        # 🔥 取消REST价格更新任务
        if self._rest_price_updater_task and not self._rest_price_updater_task.done():
            self._rest_price_updater_task.cancel()
            try:
                await self._rest_price_updater_task
            except asyncio.CancelledError:
                self.logger.info("REST价格更新任务已取消")

        # 取消所有挂单
        await self.cancel_all_orders()

        self.logger.info("网格执行引擎已停止")

    def is_running(self) -> bool:
        """是否运行中"""
        return self._running

    def __repr__(self) -> str:
        return f"GridEngine({self.exchange}, running={self._running})"

    # ==================== 价格监控相关方法 ====================

    async def _start_price_monitor(self):
        """启动智能价格监控：WebSocket优先，REST备用"""
        try:
            self.logger.info("🔄 正在订阅WebSocket价格数据流...")

            # 订阅WebSocket ticker
            await self.exchange.subscribe_ticker(self.config.symbol, self._on_price_update)
            self._price_ws_enabled = True

            self.logger.info("✅ 价格数据流订阅成功 (WebSocket)")
            self.logger.info("📡 使用WebSocket实时监控价格")

        except Exception as e:
            self.logger.error(f"❌ 价格数据流订阅失败: {e}")
            self.logger.error(f"❌ 错误类型: {type(e).__name__}")
            import traceback
            self.logger.error(f"❌ 错误堆栈:\n{traceback.format_exc()}")
            self.logger.warning("⚠️ WebSocket价格订阅失败，将使用REST API获取价格")
            self._price_ws_enabled = False

    def _on_price_update(self, ticker_data) -> None:
        """
        处理WebSocket价格更新

        Args:
            ticker_data: Ticker数据
        """
        try:
            # 提取价格
            if ticker_data.last is not None:
                price = ticker_data.last
            elif ticker_data.bid is not None and ticker_data.ask is not None:
                price = (ticker_data.bid + ticker_data.ask) / Decimal('2')
            elif ticker_data.bid is not None:
                price = ticker_data.bid
            elif ticker_data.ask is not None:
                price = ticker_data.ask
            else:
                return

            # 更新缓存
            self._current_price = price
            self._last_price_update_time = time.time()

        except Exception as e:
            self.logger.error(f"处理价格更新失败: {e}", exc_info=True)

    async def _update_price_from_rest(self) -> bool:
        """
        立即通过REST更新价格缓存（用于启动时初始化）

        🔥 启动时调用：确保价格缓存有初始值
        - 只在启动时调用一次
        - 有锁保护，避免并发请求
        - 失败返回False，成功返回True

        Returns:
            bool: 更新是否成功
        """
        async with self._rest_price_update_lock:
            try:
                current_time = time.time()
                
                # 尝试通过ticker获取价格
                ticker = await self.exchange.get_ticker(self.config.symbol)
                price = None
                
                if ticker is not None:
                    if getattr(ticker, "last", None) is not None:
                        price = ticker.last
                    elif getattr(ticker, "bid", None) is not None and getattr(ticker, "ask", None) is not None:
                        price = (ticker.bid + ticker.ask) / Decimal('2')
                    elif getattr(ticker, "bid", None) is not None:
                        price = ticker.bid
                    elif getattr(ticker, "ask", None) is not None:
                        price = ticker.ask
                
                # 如果ticker失败，尝试orderbook
                if price is None:
                    try:
                        ob = await self.exchange.get_orderbook(self.config.symbol, limit=1)
                        if ob and getattr(ob, "bids", None) and getattr(ob, "asks", None):
                            best_bid = Decimal(str(ob.bids[0][0]))
                            best_ask = Decimal(str(ob.asks[0][0]))
                            price = (best_bid + best_ask) / Decimal('2')
                    except Exception:
                        pass  # 静默失败
                
                # 更新缓存
                if price is not None:
                    self._current_price = price
                    self._last_price_update_time = current_time
                    self._last_rest_price_update_time = current_time
                    self.logger.info(f"✅ 启动价格初始化成功: {price}")
                    return True
                else:
                    self.logger.warning("⚠️ 启动价格初始化失败：无有效价格")
                    return False
            
            except Exception as e:
                self.logger.error(f"启动价格初始化异常: {e}")
                return False

    def _start_rest_price_updater(self):
        """
        启动后台REST价格更新任务

        🔥 新增：定期通过REST API更新价格缓存（30秒间隔）
        - 作为WS推送的兜底机制
        - 避免并发调用，统一由后台任务更新
        - 限流保护，30秒最小间隔
        """
        if self._rest_price_updater_task is None or self._rest_price_updater_task.done():
            self._rest_price_updater_task = asyncio.create_task(
                self._rest_price_update_loop()
            )
            self.logger.info("✅ 后台REST价格更新任务已启动（30秒间隔）")

    async def _rest_price_update_loop(self):
        """
        REST价格更新循环

        🔥 定期通过REST更新价格缓存
        - 30秒间隔（避免触发429限流）
        - 有锁保护，避免并发请求
        - 静默更新，失败不中断系统
        """
        while self._running:
            try:
                await asyncio.sleep(self._rest_price_update_interval)
                
                # 使用锁避免并发REST请求
                async with self._rest_price_update_lock:
                    current_time = time.time()
                    
                    # 二次检查：确保距离上次更新已超过间隔（避免其他地方触发）
                    if current_time - self._last_rest_price_update_time < self._rest_price_update_interval:
                        continue
                    
                    try:
                        # 尝试通过ticker获取价格
                        ticker = await self.exchange.get_ticker(self.config.symbol)
                        price = None
                        
                        if ticker is not None:
                            if getattr(ticker, "last", None) is not None:
                                price = ticker.last
                            elif getattr(ticker, "bid", None) is not None and getattr(ticker, "ask", None) is not None:
                                price = (ticker.bid + ticker.ask) / Decimal('2')
                            elif getattr(ticker, "bid", None) is not None:
                                price = ticker.bid
                            elif getattr(ticker, "ask", None) is not None:
                                price = ticker.ask
                        
                        # 如果ticker失败，尝试orderbook
                        if price is None:
                            try:
                                ob = await self.exchange.get_orderbook(self.config.symbol, limit=1)
                                if ob and getattr(ob, "bids", None) and getattr(ob, "asks", None):
                                    best_bid = Decimal(str(ob.bids[0][0]))
                                    best_ask = Decimal(str(ob.asks[0][0]))
                                    price = (best_bid + best_ask) / Decimal('2')
                            except Exception:
                                pass  # 静默失败
                        
                        # 更新缓存
                        if price is not None:
                            self._current_price = price
                            self._last_price_update_time = current_time
                            self._last_rest_price_update_time = current_time
                            self.logger.debug(f"🔄 REST价格更新成功: {price}")
                        else:
                            self.logger.warning("⚠️ REST价格更新失败：无有效价格")
                    
                    except Exception as e:
                        # REST失败不中断系统，静默记录
                        self.logger.debug(f"REST价格更新失败: {e}")
                        self._last_rest_price_update_time = current_time  # 记录尝试时间，避免频繁重试
            
            except asyncio.CancelledError:
                self.logger.info("REST价格更新任务已取消")
                break
            except Exception as e:
                self.logger.error(f"REST价格更新循环异常: {e}", exc_info=True)
                await asyncio.sleep(5)  # 异常后短暂等待再继续

    def get_price_monitor_mode(self) -> str:
        """
        获取当前价格监控方式

        Returns:
            监控方式：'WebSocket' 或 'REST'
        """
        if self._price_ws_enabled and self._current_price is not None:
            price_age = time.time() - self._last_price_update_time
            # 如果价格在10秒内更新过，认为WebSocket正常
            if price_age < 10:
                return "WebSocket"
        return "REST"

    # ==================== 订单健康检查相关方法 ====================

    def _start_order_health_check(self):
        """启动订单健康检查任务"""
        if self._health_check_task is None or self._health_check_task.done():
            # 检查是否启用健康检查
            if self.config.order_health_check_enabled:
                self._health_check_task = asyncio.create_task(
                    self._order_health_check_loop())
                self.logger.info(
                    f"✅ 订单健康检查已启动：间隔={self.config.order_health_check_interval}秒"
                )
            else:
                self.logger.info(
                    "📊 订单健康检查已禁用（配置: order_health_check_enabled=false）")

    async def _order_health_check_loop(self):
        """订单健康检查循环（使用新模块）"""
        self.logger.info("📊 订单健康检查循环已启动（使用新模块）")

        # 初始延迟，等待系统稳定
        await asyncio.sleep(60)  # 启动后1分钟开始第一次检查

        while self._running:
            try:
                # 🔥 检查健康检查是否仍然启用（支持运行时禁用）
                if not self.config.order_health_check_enabled:
                    self.logger.info("📊 健康检查已被禁用，退出健康检查循环")
                    break

                current_time = time.time()
                time_since_last_check = current_time - self._last_health_check_time

                # 检查是否到达检查间隔
                if time_since_last_check >= self.config.order_health_check_interval:
                    # 🆕 额外条件：等待5秒内无订单成交（避免高频交易时检查）
                    time_since_last_fill = current_time - self._last_order_fill_time

                    if time_since_last_fill < 5:
                        # 5秒内有订单成交，延后健康检查
                        remaining_time = 5 - time_since_last_fill
                        self.logger.info(
                            f"⏸️ 健康检查延后: 距上次订单成交仅{time_since_last_fill:.1f}秒, "
                            f"等待{remaining_time:.1f}秒后再检查（避免高频交易时冲突）"
                        )
                        # 不更新 _last_health_check_time，继续等待
                        # 🔥 优化：根据剩余时间动态等待，避免不必要的延迟
                        # 多等0.5秒确保超过5秒
                        await asyncio.sleep(remaining_time + 0.5)
                        continue

                    self.logger.info(
                        f"🔍 准备健康检查: 距上次检查={time_since_last_check:.0f}秒, "
                        f"距上次订单成交={time_since_last_fill:.1f}秒, "
                        f"配置间隔={self.config.order_health_check_interval}秒"
                    )

                    # 🔥 阶段2：快照验证（确保数据可靠）
                    snapshot_valid, snapshot_data = await self._validate_market_snapshots()

                    if not snapshot_valid:
                        self.logger.info(
                            f"⏸️ 快照验证失败（价格不稳定或有订单成交），跳过本次轮询"
                        )
                        self.logger.info(
                            f"⏭️  等待下次健康检查: {self.config.order_health_check_interval}秒后"
                        )
                        # 🔥 更新最后检查时间，跳过本次轮询，等待下一个完整周期
                        self._last_health_check_time = current_time
                        continue

                    # 🆕 调用新的健康检查模块（🚀 直接使用快照数据）
                    if self._health_checker:
                        try:
                            orders, positions = snapshot_data
                            await self._health_checker.perform_health_check(
                                snapshot_orders=orders,
                                snapshot_positions=positions
                            )
                            self.logger.info("✅ 健康检查完成")

                            # 🔥 新增：健康检查成功后尝试恢复系统（如果系统因网络故障暂停）
                            if self.coordinator and self.coordinator._paused and self.coordinator._paused_reason == 'network':
                                self.logger.info(
                                    "✅ 健康检查API调用成功，正在自动恢复系统..."
                                )
                                await self.coordinator.resume(auto=True)
                        except Exception as e:
                            self.logger.error(
                                f"❌ 健康检查执行失败: {e}", exc_info=True)
                    else:
                        self.logger.error("⚠️ 健康检查器未初始化")

                    self._last_health_check_time = current_time

                # 休眠一段时间再检查（避免频繁循环）
                await asyncio.sleep(60)  # 每分钟检查一次是否到达间隔时间

            except asyncio.CancelledError:
                self.logger.info("订单健康检查已停止")
                break
            except Exception as e:
                self.logger.error(f"订单健康检查出错: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(60)  # 出错后等待1分钟再继续

    async def _validate_market_snapshots(self) -> Tuple[bool, Optional[Tuple[List, List]]]:
        """
        验证市场快照数据可靠性

        流程：
        1. 记录当前最后成交时间
        2. 连续N次获取市场数据（订单+持仓），间隔0.5秒（N由配置决定）
        3. 🔥 每次快照后立即检查是否有新订单成交（优化：更早发现成交）
        4. 对比N次快照数据是否完全一致

        Returns:
            (验证结果, 快照数据):
            - (True, (orders, positions)): 快照有效，返回已验证的市场数据
            - (False, None): 快照无效，需要重新等待
        """
        try:
            snapshot_count = self.config.health_check_snapshot_count
            self.logger.info(f"📸 开始市场快照验证（{snapshot_count}次快照，间隔0.5秒）")

            # 记录快照开始时的最后成交时间
            snapshot_start_fill_time = self._last_order_fill_time

            # 存储N次快照数据
            snapshots = []

            # 🔥 存储原始市场数据（用于后续直接使用，避免重复API调用）
            last_orders = None
            last_positions = None

            # 连续获取N次市场数据
            for i in range(snapshot_count):
                self.logger.info(f"📸 获取第{i+1}次快照...")

                # 并发获取订单和持仓
                try:
                    orders_task = self.exchange.get_open_orders(
                        self.config.symbol)
                    positions_task = self.exchange.get_positions()

                    orders, positions = await asyncio.gather(
                        orders_task,
                        positions_task,
                        return_exceptions=False
                    )

                    # 🔥 保存最后一次的原始数据
                    last_orders = orders
                    last_positions = positions

                    # 提取关键数据用于对比
                    snapshot_data = {
                        'order_count': len(orders),
                        'order_prices': sorted([float(o.price) for o in orders]),
                        'position_size': float(positions[0].size) if positions else 0.0
                    }

                    snapshots.append(snapshot_data)

                    self.logger.info(
                        f"   ✅ 快照{i+1}: 订单={snapshot_data['order_count']}个, "
                        f"持仓={snapshot_data['position_size']}"
                    )

                except Exception as e:
                    self.logger.error(f"❌ 获取快照{i+1}失败: {e}")
                    return False, None

                # 🔥 优化：每次快照后立即检查是否有新订单成交
                # 如果有成交，立即停止后续快照，返回失败
                current_fill_time = self._last_order_fill_time
                if current_fill_time > snapshot_start_fill_time:
                    self.logger.info(
                        f"❌ 第{i+1}次快照后检测到新订单成交，立即停止快照 "
                        f"(成交时间: {time.strftime('%H:%M:%S', time.localtime(current_fill_time))})"
                    )
                    return False, None

                # 除了最后一次，都需要等待0.5秒
                if i < snapshot_count - 1:
                    await asyncio.sleep(0.5)

            self.logger.info(f"✅ {snapshot_count}次快照期间无订单成交")

            # 检查2：所有快照数据是否完全一致
            # 对比订单数量（所有快照的订单数量必须相同）
            order_counts = [snap['order_count'] for snap in snapshots]
            if len(set(order_counts)) > 1:
                counts_str = ' vs '.join(str(c) for c in order_counts)
                self.logger.info(f"❌ 快照订单数量不一致: {counts_str}")
                return False, None

            # 对比订单价格（允许极小误差，仅用于浮点数精度问题）
            # 容差 = 1个最小价格单位（例如：price_decimals=1 → 0.1, price_decimals=2 → 0.01）
            price_tolerance = 10 ** (-self.config.price_decimals)

            # 获取第一个快照的订单价格列表长度
            first_order_count = len(snapshots[0]['order_prices'])

            # 检查所有快照的订单价格列表长度是否一致
            for snap in snapshots:
                if len(snap['order_prices']) != first_order_count:
                    self.logger.info(f"❌ 快照订单数量结构不一致")
                    return False, None

            # 对比每个订单的价格（在所有快照中）
            for idx in range(first_order_count):
                # 获取所有快照中该订单的价格
                prices = [snap['order_prices'][idx] for snap in snapshots]

                # 检查价格是否在容差范围内一致
                # 方法：比较相邻快照的价格差异
                for j in range(len(prices) - 1):
                    if abs(prices[j] - prices[j+1]) > price_tolerance:
                        prices_str = ' vs '.join(f"{p:.2f}" for p in prices)
                        self.logger.info(
                            f"❌ 快照订单价格不一致 (订单{idx}): {prices_str}"
                        )
                        return False, None

            # 对比持仓数量（允许极小误差，仅用于浮点数精度问题）
            # 容差 = 1个最小数量单位（例如：quantity_precision=5 → 0.00001, quantity_precision=3 → 0.001）
            position_tolerance = 10 ** (-self.config.quantity_precision)

            # 获取所有快照的持仓数量
            position_sizes = [snap['position_size'] for snap in snapshots]

            # 检查持仓数量是否在容差范围内一致（比较相邻快照）
            for j in range(len(position_sizes) - 1):
                if abs(position_sizes[j] - position_sizes[j+1]) > position_tolerance:
                    positions_str = ' vs '.join(str(p) for p in position_sizes)
                    self.logger.info(f"❌ 快照持仓数量不一致: {positions_str}")
                    return False, None

            # 🔥 所有检查通过，返回验证成功和最后一次快照的原始数据
            self.logger.info(
                f"✅ 快照验证通过: 订单={snapshots[0]['order_count']}个, "
                f"持仓={snapshots[0]['position_size']}, 数据一致且稳定"
            )
            self.logger.info("🚀 直接使用快照数据，无需重复API调用")
            return True, (last_orders, last_positions)

        except Exception as e:
            self.logger.error(f"❌ 快照验证过程出错: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False, None

    def _notify_health_check_complete(self, filled_count: int):
        """
        通知健康检查完成

        在补充订单后调用，让外部系统（coordinator）知道需要刷新状态

        Args:
            filled_count: 成功补充的订单数量
        """
        try:
            # 统计当前订单状态（注意：GridOrderSide的值是小写 'buy' 和 'sell'）
            # 🔥 使用 get_pending_orders() 获取去重后的订单列表
            pending_orders = self.get_pending_orders()
            buy_count = sum(1 for o in pending_orders
                            if o.side.value.lower() == 'buy')
            sell_count = sum(1 for o in pending_orders
                             if o.side.value.lower() == 'sell')
            total_count = len(pending_orders)

            self.logger.info(
                f"📊 健康检查后订单统计: "
                f"总计={total_count}个, 买单={buy_count}个, 卖单={sell_count}个"
            )

            # 🔥 强制触发UI更新：直接调用coordinator的同步方法
            # 注意：这里不能直接访问coordinator，因为是循环依赖
            # 所以我们更新时间戳，让get_statistics时自动同步
            if filled_count > 0:
                self._last_health_repair_count = filled_count
                self._last_health_repair_time = time.time()

                # 记录详细日志便于调试
                self.logger.info(
                    f"✅ 健康检查已完成订单补充，补充数量={filled_count}个"
                )

        except Exception as e:
            self.logger.error(f"通知健康检查完成失败: {e}")

    async def _sync_orders_from_exchange(self, exchange_orders: List):
        """
        将交易所查询到的订单同步到本地_pending_orders缓存

        Args:
            exchange_orders: 从交易所get_open_orders()查询到的订单列表

        作用:
            修复本地缓存与交易所实际状态不一致的问题
            确保终端UI显示正确的订单数量
            🔥 同时更新 _pending_orders 和 _pending_orders_by_client_id 两个缓存
        """
        try:
            from ..models import GridOrder, GridOrderSide, GridOrderStatus
            from datetime import datetime

            # 构建交易所订单ID集合（用于对比）
            exchange_order_ids = {
                order.id for order in exchange_orders if order.id}

            # 1. 移除本地缓存中不存在于交易所的订单（可能已成交或取消）
            removed_count = 0
            removed_client_ids = 0

            # 1.1 清理 _pending_orders
            for order_id in list(self._pending_orders.keys()):
                if order_id not in exchange_order_ids:
                    del self._pending_orders[order_id]
                    removed_count += 1

            # 🔥 1.2 同步清理 _pending_orders_by_client_id（关键修复）
            # UI 从这个缓存读取订单数量，必须同步更新！
            # ⚠️  重要：只清理order_id不在交易所订单列表中的条目
            #    不要清理所有没有client_id的订单，因为交易所API可能不返回client_id
            for client_id in list(self._pending_orders_by_client_id.keys()):
                cached_order = self._pending_orders_by_client_id[client_id]
                # 只有当订单已经不存在于交易所时才删除
                if cached_order.order_id and cached_order.order_id not in exchange_order_ids:
                    del self._pending_orders_by_client_id[client_id]
                    removed_client_ids += 1

            if removed_count > 0 or removed_client_ids > 0:
                self.logger.debug(
                    f"🗑️ 清理本地缓存：移除{removed_count}个订单（_pending_orders）, "
                    f"{removed_client_ids}个client_id（_pending_orders_by_client_id）"
                )

            # 2. 将交易所订单添加到本地缓存（如果本地没有）
            added_count = 0
            added_client_ids = 0
            synced_client_ids = 0  # 已存在订单的client_id同步数

            for ex_order in exchange_orders:
                if not ex_order.id:
                    continue

                # 如果本地缓存中没有这个订单，添加它
                if ex_order.id not in self._pending_orders:
                    try:
                        # 映射到网格ID
                        grid_id = self.config.get_grid_index_by_price(
                            ex_order.price)

                        # 转换订单方向
                        side = GridOrderSide.BUY if ex_order.side.value.lower() == 'buy' else GridOrderSide.SELL

                        # 创建GridOrder对象
                        # 🔥 ex_order 是 OrderData 对象，已经包含正确解析的 client_id
                        # （通过 _parse_order_from_rest 从 client_order_index 提取）
                        grid_order = GridOrder(
                            order_id=ex_order.id,
                            grid_id=grid_id,
                            side=side,
                            price=ex_order.price,
                            amount=ex_order.amount,
                            status=GridOrderStatus.PENDING,
                            created_at=datetime.now(),
                            # 🔥 直接使用 OrderData.client_id（已从 REST API 正确解析）
                            client_id=ex_order.client_id if hasattr(
                                ex_order, 'client_id') and ex_order.client_id else None
                        )

                        # 添加到 _pending_orders
                        self._pending_orders[ex_order.id] = grid_order
                        added_count += 1

                        # 🔥 如果有 client_id，也添加到 _pending_orders_by_client_id（关键修复）
                        if grid_order.client_id:
                            self._pending_orders_by_client_id[grid_order.client_id] = grid_order
                            added_client_ids += 1

                    except Exception as e:
                        self.logger.warning(
                            f"⚠️ 同步订单{ex_order.id[:10]}...失败: {e}")

                else:
                    # 🔥🔥🔥 关键修复：订单已存在，但需要确保client_id缓存也有
                    # 这是UI显示的数据源，必须保持同步！
                    try:
                        existing_order = self._pending_orders[ex_order.id]

                        # 如果订单有client_id，但不在client_id缓存中，添加它
                        if existing_order.client_id and existing_order.client_id not in self._pending_orders_by_client_id:
                            self._pending_orders_by_client_id[existing_order.client_id] = existing_order
                            synced_client_ids += 1

                        # 如果现有订单没有client_id，但交易所订单有，更新它
                        elif not existing_order.client_id and ex_order.client_id:
                            existing_order.client_id = ex_order.client_id
                            self._pending_orders_by_client_id[ex_order.client_id] = existing_order
                            synced_client_ids += 1

                    except Exception as e:
                        self.logger.warning(
                            f"⚠️ 同步已存在订单的client_id失败: {e}")

            # 3. 统计同步结果
            total_local = len(self.get_pending_orders())
            total_exchange = len(exchange_orders)
            total_client_id_cache = len(self._pending_orders_by_client_id)

            self.logger.info(
                f"✅ 订单同步完成: 交易所={total_exchange}个, 本地缓存={total_local}个, "
                f"client_id缓存={total_client_id_cache}个"
            )

            self.logger.info(
                f"   新增订单={added_count}个, 移除订单={removed_count}个, "
                f"同步client_id={synced_client_ids}个"
            )

            # 4. 如果数量仍不匹配，记录警告
            if total_local != total_exchange:
                self.logger.warning(
                    f"⚠️ 同步后数量仍不匹配: 本地{total_local}个 vs 交易所{total_exchange}个, "
                    f"差异={abs(total_local - total_exchange)}个"
                )

            # 🔥 5. 检查 client_id 缓存的一致性（UI 数据源）
            if total_client_id_cache != total_local:
                missing_count = total_local - total_client_id_cache
                self.logger.warning(
                    f"⚠️ client_id缓存不完整: 本地订单={total_local}个, client_id缓存={total_client_id_cache}个, "
                    f"缺失={missing_count}个 (部分订单可能无client_id，将尝试下次同步)"
                )

        except Exception as e:
            self.logger.error(f"❌ 同步订单失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
