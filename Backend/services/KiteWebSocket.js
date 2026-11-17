// services/KiteWebSocket.js
// Kite Connect WebSocket wrapper using official kiteconnectjs library

import { KiteTicker } from 'kiteconnect';
import { getIO } from "../sockets/io.js";
import { onMarketTick } from "../Utils/OrderManager.js";
import KiteCredential from "../Model/KiteCredentialModel.js";

const roomFor = (token) => `sec:${token}`;

export class KiteWebSocket {
  constructor() {
    this.ticker = null;
    this.subscribedTokens = new Set();
    this.subscriptionQueue = [];
    this.last = new Map();
    this.isConnected = false;
  }

  get ns() {
    return getIO().of("/market");
  }

  /**
   * Set new access token (for token refresh)
   */
  setToken(newToken) {
    // Token is stored in database now
    this.accessToken = newToken;
  }

  /**
   * Connect to Kite WebSocket - fetches credentials from database
   */
  async connect() {
    try {
      // Fetch credentials from database
      const credential = await KiteCredential.getActive(true);

      if (!credential) {

        console.error("[KiteWS] FATAL: No active Kite credentials found in database.");
        console.error("   Please login via /api/kite/login-url first.");
        return;
      }

      const { api_key, access_token } = credential;

      if (!api_key || !access_token) {
        console.error("[KiteWS] FATAL: Kite API key or access token missing in database.");
        console.error("   Please login via /api/kite/login-url to get a valid token.");
        return;
      }

      // Check if token is expired
      if (credential.token_expiry && new Date(credential.token_expiry) < new Date()) {
        console.error("[KiteWS] WARNING: Access token has expired.");
        console.error("   Please login again via /api/kite/login-url");
      }

      // Initialize KiteTicker
      this.ticker = new KiteTicker({
        api_key: api_key,
        access_token: access_token,
        reconnect: true,
        max_retry: 50,
        max_delay: 60
      });

      // Setup event handlers
      this.setupEventHandlers();

      // Connect
      console.log("[KiteWS] Connecting to Kite WebSocket...");
      console.log(`[KiteWS]   User: ${credential.user_id || 'unknown'}`);
      console.log(`[KiteWS]   Token expires: ${credential.token_expiry ? new Date(credential.token_expiry).toLocaleString() : 'unknown'}`);
      this.ticker.connect();

    } catch (error) {
      console.error("[KiteWS] Error connecting:", error.message);
    }
  }

  setupEventHandlers() {
    // On successful connection
    this.ticker.on('connect', () => {
      console.log("[KiteWS] ✅ Connected successfully");
      this.isConnected = true;
      this.reconnectAttempts = 0; // Reset on successful connection

      // Re-subscribe all active tokens on every connect/reconnect!
      this.resubscribeAll();

      // Process queued subscriptions
      if (this.subscriptionQueue.length > 0) {
        console.log(`[KiteWS] Processing ${this.subscriptionQueue.length} queued subscriptions`);
        const queue = [...this.subscriptionQueue];
        this.subscriptionQueue = [];

        queue.forEach(({ tokens, mode }) => {
          this.subscribeTokens(tokens, mode);
        });
      }
    });

    // On tick data received
    this.ticker.on('ticks', (ticks) => {
      ticks.forEach(tick => this.processTick(tick));
    });

    // On disconnection
    this.ticker.on('disconnect', (error) => {
      console.warn("[KiteWS] ❌ Disconnected:", error?.message || error);
      this.isConnected = false;
    });

    // On error - check for auth errors
    this.ticker.on('error', (error) => {
      const errorMsg = error?.message || String(error);
      console.error("[KiteWS] Error:", errorMsg);

      // Check for authentication errors (403, token expired, etc.)
      if (errorMsg.includes('403') || errorMsg.includes('Forbidden') ||
        errorMsg.includes('token') || errorMsg.includes('unauthorized') ||
        errorMsg.includes('Unexpected server response: 403')) {
        console.error("[KiteWS] ⚠️ Authentication error detected - will attempt auto-login");
        this.handleAuthError();
      }
    });

    // On reconnection attempt
    this.ticker.on('reconnect', (reconnect_count, reconnect_interval) => {
      console.log(`[KiteWS] 🔄 Reconnecting: attempt ${reconnect_count}, interval ${reconnect_interval}s`);
    });

    // On max reconnection attempts reached - DON'T CRASH, HANDLE GRACEFULLY
    this.ticker.on('noreconnect', () => {
      console.warn("[KiteWS] ⚠️ Max reconnection attempts reached by KiteTicker.");
      console.log("[KiteWS] 🔄 Will attempt to refresh token and reconnect in 60 seconds...");
      this.isConnected = false;

      // Schedule a recovery attempt - don't crash the server
      this.scheduleRecovery();
    });

    // On order update (postback)
    this.ticker.on('order_update', (order) => {
      console.log("[KiteWS] 📦 Order update received:", order?.order_id || order);
      this.ns.emit("order_update", order);
    });

    // On close
    this.ticker.on('close', (reason) => {
      const reasonStr = String(reason?.code || reason?.reason || reason || '');
      console.warn("[KiteWS] Connection closed:", reasonStr);
      this.isConnected = false;

      if (reasonStr.includes('403') || reasonStr.includes('Forbidden')) {
        console.error("[KiteWS] ⚠️ Connection closed due to 403 Forbidden - will attempt auto-login");
        this.handleAuthError();
      }
    });

    // On raw message (optional - for debugging)
    this.ticker.on('message', (data) => {
      // Uncomment for debugging: console.log("[KiteWS] Raw message:", data);
    });
  }

  /**
   * Re-subscribes all previously tracked tokens upon connect or reconnect
   */
  resubscribeAll() {
    if (!this.ticker || !this.isConnected) return;

    const tokens = Array.from(this.subscribedTokens)
      .map(t => parseInt(t))
      .filter(t => !isNaN(t) && t > 0);

    if (tokens.length > 0) {
      console.log(`[KiteWS] 🔄 Re-subscribing to ${tokens.length} active tokens on connected ticker`);
      try {
        this.ticker.subscribe(tokens);
        this.ticker.setMode(this.ticker.modeFull, tokens);
        console.log(`[KiteWS] ✅ Successfully re-subscribed ${tokens.length} tokens (modeFull)`);
      } catch (err) {
        console.error("[KiteWS] ❌ Re-subscribe failed:", err?.message || err);
      }
    }
  }

  /**
   * Handle authentication errors by triggering auto-login
   */
  async handleAuthError() {
    if (this.isHandlingAuthError) {
      console.log("[KiteWS] Auth recovery already running. Skipping duplicate call.");
      return;
    }
    this.isHandlingAuthError = true;

    try {
      console.log("[KiteWS] 🔐 Attempting auto-login due to auth error...");

      // Dynamic import to avoid circular dependency
      const { runAutoLogin } = await import('./AutoLoginService.js');
      const result = await runAutoLogin();

      if (result.success) {
        console.log("[KiteWS] ✅ Auto-login successful, reconnecting...");
        await this.reconnectWithNewCredentials();
      } else {
        console.error("[KiteWS] ❌ Auto-login failed:", result.error);
        // Schedule another attempt later
        this.scheduleRecovery(180000); // 3 minutes
      }
    } catch (error) {
      console.error("[KiteWS] Error during auth recovery:", error.message);
      this.scheduleRecovery(180000); // 3 minutes
    } finally {
      this.isHandlingAuthError = false;
    }
  }

  /**
   * Schedule a recovery attempt without crashing the server
   */
  scheduleRecovery(delayMs = 60000) {
    if (this.recoveryTimeout) {
      clearTimeout(this.recoveryTimeout);
    }

    console.log(`[KiteWS] ⏰ Scheduling recovery attempt in ${delayMs / 1000} seconds...`);

    this.recoveryTimeout = setTimeout(async () => {
      console.log("[KiteWS] 🔄 Running scheduled recovery...");
      await this.reconnectWithNewCredentials();
    }, delayMs);
  }

  /**
   * Reconnect with fresh credentials from database
   */
  async reconnectWithNewCredentials() {
    try {
      console.log("[KiteWS] 🔄 Reconnecting with fresh credentials from database...");

      if (this.recoveryTimeout) {
        clearTimeout(this.recoveryTimeout);
        this.recoveryTimeout = null;
      }

      // Close existing connection if any
      if (this.ticker) {
        try {
          this.ticker.disconnect();
        } catch (e) {
          // Ignore close errors
        }
        this.ticker = null;
      }

      this.isConnected = false;

      // Wait a bit before reconnecting
      await new Promise(r => setTimeout(r, 1500));

      // Connect with fresh credentials
      await this.connect();

    } catch (error) {
      console.error("[KiteWS] Error during reconnection:", error.message);
      // Don't crash - schedule another attempt
      this.scheduleRecovery(120000); // 2 minutes
    }
  }

  /**
   * Subscribe to instruments
   * @param {Array} list - Array of objects with instrument_token property
   * @param {string} mode - 'ltp', 'quote', or 'full' (default: 'full')
   */
  subscribe(list, mode = 'full') {
    if (!Array.isArray(list) || !list.length) return;

    // Extract instrument tokens (convert to integers)
    const tokens = list
      .map(inst => {
        // Support both instrument_token and securityId for backward compatibility
        const token = inst.instrument_token || inst.securityId;
        return parseInt(token);
      })
      .filter(token => !isNaN(token) && token > 0);

    if (!tokens.length) {
      console.warn("[KiteWS] No valid instrument tokens provided");
      return;
    }

    // If not connected, queue the subscription
    if (!this.isConnected || !this.ticker) {
      console.log(`[KiteWS] Not connected. Queueing ${tokens.length} tokens`);
      this.subscriptionQueue.push({ tokens, mode });

      // Attempt to connect if ticker not initialized
      if (!this.ticker) {
        this.connect();
      }
      return;
    }

    this.subscribeTokens(tokens, mode);
  }

  subscribeTokens(tokens, mode = 'full') {
    if (!tokens || !tokens.length) return;

    // Always remember tokens in this.subscribedTokens
    tokens.forEach(token => this.subscribedTokens.add(token));

    if (!this.isConnected || !this.ticker) {
      this.subscriptionQueue.push({ tokens, mode });
      return;
    }

    try {
      // Send subscription to Kite
      this.ticker.subscribe(tokens);

      const modeMap = {
        'ltp': this.ticker.modeLTP,
        'quote': this.ticker.modeQuote,
        'full': this.ticker.modeFull
      };
      const tickerMode = modeMap[mode] || this.ticker.modeFull;

      this.ticker.setMode(tickerMode, tokens);
      console.log(`[KiteWS] Subscribed & set mode '${mode}' for ${tokens.length} tokens (Total tracked: ${this.subscribedTokens.size})`);
    } catch (error) {
      console.error("[KiteWS] Subscription error:", error?.message || error);
    }
  }

  /**
   * Unsubscribe from instruments
   * @param {Array} list - Array of objects with instrument_token property
   */
  unsubscribe(list) {
    if (!Array.isArray(list) || !list.length) return;
    if (!this.isConnected || !this.ticker) return;

    const tokens = list
      .map(inst => {
        const token = inst.instrument_token || inst.securityId;
        return parseInt(token);
      })
      .filter(token => !isNaN(token) && this.subscribedTokens.has(token));

    if (!tokens.length) return;

    console.log(`[KiteWS] Unsubscribing from ${tokens.length} tokens`);

    try {
      this.ticker.unsubscribe(tokens);
      tokens.forEach(token => this.subscribedTokens.delete(token));

      console.log(`[KiteWS] ✅ Unsubscribed. Remaining: ${this.subscribedTokens.size}`);
    } catch (error) {
      console.error("[KiteWS] Unsubscribe error:", error?.message || error);
    }
  }

  /**
   * Change mode for subscribed instruments
   * @param {Array} list - Array of objects with instrument_token property
   * @param {string} mode - 'ltp', 'quote', or 'full'
   */
  setMode(list, mode = 'full') {
    if (!Array.isArray(list) || !list.length) return;
    if (!this.isConnected || !this.ticker) return;

    const tokens = list
      .map(inst => {
        const token = inst.instrument_token || inst.securityId;
        return parseInt(token);
      })
      .filter(token => !isNaN(token));

    if (!tokens.length) return;

    console.log(`[KiteWS] Setting mode '${mode}' for ${tokens.length} tokens`);

    try {
      const modeMap = {
        'ltp': this.ticker.modeLTP,
        'quote': this.ticker.modeQuote,
        'full': this.ticker.modeFull
      };
      const tickerMode = modeMap[mode] || this.ticker.modeFull;
      this.ticker.setMode(tickerMode, tokens);
    } catch (error) {
      console.error("[KiteWS] Set mode error:", error?.message || error);
    }
  }

  /**
   * Process incoming tick data - Kite format (no Dhan backward compatibility)
   */
  processTick(tick) {
    const token = String(tick.instrument_token);

    // Calculate net change and percent change
    const ltp = tick.last_price || 0;
    const close = tick.ohlc?.close || 0;
    const netChange = close > 0 ? ltp - close : 0;
    const percentChange = close > 0 ? ((ltp - close) / close) * 100 : 0;

    // Build normalized payload with Kite fields
    const payload = {
      instrument_token: tick.instrument_token,
      mode: tick.mode,
      ltp: ltp,
      tradable: tick.tradable,
      change: tick.change || 0,
      netChange: netChange,
      percentChange: percentChange
    };

    // Add mode-specific fields
    if (tick.mode === 'quote' || tick.mode === 'full') {
      Object.assign(payload, {
        lastTradeQty: tick.last_traded_quantity,
        avgPrice: tick.average_traded_price,
        volume: tick.volume_traded,
        totalBuyQty: tick.total_buy_quantity,
        totalSellQty: tick.total_sell_quantity,
        open: tick.ohlc?.open,
        high: tick.ohlc?.high,
        low: tick.ohlc?.low,
        close: tick.ohlc?.close
      });
    }

    if (tick.mode === 'full') {
      Object.assign(payload, {
        lastTradeTime: tick.last_trade_time,
        exchangeTimestamp: tick.exchange_timestamp,
        oi: tick.oi,
        oiDayHigh: tick.oi_day_high,
        oiDayLow: tick.oi_day_low,
        depth: tick.depth
      });

      // Extract best bid/ask from depth
      if (tick.depth) {
        payload.bestBidPrice = tick.depth.buy?.[0]?.price || null;
        payload.bestBidQuantity = tick.depth.buy?.[0]?.quantity || null;
        payload.bestAskPrice = tick.depth.sell?.[0]?.price || null;
        payload.bestAskQuantity = tick.depth.sell?.[0]?.quantity || null;
      }
    }

    // Update cache
    this.last.set(token, { ...this.last.get(token), ...payload });

    // Emit to Socket.IO room (room key = instrument_token)
    this.ns.to(roomFor(token)).emit("market_update", payload);

    // Trigger OrderManager callback for stop-loss/target monitoring
    if (ltp > 0) {
      onMarketTick({ token, ltp });
    }
  }

  /**
   * Get snapshot of cached data
   * @param {Array} ids - Array of instrument tokens
   * @returns {Object} Map of token -> data
   */
  getSnapshot(ids = []) {
    const out = {};
    for (const id of ids) {
      out[String(id)] = this.last.get(String(id)) || this._empty();
    }
    return out;
  }

  _empty() {
    return {
      ltp: null,
      open: null,
      high: null,
      low: null,
      close: null,
      volume: null,
      oi: null,
      bestBidPrice: null,
      bestBidQuantity: null,
      bestAskPrice: null,
      bestAskQuantity: null,
      lastTradeQty: null,
      lastTradeTime: null,
      avgPrice: null,
      change: null
    };
  }

  /**
   * Close WebSocket connection
   */
  close() {
    if (this.ticker) {
      console.log("[KiteWS] Closing connection...");
      try {
        this.ticker.disconnect();
      } catch (e) {
        // Ignore disconnect errors
      }
      this.ticker = null;
      this.isConnected = false;
      this.subscribedTokens.clear();
    }
  }

  /**
   * Check if connected
   */
  connected() {
    return this.isConnected;
  }
}

// Export singleton-style initializer for consistency with existing codebase
let kiteWSInstance = null;

export function initializeKiteWebSocket() {
  if (!kiteWSInstance) {
    kiteWSInstance = new KiteWebSocket();
  }
  return kiteWSInstance;
}

export function getKiteWebSocket() {
  return kiteWSInstance;
}
