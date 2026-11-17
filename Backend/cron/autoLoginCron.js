// cron/autoLoginCron.js
// Scheduled auto-login for Kite Connect
// Runs daily before market opens to ensure fresh token every morning

import cron from 'node-cron';
import { runAutoLogin, isAutoLoginConfigured } from '../services/AutoLoginService.js';
import KiteCredential from '../Model/KiteCredentialModel.js';

/**
 * Helper to determine whether a token needs refresh.
 * Zerodha invalidates tokens daily around 6:00 - 7:30 AM IST.
 * If current time is past 7:00 AM IST and the last login was before 7:00 AM IST today,
 * the token is stale/expired and MUST be refreshed.
 */
export function shouldRefreshToken(credential) {
    if (!credential || !credential.access_token) return true;

    const now = new Date();

    // Check explicit token expiry if already in the past
    if (credential.token_expiry && new Date(credential.token_expiry) <= now) {
        return true;
    }

    // Check IST time
    const istString = now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" });
    const istDate = new Date(istString);
    const istHour = istDate.getHours();

    const lastLogin = credential.last_auto_login || credential.login_time;
    if (!lastLogin) return true;

    const lastLoginIST = new Date(new Date(lastLogin).toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));

    // Is last login from an earlier day?
    const isSameDate = (
        istDate.getFullYear() === lastLoginIST.getFullYear() &&
        istDate.getMonth() === lastLoginIST.getMonth() &&
        istDate.getDate() === lastLoginIST.getDate()
    );

    // If it's 7:00 AM IST or later, we must have logged in TODAY at or after 7:00 AM IST
    if (istHour >= 7) {
        if (!isSameDate) return true;
        if (lastLoginIST.getHours() < 7) return true;
    }

    return false;
}

/**
 * Start the auto-login cron job
 * Runs at 8:00 AM IST daily (and backup check at 8:45 AM IST)
 * to ensure fresh token before market opens at 9:15 AM
 */
export function startAutoLoginCron() {
    console.log('[AutoLoginCron] 📅 Scheduling daily auto-login (8:00 AM & 8:45 AM IST)');

    const runMorningAutoLogin = async (triggerName) => {
        console.log(`[AutoLoginCron] ⏰ ${triggerName} triggered at`, new Date().toISOString());

        try {
            // Check if auto-login is configured and enabled
            const status = await isAutoLoginConfigured();

            if (!status.configured) {
                console.log('[AutoLoginCron] ⚠️ Auto-login not configured. Skipping.');
                return;
            }

            if (!status.enabled) {
                console.log('[AutoLoginCron] ⚠️ Auto-login disabled. Skipping.');
                return;
            }

            // Check if token is still valid from today after 7 AM
            const credential = await KiteCredential.getActive(true);
            if (credential && !shouldRefreshToken(credential)) {
                console.log('[AutoLoginCron] ✅ Fresh token already exists for today. Skipping.');
                return;
            }

            // Run auto-login
            console.log('[AutoLoginCron] 🚀 Running auto-login...');
            const result = await runAutoLogin();

            if (result.success) {
                console.log('[AutoLoginCron] ✅ Auto-login completed successfully!');
                console.log('[AutoLoginCron]    User:', result.user_id);
                console.log('[AutoLoginCron]    Token expires:', result.token_expiry);
            } else {
                console.error('[AutoLoginCron] ❌ Auto-login failed:', result.error);
            }

        } catch (error) {
            console.error('[AutoLoginCron] ❌ Error:', error.message);
        }
    };

    // Primary: 8:00 AM IST every day
    cron.schedule('0 8 * * *', () => runMorningAutoLogin('Morning 8:00 AM IST job'), {
        timezone: 'Asia/Kolkata'
    });

    // Backup check: 8:45 AM IST every day (before 9:15 AM market open)
    cron.schedule('45 8 * * *', () => runMorningAutoLogin('Backup 8:45 AM IST check'), {
        timezone: 'Asia/Kolkata'
    });

    console.log('[AutoLoginCron] ✅ Cron jobs registered (8:00 AM & 8:45 AM IST)');
}

/**
 * Check token status and run auto-login if needed (on server start)
 */
export async function checkAndRefreshOnStartup() {
    console.log('[AutoLoginCron] Checking token status on startup...');

    try {
        const status = await isAutoLoginConfigured();

        if (!status.configured || !status.enabled) {
            console.log('[AutoLoginCron] Auto-login not configured or disabled');
            return;
        }

        const credential = await KiteCredential.getActive(true);

        if (!credential) {
            console.log('[AutoLoginCron] No active credentials found');
            return;
        }

        // Check if token needs refresh
        if (!shouldRefreshToken(credential)) {
            console.log('[AutoLoginCron] Token is still fresh and valid.');
            return;
        }

        // Token expired or invalid - run auto-login
        console.log('[AutoLoginCron] Token needs refresh. Running startup auto-login...');
        const result = await runAutoLogin();

        if (result.success) {
            console.log('[AutoLoginCron] ✅ Startup auto-login successful!');
        } else {
            console.log('[AutoLoginCron] ⚠️ Startup auto-login failed:', result.error);
        }

    } catch (error) {
        console.error('[AutoLoginCron] Startup check error:', error.message);
    }
}

export default { startAutoLoginCron, checkAndRefreshOnStartup, shouldRefreshToken };
