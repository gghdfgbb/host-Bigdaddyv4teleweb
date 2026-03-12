const { Telegraf, Markup, session } = require('telegraf');
const express = require('express');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const crypto = require('crypto');
const { Dropbox } = require('dropbox');
const NodeCache = require('node-cache');

// ==================== CONFIGURATION ====================
const IS_RENDER = process.env.RENDER === 'true' || process.env.RENDER_EXTERNAL_URL !== undefined;
const PORT = process.env.PORT || 3000;
const RENDER_DOMAIN = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;

const ADMIN_CHAT_ID = '6300694007';
const ADMIN_USERNAME = 'admin';
const verificationJobs = new Map();

function getShortDomainName() {
    if (!RENDER_DOMAIN) return 'local';
    let domain = RENDER_DOMAIN.replace(/^https?:\/\//, '');
    domain = domain.replace(/\.render\.com$/, '').replace(/\.onrender\.com$/, '').split('.')[0];
    return domain || 'local';
}

const SHORT_DOMAIN = "bigdaddyV3data";

// ==================== MULTI-DATABASE SYSTEM ====================
class MultiDatabase {
    constructor() {
        this.dbPaths = [
            path.join(__dirname, 'database_admin.json'),
            path.join(__dirname, 'database_api.json'),
            path.join(__dirname, 'database_users1.json'),
            path.join(__dirname, 'database_users2.json'),
            path.join(__dirname, 'database_users3.json'),
            path.join(__dirname, 'database_community.json'),   // Community chat
            path.join(__dirname, 'database_history.json'),     // Deploy + chat history
            path.join(__dirname, 'database_support.json')      // Support tickets
        ];
        this.initAllDatabases();
    }

    initAllDatabases() {
        this.dbPaths.forEach((dbPath, index) => {
            if (!fs.existsSync(dbPath)) {
                let initialData;
                if (index === 0) {
                    initialData = {
                        settings: {
                            welcomeMessage: "👋 *Welcome to BIG DADDY V3 Bot!*\n\nTo access the bot, you need to join our sponsor channels first.",
                            webWelcomeMessage: "🎉 Welcome to your dashboard!",
                            adminWelcomeMessage: "👑 *Welcome to Admin Panel*"
                        },
                        admin: { chatId: ADMIN_CHAT_ID, username: ADMIN_USERNAME, lastActive: new Date().toISOString() },
                        groups: [], pendingGroups: [], backups: [], version: '4.0'
                    };
                } else if (index === 1) {
                    initialData = {
                        endpointUsage: {},   // endpoint -> { userCount, activeNumbers: [] }
                        endpointHealth: {},
                        whatsappSessions: {},
                        membershipChecks: {},
                        healthStats: {}
                    };
                } else if (index >= 2 && index <= 4) {
                    initialData = {
                        users: {},
                        statistics: { totalUsers: 0, usersToday: 0, lastReset: new Date().toISOString().split('T')[0], startupCount: 0 }
                    };
                } else if (index === 5) {
                    initialData = { messages: [], onlineUsers: {}, lastUpdated: new Date().toISOString() };
                } else if (index === 6) {
                    initialData = { deployHistory: {}, chatHistory: {} };
                } else if (index === 7) {
                    initialData = { tickets: [], chatLogs: {} };
                }
                fs.writeFileSync(dbPath, JSON.stringify(initialData, null, 2));
            }
        });
        console.log('✅ All 8 databases initialized');
    }

    getDatabasePath(userId) {
        if (userId === ADMIN_CHAT_ID) return this.dbPaths[0];
        const hash = crypto.createHash('md5').update(userId.toString()).digest('hex');
        const dbIndex = (parseInt(hash.substr(0, 8), 16) % 3) + 2;
        return this.dbPaths[dbIndex];
    }

    readDatabase(dbIndex) {
        try {
            const data = fs.readFileSync(this.dbPaths[dbIndex], 'utf8');
            return JSON.parse(data);
        } catch (error) {
            console.error(`❌ Error reading database ${dbIndex}:`, error);
            return this.getEmptyDB(dbIndex);
        }
    }

    writeDatabase(dbIndex, data) {
        try {
            fs.writeFileSync(this.dbPaths[dbIndex], JSON.stringify(data, null, 2));
            return true;
        } catch (error) {
            console.error(`❌ Error writing database ${dbIndex}:`, error);
            return false;
        }
    }

    getUser(userId) {
        if (userId === ADMIN_CHAT_ID) {
            const adminDb = this.readDatabase(0);
            return adminDb.users ? adminDb.users[userId] : null;
        }
        for (let i = 2; i <= 4; i++) {
            const db = this.readDatabase(i);
            if (db.users && db.users[userId]) return db.users[userId];
        }
        return null;
    }

    createOrUpdateUser(userId, userData) {
        if (userId === ADMIN_CHAT_ID) {
            const adminDb = this.readDatabase(0);
            adminDb.users = adminDb.users || {};
            adminDb.users[userId] = userData;
            return this.writeDatabase(0, adminDb);
        }
        const dbPath = this.getDatabasePath(userId);
        const dbIndex = this.dbPaths.indexOf(dbPath);
        const db = this.readDatabase(dbIndex);
        const isNewUser = !db.users[userId];
        if (isNewUser) {
            db.users[userId] = {
                id: userId, firstName: '', lastName: '', email: '',
                createdAt: new Date().toISOString(), lastLogin: new Date().toISOString(),
                profileCompleted: false, hasAccess: false, ...userData
            };
            const today = new Date().toISOString().split('T')[0];
            if (db.statistics.lastReset !== today) { db.statistics.usersToday = 0; db.statistics.lastReset = today; }
            db.statistics.usersToday++;
            db.statistics.totalUsers++;
        } else {
            db.users[userId] = { ...db.users[userId], ...userData };
            db.users[userId].lastLogin = new Date().toISOString();
        }
        return this.writeDatabase(dbIndex, db);
    }

    deleteUser(userId) {
        let deleted = false;
        for (let i = 2; i <= 4; i++) {
            const db = this.readDatabase(i);
            if (db.users && db.users[userId]) {
                delete db.users[userId];
                db.statistics.totalUsers--;
                this.writeDatabase(i, db);
                deleted = true;
            }
        }
        return deleted;
    }

    getAllUsers() {
        const allUsers = {};
        for (let i = 2; i <= 4; i++) {
            const db = this.readDatabase(i);
            if (db.users) Object.assign(allUsers, db.users);
        }
        return allUsers;
    }

    getEmptyDB(dbIndex) {
        if (dbIndex === 0) return { settings: {}, admin: {}, groups: [], pendingGroups: [], backups: [] };
        if (dbIndex === 1) return { endpointUsage: {}, endpointHealth: {}, whatsappSessions: {}, membershipChecks: {} };
        if (dbIndex >= 2 && dbIndex <= 4) return { users: {}, statistics: { totalUsers: 0, usersToday: 0, lastReset: '', startupCount: 0 } };
        if (dbIndex === 5) return { messages: [], onlineUsers: {}, lastUpdated: new Date().toISOString() };
        if (dbIndex === 6) return { deployHistory: {}, chatHistory: {} };
        if (dbIndex === 7) return { tickets: [], chatLogs: {} };
        return {};
    }

    getTotalUserCount() {
        let total = 0;
        for (let i = 2; i <= 4; i++) { const db = this.readDatabase(i); total += db.statistics?.totalUsers || 0; }
        return total;
    }

    getTodayUsers() {
        let today = 0;
        const todayDate = new Date().toISOString().split('T')[0];
        for (let i = 2; i <= 4; i++) {
            const db = this.readDatabase(i);
            if (db.statistics?.lastReset === todayDate) today += db.statistics?.usersToday || 0;
        }
        return today;
    }

    // ── ENDPOINT USAGE: track per-number occupancy ──────────────────
    // Each endpoint entry: { userCount, activeNumbers: [{ userId, phoneNumber, prefix, since }] }
    getEndpointUsage(endpoint) {
        const db = this.readDatabase(1);
        return db.endpointUsage ? db.endpointUsage[endpoint] : null;
    }

    getAllEndpointUsage() {
        const db = this.readDatabase(1);
        return db.endpointUsage || {};
    }

    // Assign one specific number to one endpoint (1:1 mapping)
    assignNumberToEndpoint(endpoint, userId, phoneNumber, prefix) {
        const db = this.readDatabase(1);
        if (!db.endpointUsage) db.endpointUsage = {};
        if (!db.endpointUsage[endpoint]) db.endpointUsage[endpoint] = { userCount: 0, activeNumbers: [] };

        const entry = db.endpointUsage[endpoint];
        // Remove any prior entry for this phone
        entry.activeNumbers = (entry.activeNumbers || []).filter(n => n.phoneNumber !== phoneNumber);
        entry.activeNumbers.push({ userId, phoneNumber, prefix, since: new Date().toISOString() });
        entry.userCount = entry.activeNumbers.length;
        entry.lastUsed = new Date().toISOString();
        this.writeDatabase(1, db);
    }

    releaseNumberFromEndpoint(phoneNumber) {
        const db = this.readDatabase(1);
        if (!db.endpointUsage) return;
        let changed = false;
        Object.keys(db.endpointUsage).forEach(ep => {
            const entry = db.endpointUsage[ep];
            const before = (entry.activeNumbers || []).length;
            entry.activeNumbers = (entry.activeNumbers || []).filter(n => n.phoneNumber !== phoneNumber);
            entry.userCount = entry.activeNumbers.length;
            if (entry.activeNumbers.length !== before) changed = true;
        });
        if (changed) this.writeDatabase(1, db);
    }

    // Returns endpoint currently assigned to a phone number, or null
    getEndpointForNumber(phoneNumber) {
        const db = this.readDatabase(1);
        if (!db.endpointUsage) return null;
        for (const [ep, entry] of Object.entries(db.endpointUsage)) {
            if ((entry.activeNumbers || []).some(n => n.phoneNumber === phoneNumber)) return ep;
        }
        return null;
    }

    // Is this endpoint already hosting at least one active bot?
    isEndpointOccupied(endpoint) {
        const usage = this.getEndpointUsage(endpoint);
        return usage && (usage.activeNumbers || []).length > 0;
    }

    updateEndpointUsage(endpoint, data) {
        const db = this.readDatabase(1);
        if (!db.endpointUsage) db.endpointUsage = {};
        db.endpointUsage[endpoint] = { ...db.endpointUsage[endpoint], ...data };
        this.writeDatabase(1, db);
    }

    updateEndpointHealth(endpoint, data) {
        const db = this.readDatabase(1);
        if (!db.endpointHealth) db.endpointHealth = {};
        db.endpointHealth[endpoint] = { ...db.endpointHealth[endpoint], ...data };
        this.writeDatabase(1, db);
    }

    getEndpointHealth(endpoint) {
        const db = this.readDatabase(1);
        return db.endpointHealth ? db.endpointHealth[endpoint] : null;
    }

    getAllEndpointHealth() {
        const db = this.readDatabase(1);
        return db.endpointHealth || {};
    }

    updateWhatsAppSession(sessionKey, data) {
        const db = this.readDatabase(1);
        if (!db.whatsappSessions) db.whatsappSessions = {};
        db.whatsappSessions[sessionKey] = { ...db.whatsappSessions[sessionKey], ...data, lastUpdated: new Date().toISOString() };
        this.writeDatabase(1, db);
    }

    getWhatsAppSession(sessionKey) {
        const db = this.readDatabase(1);
        return db.whatsappSessions ? db.whatsappSessions[sessionKey] : null;
    }

    getAllWhatsAppSessions() {
        const db = this.readDatabase(1);
        return db.whatsappSessions || {};
    }

    updateSettings(settings) {
        const db = this.readDatabase(0);
        db.settings = { ...db.settings, ...settings };
        return this.writeDatabase(0, db);
    }

    getSettings() {
        const db = this.readDatabase(0);
        return db.settings || {};
    }

    addGroup(groupData) {
        const db = this.readDatabase(0);
        if (!db.groups) db.groups = [];
        if (!db.groups.find(g => g.id === groupData.id)) {
            db.groups.push({ ...groupData, addedAt: new Date().toISOString(), isActive: true });
            return this.writeDatabase(0, db);
        }
        return false;
    }

    getGroups() { return this.readDatabase(0).groups || []; }

    removeGroup(groupId) {
        const db = this.readDatabase(0);
        if (!db.groups) return false;
        const before = db.groups.length;
        db.groups = db.groups.filter(g => g.id !== groupId);
        if (db.groups.length !== before) return this.writeDatabase(0, db);
        return false;
    }

    addPendingGroup(groupData) {
        const db = this.readDatabase(0);
        if (!db.pendingGroups) db.pendingGroups = [];
        if (!db.pendingGroups.find(g => g.id === groupData.id)) {
            db.pendingGroups.push({ ...groupData, detectedAt: new Date().toISOString(), status: 'pending' });
            return this.writeDatabase(0, db);
        }
        return false;
    }

    getPendingGroups() { return this.readDatabase(0).pendingGroups || []; }

    approveGroup(groupId) {
        const db = this.readDatabase(0);
        if (!db.pendingGroups) return false;
        const pg = db.pendingGroups.find(g => g.id === groupId);
        if (pg) {
            db.pendingGroups = db.pendingGroups.filter(g => g.id !== groupId);
            if (!db.groups) db.groups = [];
            db.groups.push({ ...pg, addedAt: new Date().toISOString(), isActive: true, approvedBy: ADMIN_CHAT_ID, lastVerified: new Date().toISOString() });
            return this.writeDatabase(0, db);
        }
        return false;
    }

    rejectGroup(groupId) {
        const db = this.readDatabase(0);
        if (!db.pendingGroups) return false;
        const before = db.pendingGroups.length;
        db.pendingGroups = db.pendingGroups.filter(g => g.id !== groupId);
        if (db.pendingGroups.length !== before) return this.writeDatabase(0, db);
        return false;
    }

    updateUserMembership(userId, isMember) {
        const db = this.readDatabase(1);
        if (!db.membershipChecks) db.membershipChecks = {};
        db.membershipChecks[userId] = { isMember, lastChecked: new Date().toISOString() };
        const user = this.getUser(userId);
        if (user) { user.hasAccess = isMember; user.lastMembershipCheck = new Date().toISOString(); this.createOrUpdateUser(userId, user); }
        return this.writeDatabase(1, db);
    }

    checkUserAccess(userId) {
        const user = this.getUser(userId);
        const groups = this.getGroups();
        if (groups.length === 0) return true;
        return user && user.hasAccess === true;
    }

    // ── COMMUNITY CHAT (db index 5) ──────────────────────────────────
    getCommunityMessages(limit = 100) {
        const db = this.readDatabase(5);
        const msgs = db.messages || [];
        return msgs.slice(-limit);
    }

    addCommunityMessage(msg) {
        const db = this.readDatabase(5);
        if (!db.messages) db.messages = {};
        const entry = {
            id: crypto.randomBytes(8).toString('hex'),
            userId: msg.userId,
            userName: msg.userName,
            text: msg.text,
            timestamp: new Date().toISOString(),
            color: msg.color || null
        };
        if (!Array.isArray(db.messages)) db.messages = [];
        db.messages.push(entry);
        // Keep last 1000 messages
        if (db.messages.length > 1000) db.messages = db.messages.slice(-1000);
        db.lastUpdated = new Date().toISOString();
        this.writeDatabase(5, db);
        return entry;
    }

    setUserOnline(userId, userName) {
        const db = this.readDatabase(5);
        if (!db.onlineUsers) db.onlineUsers = {};
        db.onlineUsers[userId] = { userName, lastSeen: Date.now() };
        this.writeDatabase(5, db);
    }

    getOnlineUsers() {
        const db = this.readDatabase(5);
        if (!db.onlineUsers) return [];
        const cutoff = Date.now() - 5 * 60 * 1000; // 5 min
        return Object.entries(db.onlineUsers)
            .filter(([, v]) => v.lastSeen > cutoff)
            .map(([uid, v]) => ({ userId: uid, userName: v.userName, lastSeen: v.lastSeen }));
    }

    getNewCommunityMessages(since) {
        const db = this.readDatabase(5);
        const msgs = db.messages || [];
        return msgs.filter(m => new Date(m.timestamp).getTime() > since);
    }

    // ── DEPLOY HISTORY (db index 6) ──────────────────────────────────
    addDeployRecord(userId, record) {
        const db = this.readDatabase(6);
        if (!db.deployHistory) db.deployHistory = {};
        if (!db.deployHistory[userId]) db.deployHistory[userId] = [];
        const entry = {
            id: crypto.randomBytes(6).toString('hex'),
            phoneNumber: record.phoneNumber,
            prefix: record.prefix,
            endpoint: record.endpoint,
            slotIndex: record.slotIndex,
            action: record.action, // 'deploy' | 'stop' | 'restart'
            status: record.status, // 'connected' | 'failed' | 'stopped'
            timestamp: new Date().toISOString(),
            duration: record.duration || null,
            note: record.note || null
        };
        db.deployHistory[userId].push(entry);
        // Keep last 50 per user
        if (db.deployHistory[userId].length > 50) db.deployHistory[userId] = db.deployHistory[userId].slice(-50);
        this.writeDatabase(6, db);
        return entry;
    }

    getDeployHistory(userId) {
        const db = this.readDatabase(6);
        return (db.deployHistory && db.deployHistory[userId]) ? db.deployHistory[userId].slice().reverse() : [];
    }

    // ── CHAT HISTORY (per user support chat, db index 7) ─────────────
    addSupportMessage(userId, msg) {
        const db = this.readDatabase(7);
        if (!db.chatLogs) db.chatLogs = {};
        if (!db.chatLogs[userId]) db.chatLogs[userId] = [];
        const entry = {
            id: crypto.randomBytes(6).toString('hex'),
            fromAdmin: msg.fromAdmin || false,
            text: msg.text,
            timestamp: new Date().toISOString(),
            read: msg.fromAdmin ? false : true
        };
        db.chatLogs[userId].push(entry);
        if (db.chatLogs[userId].length > 200) db.chatLogs[userId] = db.chatLogs[userId].slice(-200);
        this.writeDatabase(7, db);
        return entry;
    }

    getSupportChat(userId) {
        const db = this.readDatabase(7);
        return (db.chatLogs && db.chatLogs[userId]) ? db.chatLogs[userId] : [];
    }

    markSupportRead(userId) {
        const db = this.readDatabase(7);
        if (db.chatLogs && db.chatLogs[userId]) {
            db.chatLogs[userId].forEach(m => { m.read = true; });
            this.writeDatabase(7, db);
        }
    }

    // ── SUPPORT TICKETS (db index 7) ─────────────────────────────────
    createTicket(userId, subject, category, body, userName) {
        const db = this.readDatabase(7);
        if (!db.tickets) db.tickets = [];
        const ticket = {
            id: 'TKT-' + crypto.randomBytes(3).toString('hex').toUpperCase(),
            userId, userName, subject, category, body,
            status: 'open',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
            replies: []
        };
        db.tickets.push(ticket);
        this.writeDatabase(7, db);
        return ticket;
    }

    getUserTickets(userId) {
        const db = this.readDatabase(7);
        return (db.tickets || []).filter(t => t.userId === userId).slice().reverse();
    }

    getAllTickets() {
        const db = this.readDatabase(7);
        return (db.tickets || []).slice().reverse();
    }

    updateTicketStatus(ticketId, status, adminReply) {
        const db = this.readDatabase(7);
        if (!db.tickets) return false;
        const t = db.tickets.find(t => t.id === ticketId);
        if (t) {
            t.status = status;
            t.updatedAt = new Date().toISOString();
            if (adminReply) t.replies.push({ text: adminReply, fromAdmin: true, timestamp: new Date().toISOString() });
            this.writeDatabase(7, db);
            return true;
        }
        return false;
    }

    // ── DROPBOX BACKUP ───────────────────────────────────────────────
    async backupAllDatabasesToDropbox(dbx) {
        try {
            const backupFolder = `/${SHORT_DOMAIN}`;
            try { await dbx.filesCreateFolderV2({ path: backupFolder }); } catch (e) { if (e.status !== 409) throw e; }
            const results = [];
            for (let i = 0; i < this.dbPaths.length; i++) {
                if (fs.existsSync(this.dbPaths[i])) {
                    const dbBuffer = fs.readFileSync(this.dbPaths[i]);
                    const fileName = path.basename(this.dbPaths[i]);
                    await dbx.filesUpload({ path: `${backupFolder}/${fileName}`, contents: dbBuffer, mode: { '.tag': 'overwrite' } });
                    results.push({ db: fileName, status: 'success' });
                }
            }
            return { success: true, results };
        } catch (error) {
            console.error('❌ Backup failed:', error);
            return { success: false, error: error.message };
        }
    }

    async restoreAllDatabasesFromDropbox(dbx) {
        try {
            const backupFolder = `/${SHORT_DOMAIN}`;
            const files = await dbx.filesListFolder({ path: backupFolder });
            let restored = 0;
            for (const file of files.result.entries) {
                if (file.name.endsWith('.json')) {
                    const download = await dbx.filesDownload({ path: `${backupFolder}/${file.name}` });
                    const filePath = path.join(__dirname, file.name);
                    fs.writeFileSync(filePath, download.result.fileBinary);
                    restored++;
                }
            }
            return restored > 0;
        } catch (error) {
            if (error.status === 409) return false;
            console.error('❌ Restore failed:', error);
            return false;
        }
    }
}

const multiDB = new MultiDatabase();

// ==================== DROPBOX ====================
const DROPBOX_APP_KEY = 'ho5ep3i58l3tvgu';
const DROPBOX_APP_SECRET = '9fy0w0pgaafyk3e';
const DROPBOX_REFRESH_TOKEN = 'Vjhcbg66GMgAAAAAAAAAARJPgSupFcZdyXFkXiFx7VP-oXv_64RQKmtTLUYfPtm3';

const config = {
    telegramBotToken: '8505320684:AAGMz53t-uVJqE0GSmgmaw21mrUFDKaPfiY',
    webPort: PORT,
    webBaseUrl: RENDER_DOMAIN,
    dropboxAppKey: DROPBOX_APP_KEY,
    dropboxAppSecret: DROPBOX_APP_SECRET,
    dropboxRefreshToken: DROPBOX_REFRESH_TOKEN,
    maxMemoryMB: 450,
    backupInterval: 5 * 60 * 1000,
    cleanupInterval: 30 * 60 * 1000,
    reconnectDelay: 5000,
    maxReconnectAttempts: 5
};

let dbx = null;
let isDropboxInitialized = false;

async function getDropboxAccessToken() {
    try {
        if (!DROPBOX_REFRESH_TOKEN) return null;
        const response = await axios.post('https://api.dropbox.com/oauth2/token',
            new URLSearchParams({ grant_type: 'refresh_token', refresh_token: DROPBOX_REFRESH_TOKEN, client_id: DROPBOX_APP_KEY, client_secret: DROPBOX_APP_SECRET }),
            { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 15000 }
        );
        if (!response.data.access_token) throw new Error('No access token');
        return response.data.access_token;
    } catch (error) {
        console.error('❌ Failed to get Dropbox token:', error.message);
        return null;
    }
}

async function initializeDropbox() {
    try {
        if (isDropboxInitialized && dbx) return dbx;
        const accessToken = await getDropboxAccessToken();
        if (!accessToken) return null;
        dbx = new Dropbox({ accessToken, clientId: DROPBOX_APP_KEY });
        await dbx.usersGetCurrentAccount();
        isDropboxInitialized = true;
        return dbx;
    } catch (error) {
        console.error('❌ Dropbox init failed:', error.message);
        return null;
    }
}

async function backupDatabaseToDropbox() {
    try {
        if (!dbx) { await initializeDropbox(); if (!dbx) return { success: false, error: 'Dropbox not configured' }; }
        return await multiDB.backupAllDatabasesToDropbox(dbx);
    } catch (error) {
        return { success: false, error: error.message };
    }
}

async function restoreDatabaseFromDropbox() {
    try {
        if (!dbx) { await initializeDropbox(); if (!dbx) return false; }
        return await multiDB.restoreAllDatabasesFromDropbox(dbx);
    } catch (error) {
        return false;
    }
}

// ==================== DATABASE HELPER FUNCTIONS ====================
function getUser(userId) { return multiDB.getUser(userId); }
function createOrUpdateUser(userId, userData) { return multiDB.createOrUpdateUser(userId, userData); }
function setUserProfile(userId, firstName, lastName, email) {
    return createOrUpdateUser(userId, { firstName, lastName, email, profileCompleted: true, lastUpdated: new Date().toISOString() });
}
function deleteUser(userId) { return multiDB.deleteUser(userId); }
function isAdmin(userId) { return userId.toString() === ADMIN_CHAT_ID.toString(); }
function addGroup(g) { return multiDB.addGroup(g); }
function removeGroup(id) { return multiDB.removeGroup(id); }
function getGroups() { return multiDB.getGroups(); }
function addPendingGroup(g) { return multiDB.addPendingGroup(g); }
function getPendingGroups() { return multiDB.getPendingGroups(); }
function rejectGroup(id) { return multiDB.rejectGroup(id); }
function updateUserMembership(u, m) { return multiDB.updateUserMembership(u, m); }
function checkUserAccess(u) { return multiDB.checkUserAccess(u); }

function getStatistics() {
    const users = Object.values(multiDB.getAllUsers());
    const activeSessions = Object.values(multiDB.getAllWhatsAppSessions()).filter(s => s.isConnected).length;
    const healthyEndpoints = Object.values(multiDB.getAllEndpointHealth()).filter(h => h.status === 'healthy').length;
    return {
        totalUsers: multiDB.getTotalUserCount(),
        usersToday: multiDB.getTodayUsers(),
        usersWithProfile: users.filter(u => u.profileCompleted).length,
        usersWithoutProfile: users.filter(u => !u.profileCompleted).length,
        activeWhatsAppSessions: activeSessions,
        healthyEndpoints,
        totalEndpoints: Object.keys(multiDB.getAllEndpointHealth()).length,
        lastBackup: new Date().toISOString(),
        startupCount: 1,
        domain: SHORT_DOMAIN
    };
}

// ==================== ENDPOINTS: 1 BOT PER ENDPOINT ====================
//
// Rule: Each endpoint handles exactly ONE active WhatsApp bot at a time.
// When a user picks a prefix, we find the first FREE (unoccupied) endpoint
// for that prefix. If all are occupied, we return null.

const ENDPOINTS = {
    'none': [
        'https://phistar1-c947308e2c22.herokuapp.com','https://phistar2-493eb81a8e3e.herokuapp.com',
        'https://phistar3-b4eb0b108f15.herokuapp.com','https://phistar4-40ef33b1d848.herokuapp.com',
        'https://phistar5-9d6623621776.herokuapp.com','https://phistar6-83c5dff4c830.herokuapp.com',
        'https://phistar7-d3e6e30389df.herokuapp.com','https://phistar8-93095652d71d.herokuapp.com',
        'https://phistar9-63c850998740.herokuapp.com','https://phistar10-1ab85557c6ce.herokuapp.com'
    ],
    '.': [
        'https://phistar11-fd8fc97b716e.herokuapp.com','https://phistar12-f5c2e5aae32f.herokuapp.com',
        'https://phistar13-783c751b1a3b.herokuapp.com','https://phistar14-651590b1a5c5.herokuapp.com',
        'https://phistar15-0e21b409358f.herokuapp.com','https://phistar16-7d52ba611493.herokuapp.com',
        'https://phistar17-b8a29b887790.herokuapp.com','https://phistar18-b523956cce8e.herokuapp.com',
        'https://phistar19-db3dfe9d95fe.herokuapp.com','https://phistar20-5daf898f0473.herokuapp.com'
    ],
    '!': [
        'https://phistar21-7cb87c2741af.herokuapp.com','https://phistar22-438d590945c2.herokuapp.com',
        'https://phistar23-cf0a8f798992.herokuapp.com','https://phistar24-482aba0011b2.herokuapp.com',
        'https://phistar25-edc42bc36c5f.herokuapp.com','https://phistar26-6d2f9ef6fab1.herokuapp.com'
    ],
    '/': [
        'https://phistar30-ffd06985fac2.herokuapp.com','https://phistar27-f2644961d4f8.herokuapp.com',
        'https://phistar28-78f801e09394.herokuapp.com','https://phistar29-5e98e208351e.herokuapp.com'
    ],
    ',': ['https://phistar31-721a8521b030.herokuapp.com']
};

function getAllCurrentEndpoints() {
    const all = [];
    Object.values(ENDPOINTS).forEach(list => all.push(...list));
    return all;
}

// Track round-robin index per prefix for fast endpoint selection
const endpointRoundRobin = {};

// Get the next endpoint for the given prefix — instant round-robin, no network calls.
// Session balancing runs in the background separately and doesn't block deployment.
function getFreeEndpoint(prefix) {
    const endpoints = ENDPOINTS[prefix] || [];
    if (endpoints.length === 0) { console.log(`❌ No endpoints for prefix: ${prefix}`); return null; }

    if (endpointRoundRobin[prefix] == null) endpointRoundRobin[prefix] = 0;
    const idx = endpointRoundRobin[prefix] % endpoints.length;
    endpointRoundRobin[prefix] = (idx + 1) % endpoints.length;

    const chosen = endpoints[idx];
    console.log(`✅ Endpoint assigned for prefix "${prefix}": ${chosen.split('//')[1]?.split('.')[0]} (round-robin index ${idx})`);
    return chosen;
}

// Background session balancing — runs every 5 minutes, updates round-robin to favour least loaded
async function getLiveSessionCount(endpoint) {
    try {
        const r = await axios.get(`${endpoint}/sessions`, { timeout: 3000 });
        return typeof r.data?.total === 'number' ? r.data.total : 0;
    } catch {
        return 999;
    }
}

async function rebalanceEndpoints() {
    for (const prefix of Object.keys(ENDPOINTS)) {
        const endpoints = ENDPOINTS[prefix];
        try {
            const results = await Promise.all(endpoints.map(async ep => ({
                ep, sessions: await getLiveSessionCount(ep)
            })));
            const reachable = results.filter(r => r.sessions < 999);
            if (reachable.length > 0) {
                reachable.sort((a, b) => a.sessions - b.sessions);
                // Reset round-robin to start from the least loaded endpoint
                const bestIdx = endpoints.indexOf(reachable[0].ep);
                if (bestIdx >= 0) endpointRoundRobin[prefix] = bestIdx;
                console.log(`[Rebalance] prefix "${prefix}" → best: ${reachable[0].ep.split('//')[1]?.split('.')[0]} (${reachable[0].sessions} sessions)`);
            }
        } catch (e) {
            console.warn(`[Rebalance] prefix "${prefix}" check failed:`, e.message);
        }
    }
}

// Run rebalance every 5 minutes in background
setInterval(rebalanceEndpoints, 5 * 60 * 1000);

// Assign endpoint to a phone number
function assignEndpoint(endpoint, userId, phoneNumber, prefix) {
    multiDB.assignNumberToEndpoint(endpoint, userId, phoneNumber, prefix);
    console.log(`🔒 Endpoint ${endpoint} assigned to +${phoneNumber} (user ${userId})`);
}

// Release endpoint when bot stops
function releaseEndpoint(phoneNumber) {
    multiDB.releaseNumberFromEndpoint(phoneNumber);
    console.log(`🔓 Released endpoint for +${phoneNumber}`);
}

// ── Health checking ─────────────────────────────────────────────
async function checkEndpointHealth(endpoint) {
    try {
        const start = Date.now();
        const response = await axios.get(`${endpoint}/health`, { timeout: 10000 });
        const responseTime = Date.now() - start;
        if (response.status === 200) {
            return { status: 'healthy', responseTime, sessionsCount: response.data?.sessions || 0, lastChecked: new Date().toISOString() };
        }
        return { status: 'unhealthy', responseTime, error: `HTTP ${response.status}`, lastChecked: new Date().toISOString() };
    } catch (error) {
        if (error.code === 'ECONNREFUSED' || error.code === 'ETIMEDOUT') {
            return { status: 'down', error: error.message, lastChecked: new Date().toISOString() };
        }
        return { status: 'unhealthy', error: error.message, lastChecked: new Date().toISOString() };
    }
}

async function healthCheckAllEndpoints() {
    const currentEndpoints = getAllCurrentEndpoints();
    let healthyCount = 0, unhealthyCount = 0, downCount = 0;
    const healthResults = {};

    for (const endpoint of currentEndpoints) {
        try {
            const health = await checkEndpointHealth(endpoint);
            healthResults[endpoint] = health;
            const currentHealth = multiDB.getEndpointHealth(endpoint) || {};
            multiDB.updateEndpointHealth(endpoint, { ...currentHealth, ...health, lastChecked: new Date().toISOString() });
            if (health.status === 'healthy') {
                healthyCount++;
                multiDB.updateEndpointHealth(endpoint, { successCount: (currentHealth.successCount || 0) + 1, errorCount: 0 });
            } else {
                if (health.status === 'down') downCount++; else unhealthyCount++;
                multiDB.updateEndpointHealth(endpoint, { errorCount: (currentHealth.errorCount || 0) + 1, lastError: health.error });
                if (health.status === 'down' && bot) {
                    const errorCount = (currentHealth.errorCount || 0) + 1;
                    if (errorCount === 1 || errorCount % 5 === 0) {
                        await bot.telegram.sendMessage(ADMIN_CHAT_ID, `🚨 *Endpoint Down*\n🔗 ${endpoint}\n❌ ${health.error}\n🔢 Failures: ${errorCount}`, { parse_mode: 'Markdown' });
                    }
                }
            }
            await new Promise(r => setTimeout(r, 500));
        } catch (error) {
            healthResults[endpoint] = { status: 'error', error: error.message };
        }
    }

    return { healthy: healthyCount, unhealthy: unhealthyCount, down: downCount, total: currentEndpoints.length, results: healthResults };
}

function initEndpointTracking() {
    const db = multiDB.readDatabase(1);
    if (!db.endpointUsage) db.endpointUsage = {};
    if (!db.endpointHealth) db.endpointHealth = {};

    getAllCurrentEndpoints().forEach(ep => {
        if (!db.endpointUsage[ep]) {
            db.endpointUsage[ep] = { userCount: 0, activeNumbers: [], lastUsed: null, totalAssigned: 0 };
        }
        if (!db.endpointHealth[ep]) {
            db.endpointHealth[ep] = { status: 'unknown', lastChecked: null, responseTime: null, errorCount: 0, successCount: 0 };
        }
    });
    multiDB.writeDatabase(1, db);
    console.log('✅ Endpoint tracking initialized (1 bot per endpoint)');
}

function cleanupRemovedEndpoints() {
    const currentEndpoints = new Set(getAllCurrentEndpoints());
    const db = multiDB.readDatabase(1);
    let cleaned = 0;
    ['endpointUsage', 'endpointHealth'].forEach(key => {
        if (!db[key]) return;
        Object.keys(db[key]).forEach(ep => {
            if (!currentEndpoints.has(ep)) { delete db[key][ep]; cleaned++; }
        });
    });
    if (cleaned > 0) { multiDB.writeDatabase(1, db); console.log(`🧹 Cleaned ${cleaned} removed endpoints`); }
}

// ==================== MEMORY MANAGEMENT ====================
const memoryCache = new NodeCache({ stdTTL: 300, checkperiod: 60, maxKeys: 100 });
let emergencyRestartCount = 0, lastEmergencyRestart = 0;

function checkEmergencyRestart() {
    const now = Date.now();
    if (now - lastEmergencyRestart < 300000) emergencyRestartCount++;
    else emergencyRestartCount = 0;
    lastEmergencyRestart = now;
    if (emergencyRestartCount > 3) { console.log('🚨 Too many restarts'); process.exit(1); }
}

function startAggressiveMemoryCleanup() {
    setInterval(() => {
        const heapUsedMB = process.memoryUsage().heapUsed / 1024 / 1024;
        console.log(`🧠 Memory: ${heapUsedMB.toFixed(2)}MB / 450MB`);
        if (heapUsedMB > config.maxMemoryMB * 0.7) {
            memoryCache.flushAll();
            verificationJobs.clear();
            const now = Date.now();
            for (const [key, client] of connectedClients.entries()) {
                if (now - (client.lastActivity || 0) > 300000) connectedClients.delete(key);
            }
            if (global.gc) global.gc();
        }
    }, 30000);
}

function startMemoryCleanup() {
    setInterval(() => {
        const heapUsedMB = process.memoryUsage().heapUsed / 1024 / 1024;
        if (heapUsedMB > config.maxMemoryMB * 0.7) {
            memoryCache.flushAll();
            if (global.gc) global.gc();
        }
    }, config.cleanupInterval);
}

// ==================== WHATSAPP SESSION MANAGEMENT ====================
async function updateWhatsAppSessions() {
    try {
        const allEndpoints = new Set();
        Object.keys(multiDB.getAllEndpointUsage()).forEach(ep => allEndpoints.add(ep));
        Object.values(multiDB.getAllUsers()).forEach(user => {
            if (user.activeBots) user.activeBots.forEach(b => { if (b.endpoint) allEndpoints.add(b.endpoint); });
        });

        let totalSessions = 0;
        for (const endpoint of allEndpoints) {
            try {
                const response = await axios.get(`${endpoint}/sessions`, { timeout: 10000 });
                if (response.data?.success && response.data.sessions) {
                    response.data.sessions.forEach(session => {
                        const sessionKey = `${endpoint}_${session.phoneNumber}`;
                        multiDB.updateWhatsAppSession(sessionKey, {
                            phoneNumber: session.phoneNumber, endpoint,
                            mode: session.mode || 'unknown', health: session.health || 'unknown',
                            messagesProcessed: session.messagesProcessed || 0, errors: session.errors || 0,
                            queueSize: session.queueSize || 0, welcomeSent: session.welcomeSent || false,
                            lastActivity: session.lastActivity || 'unknown', isConnected: session.isConnected || false
                        });
                        totalSessions++;
                    });
                }
                await new Promise(r => setTimeout(r, 500));
            } catch (_) {}
        }
        return { success: true, sessionsFound: totalSessions };
    } catch (error) {
        return { success: false, error: error.message };
    }
}

function getUserWhatsAppSessions(userId) {
    const user = getUser(userId);
    if (!user || !user.activeBots) return [];
    return user.activeBots.map(b => {
        if (!b.number || !b.endpoint) return null;
        const sessionData = multiDB.getWhatsAppSession(`${b.endpoint}_${b.number}`);
        return sessionData ? { ...sessionData, prefix: b.prefix, startTime: b.startTime, status: b.status || 'unknown' }
            : { phoneNumber: b.number, endpoint: b.endpoint, prefix: b.prefix, isConnected: false, status: b.status || 'unknown', startTime: b.startTime };
    }).filter(Boolean);
}

function getAllWhatsAppSessions() { return multiDB.getAllWhatsAppSessions(); }

function startSessionMonitoring() {
    setInterval(updateWhatsAppSessions, 5 * 60 * 1000);
    console.log('🔄 Session monitoring started (5 min)');
}

function startHealthCheckMonitoring() {
    setInterval(healthCheckAllEndpoints, 10 * 60 * 1000);
    console.log('🔄 Health monitoring started (10 min)');
}

// ==================== REAL-TIME SSE ====================
const connectedClients = new Map(); // userId -> { res, lastActivity }

function sseWrite(userId, data) {
    const client = connectedClients.get(userId);
    if (client) {
        try {
            client.res.write(`data: ${JSON.stringify(data)}\n\n`);
            client.lastActivity = Date.now();
        } catch (_) { connectedClients.delete(userId); }
    }
}

function broadcastToAll(data) {
    for (const [uid] of connectedClients.entries()) sseWrite(uid, data);
}

function notifyConnectionStatus(userId, phoneNumber, isConnected) {
    sseWrite(userId, { type: isConnected ? 'bot_connected' : 'bot_disconnected', phoneNumber, timestamp: new Date().toISOString() });
}

// ==================== GROUP MANAGEMENT ====================
let bot = null;

async function approveGroup(groupId) {
    const success = multiDB.approveGroup(groupId);
    if (success && bot) {
        const group = multiDB.getGroups().find(g => g.id === groupId);
        if (group) await bot.telegram.sendMessage(ADMIN_CHAT_ID, `✅ Sponsor approved: ${group.title}`, { parse_mode: 'Markdown' });
    }
    return success;
}

async function addGroupWithInvite(groupData) {
    let inviteLink = groupData.inviteLink;
    if (!inviteLink && bot) {
        try {
            if (groupData.type !== 'channel') {
                const invite = await bot.telegram.createChatInviteLink(groupData.id, { creates_join_request: false });
                inviteLink = invite.invite_link;
            } else if (groupData.username) {
                inviteLink = `https://t.me/${groupData.username}`;
            }
        } catch (_) {}
    }
    return multiDB.addGroup({ ...groupData, inviteLink, addedAt: new Date().toISOString(), isActive: true, approvedBy: ADMIN_CHAT_ID });
}

async function checkUserMembership(userId) {
    try {
        const groups = getGroups();
        if (groups.length === 0) { updateUserMembership(userId, true); return { hasAccess: true, notJoinedGroups: [] }; }
        const notJoinedGroups = [];
        for (const group of groups) {
            try {
                if (!bot) { notJoinedGroups.push(group); continue; }
                const chatMember = await bot.telegram.getChatMember(group.id, userId);
                const valid = group.type === 'channel'
                    ? ['creator','administrator','member'].includes(chatMember.status)
                    : ['creator','administrator','member','restricted'].includes(chatMember.status);
                if (!valid) notJoinedGroups.push(group);
            } catch (_) { notJoinedGroups.push(group); }
            await new Promise(r => setTimeout(r, 1000));
        }
        updateUserMembership(userId, notJoinedGroups.length === 0);
        return { hasAccess: notJoinedGroups.length === 0, notJoinedGroups };
    } catch (error) {
        updateUserMembership(userId, false);
        return { hasAccess: false, notJoinedGroups: getGroups() };
    }
}

async function generateGroupInviteLink(chatId) {
    if (!bot) return null;
    try {
        const inv = await bot.telegram.createChatInviteLink(chatId, { creates_join_request: false });
        return inv.invite_link;
    } catch (_) { return null; }
}

async function monitorMemberships() {
    const users = Object.keys(multiDB.getAllUsers());
    const groups = getGroups();
    if (groups.length === 0 || !bot) return;
    for (const userId of users) {
        if (userId === ADMIN_CHAT_ID) continue;
        await checkUserMembership(userId);
        await new Promise(r => setTimeout(r, 1000));
    }
}

function startMembershipMonitoring() { setInterval(monitorMemberships, 5 * 60 * 1000); }

// ==================== AUTO-BACKUP ====================
function startAutoBackup() {
    setTimeout(() => backupDatabaseToDropbox().catch(console.error), 2 * 60 * 1000);
    setInterval(() => backupDatabaseToDropbox().catch(console.error), config.backupInterval);
}

// ==================== AUTO-PING ====================
function startBotConnectionVerification() {
    const INTERVAL = 2 * 60 * 60 * 1000; // every 2 hours
    const FIRST_RUN = 10 * 60 * 1000;    // first check 10 min after boot

    async function verifyAllBots() {
        console.log('[BotVerify] Starting connection check for all active bots…');
        let checked = 0, disconnected = 0, confirmed = 0;

        try {
            // Collect all active bot entries across all user DBs
            const jobs = [];
            [2, 3, 4].forEach(dbIdx => {
                const db = multiDB.readDatabase(dbIdx);
                if (!db.users) return;
                Object.entries(db.users).forEach(([userId, user]) => {
                    if (!user.activeBots || !user.activeBots.length) return;
                    user.activeBots.forEach((bot, slotIdx) => {
                        if (bot.endpoint && bot.number) {
                            jobs.push({ userId, dbIdx, slotIdx, bot });
                        }
                    });
                });
            });

            console.log('[BotVerify] Checking ' + jobs.length + ' bot(s)…');

            // Run in batches of 5 to avoid hammering endpoints
            for (let i = 0; i < jobs.length; i += 5) {
                const batch = jobs.slice(i, i + 5);
                await Promise.allSettled(batch.map(async ({ userId, dbIdx, slotIdx, bot }) => {
                    try {
                        checked++;
                        const res = await axios.get(
                            bot.endpoint + '/checkverification?phoneNumber=' + bot.number,
                            { timeout: 8000 }
                        );
                        const connected = res.data?.connected === true;

                        // Read fresh DB copy so we don't stomp concurrent writes
                        const db = multiDB.readDatabase(dbIdx);
                        if (!db.users || !db.users[userId] || !db.users[userId].activeBots) return;
                        const entry = db.users[userId].activeBots[slotIdx];
                        if (!entry) return;

                        if (connected) {
                            entry.status = 'connected';
                            confirmed++;
                        } else {
                            // Remove dead bot entry
                            db.users[userId].activeBots.splice(slotIdx, 1);
                            // Also clear from endpoint usage
                            multiDB.removeFromEndpoint(bot.number);
                            disconnected++;
                            console.log('[BotVerify] Removed dead session: +' + bot.number);
                        }
                        multiDB.writeDatabase(dbIdx, db);
                    } catch (_) {
                        // Endpoint unreachable — leave status as-is, don't wipe
                    }
                }));
                // Small pause between batches
                await new Promise(r => setTimeout(r, 1000));
            }

            console.log('[BotVerify] Done — checked:' + checked + ' confirmed:' + confirmed + ' removed:' + disconnected);
        } catch (err) {
            console.error('[BotVerify] Error:', err.message);
        }
    }

    // First run after 10 min, then every 2 hours
    setTimeout(() => {
        verifyAllBots();
        setInterval(verifyAllBots, INTERVAL);
    }, FIRST_RUN);
}

function startAutoPing() {
    if (!IS_RENDER) return;
    const pingInterval = 5 * 60 * 1000;
    let failures = 0;
    async function pingServer() {
        try {
            await axios.get(`${config.webBaseUrl}/health`, { timeout: 10000 });
            failures = 0;
        } catch (error) {
            failures++;
            if (failures >= 3 && bot) {
                await bot.telegram.sendMessage(ADMIN_CHAT_ID, `🚨 *Ping Alert*\n${config.webBaseUrl}\nFailures: ${failures}`, { parse_mode: 'Markdown' }).catch(()=>{});
            }
        }
    }
    setTimeout(() => { pingServer(); setInterval(pingServer, pingInterval); }, 30000);
}

// ==================== EXPRESS APP ====================
const app = express();
app.use(express.static('views'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, User-Id');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

// ── Health / Ping ────────────────────────────────────────────────
app.get('/ping', (req, res) => res.json({ status: 'ok', timestamp: new Date().toISOString(), service: 'BIG_DADDY_V3', totalUsers: multiDB.getTotalUserCount() }));
app.get('/health', (req, res) => {
    const heapUsedMB = process.memoryUsage().heapUsed / 1024 / 1024;
    res.json({ status: 'healthy', memory: `${heapUsedMB.toFixed(2)}MB`, uptime: process.uptime(), timestamp: new Date().toISOString(), totalUsers: multiDB.getTotalUserCount() });
});
app.get('/simple-health', (req, res) => res.redirect('/health'));

// ── Registration ────────────────────────────────────────────────
app.get('/register/:userId', (req, res) => {
    const user = getUser(req.params.userId);
    if (user && user.profileCompleted) return res.redirect(`/profile/${req.params.userId}`);
    res.sendFile(path.join(__dirname, 'views', 'registration.html'));
});

app.post('/register/:userId', (req, res) => {
    try {
        const { userId } = req.params;
        const { firstName, lastName, email } = req.body;
        if (!firstName || !lastName || !email) return res.json({ success: false, error: 'All fields are required' });
        if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.json({ success: false, error: 'Invalid email' });
        const success = setUserProfile(userId, firstName, lastName, email);
        if (success) {
            if (bot) {
                bot.telegram.sendMessage(ADMIN_CHAT_ID, `👤 *New Registration*\n📛 ${firstName} ${lastName}\n📧 ${email}\n🆔 ${userId}`, { parse_mode: 'Markdown' }).catch(()=>{});
            }
            res.json({ success: true, message: 'Account created!', redirectUrl: `/webapp/${userId}` });
        } else {
            res.json({ success: false, error: 'Failed to create account' });
        }
    } catch (error) {
        res.json({ success: false, error: 'Internal server error' });
    }
});

// ── Profile ──────────────────────────────────────────────────────
app.get('/profile/:userId', (req, res) => {
    const user = getUser(req.params.userId);
    if (!user || !user.profileCompleted) return res.redirect(`/register/${req.params.userId}`);
    res.redirect(`/webapp/${req.params.userId}`);
});

// ── Admin Panel ─────────────────────────────────────────────────
app.get('/admin-panel/:userId', (req, res) => {
    if (req.params.userId !== ADMIN_CHAT_ID) return res.redirect(`/webapp/${req.params.userId}`);
    res.sendFile(path.join(__dirname, 'views', 'admin.html'));
});

// ── Dashboard ────────────────────────────────────────────────────
app.get('/webapp/:userId', (req, res) => {
    // Admin always gets the admin panel — skip profile check
    if (req.params.userId === ADMIN_CHAT_ID) return res.redirect(`/admin-panel/${req.params.userId}`);
    const user = getUser(req.params.userId);
    if (!user || !user.profileCompleted) return res.redirect(`/register/${req.params.userId}`);
    res.sendFile(path.join(__dirname, 'views', 'dashboard.html'));
});

app.get('/loading/:userId', (req, res) => res.redirect(`/webapp/${req.params.userId}`));

// ── User API ─────────────────────────────────────────────────────
app.get('/api/user/:userId', (req, res) => {
    const user = getUser(req.params.userId);
    if (!user) return res.json({ success: false, error: 'User not found' });
    res.json({ success: true, user: {
        firstName: user.firstName, lastName: user.lastName, email: user.email,
        hasAccess: user.hasAccess, createdAt: user.createdAt,
        activeBots: user.activeBots || [],
        isAdmin: user.id === ADMIN_CHAT_ID
    }});
});

// ── SSE: real-time status stream ─────────────────────────────────
app.get('/api/status-stream/:userId', (req, res) => {
    const { userId } = req.params;
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no'
    });
    connectedClients.set(userId, { res, lastActivity: Date.now() });
    res.write(`data: ${JSON.stringify({ type: 'connected', message: 'Real-time connected', timestamp: new Date().toISOString() })}\n\n`);

    // Send heartbeat every 25s to keep connection alive
    const heartbeat = setInterval(() => {
        try { res.write(': heartbeat\n\n'); } catch (_) { clearInterval(heartbeat); connectedClients.delete(userId); }
    }, 25000);

    req.on('close', () => { clearInterval(heartbeat); connectedClients.delete(userId); });
});

// ── SSE: events (alias) ──────────────────────────────────────────
app.get('/api/events/:userId', (req, res) => {
    req.params = { userId: req.params.userId };
    app.handle(Object.assign(req, { url: `/api/status-stream/${req.params.userId}` }), res);
});

// ── Webhook: phistar endpoints push bot status here ──────────────
// Each phistar Heroku app calls POST /api/webhook/bot-status when a
// WhatsApp session connects or disconnects.
// Required env vars on phistar side:
//   DASHBOARD_URL = this Render app URL (e.g. https://bigdaddy.onrender.com)
//   WEBHOOK_SECRET = same value as WEBHOOK_SECRET here (default below)
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || 'bigdaddyv4_webhook_2025';

app.post('/api/webhook/bot-status', async (req, res) => {
    try {
        const { secret, phoneNumber, status, endpoint, slotIndex } = req.body;

        // Validate secret
        if (!secret || secret !== WEBHOOK_SECRET) {
            return res.status(401).json({ success: false, error: 'Unauthorized' });
        }
        if (!phoneNumber || !status) {
            return res.status(400).json({ success: false, error: 'Missing phoneNumber or status' });
        }

        // Find which user owns this phone number across all user DBs
        let foundUserId = null;
        let foundUser   = null;
        for (let i = 2; i <= 4; i++) {
            const db = multiDB.readDatabase(i);
            for (const [uid, user] of Object.entries(db.users || {})) {
                const bots = user.activeBots || [];
                if (bots.some(b => b.number === phoneNumber)) {
                    foundUserId = uid;
                    foundUser   = user;
                    break;
                }
            }
            if (foundUserId) break;
        }

        if (!foundUserId) {
            // Not found — still acknowledge so phistar doesn't retry forever
            console.warn(`[Webhook] No user found for +${phoneNumber} (status: ${status})`);
            if (bot) {
                await bot.telegram.sendMessage(
                    ADMIN_CHAT_ID,
                    `⚠️ *Webhook — User Not Found*\n📱 +${phoneNumber}\n📌 Status: ${status}\n🔍 Not in any DB — bot not registered via start-verification?`,
                    { parse_mode: 'Markdown' }
                ).catch(() => {});
            }
            return res.json({ success: true, message: 'No matching user found for this number' });
        }

        // Update bot status in DB — always persist so slot survives redeployments
        const resolvedSlot = slotIndex ?? null;
        const resolvedEp   = endpoint || null;
        let botEntry = (foundUser.activeBots || []).find(b => b.number === phoneNumber);
        if (!botEntry) {
            // Entry missing — create it so it's always findable
            botEntry = { number: phoneNumber, prefix: '?', endpoint: resolvedEp, slotIndex: resolvedSlot ?? 0, status: 'verifying', startTime: new Date().toISOString() };
            foundUser.activeBots = foundUser.activeBots || [];
            foundUser.activeBots.push(botEntry);
        }
        botEntry.status = status === 'connected' ? 'connected'
                        : status === 'disconnected' ? 'stopped'
                        : 'failed';
        if (status === 'connected') {
            botEntry.connectedAt = new Date().toISOString();
            // Always update endpoint + slotIndex from the live webhook payload
            if (resolvedEp)   botEntry.endpoint  = resolvedEp;
            if (resolvedSlot != null) botEntry.slotIndex = resolvedSlot;
        }
        if (status === 'disconnected' || status === 'failed') {
            // Release endpoint so it can be reused
            releaseEndpoint(phoneNumber);
        }
        multiDB.createOrUpdateUser(foundUserId, foundUser);

        // Fire SSE event to the user's dashboard in real-time
        const sseType = status === 'connected'    ? 'bot_connected'
                      : status === 'disconnected' ? 'bot_disconnected'
                      : 'bot_failed';

        sseWrite(foundUserId, {
            type: sseType,
            phoneNumber,
            endpoint: endpoint || (botEntry && botEntry.endpoint),
            slotIndex: slotIndex ?? (botEntry && botEntry.slotIndex) ?? 0,
            timestamp: new Date().toISOString()
        });

        // 🔔 Notify admin on Telegram so we can track webhook delivery
        if (bot) {
            const emoji   = status === 'connected' ? '🟢' : status === 'disconnected' ? '🔴' : '❌';
            const label   = status === 'connected' ? 'CONNECTED' : status === 'disconnected' ? 'DISCONNECTED' : 'FAILED';
            const userTag = foundUser ? `${foundUser.firstName || ''} ${foundUser.lastName || ''}`.trim() || foundUserId : foundUserId;
            const slot    = (slotIndex ?? botEntry?.slotIndex ?? 0) + 1;
            const ep      = endpoint || botEntry?.endpoint || '—';
            await bot.telegram.sendMessage(
                ADMIN_CHAT_ID,
                `${emoji} *Webhook received*\n` +
                `📱 +${phoneNumber}\n` +
                `📌 Status: *${label}*\n` +
                `👤 User: ${userTag} (${foundUserId})\n` +
                `🎰 Slot: ${slot}\n` +
                `🌐 Endpoint: ${ep.replace('https://','')}\n` +
                `⏰ ${new Date().toLocaleTimeString()}`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.warn('[Webhook] Telegram notify failed:', e.message));
        }

        // Record in deploy history
        multiDB.addDeployRecord(foundUserId, {
            phoneNumber,
            prefix: botEntry?.prefix || '?',
            endpoint: endpoint || botEntry?.endpoint || '?',
            slotIndex: slotIndex ?? botEntry?.slotIndex ?? 0,
            action: 'webhook',
            status
        });

        console.log(`[Webhook] ${phoneNumber} → ${status} (user: ${foundUserId})`);
        res.json({ success: true, userId: foundUserId, status });

    } catch (error) {
        console.error('[Webhook] Error:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ── Webhook secret (admin only) ───────────────────────────────────
app.get('/api/webhook/secret', (req, res) => {
    const userId = req.headers['user-id'];
    if (userId !== ADMIN_CHAT_ID) return res.status(403).json({ success: false, error: 'Admin only' });
    res.json({
        success: true,
        secret: WEBHOOK_SECRET,
        webhookUrl: `${config.webBaseUrl}/api/webhook/bot-status`,
        note: 'Set DASHBOARD_URL and WEBHOOK_SECRET on each phistar Heroku app, then call notifyDashboard() on connect/disconnect'
    });
});

// ══════════════════════════════════════════════════════════════════
// PHISTAR INTEGRATION — paste this into each phistar endpoint app
// ══════════════════════════════════════════════════════════════════
// Required Heroku Config Vars on each phistar app:
//   DASHBOARD_URL   = https://your-bigdaddy-app.onrender.com
//   WEBHOOK_SECRET  = bigdaddyv4_webhook_2025   (match WEBHOOK_SECRET here)
//
// async function notifyDashboard(phoneNumber, status, slotIndex) {
//     const DASHBOARD_URL    = process.env.DASHBOARD_URL;
//     const WEBHOOK_SECRET   = process.env.WEBHOOK_SECRET || 'bigdaddyv4_webhook_2025';
//     if (!DASHBOARD_URL) return;
//     try {
//         await axios.post(DASHBOARD_URL + '/api/webhook/bot-status', {
//             secret: WEBHOOK_SECRET,
//             phoneNumber,          // e.g. '2348012345678'
//             status,               // 'connected' | 'disconnected' | 'failed'
//             endpoint: process.env.RENDER_EXTERNAL_URL || '',
//             slotIndex: slotIndex ?? 0
//         }, { timeout: 8000 });
//     } catch (err) {
//         console.error('[Webhook] Failed to notify dashboard:', err.message);
//     }
// }
//
// Call it at these points in your phistar WhatsApp connection code:
//   client.on('ready',        () => notifyDashboard(phoneNumber, 'connected',    slotIndex));
//   client.on('disconnected', () => notifyDashboard(phoneNumber, 'disconnected', slotIndex));
//   client.on('auth_failure', () => notifyDashboard(phoneNumber, 'failed',       slotIndex));
// ══════════════════════════════════════════════════════════════════

// ── Endpoint Selection (1 bot per endpoint) ──────────────────────
app.post('/api/get-endpoint', (req, res) => {
    try {
        const { prefix } = req.body;
        if (!prefix || !ENDPOINTS[prefix]) return res.json({ success: false, error: 'Invalid prefix' });

        const endpoint = getFreeEndpoint(prefix);
        if (!endpoint) return res.json({ success: false, error: 'No endpoints configured for this prefix.' });

        res.json({ success: true, endpoint, prefix });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ── Start verification / pairing ─────────────────────────────────
app.post('/api/start-verification', (req, res) => {
    try {
        const { phoneNumber, endpoint, userId, prefix, attempts = 6, slotIndex = 0 } = req.body;
        const jobId = `${userId}_${phoneNumber}`;

        // Assign this endpoint to this number (1:1)
        assignEndpoint(endpoint, userId, phoneNumber, prefix);

        // Update user's activeBots
        const user = getUser(userId);
        if (user) {
            user.activeBots = user.activeBots || [];
            user.activeBots = user.activeBots.filter(b => b.number !== phoneNumber);
            user.activeBots.push({ number: phoneNumber, prefix, endpoint, slotIndex, status: 'verifying', startTime: new Date().toISOString() });
            multiDB.createOrUpdateUser(userId, user);
        }

        // Record deploy history
        multiDB.addDeployRecord(userId, { phoneNumber, prefix, endpoint, slotIndex, action: 'deploy', status: 'verifying' });

        verificationJobs.set(jobId, {
            phoneNumber, endpoint, userId, prefix, slotIndex,
            attemptsRemaining: attempts, status: 'active',
            startTime: new Date(), nextCheck: new Date(Date.now() + 2 * 60 * 1000)
        });

        res.json({ success: true, message: 'Background verification started', jobId });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// Background verification worker
setInterval(async () => {
    const now = new Date();
    for (const [jobId, job] of verificationJobs.entries()) {
        if (job.status !== 'active' || job.nextCheck > now) continue;
        try {
            const response = await axios.get(`${job.endpoint}/checkverification?phoneNumber=${job.phoneNumber}`, { timeout: 10000 });
            const data = response.data;
            if (data.success && data.connected) {
                job.status = 'completed';
                job.connected = true;

                // Update user bot status
                const user = getUser(job.userId);
                if (user) {
                    const botEntry = (user.activeBots || []).find(b => b.number === job.phoneNumber);
                    if (botEntry) { botEntry.status = 'connected'; botEntry.connectedAt = new Date().toISOString(); }
                    multiDB.createOrUpdateUser(job.userId, user);
                }

                // Record history
                multiDB.addDeployRecord(job.userId, { phoneNumber: job.phoneNumber, prefix: job.prefix, endpoint: job.endpoint, slotIndex: job.slotIndex, action: 'deploy', status: 'connected' });

                // SSE notify
                sseWrite(job.userId, { type: 'bot_connected', phoneNumber: job.phoneNumber, endpoint: job.endpoint, slotIndex: job.slotIndex, timestamp: new Date().toISOString() });

                // Telegram notify
                if (bot) {
                    const user = getUser(job.userId);
                    const name = user ? `${user.firstName} ${user.lastName}`.trim() : job.userId;
                    await bot.telegram.sendMessage(ADMIN_CHAT_ID, `✅ Bot connected!\n👤 ${name}\n📱 +${job.phoneNumber}\n⌨️ Prefix: ${job.prefix}\n🌐 ${job.endpoint}`).catch(()=>{});
                }
            } else {
                job.attemptsRemaining--;
                if (job.attemptsRemaining > 0) job.nextCheck = new Date(Date.now() + 2 * 60 * 1000);
                else {
                    job.status = 'failed';
                    releaseEndpoint(job.phoneNumber);
                    multiDB.addDeployRecord(job.userId, { phoneNumber: job.phoneNumber, prefix: job.prefix, endpoint: job.endpoint, slotIndex: job.slotIndex, action: 'deploy', status: 'failed' });
                    sseWrite(job.userId, { type: 'bot_failed', phoneNumber: job.phoneNumber, slotIndex: job.slotIndex, timestamp: new Date().toISOString() });
                }
            }
        } catch (error) {
            job.attemptsRemaining--;
            if (job.attemptsRemaining <= 0) {
                job.status = 'failed';
                releaseEndpoint(job.phoneNumber);
            } else {
                job.nextCheck = new Date(Date.now() + 2 * 60 * 1000);
            }
        }
    }
}, 30000);

app.get('/api/verification-status/:userId', (req, res) => {
    const userJobs = Array.from(verificationJobs.entries())
        .filter(([k]) => k.startsWith(req.params.userId + '_'))
        .map(([, j]) => j);
    res.json({ success: true, verification: userJobs[0] || null });
});

app.post('/api/clear-verification', (req, res) => {
    const { userId } = req.body;
    Array.from(verificationJobs.keys()).filter(k => k.startsWith(userId + '_')).forEach(k => verificationJobs.delete(k));
    res.json({ success: true });
});

// ── Stop bot ─────────────────────────────────────────────────────
app.post('/api/stop-bot', async (req, res) => {
    try {
        const { phoneNumber, userId, endpoint } = req.body;
        if (!phoneNumber || !userId) return res.json({ success: false, error: 'Missing params' });

        const user = getUser(userId);
        let botEntry = null;
        if (user && user.activeBots) {
            botEntry = user.activeBots.find(b => b.number === phoneNumber);
            user.activeBots = user.activeBots.filter(b => b.number !== phoneNumber);
            multiDB.createOrUpdateUser(userId, user);
        }

        const ep = endpoint || (botEntry && botEntry.endpoint) || multiDB.getEndpointForNumber(phoneNumber);
        releaseEndpoint(phoneNumber);

        if (ep) {
            try { await axios.get(`${ep}/delpair?phoneNumber=${phoneNumber}`, { timeout: 10000 }); } catch (_) {}
        }

        multiDB.addDeployRecord(userId, {
            phoneNumber, prefix: botEntry?.prefix || '?', endpoint: ep || '?',
            slotIndex: botEntry?.slotIndex ?? -1, action: 'stop', status: 'stopped'
        });

        sseWrite(userId, { type: 'bot_disconnected', phoneNumber, timestamp: new Date().toISOString() });
        res.json({ success: true, message: 'Bot stopped', endpoint: ep });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ── Restart bot ──────────────────────────────────────────────────
app.post('/api/restart-bot', async (req, res) => {
    try {
        const { phoneNumber, prefix, userId } = req.body;
        if (!phoneNumber || !prefix || !userId) return res.json({ success: false, error: 'Missing params' });

        const user = getUser(userId);
        let slotIndex = 0;
        if (user && user.activeBots) {
            const old = user.activeBots.find(b => b.number === phoneNumber);
            if (old) { slotIndex = old.slotIndex || 0; }
            user.activeBots = user.activeBots.filter(b => b.number !== phoneNumber);
            multiDB.createOrUpdateUser(userId, user);
        }
        releaseEndpoint(phoneNumber);

        const newEndpoint = getFreeEndpoint(prefix);
        if (!newEndpoint) return res.json({ success: false, error: 'No free endpoints for this prefix' });

        assignEndpoint(newEndpoint, userId, phoneNumber, prefix);
        if (user) {
            user.activeBots = user.activeBots || [];
            user.activeBots.push({ number: phoneNumber, prefix, endpoint: newEndpoint, slotIndex, status: 'pairing', startTime: new Date().toISOString() });
            multiDB.createOrUpdateUser(userId, user);
        }

        multiDB.addDeployRecord(userId, { phoneNumber, prefix, endpoint: newEndpoint, slotIndex, action: 'restart', status: 'verifying' });
        res.json({ success: true, endpoint: newEndpoint, message: 'Restarted on new endpoint' });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ── Deploy history ───────────────────────────────────────────────
app.get('/api/deploy-history/:userId', (req, res) => {
    const history = multiDB.getDeployHistory(req.params.userId);
    res.json({ success: true, history });
});

// ── Bot status ───────────────────────────────────────────────────
app.post('/api/update-bot-status', (req, res) => {
    try {
        const { phoneNumber, status, userId } = req.body;
        const user = getUser(userId);
        if (user && user.activeBots) {
            const b = user.activeBots.find(b => b.number === phoneNumber);
            if (b) {
                b.status = status;
                b.lastChecked = new Date().toISOString();
                if (status === 'connected') b.connectedAt = new Date().toISOString();
                multiDB.createOrUpdateUser(userId, user);
                return res.json({ success: true });
            }
        }
        res.json({ success: false, error: 'Bot not found' });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ── Pair number (legacy) ─────────────────────────────────────────
app.post('/api/pair-number', async (req, res) => {
    try {
        const { phoneNumber, prefix } = req.body;
        const userId = req.headers['user-id'] || req.body.userId || 'unknown';
        if (!phoneNumber || !prefix) return res.json({ success: false, error: 'Missing params' });

        const endpoint = getFreeEndpoint(prefix);
        if (!endpoint) return res.json({ success: false, error: 'No free endpoints for prefix' });

        assignEndpoint(endpoint, userId, phoneNumber, prefix);
        const user = getUser(userId);
        if (user) {
            user.activeBots = (user.activeBots || []).filter(b => b.number !== phoneNumber);
            user.activeBots.push({ number: phoneNumber, prefix, endpoint, status: 'pairing', startTime: new Date().toISOString() });
            multiDB.createOrUpdateUser(userId, user);
        }

        res.json({ success: true, endpoint });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ── Endpoint health / stats ──────────────────────────────────────
app.get('/api/endpoint-health', (req, res) => res.json({ success: true, health: multiDB.getAllEndpointHealth() }));
app.post('/api/health-check', async (req, res) => { const r = await healthCheckAllEndpoints(); res.json(r); });
app.get('/api/endpoint-stats', (req, res) => {
    const stats = multiDB.getAllEndpointUsage();
    const health = multiDB.getAllEndpointHealth();
    res.json({ success: true, stats, health, endpoints: ENDPOINTS });
});

// ══════════════════════════════════════════════════════════════════
//  COMMUNITY CHAT — Real-time (no fake data)
// ══════════════════════════════════════════════════════════════════

// GET /api/community/messages?since=<timestamp_ms>
app.get('/api/community/messages', (req, res) => {
    try {
        const userId = req.headers['user-id'] || req.query.userId;
        const since = parseInt(req.query.since) || 0;
        const limit = parseInt(req.query.limit) || 60;

        // Mark user as online
        if (userId) {
            const user = getUser(userId);
            multiDB.setUserOnline(userId, user ? `${user.firstName} ${user.lastName}`.trim() : 'User');
        }

        const messages = since > 0
            ? multiDB.getNewCommunityMessages(since)
            : multiDB.getCommunityMessages(limit);

        const onlineUsers = multiDB.getOnlineUsers();

        res.json({ success: true, messages, onlineCount: onlineUsers.length, onlineUsers, serverTime: Date.now() });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// POST /api/community/send
app.post('/api/community/send', (req, res) => {
    try {
        const userId = req.headers['user-id'] || req.body.userId;
        const { message, userName } = req.body;

        if (!userId || !message || !message.trim()) return res.json({ success: false, error: 'Missing userId or message' });
        if (message.trim().length > 500) return res.json({ success: false, error: 'Message too long (max 500 chars)' });

        const user = getUser(userId);
        const name = userName || (user ? `${user.firstName} ${user.lastName}`.trim() : 'User');
        const displayName = name.trim() || 'User';

        // Mark user online
        multiDB.setUserOnline(userId, displayName);

        const entry = multiDB.addCommunityMessage({ userId, userName: displayName, text: message.trim() });

        // Broadcast via SSE to all connected dashboard users
        broadcastToAll({ type: 'community_message', message: entry });

        res.json({ success: true, message: entry });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// GET /api/community/online
app.get('/api/community/online', (req, res) => {
    const userId = req.headers['user-id'] || req.query.userId;
    if (userId) {
        const user = getUser(userId);
        multiDB.setUserOnline(userId, user ? `${user.firstName} ${user.lastName}`.trim() : 'User');
    }
    const onlineUsers = multiDB.getOnlineUsers();
    res.json({ success: true, onlineCount: onlineUsers.length, onlineUsers });
});

// ══════════════════════════════════════════════════════════════════
//  SUPPORT TICKETS
// ══════════════════════════════════════════════════════════════════

app.post('/api/support/ticket', (req, res) => {
    try {
        const userId = req.headers['user-id'] || req.body.userId;
        const { subject, category, body } = req.body;
        if (!userId || !subject || !body) return res.json({ success: false, error: 'Missing fields' });

        const user = getUser(userId);
        const userName = user ? `${user.firstName} ${user.lastName}`.trim() : userId;
        const ticket = multiDB.createTicket(userId, subject, category || 'other', body, userName);

        // Notify admin via SSE and Telegram
        sseWrite(ADMIN_CHAT_ID, { type: 'new_ticket', ticket });
        if (bot) {
            bot.telegram.sendMessage(ADMIN_CHAT_ID, `🎫 *New Ticket ${ticket.id}*\n👤 ${userName}\n📋 ${subject}\n🏷 ${category || 'other'}\n\n${body}`, { parse_mode: 'Markdown' }).catch(()=>{});
        }

        res.json({ success: true, ticket });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/api/support/tickets/:userId', (req, res) => {
    const tickets = multiDB.getUserTickets(req.params.userId);
    res.json({ success: true, tickets });
});

app.post('/api/support/ticket/reply', (req, res) => {
    try {
        const { ticketId, status, reply } = req.body;
        const ok = multiDB.updateTicketStatus(ticketId, status || 'pending', reply);
        if (ok) {
            // Find ticket owner and notify
            const ticket = multiDB.getAllTickets().find(t => t.id === ticketId);
            if (ticket) {
                sseWrite(ticket.userId, { type: 'ticket_updated', ticketId, status: status || 'pending', reply, timestamp: new Date().toISOString() });
            }
        }
        res.json({ success: ok });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ══════════════════════════════════════════════════════════════════
//  SUPPORT CHAT (1:1 user ↔ admin)
// ══════════════════════════════════════════════════════════════════

app.post('/api/support/chat/send', (req, res) => {
    try {
        const userId = req.headers['user-id'] || req.body.userId;
        const { text } = req.body;
        if (!userId || !text) return res.json({ success: false, error: 'Missing params' });

        const entry = multiDB.addSupportMessage(userId, { text, fromAdmin: false });

        // Alert admin via SSE
        sseWrite(ADMIN_CHAT_ID, { type: 'support_chat', userId, message: entry });
        // Alert admin via Telegram
        if (bot) {
            const user = getUser(userId);
            const name = user ? `${user.firstName} ${user.lastName}`.trim() : userId;
            bot.telegram.sendMessage(ADMIN_CHAT_ID, `💬 *Support Chat from ${name}*\n${text}`, { parse_mode: 'Markdown' }).catch(()=>{});
        }

        res.json({ success: true, message: entry });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/api/support/chat/:userId', (req, res) => {
    multiDB.markSupportRead(req.params.userId);
    const messages = multiDB.getSupportChat(req.params.userId);
    res.json({ success: true, messages });
});

// Admin: reply to user support chat
app.post('/api/support/chat/admin-reply', (req, res) => {
    try {
        const { userId, text } = req.body;
        if (!userId || !text) return res.json({ success: false, error: 'Missing params' });
        const entry = multiDB.addSupportMessage(userId, { text, fromAdmin: true });
        sseWrite(userId, { type: 'support_reply', message: entry });
        res.json({ success: true, message: entry });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ── Logout ───────────────────────────────────────────────────────
app.post('/api/logout', (req, res) => {
    const userId = req.headers['user-id'] || req.body.userId;
    if (userId) connectedClients.delete(userId);
    res.json({ success: true });
});

// ── Admin routes ─────────────────────────────────────────────────
app.get('/admin/statistics', (req, res) => {
    const stats = getStatistics();
    res.json({ success: true, statistics: stats });
});

app.get('/admin/users', (req, res) => {
    const users = Object.values(multiDB.getAllUsers()).map(u => ({
        id: u.id, firstName: u.firstName, lastName: u.lastName, email: u.email,
        profileCompleted: u.profileCompleted, createdAt: u.createdAt, activeBots: u.activeBots || []
    }));
    res.json({ success: true, users, total: users.length });
});

app.get('/admin/tickets', (req, res) => {
    res.json({ success: true, tickets: multiDB.getAllTickets() });
});

app.get('/admin/endpoints', (req, res) => {
    const usage = multiDB.getAllEndpointUsage();
    const health = multiDB.getAllEndpointHealth();
    const byPrefix = {};
    Object.entries(ENDPOINTS).forEach(([prefix, endpoints]) => {
        byPrefix[prefix] = endpoints.map(ep => ({
            url: ep,
            activeNumbers: usage[ep]?.activeNumbers || [],
            userCount: usage[ep]?.userCount || 0,
            health: health[ep]?.status || 'unknown',
            responseTime: health[ep]?.responseTime,
            lastChecked: health[ep]?.lastChecked
        }));
    });
    res.json({ success: true, byPrefix, availablePrefixes: Object.keys(ENDPOINTS) });
});

app.post('/admin/endpoints/add', (req, res) => {
    const { endpointUrl, prefix } = req.body;
    if (!endpointUrl || !prefix) return res.json({ success: false, error: 'Missing params' });
    try { new URL(endpointUrl); } catch (_) { return res.json({ success: false, error: 'Invalid URL' }); }
    if (!ENDPOINTS[prefix]) ENDPOINTS[prefix] = [];
    for (const p of Object.keys(ENDPOINTS)) {
        if (ENDPOINTS[p].includes(endpointUrl)) return res.json({ success: false, error: `Already exists in prefix: ${p}` });
    }
    ENDPOINTS[prefix].push(endpointUrl);
    multiDB.updateEndpointUsage(endpointUrl, { userCount: 0, activeNumbers: [], prefix, addedAt: new Date().toISOString() });
    multiDB.updateEndpointHealth(endpointUrl, { status: 'unknown', errorCount: 0 });
    res.json({ success: true, message: `Added to ${prefix}` });
});

app.delete('/admin/endpoints/remove', (req, res) => {
    const { endpointUrl } = req.body;
    if (!endpointUrl) return res.json({ success: false, error: 'Missing endpointUrl' });
    let removed = false;
    Object.keys(ENDPOINTS).forEach(p => {
        const idx = ENDPOINTS[p].indexOf(endpointUrl);
        if (idx > -1) { ENDPOINTS[p].splice(idx, 1); removed = true; }
    });
    if (!removed) return res.json({ success: false, error: 'Endpoint not found' });
    const db = multiDB.readDatabase(1);
    delete db.endpointHealth?.[endpointUrl];
    delete db.endpointUsage?.[endpointUrl];
    multiDB.writeDatabase(1, db);
    res.json({ success: true });
});

app.post('/admin/endpoints/cleanup', async (req, res) => {
    cleanupRemovedEndpoints();
    const hc = await healthCheckAllEndpoints();
    res.json({ success: true, healthCheck: hc });
});

app.get('/admin/endpoints/stats', (req, res) => {
    const usage = multiDB.getAllEndpointUsage();
    const health = multiDB.getAllEndpointHealth();
    const byPrefix = {};
    Object.entries(ENDPOINTS).forEach(([prefix, eps]) => {
        byPrefix[prefix] = {
            total: eps.length,
            free: eps.filter(ep => !multiDB.isEndpointOccupied(ep)).length,
            occupied: eps.filter(ep => multiDB.isEndpointOccupied(ep)).length,
            healthy: eps.filter(ep => health[ep]?.status === 'healthy').length
        };
    });
    res.json({ success: true, byPrefix });
});


// ── Admin: Clear Active Sessions ─────────────────────────────────
app.post('/admin/clear-sessions', (req, res) => {
    try {
        // 1. Clear all activeNumbers from every endpoint in database_api
        const apiDb = multiDB.readDatabase(1);
        let clearedEndpoints = 0;
        let clearedNumbers = 0;
        if (apiDb.endpointUsage) {
            Object.keys(apiDb.endpointUsage).forEach(ep => {
                const entry = apiDb.endpointUsage[ep];
                clearedNumbers += (entry.activeNumbers || []).length;
                entry.activeNumbers = [];
                entry.userCount = 0;
                clearedEndpoints++;
            });
            multiDB.writeDatabase(1, apiDb);
        }

        // 2. Clear activeBots from all users across all user databases
        let clearedUsers = 0;
        [2, 3, 4].forEach(dbIdx => {
            const db = multiDB.readDatabase(dbIdx);
            if (!db.users) return;
            Object.keys(db.users).forEach(uid => {
                if (db.users[uid].activeBots && db.users[uid].activeBots.length > 0) {
                    db.users[uid].activeBots = [];
                    clearedUsers++;
                }
            });
            multiDB.writeDatabase(dbIdx, db);
        });

        res.json({ success: true, clearedEndpoints, clearedNumbers, clearedUsers });
    } catch (err) {
        res.json({ success: false, error: err.message });
    }
});

// ── Admin Groups / Sponsors ───────────────────────────────────────
app.get('/admin/groups', (req, res) => {
    const groups = multiDB.getGroups();
    const pending = multiDB.getPendingGroups();
    res.json({ success: true, groups, pending });
});

app.post('/admin/groups/approve', async (req, res) => {
    const { groupId } = req.body;
    if (!groupId) return res.json({ success: false, error: 'Missing groupId' });
    const success = await approveGroup(groupId);
    res.json({ success });
});

app.post('/admin/groups/reject', (req, res) => {
    const { groupId } = req.body;
    if (!groupId) return res.json({ success: false, error: 'Missing groupId' });
    const success = multiDB.rejectGroup ? multiDB.rejectGroup(groupId) : multiDB.removeGroup(groupId);
    res.json({ success });
});

app.delete('/admin/groups/remove', (req, res) => {
    const { groupId } = req.body;
    if (!groupId) return res.json({ success: false, error: 'Missing groupId' });
    const success = removeGroup(groupId);
    res.json({ success });
});

app.post('/admin/groups/add', async (req, res) => {
    const { id, title, type, inviteLink, username } = req.body;
    if (!id || !title) return res.json({ success: false, error: 'Missing id or title' });
    const success = await addGroupWithInvite({ id, title, type: type || 'channel', inviteLink: inviteLink || '', username: username || '' });
    res.json({ success });
});

// ── Backup / restore ─────────────────────────────────────────────
app.get('/trigger-backup', async (req, res) => {
    const r = await backupDatabaseToDropbox();
    res.json(r);
});

app.get('/backup-status', (req, res) => {
    res.json({ success: true, databases: multiDB.dbPaths.map(p => ({ name: path.basename(p), exists: fs.existsSync(p), size: fs.existsSync(p) ? fs.statSync(p).size : 0 })) });
});

// ── Home ─────────────────────────────────────────────────────────
app.get('/', (req, res) => {
    const stats = getStatistics();
    res.json({ status: 'BIG DADDY V3 Online', totalUsers: stats.totalUsers, uptime: process.uptime(), timestamp: new Date().toISOString() });
});

// ==================== TELEGRAM BOT ====================

function ensureSession(ctx, next) { if (!ctx.session) ctx.session = {}; return next(); }

async function handleAutoGroupDetection(ctx) {
    try {
        const chat = ctx.chat;
        if (!chat || !['group','supergroup','channel'].includes(chat.type)) return;
        if (ctx.message?.new_chat_members) {
            const botInfo = await bot.telegram.getMe();
            if (ctx.message.new_chat_members.some(m => m.id === botInfo.id)) {
                let inviteLink = null;
                try {
                    inviteLink = chat.type !== 'channel' ? await generateGroupInviteLink(chat.id) : (chat.username ? `https://t.me/${chat.username}` : null);
                } catch (_) {}
                const groupData = { id: chat.id.toString(), title: chat.title || 'Unknown', username: chat.username, inviteLink, type: chat.type === 'channel' ? 'channel' : 'group' };
                if (addPendingGroup(groupData)) {
                    await bot.telegram.sendMessage(ADMIN_CHAT_ID,
                        `🆕 *New ${chat.type} Detected*\n📝 ${chat.title}\n🆔 ${chat.id}\n🔗 ${inviteLink || 'N/A'}`,
                        { parse_mode: 'Markdown', ...Markup.inlineKeyboard([[Markup.button.callback('✅ Approve', `approve_${chat.id}`)],[Markup.button.callback('❌ Reject', `reject_${chat.id}`)]])}
                    );
                }
            }
        }
        if (ctx.message?.left_chat_member) {
            const botInfo = await bot.telegram.getMe();
            if (ctx.message.left_chat_member.id === botInfo.id) {
                removeGroup(chat.id.toString()); rejectGroup(chat.id.toString());
                await bot.telegram.sendMessage(ADMIN_CHAT_ID, `🚫 Bot removed from ${chat.title}`, { parse_mode: 'Markdown' });
            }
        }
    } catch (_) {}
}

async function handleUserStart(ctx) {
    const userId = ctx.from.id.toString();
    let user = getUser(userId);
    if (!user) {
        user = { id: userId, firstName: ctx.from.first_name || '', lastName: ctx.from.last_name || '', telegramUsername: ctx.from.username || '', profileCompleted: false, hasAccess: false, createdAt: new Date().toISOString() };
        createOrUpdateUser(userId, user);
    }

    const membershipCheck = await checkUserMembership(userId);
    if (!membershipCheck.hasAccess && multiDB.getGroups().length > 0) {
        const groupButtons = membershipCheck.notJoinedGroups.map(g =>
            [Markup.button.url(`📢 Join ${g.title}`, g.inviteLink || `https://t.me/${g.username || ''}`)]
        );
        return ctx.reply('⚠️ Please join the required sponsor channels first.', {
            parse_mode: 'Markdown',
            ...Markup.inlineKeyboard([...groupButtons, [Markup.button.callback('✅ I Joined — Check Access', 'check_access')]])
        });
    }

    if (!user.profileCompleted) {
        return ctx.reply(
            '👋 Welcome to *BIG DADDY V3*!\n\nCreate your account to get started.',
            { parse_mode: 'Markdown', ...Markup.inlineKeyboard([[Markup.button.webApp('📝 Create Account', `${config.webBaseUrl}/register/${userId}`)]]) }
        );
    }

    return ctx.reply(
        `🤖 Welcome back, *${user.firstName}*!\n\nYour dashboard is ready.`,
        { parse_mode: 'Markdown', ...Markup.inlineKeyboard([[Markup.button.webApp('🚀 Open Dashboard', `${config.webBaseUrl}/webapp/${userId}`)]]) }
    );
}

async function handleAdminStart(ctx) {
    const stats = getStatistics();
    await ctx.reply(
        `👑 *Admin Panel — BIG DADDY V3*\n\n👥 Users: ${stats.totalUsers}\n📅 Today: ${stats.usersToday}\n🤖 Active Sessions: ${stats.activeWhatsAppSessions}\n🌐 Healthy Endpoints: ${stats.healthyEndpoints}/${stats.totalEndpoints}`,
        { parse_mode: 'Markdown', ...Markup.inlineKeyboard([
            [Markup.button.webApp('📊 Dashboard', `${config.webBaseUrl}/webapp/${ADMIN_CHAT_ID}`)],
            [Markup.button.callback('📈 Stats', 'admin_stats'), Markup.button.callback('👥 Users', 'admin_users')],
            [Markup.button.callback('💾 Backup Now', 'admin_backup'), Markup.button.callback('🔍 Health Check', 'admin_health')]
        ])}
    );
}

async function showAdminPanel(ctx) { return handleAdminStart(ctx); }

async function showStatistics(ctx) {
    const stats = getStatistics();
    const groups = getGroups();
    const pendingGroups = getPendingGroups();
    await ctx.reply(
        `📊 *System Statistics*\n\n👥 *Users:*\n• Total: ${stats.totalUsers}\n• Today: ${stats.usersToday}\n• With Profile: ${stats.usersWithProfile}\n\n📋 *Sponsors:* ${groups.length} approved, ${pendingGroups.length} pending\n\n🤖 *WhatsApp:* ${stats.activeWhatsAppSessions} active sessions\n🌐 *Endpoints:* ${stats.healthyEndpoints}/${stats.totalEndpoints} healthy`,
        { parse_mode: 'Markdown' }
    );
}

async function listUsers(ctx) {
    const users = Object.values(multiDB.getAllUsers()).slice(0, 10);
    const userList = users.map((u, i) => `${i+1}. ${u.firstName || 'Unknown'} ${u.lastName || ''}\n   📧 ${u.email || 'N/A'}\n   🆔 ${u.id}\n   📅 ${new Date(u.createdAt).toLocaleDateString()}\n`).join('\n');
    await ctx.reply(`👥 *Users (${multiDB.getTotalUserCount()} total)*\n\n${userList}`, { parse_mode: 'Markdown' });
}

async function triggerBackup(ctx) {
    await ctx.reply('🔄 Starting backup...');
    const r = await backupDatabaseToDropbox();
    await ctx.reply(r.success ? '✅ Backup completed!' : `❌ Backup failed: ${r.error}`);
}

async function initializeTelegramBot() {
    try {
        bot = new Telegraf(config.telegramBotToken);
        bot.use(session());
        bot.use(ensureSession);

        bot.start(async ctx => {
            const userId = ctx.from.id.toString();
            if (isAdmin(userId)) await handleAdminStart(ctx);
            else await handleUserStart(ctx);
        });

        bot.command('admin', async ctx => { if (isAdmin(ctx.from.id.toString())) await showAdminPanel(ctx); else await ctx.reply('❌ Access denied'); });
        bot.command('stats', async ctx => { if (isAdmin(ctx.from.id.toString())) await showStatistics(ctx); else await ctx.reply('❌ Access denied'); });
        bot.command('users', async ctx => { if (isAdmin(ctx.from.id.toString())) await listUsers(ctx); else await ctx.reply('❌ Access denied'); });
        bot.command('backup', async ctx => { if (isAdmin(ctx.from.id.toString())) await triggerBackup(ctx); else await ctx.reply('❌ Access denied'); });

        bot.command('pending', async ctx => {
            if (!isAdmin(ctx.from.id.toString())) return ctx.reply('❌ Access denied');
            const pg = getPendingGroups();
            if (pg.length === 0) return ctx.reply('📭 No pending groups');
            await ctx.reply(`⏳ *Pending (${pg.length}):*\n\n${pg.map((g,i)=>`${i+1}. ${g.title}\n   ID: ${g.id}`).join('\n')}`, { parse_mode: 'Markdown' });
        });

        bot.command('removechannel', async ctx => {
            if (!isAdmin(ctx.from.id.toString())) return ctx.reply('Access denied');
            const args = ctx.message.text.split(' ').slice(1);
            if (!args.length) {
                const groups = getGroups();
                if (!groups.length) return ctx.reply('No approved channels to remove.');
                const lines = groups.map((g, i) => (i + 1) + '. ' + g.title + '\n   ID: ' + g.id);
                return ctx.reply('Approved Channels:\n\n' + lines.join('\n\n') + '\n\nUsage: /removechannel CHANNEL_ID');
            }
            const channelId = args[0].trim();
            const allGroups = getGroups();
            const found = allGroups.find(g => g.id === channelId);
            if (!found) return ctx.reply('Channel ID ' + channelId + ' not found. Send /removechannel to see all channels.');
            const success = removeGroup(channelId);
            await ctx.reply(success ? 'Removed: ' + found.title + '. Users no longer need to join this channel.' : 'Failed to remove channel.');
        });

        bot.command('addchannel', async ctx => {
            if (!isAdmin(ctx.from.id.toString())) return ctx.reply('❌ Access denied');
            const args = ctx.message.text.split(' ').slice(1);
            if (args.length < 2) return ctx.reply('Usage: /addchannel CHANNEL_ID Channel Name\nChannel IDs start with -100');
            const [channelId, ...nameParts] = args;
            if (!channelId.startsWith('-100')) return ctx.reply('❌ Invalid Channel ID. Must start with -100');
            const success = addGroup({ id: channelId, title: nameParts.join(' '), type: 'channel', isActive: true });
            await ctx.reply(success ? `✅ Added channel: ${nameParts.join(' ')}` : '⚠️ Channel already exists');
        });

        // Callback handlers
        bot.action('check_access', async ctx => {
            await ctx.answerCbQuery('Checking...');
            const userId = ctx.from.id.toString();
            const check = await checkUserMembership(userId);
            if (check.hasAccess) await handleUserStart(ctx);
            else await ctx.reply('❌ You still need to join all required channels.');
        });

        bot.action(/^approve_(.+)$/, async ctx => {
            if (!isAdmin(ctx.from.id.toString())) return ctx.answerCbQuery('Access denied');
            await ctx.answerCbQuery('Approving...');
            const groupId = ctx.match[1];
            const success = await approveGroup(groupId);
            await ctx.editMessageText(success ? `✅ Sponsor approved!` : '❌ Failed to approve');
        });

        bot.action(/^reject_(.+)$/, async ctx => {
            if (!isAdmin(ctx.from.id.toString())) return ctx.answerCbQuery('Access denied');
            await ctx.answerCbQuery('Rejected');
            rejectGroup(ctx.match[1]);
            await ctx.editMessageText('❌ Rejected');
        });

        bot.action('admin_stats', async ctx => { await ctx.answerCbQuery(); await showStatistics(ctx); });
        bot.action('admin_users', async ctx => { await ctx.answerCbQuery(); await listUsers(ctx); });
        bot.action('admin_backup', async ctx => { await ctx.answerCbQuery('Starting backup...'); await triggerBackup(ctx); });
        bot.action('admin_health', async ctx => {
            await ctx.answerCbQuery('Checking...');
            const r = await healthCheckAllEndpoints();
            await ctx.reply(`🔍 Health check done\n✅ Healthy: ${r.healthy}\n⚠️ Unhealthy: ${r.unhealthy}\n❌ Down: ${r.down}\n📦 Total: ${r.total}`);
        });

        // Group detection
        bot.on('new_chat_members', handleAutoGroupDetection);
        bot.on('left_chat_member', handleAutoGroupDetection);

        return bot;
    } catch (error) {
        console.error('❌ Bot init failed:', error);
        return null;
    }
}

// ==================== START ====================
async function startServers() {
    try {
        console.log('🚀 Starting BIG DADDY V3...');
        console.log(`connecting to bigdaddy database`);
        initEndpointTracking();
        await restoreDatabaseFromDropbox();

        const server = app.listen(config.webPort, '0.0.0.0', () => {
            console.log(`✅ Web server running on port ${config.webPort}`);
            console.log(`database connected bigdaddyv3`);
            console.log(`📊 Dashboard: ${config.webBaseUrl}/webapp/{userId}`);
        });

        startAutoPing();
        startAutoBackup();
        startMemoryCleanup();
        startAggressiveMemoryCleanup();
        startMembershipMonitoring();
        startSessionMonitoring();
        startHealthCheckMonitoring();
        startBotConnectionVerification();

        // Safety ping every 4 min
        if (IS_RENDER) {
            setInterval(() => {
                axios.get(`${config.webBaseUrl}/ping`, { timeout: 5000 }).catch(()=>{});
            }, 4 * 60 * 1000);
        }

        const telegramBot = await initializeTelegramBot();
        if (telegramBot) {
            await telegramBot.telegram.deleteWebhook({ drop_pending_updates: true }).catch(() => {});
            await telegramBot.launch({ dropPendingUpdates: true });
            console.log('✅ Telegram bot started');
            try {
                await telegramBot.telegram.sendMessage(ADMIN_CHAT_ID,
                    `🤖 *BIG DADDY V3 Started*\n🕒 ${new Date().toLocaleString()}\n🌐 ${config.webBaseUrl}\n👥 Users: ${multiDB.getTotalUserCount()}\n✅ 8 databases online\n✅ 1 bot per endpoint enforced`,
                    { parse_mode: 'Markdown' }
                );
            } catch (_) {}
        }

        process.once('SIGINT', () => gracefulShutdown(telegramBot, server));
        process.once('SIGTERM', () => gracefulShutdown(telegramBot, server));

    } catch (error) {
        console.error('❌ Failed to start:', error);
        process.exit(1);
    }
}

async function gracefulShutdown(telegramBot, server) {
    console.log('🛑 Shutting down...');
    await backupDatabaseToDropbox().catch(()=>{});
    if (telegramBot) await telegramBot.stop();
    server.close(() => { console.log('✅ Shutdown complete'); process.exit(0); });
}

process.on('unhandledRejection', (reason) => console.error('❌ Unhandled Rejection:', reason));
process.on('uncaughtException', (error) => { console.error('❌ Uncaught Exception:', error); checkEmergencyRestart(); process.exit(1); });

console.log('connecting to bigdaddy database');
console.log('database connected bigdaddyv3');
startServers();

module.exports = {
    readDatabase: multiDB.readDatabase.bind(multiDB),
    getUser, createOrUpdateUser, deleteUser, isAdmin, getStatistics,
    backupDatabaseToDropbox, updateWhatsAppSessions,
    getUserWhatsAppSessions, getAllWhatsAppSessions,
    healthCheckAllEndpoints, getFreeEndpoint, multiDB
};
