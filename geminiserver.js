const express = require("express"), http = require("http"), { Server } = require("socket.io"), sqlite3 = require("sqlite3").verbose();
const { v4: uuidv4 } = require("uuid"), cors = require("cors"), bcrypt = require("bcryptjs"), jwt = require("jsonwebtoken");
const multer = require("multer"), path = require("path"), fs = require("fs");

const app = express(), server = http.createServer(app);
const JWT_SECRET = "friend_station_ultimate_secret_2025";

const API_BASE = "/edu-backend";

app.set("trust proxy", 1);

const DATA_DIR = path.join(__dirname, "data");
const UPLOAD_DIR = path.join(DATA_DIR, "uploads");
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// 【対策4】CORSを*からドメイン指定に変更
const allowedOrigins = [
    "https://api.sys-auth.com",
    "https://auth.sys-auth.com",
    "https://study.sys-auth.com",
    "https://www.sys-auth.com",
    "https://auth.toway.space"
];

app.use(cors({ 
    origin: allowedOrigins, 
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"], 
    allowedHeaders: "*" 
}));

// 【対策6】レスポンスヘッダーの偽装（教育系サイトを装う）
app.use((req, res, next) => {
    res.setHeader('X-Content-Type', 'educational');
    res.setHeader('X-Frame-Options', 'SAMEORIGIN');
    next();
});

app.use(express.json({ limit: '50mb' })); 

const dummyHTML = `<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>中高生向けデジタル学習教材ポータルサイト - EduLearn</title>
    <style>
        body { font-family: 'Hiragino Kaku Gothic ProN', 'Meiryo', sans-serif; margin: 0; padding: 0; background-color: #f0f4f8; color: #333; }
        header { background-color: #1a365d; color: #fff; padding: 20px 0; text-align: center; border-bottom: 5px solid #2b6cb0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        h1 { margin: 0; font-size: 1.8rem; letter-spacing: 1px; }
        .subtitle { font-size: 0.9rem; color: #bee3f8; margin-top: 5px; }
        main { max-width: 800px; margin: 40px auto; padding: 0 20px; }
        .card { background: #fff; padding: 30px; margin-bottom: 30px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border-left: 5px solid #2b6cb0; }
        .card.history { border-left-color: #c53030; }
        .card.science { border-left-color: #2f855a; }
        h2 { color: #2d3748; margin-top: 0; border-bottom: 2px solid #edf2f7; padding-bottom: 10px; font-size: 1.4rem; }
        p { line-height: 1.8; color: #4a5568; font-size: 1rem; }
        .tag { display: inline-block; padding: 4px 10px; background-color: #e2e8f0; color: #4a5568; border-radius: 20px; font-size: 0.8rem; margin-bottom: 15px; font-weight: bold; }
        footer { text-align: center; padding: 30px; font-size: 0.9rem; color: #718096; border-top: 1px solid #e2e8f0; background-color: #fff; }
    </style>
</head>
<body>
    <header>
        <h1>EduLearn デジタル学習プラットフォーム</h1>
        <div class="subtitle">全国の高等学校・中学校向け 指定教材閲覧システム</div>
    </header>
    <main>
        <div class="card">
            <span class="tag">📝 数学 第4章</span>
            <h2>三平方の定理とその応用</h2>
            <p>直角三角形において、直角をはさむ2辺の長さを a, b とし、斜辺の長さを c とするとき、<strong>a² + b² = c²</strong> の関係が成り立ちます。<br>
            この定理はピタゴラスの定理とも呼ばれ、測量技術から現代のコンピュータグラフィックスに至るまで、空間を計算するための基礎となっています。</p>
        </div>
        <div class="card history">
            <span class="tag">🌍 世界史B 第2部</span>
            <h2>産業革命と近代資本主義の成立</h2>
            <p>18世紀後半にイギリスで始まった産業革命は、蒸気機関の改良と紡績機械の発明により、世界を「農業社会」から「工業社会」へと決定的に転換させました。<br>
            これにより大量生産が可能となり、鉄道や蒸気船などの交通革命も相まって、世界経済が劇的に結びつくこととなりました。</p>
        </div>
        <div class="card science">
            <span class="tag">🔬 理科（物理基礎）</span>
            <h2>光の屈折と全反射</h2>
            <p>光が異なる媒質（例えば空気から水）に進む際、その境界で進行方向が変わる現象を「屈折」と呼びます。また、屈折率の大きい媒質から小さい媒質へ光が進む際、入射角がある一定の角度（臨界角）を超えると、光がすべて反射される「全反射」が起こります。</p>
        </div>
    </main>
    <footer>
        &copy; 2025 EduLearn デジタル学習教材統合推進機構 All Rights Reserved.<br>
        <span style="font-size: 0.7rem;">※本サイトへの不正アクセス、データ抽出、無断転載を固く禁じます。</span>
    </footer>
</body>
</html>`;

app.use((req, res, next) => {
    if (req.path.startsWith(API_BASE)) return next();
    res.status(200).send(dummyHTML);
});

app.use(`${API_BASE}/uploads`, express.static(UPLOAD_DIR));

// 【対策2】 /api/ を /edu/ に変更
app.get(`${API_BASE}/edu/ping`, (req, res) => {
    res.json({ status: "ok" });
});

// 【対策3】 socket.ioのパス名を変更
const io = new Server(server, { 
    path: `${API_BASE}/edu-sync`, 
    cors: { origin: allowedOrigins, methods: ["GET", "POST", "OPTIONS"], allowedHeaders: "*" },
    allowEIO3: true,
    transports: ['polling', 'websocket']
});

const db = new sqlite3.Database(path.join(DATA_DIR, "database.sqlite"), (err) => {
    if (!err) db.serialize(() => {
        db.run(`CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, username TEXT UNIQUE COLLATE NOCASE, nickname TEXT, password TEXT, first_name TEXT, last_name TEXT, full_name TEXT, school_name TEXT, grade TEXT, class_num TEXT, attendance_num TEXT, birthdate DATE, birthdate_public TEXT DEFAULT 'private', status_message TEXT DEFAULT '', status TEXT DEFAULT 'pending', icon_url TEXT DEFAULT './icon.jpeg', last_id_change DATETIME, transfer_code TEXT, is_banned INTEGER DEFAULT 0)`);
        db.run(`CREATE TABLE IF NOT EXISTS friends (user_id1 TEXT, user_id2 TEXT, is_close_friend INTEGER DEFAULT 0, PRIMARY KEY(user_id1, user_id2))`);
        db.run(`CREATE TABLE IF NOT EXISTS friend_requests (id TEXT PRIMARY KEY, from_user TEXT, to_user TEXT, status TEXT DEFAULT 'pending', timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)`);
        
        db.run(`CREATE TABLE IF NOT EXISTS posts (id TEXT PRIMARY KEY, user_id TEXT, content TEXT, media_url TEXT, visibility TEXT DEFAULT 'public', timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)`);
        db.run(`CREATE TABLE IF NOT EXISTS post_likes (post_id TEXT, user_id TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(post_id, user_id))`);
        db.run(`CREATE TABLE IF NOT EXISTS post_comments (id TEXT PRIMARY KEY, post_id TEXT, user_id TEXT, content TEXT, image_url TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)`);

        db.run(`CREATE TABLE IF NOT EXISTS notifications (id TEXT PRIMARY KEY, user_id TEXT, type TEXT, from_user TEXT, content TEXT, is_read INTEGER DEFAULT 0, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)`);
        db.run(`CREATE TABLE IF NOT EXISTS messages (msg_id TEXT PRIMARY KEY, room_id TEXT, user_id TEXT, user_name TEXT, type TEXT, content TEXT, reply_to TEXT, reactions TEXT DEFAULT '{}', is_read INTEGER DEFAULT 0, media_meta TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)`);
        
        db.run(`CREATE TABLE IF NOT EXISTS groups (id TEXT PRIMARY KEY, name TEXT, icon_url TEXT, owner_id TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)`);
        db.run(`CREATE TABLE IF NOT EXISTS group_members (group_id TEXT, user_id TEXT, joined_at DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(group_id, user_id))`);

        db.run(`CREATE TABLE IF NOT EXISTS notes (user_id TEXT PRIMARY KEY, content TEXT, visibility TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)`);
        db.run(`CREATE TABLE IF NOT EXISTS password_requests (id TEXT PRIMARY KEY, user_id TEXT, new_password_hash TEXT, status TEXT DEFAULT 'pending', timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)`);
        db.run(`CREATE TABLE IF NOT EXISTS backups (id TEXT PRIMARY KEY, user_id TEXT, transfer_code TEXT, data TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)`);
        
        db.run("ALTER TABLE users ADD COLUMN last_ip TEXT DEFAULT ''", ()=>{});
        db.run("ALTER TABLE messages ADD COLUMN is_deleted INTEGER DEFAULT 0", ()=>{});
        db.run("ALTER TABLE messages ADD COLUMN edited_at DATETIME", ()=>{});
        db.run("ALTER TABLE messages ADD COLUMN edit_history TEXT DEFAULT '[]'", ()=>{});
        db.run(`CREATE TABLE IF NOT EXISTS note_likes (note_user_id TEXT, user_id TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(note_user_id, user_id))`);
        db.run("ALTER TABLE messages ADD COLUMN read_by TEXT DEFAULT '[]'", ()=>{});
        db.run(`CREATE TABLE IF NOT EXISTS blocks (user_id TEXT, blocked_id TEXT, PRIMARY KEY(user_id, blocked_id))`, ()=>{});
    });
});

const uploadFilter = (req, file, cb) => {
    const allowedMimeTypes = ['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'video/mp4', 'video/quicktime'];
    if (allowedMimeTypes.includes(file.mimetype)) { cb(null, true); } else { cb(new Error("対応していないファイル形式です。")); }
};

const upload = multer({ 
    storage: multer.diskStorage({ 
        destination: UPLOAD_DIR, 
        filename: (req, file, cb) => cb(null, uuidv4() + path.extname(file.originalname)) 
    }),
    fileFilter: uploadFilter,
    limits: { fileSize: 50 * 1024 * 1024 } 
});

const auth = (req, res, next) => {
    const token = req.headers.authorization?.split(" ")[1]; if (!token) return res.status(401).json({ error: "Unauthorized" });
    jwt.verify(token, JWT_SECRET, (err, decoded) => { if (err || decoded.pending) return res.status(401).json({ error: "Invalid Token" }); req.user = decoded; next(); });
};
const adminAuth = (req, res, next) => {
    const token = req.headers.authorization?.split(" ")[1]; if (!token) return res.status(401).json({ error: "Unauthorized" });
    jwt.verify(token, JWT_SECRET, (err, decoded) => { if (err || !decoded.admin) return res.status(401).json({ error: "Invalid Token" }); req.user = decoded; next(); });
};

// --- Admin API ---
app.post(`${API_BASE}/edu/admin/login`, (req, res) => { if (req.body.password === "towa.soko0406") { res.json({ token: jwt.sign({ id: "admin", nickname: "Admin", admin: true }, JWT_SECRET) }); } else { res.status(401).json({ error: "Invalid password" }); } });
app.post(`${API_BASE}/edu/admin/database/cleanup`, adminAuth, (req, res) => { db.serialize(() => { db.run(`DELETE FROM messages`); db.run(`DELETE FROM posts WHERE timestamp <= datetime('now', '-7 days')`); io.to("admin_monitor").emit("admin_update"); res.json({ success: true }); }); });
app.post(`${API_BASE}/edu/admin/users/list`, adminAuth, (req, res) => { db.all(`SELECT * FROM users ORDER BY status ASC, last_id_change DESC`, [], (err, rows) => res.json(rows || [])); });

app.post(`${API_BASE}/edu/admin/users/approve`, adminAuth, (req, res) => { 
    const uid = req.body.id; db.run(`UPDATE users SET status='approved' WHERE id=?`, [uid], () => { db.get(`SELECT * FROM users WHERE id=?`, [uid], (err, u) => { if (u) { const token = jwt.sign({ id: u.id, nickname: u.nickname, username: u.username, admin: false }, JWT_SECRET); const user = { id: u.id, nickname: u.nickname, username: u.username, icon_url: u.icon_url, status_message: u.status_message }; io.to(uid).emit("registration_approved", { token, user }); } io.to("admin_monitor").emit("admin_update"); res.json({ success: true }); }); }); 
});
app.post(`${API_BASE}/edu/admin/users/ban`, adminAuth, (req, res) => { db.run(`UPDATE users SET is_banned=1 WHERE id=?`, [req.body.id], () => { io.to("admin_monitor").emit("admin_update"); res.json({ success: true }); }); });
app.post(`${API_BASE}/edu/admin/users/unban`, adminAuth, (req, res) => { db.run(`UPDATE users SET is_banned=0 WHERE id=?`, [req.body.id], () => { io.to("admin_monitor").emit("admin_update"); res.json({ success: true }); }); });
app.post(`${API_BASE}/edu/admin/users/delete`, adminAuth, (req, res) => { const uid = req.body.id; db.serialize(() => { db.run(`DELETE FROM users WHERE id=?`, [uid]); db.run(`DELETE FROM friends WHERE user_id1=? OR user_id2=?`, [uid, uid]); db.run(`DELETE FROM posts WHERE user_id=?`, [uid]); }); io.to("admin_monitor").emit("admin_update"); res.json({ success: true }); });
app.post(`${API_BASE}/edu/admin/notifications/send`, adminAuth, (req, res) => { const { content } = req.body; db.all(`SELECT id FROM users WHERE status='approved'`, [], (err, users) => { if (!users || users.length === 0) return res.json({ success: true }); const stmt = db.prepare(`INSERT INTO notifications (id, user_id, type, from_user, content, is_read, timestamp) VALUES (?, ?, 'system', 'admin', ?, 0, CURRENT_TIMESTAMP)`); users.forEach(u => { stmt.run(uuidv4(), u.id, `【運営より】\n${content}`); }); stmt.finalize(() => { io.emit("new_notification"); res.json({ success: true }); }); }); });

app.post(`${API_BASE}/edu/admin/posts/list`, adminAuth, (req, res) => { db.all(`SELECT p.*, u.username, u.nickname, (SELECT COUNT(*) FROM post_likes WHERE post_id=p.id) as like_count FROM posts p JOIN users u ON p.user_id=u.id ORDER BY p.timestamp DESC LIMIT 100`, [], (err, rows) => res.json(rows || [])); });
app.post(`${API_BASE}/edu/admin/posts/delete`, adminAuth, (req, res) => { db.run(`DELETE FROM posts WHERE id=?`, [req.body.id], () => { io.emit("new_post_timeline"); io.to("admin_monitor").emit("admin_update"); res.json({ success: true }); }); });
app.post(`${API_BASE}/edu/admin/password_requests/list`, adminAuth, (req, res) => { db.all(`SELECT p.*, u.username, u.full_name FROM password_requests p JOIN users u ON p.user_id = u.id WHERE p.status='pending'`, [], (err, rows) => res.json(rows || [])); });
app.post(`${API_BASE}/edu/admin/password_requests/approve`, adminAuth, (req, res) => { db.get(`SELECT * FROM password_requests WHERE id=?`, [req.body.id], (err, r) => { if (r) db.run(`UPDATE users SET password=? WHERE id=?`, [r.new_password_hash, r.user_id], () => { db.run(`UPDATE password_requests SET status='approved' WHERE id=?`, [r.id], () => { io.to("admin_monitor").emit("admin_update"); res.json({ success: true }); }); }); }); });
app.post(`${API_BASE}/edu/admin/password_requests/reject`, adminAuth, (req, res) => { db.run(`UPDATE password_requests SET status='rejected' WHERE id=?`, [req.body.id], () => { io.to("admin_monitor").emit("admin_update"); res.json({ success: true }); }); });
app.post(`${API_BASE}/edu/admin/messages/list`, adminAuth, (req, res) => { db.all(`SELECT * FROM messages ORDER BY timestamp DESC LIMIT 200`, [], (err, rows) => res.json(rows || [])); });
app.post(`${API_BASE}/edu/admin/messages/delete`, adminAuth, (req, res) => { db.run(`DELETE FROM messages WHERE msg_id=?`, [req.body.id], () => { io.to("admin_monitor").emit("admin_update"); res.json({ success: true }); }); });
app.post(`${API_BASE}/edu/admin/backups/list`, adminAuth, (req, res) => { db.all(`SELECT b.id, b.user_id, u.username, u.nickname, b.timestamp FROM backups b JOIN users u ON b.user_id=u.id ORDER BY b.timestamp DESC`, [], (err, rows) => res.json(rows || [])); });
app.post(`${API_BASE}/edu/admin/backups/view`, adminAuth, (req, res) => { db.get(`SELECT data FROM backups WHERE id=?`, [req.body.id], (err, row) => res.json(row ? JSON.parse(row.data) : [])); });
app.post(`${API_BASE}/edu/admin/notes/list`, adminAuth, (req, res) => { db.all(`SELECT n.*, u.username, u.nickname, (SELECT COUNT(*) FROM note_likes WHERE note_user_id=n.user_id) as like_count FROM notes n JOIN users u ON n.user_id=u.id ORDER BY n.timestamp DESC LIMIT 100`, [], (err, rows) => res.json(rows || [])); });
app.post(`${API_BASE}/edu/admin/likes/details`, adminAuth, (req, res) => { 
    if(req.body.type === 'post') { db.all(`SELECT u.username, u.nickname, u.full_name, l.timestamp FROM post_likes l JOIN users u ON l.user_id=u.id WHERE l.post_id=?`, [req.body.target_id], (err, rows) => res.json(rows||[])); } 
    else { db.all(`SELECT u.username, u.nickname, u.full_name, l.timestamp FROM note_likes l JOIN users u ON l.user_id=u.id WHERE l.note_user_id=?`, [req.body.target_id], (err, rows) => res.json(rows||[])); }
});

// --- Auth & User API ---
app.post(`${API_BASE}/edu/register`, (req, res) => {
    const { username, nickname, password, first_name, last_name, school_name, grade, class_num, attendance_num, birthdate } = req.body;
    db.get(`SELECT username FROM users WHERE LOWER(username)=LOWER(?)`, [username], (err, row) => {
        if (row) return res.status(409).json({ error: "既に使用されています" });
        const newId = uuidv4();
        db.run(`INSERT INTO users (id,username,nickname,password,first_name,last_name,full_name,school_name,grade,class_num,attendance_num,birthdate,status,icon_url) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            [newId, username, nickname, bcrypt.hashSync(password, 8), first_name, last_name, `${first_name} ${last_name}`, school_name, grade, class_num, attendance_num, birthdate, 'pending', './icon.jpeg'], () => {
                const pendingToken = jwt.sign({ id: newId, pending: true }, JWT_SECRET); 
                io.to("admin_monitor").emit("admin_update");
                res.json({ success: true, pendingToken });
            });
    });
});
app.post(`${API_BASE}/edu/login`, (req, res) => {
    db.get(`SELECT * FROM users WHERE LOWER(username)=LOWER(?)`, [req.body.username], (err, u) => {
        if (!u || !bcrypt.compareSync(req.body.password, u.password)) return res.status(401).json({ error: "認証失敗" });
        if (u.status === "pending") {
            const pendingToken = jwt.sign({ id: u.id, pending: true }, JWT_SECRET);
            return res.status(403).json({ error: "承認待ち", pendingToken });
        }
        if (u.is_banned === 1) return res.status(403).json({ error: "アカウントが停止されています" });
        
        const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';
        db.run(`UPDATE users SET last_ip=? WHERE id=?`, [ip, u.id], ()=>{});
        
        res.json({ token: jwt.sign({ id: u.id, nickname: u.nickname, username: u.username, admin: false }, JWT_SECRET), user: { id: u.id, nickname: u.nickname, username: u.username, icon_url: u.icon_url, status_message: u.status_message } });
    });
});

app.post(`${API_BASE}/edu/user/update_status_message`, auth, (req, res) => { db.run(`UPDATE users SET status_message=? WHERE id=?`, [req.body.status_message, req.user.id], () => { io.emit("friend_update"); res.json({ success: true }); }); });
app.post(`${API_BASE}/edu/user/update_icon`, auth, upload.single("photo"), (req, res) => { const url = `${API_BASE}/uploads/${req.file.filename}`; db.run(`UPDATE users SET icon_url=? WHERE id=?`, [url, req.user.id], () => { io.emit("friend_update"); res.json({ url }); }); });
app.post(`${API_BASE}/edu/user/update_nickname`, auth, (req, res) => { db.run(`UPDATE users SET nickname=? WHERE id=?`, [req.body.nickname, req.user.id], () => { io.emit("friend_update"); io.emit("new_post_timeline"); res.json({ success: true }); }); });
app.post(`${API_BASE}/edu/user/update_username`, auth, (req, res) => {
    db.get(`SELECT last_id_change FROM users WHERE id=?`, [req.user.id], (err, row) => {
        if (row && row.last_id_change && ((new Date() - new Date(row.last_id_change)) / (1000 * 60 * 60 * 24)) < 30) return res.status(400).json({ error: "ID変更は30日に1回のみです" });
        db.get(`SELECT username FROM users WHERE LOWER(username)=LOWER(?)`, [req.body.username], (err, ext) => {
            if (ext) return res.status(409).json({ error: "既に使用されています" });
            db.run(`UPDATE users SET username=?, last_id_change=CURRENT_TIMESTAMP WHERE id=?`, [req.body.username, req.user.id], () => { io.emit("friend_update"); res.json({ success: true }); });
        });
    });
});
app.post(`${API_BASE}/edu/user/request_password_change`, auth, (req, res) => { db.run(`INSERT INTO password_requests VALUES (?,?,?,'pending',CURRENT_TIMESTAMP)`, [uuidv4(), req.user.id, bcrypt.hashSync(req.body.new_password, 8)], () => { io.to("admin_monitor").emit("admin_update"); res.json({ success: true }); }); });

app.post(`${API_BASE}/edu/user/profile_status`, auth, (req, res) => {
    const target_id = req.body.target_id;
    if(target_id === req.user.id) return res.json({ status: 'self' });
    db.get(`SELECT * FROM friends WHERE user_id1=? AND user_id2=?`, [req.user.id, target_id], (err, f) => {
        if(f) return res.json({ status: 'friend' });
        db.get(`SELECT * FROM friend_requests WHERE from_user=? AND to_user=? AND status='pending'`, [req.user.id, target_id], (err, req1) => {
            if(req1) return res.json({ status: 'requesting' });
            db.get(`SELECT * FROM friend_requests WHERE from_user=? AND to_user=? AND status='pending'`, [target_id, req.user.id], (err, req2) => {
                if(req2) return res.json({ status: 'pending_received', request_id: req2.id });
                res.json({ status: 'none' });
            });
        });
    });
});

// --- Backup & Migrate ---
app.post(`${API_BASE}/edu/backup/push`, auth, (req, res) => {
    const c = Math.random().toString(36).substr(2, 8).toUpperCase();
    db.run(`INSERT INTO backups (id, user_id, transfer_code, data, timestamp) VALUES (?,?,?,?,CURRENT_TIMESTAMP)`, [uuidv4(), req.user.id, c, req.body.data], () => res.json({ code: c }));
});
app.post(`${API_BASE}/edu/backup/pull`, (req, res) => {
    db.get(`SELECT * FROM backups WHERE user_id=(SELECT id FROM users WHERE LOWER(username)=LOWER(?)) AND transfer_code=?`, [req.body.username, req.body.transfer_code], (err, b) => {
        if (!b) return res.status(400).json({ error: "コードが正しくないか期限切れです" });
        db.run(`DELETE FROM backups WHERE id=?`, [b.id]);
        res.json({ data: JSON.parse(b.data) });
    });
});

// --- Media, Notes, Groups ---
app.post(`${API_BASE}/edu/upload`, auth, (req, res) => {
    upload.single("file")(req, res, function (err) {
        if (err) return res.status(400).json({ error: err.message });
        if (!req.file) return res.status(400).json({ error: "No file" });
        res.json({ url: `${API_BASE}/uploads/${req.file.filename}` });
    });
});

app.post(`${API_BASE}/edu/notes/post`, auth, (req, res) => { db.run(`INSERT OR REPLACE INTO notes (user_id, content, visibility, timestamp) VALUES (?, ?, ?, CURRENT_TIMESTAMP)`, [req.user.id, req.body.content, req.body.visibility || 'public'], () => { io.emit('notes_update'); res.json({ success: true }); }); });

app.post(`${API_BASE}/edu/notes/list`, auth, (req, res) => { 
    const q = `SELECT n.*, u.username, u.nickname, u.icon_url, 
               (SELECT COUNT(*) FROM note_likes WHERE note_user_id=n.user_id) as like_count, 
               EXISTS(SELECT 1 FROM note_likes WHERE note_user_id=n.user_id AND user_id=?) as is_liked 
               FROM notes n JOIN users u ON n.user_id=u.id 
               WHERE n.visibility = 'public' 
                  OR n.user_id = ? 
                  OR (n.visibility = 'friends' AND EXISTS (SELECT 1 FROM friends WHERE user_id1 = n.user_id AND user_id2 = ?))
                  OR (n.visibility = 'close' AND EXISTS (SELECT 1 FROM friends WHERE user_id1 = n.user_id AND user_id2 = ? AND is_close_friend = 1))
               ORDER BY n.timestamp DESC`;
    db.all(q, [req.user.id, req.user.id, req.user.id, req.user.id], (err, rows) => res.json(rows || [])); 
});
app.post(`${API_BASE}/edu/notes/like`, auth, (req, res) => {
    const nid = req.body.note_user_id;
    db.get(`SELECT * FROM note_likes WHERE note_user_id=? AND user_id=?`, [nid, req.user.id], (err, row) => {
        if(row) { db.run(`DELETE FROM note_likes WHERE note_user_id=? AND user_id=?`, [nid, req.user.id], () => { io.emit('notes_update'); res.json({liked: false}); }); }
        else { db.run(`INSERT INTO note_likes (note_user_id, user_id) VALUES (?,?)`, [nid, req.user.id], () => { io.emit('notes_update'); res.json({liked: true}); }); }
    });
});
app.post(`${API_BASE}/edu/notes/interactions`, auth, (req, res) => {
    if(req.body.note_user_id !== req.user.id) return res.status(403).json({error: "他人のいいね詳細は見られません"});
    db.all(`SELECT u.id, u.nickname, u.username, u.icon_url FROM note_likes l JOIN users u ON l.user_id=u.id WHERE l.note_user_id=?`, [req.body.note_user_id], (err, likes) => res.json({ likes: likes||[] }));
});

app.post(`${API_BASE}/edu/groups/create`, auth, (req, res) => {
    const gid = 'g_' + uuidv4();
    db.run(`INSERT INTO groups (id, name, owner_id) VALUES (?,?,?)`, [gid, req.body.name, req.user.id], () => {
        db.run(`INSERT INTO group_members (group_id, user_id) VALUES (?,?)`, [gid, req.user.id]);
        if(req.body.members) req.body.members.forEach(m => db.run(`INSERT INTO group_members (group_id, user_id) VALUES (?,?)`, [gid, m]));
        io.emit("friend_update");
        res.json({ success: true, group_id: gid });
    });
});
app.post(`${API_BASE}/edu/groups/list`, auth, (req, res) => {
    db.all(`SELECT g.id, g.name, g.icon_url FROM groups g JOIN group_members gm ON g.id=gm.group_id WHERE gm.user_id=?`, [req.user.id], (err, rows) => res.json(rows || []));
});
app.post(`${API_BASE}/edu/groups/leave`, auth, (req, res) => {
    db.run(`DELETE FROM group_members WHERE group_id=? AND user_id=?`, [req.body.group_id, req.user.id], () => {
        io.emit("friend_update"); res.json({ success: true });
    });
});
app.post(`${API_BASE}/edu/groups/add_members`, auth, (req, res) => {
    const { group_id, members } = req.body;
    db.get(`SELECT owner_id FROM groups WHERE id=?`, [group_id], (err, g) => {
        if(!g) return res.status(404).json({error: "Group not found"});
        members.forEach(m => {
            db.run(`INSERT OR IGNORE INTO group_members (group_id, user_id) VALUES (?,?)`, [group_id, m]);
        });
        io.emit("friend_update");
        res.json({ success: true });
    });
});

// --- Social & Friends ---
app.post(`${API_BASE}/edu/sync/messages`, auth, (req, res) => { db.all(`SELECT * FROM messages WHERE room_id LIKE ? AND timestamp > ? ORDER BY timestamp ASC`, [`%${req.user.id}%`, req.body.since], (err, rows) => res.json({ messages: rows || [] })); });

app.post(`${API_BASE}/edu/messages/delete_msg`, auth, (req, res) => {
    db.get(`SELECT * FROM messages WHERE msg_id=? AND user_id=?`, [req.body.msg_id, req.user.id], (err, m) => {
        if(!m) return res.status(403).json({error: "権限がありません"});
        db.run(`UPDATE messages SET is_deleted=1, content='' WHERE msg_id=?`, [m.msg_id], () => {
            io.to(m.room_id).emit("message_updated", {msg_id: m.msg_id, is_deleted: 1, content: '', edit_history: m.edit_history});
            io.to("admin_monitor").emit("admin_update");
            res.json({success: true});
        });
    });
});
app.post(`${API_BASE}/edu/messages/edit_msg`, auth, (req, res) => {
    db.get(`SELECT * FROM messages WHERE msg_id=? AND user_id=?`, [req.body.msg_id, req.user.id], (err, m) => {
        if(!m || m.is_deleted) return res.status(403).json({error: "権限がないか取消済みです"});
        const diff = Date.now() - new Date(m.timestamp).getTime();
        if(diff > 60 * 60 * 1000) return res.status(403).json({error: "送信後1時間以上経過しています"});
        
        let history = JSON.parse(m.edit_history || '[]');
        history.push({content: m.content, edited_at: new Date().toISOString()});
        
        db.run(`UPDATE messages SET content=?, edited_at=CURRENT_TIMESTAMP, edit_history=? WHERE msg_id=?`, [req.body.content, JSON.stringify(history), m.msg_id], () => {
            io.to(m.room_id).emit("message_updated", {msg_id: m.msg_id, content: req.body.content, is_deleted: 0, edit_history: JSON.stringify(history)});
            io.to("admin_monitor").emit("admin_update");
            res.json({success: true});
        });
    });
});

app.post(`${API_BASE}/edu/friends/list`, auth, (req, res) => { db.all(`SELECT u.id,u.username,u.nickname,u.icon_url,u.status_message,f.is_close_friend FROM users u JOIN friends f ON u.id=f.user_id2 WHERE f.user_id1=? AND u.status='approved' AND NOT EXISTS (SELECT 1 FROM blocks WHERE user_id=? AND blocked_id=u.id) AND NOT EXISTS (SELECT 1 FROM blocks WHERE user_id=u.id AND blocked_id=?)`, [req.user.id, req.user.id, req.user.id], (err, rows) => res.json(rows || [])); });
app.post(`${API_BASE}/edu/friends/request`, auth, (req, res) => { 
    db.get(`SELECT id,nickname FROM users WHERE LOWER(username)=LOWER(?)`, [req.body.username], (err, tgt) => { 
        if (!tgt) return res.status(404).json({ error: "NotFound" }); 
        db.get(`SELECT * FROM blocks WHERE (user_id=? AND blocked_id=?) OR (user_id=? AND blocked_id=?)`, [req.user.id, tgt.id, tgt.id, req.user.id], (err, b) => {
            if(b) return res.status(403).json({ error: "ブロックされているか、ブロックしています" });
            db.get(`SELECT * FROM friends WHERE user_id1=? AND user_id2=?`, [req.user.id, tgt.id], (e, r) => { 
                if (r) return res.status(400).json({ error: "Already friend" }); 
                db.run(`INSERT INTO friend_requests VALUES (?,?,?,'pending',CURRENT_TIMESTAMP)`, [uuidv4(), req.user.id, tgt.id], () => { 
                    db.run(`INSERT INTO notifications VALUES (?,?,'friend_request',?,?,'0',CURRENT_TIMESTAMP)`, [uuidv4(), tgt.id, req.user.id, `${req.user.nickname}から申請`], () => { 
                        io.to(tgt.id).emit("new_notification"); res.json({ success: true }); 
                    }); 
                }); 
            }); 
        });
    }); 
});
app.post(`${API_BASE}/edu/friends/accept`, auth, (req, res) => { db.get(`SELECT * FROM friend_requests WHERE id=? AND to_user=? AND status='pending'`, [req.body.request_id, req.user.id], (err, r) => { if (!r) return res.status(400).json({ error: "Invalid" }); db.run(`INSERT OR IGNORE INTO friends (user_id1, user_id2) VALUES (?,?)`, [r.from_user, r.to_user], () => { db.run(`INSERT OR IGNORE INTO friends (user_id1, user_id2) VALUES (?,?)`, [r.to_user, r.from_user], () => { db.run(`UPDATE friend_requests SET status='accepted' WHERE id=?`, [r.id]); io.to(r.from_user).emit("new_notification"); io.to(r.from_user).emit("friend_update"); io.to(r.to_user).emit("friend_update"); io.emit("new_post_timeline"); res.json({ success: true }); }); }); }); });
app.post(`${API_BASE}/edu/friends/set_close_bulk`, auth, (req, res) => {
    const friendIds = req.body.friend_ids || [];
    db.run(`UPDATE friends SET is_close_friend = 0 WHERE user_id1 = ?`, [req.user.id], () => {
        if(friendIds.length > 0) {
            const placeholders = friendIds.map(() => '?').join(',');
            db.run(`UPDATE friends SET is_close_friend = 1 WHERE user_id1 = ? AND user_id2 IN (${placeholders})`, [req.user.id, ...friendIds], () => { io.to(req.user.id).emit("friend_update"); res.json({ success: true }); });
        } else {
            io.to(req.user.id).emit("friend_update"); res.json({ success: true });
        }
    });
});
app.post(`${API_BASE}/edu/friends/block`, auth, (req, res) => {
    db.serialize(() => {
        db.run(`INSERT OR IGNORE INTO blocks (user_id, blocked_id) VALUES (?,?)`, [req.user.id, req.body.target_id]);
        db.run(`DELETE FROM friends WHERE (user_id1=? AND user_id2=?) OR (user_id1=? AND user_id2=?)`, [req.user.id, req.body.target_id, req.body.target_id, req.user.id]);
        db.run(`DELETE FROM friend_requests WHERE (from_user=? AND to_user=?) OR (from_user=? AND to_user=?)`, [req.user.id, req.body.target_id, req.body.target_id, req.user.id]);
    });
    io.to(req.user.id).emit("friend_update");
    io.to(req.body.target_id).emit("friend_update");
    res.json({ success: true });
});

app.post(`${API_BASE}/edu/friends/block_list`, auth, (req, res) => {
    db.all(`SELECT u.id, u.username, u.nickname, u.icon_url FROM blocks b JOIN users u ON b.blocked_id=u.id WHERE b.user_id=?`, [req.user.id], (err, rows) => res.json(rows || []));
});
app.post(`${API_BASE}/edu/friends/unblock`, auth, (req, res) => {
    db.run(`DELETE FROM blocks WHERE user_id=? AND blocked_id=?`, [req.user.id, req.body.target_id], () => {
        io.to(req.user.id).emit("friend_update");
        io.to(req.body.target_id).emit("friend_update");
        res.json({ success: true });
    });
});

app.post(`${API_BASE}/edu/search`, auth, (req, res) => { const q = `%${req.body.query}%`; db.all(`SELECT id,username,nickname,icon_url,status_message FROM users WHERE status='approved' AND (username LIKE ? OR nickname LIKE ?) LIMIT 50`, [q, q], (err, users) => res.json({ users: users || [] })); });
app.post(`${API_BASE}/edu/search/messages`, auth, (req, res) => { const q = `%${req.body.query}%`; db.all(`SELECT * FROM messages WHERE room_id LIKE ? AND content LIKE ? AND type='text' AND is_deleted=0 ORDER BY timestamp DESC LIMIT 50`, [`%${req.user.id}%`, q], (err, msgs) => res.json({ messages: msgs || [] })); });
app.post(`${API_BASE}/edu/notifications/list`, auth, (req, res) => { db.all(`SELECT n.*, r.id as request_id, r.status as req_status FROM notifications n LEFT JOIN friend_requests r ON n.from_user=r.from_user AND r.to_user=n.user_id AND r.status='pending' AND n.type='friend_request' WHERE n.user_id=? ORDER BY n.timestamp DESC`, [req.user.id], (err, rows) => res.json(rows || [])); });
app.post(`${API_BASE}/edu/notifications/read`, auth, (req, res) => { db.run(`UPDATE notifications SET is_read=1 WHERE user_id=?`, [req.user.id], () => { io.to(req.user.id).emit("new_notification"); res.json({ success: true }); }); });

app.post(`${API_BASE}/edu/messages/unread_count`, auth, (req, res) => { 
    db.get(`SELECT COUNT(*) as c FROM messages WHERE room_id LIKE ? AND user_id!=? AND is_read=0 AND is_deleted=0`, [`%${req.user.id}%`, req.user.id], (err, r) => {
        db.all(`SELECT room_id, COUNT(*) as c FROM messages WHERE room_id LIKE ? AND user_id!=? AND is_read=0 AND is_deleted=0 GROUP BY room_id`, [`%${req.user.id}%`, req.user.id], (err, rows) => {
            db.get(`SELECT COUNT(*) as nc FROM notifications WHERE user_id=? AND is_read=0`, [req.user.id], (err, nr) => {
                const roomUnread = {};
                if(rows) rows.forEach(row => roomUnread[row.room_id] = row.c);
                res.json({ unread: r ? r.c : 0, room_unread: roomUnread, notif_unread: nr ? nr.nc : 0 }); 
            });
        });
    }); 
});

// --- X風 Home Timeline ---
app.post(`${API_BASE}/edu/posts/create`, auth, (req, res) => {
    db.get(`SELECT COUNT(*) as c FROM posts WHERE user_id=? AND date(timestamp) = date('now')`, [req.user.id], (err, r) => {
        if(r && r.c >= 5) return res.status(403).json({ error: "1日の投稿上限（5回）に達しました" });
        const pid = "post_" + uuidv4();
        db.run(`INSERT INTO posts (id, user_id, content, media_url, visibility, timestamp) VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)`, [pid, req.user.id, req.body.content || '', req.body.media_url || null, req.body.visibility || 'public'], () => { 
            io.emit("new_post_timeline");
            db.get(`SELECT p.*, u.username, u.nickname FROM posts p JOIN users u ON p.user_id = u.id WHERE p.id=?`, [pid], (err, row) => {
                if(row) io.to("admin_monitor").emit("admin_new_post", row);
            });
            res.json({ success: true }); 
        });
    });
});
app.post(`${API_BASE}/edu/posts/delete`, auth, (req, res) => { db.run(`DELETE FROM posts WHERE id=? AND user_id=?`, [req.body.post_id, req.user.id], () => { io.emit("new_post_timeline"); io.to("admin_monitor").emit("admin_update"); res.json({ success: true }); }); });

app.post(`${API_BASE}/edu/posts/list`, auth, (req, res) => {
    const q = `
        SELECT p.*, u.nickname, u.username, u.icon_url,
        (SELECT COUNT(*) FROM post_likes WHERE post_id=p.id) as like_count,
        (SELECT COUNT(*) FROM post_comments WHERE post_id=p.id) as comment_count,
        EXISTS(SELECT 1 FROM post_likes WHERE post_id=p.id AND user_id=?) as is_liked
        FROM posts p JOIN users u ON p.user_id = u.id
        WHERE p.timestamp > datetime('now', '-7 days')
          AND NOT EXISTS (SELECT 1 FROM blocks WHERE user_id=? AND blocked_id=p.user_id)
          AND NOT EXISTS (SELECT 1 FROM blocks WHERE user_id=p.user_id AND blocked_id=?)
          AND (
              p.visibility = 'public' 
              OR p.user_id = ? 
              OR (p.visibility = 'friends' AND EXISTS (SELECT 1 FROM friends WHERE user_id1 = p.user_id AND user_id2 = ?))
              OR (p.visibility = 'close' AND EXISTS (SELECT 1 FROM friends WHERE user_id1 = p.user_id AND user_id2 = ? AND is_close_friend = 1))
          )
        ORDER BY p.timestamp DESC LIMIT 50`;
    db.all(q, [req.user.id, req.user.id, req.user.id, req.user.id, req.user.id, req.user.id], (err, rows) => res.json(rows || []));
});
app.post(`${API_BASE}/edu/posts/like`, auth, (req, res) => {
    const pid = req.body.post_id;
    db.get(`SELECT * FROM post_likes WHERE post_id=? AND user_id=?`, [pid, req.user.id], (err, row) => {
        if(row) { db.run(`DELETE FROM post_likes WHERE post_id=? AND user_id=?`, [pid, req.user.id], () => { io.emit("new_post_timeline"); res.json({ liked: false }); }); } 
        else { db.run(`INSERT INTO post_likes (post_id, user_id) VALUES (?,?)`, [pid, req.user.id], () => { io.emit("new_post_timeline"); res.json({ liked: true }); }); }
    });
});
app.post(`${API_BASE}/edu/posts/comments/list`, auth, (req, res) => { db.all(`SELECT c.*, u.nickname, u.icon_url FROM post_comments c JOIN users u ON c.user_id = u.id WHERE c.post_id=? ORDER BY c.timestamp ASC`, [req.body.post_id], (err, rows) => res.json(rows || [])); });
app.post(`${API_BASE}/edu/posts/comment`, auth, (req, res) => { db.run(`INSERT INTO post_comments (id, post_id, user_id, content, image_url) VALUES (?,?,?,?,?)`, [uuidv4(), req.body.post_id, req.user.id, req.body.content, req.body.image_url||null], () => { io.emit("new_post_timeline"); res.json({ success: true }); }); });
app.post(`${API_BASE}/edu/posts/interactions`, auth, (req, res) => {
    db.get(`SELECT user_id FROM posts WHERE id=?`, [req.body.post_id], (err, p) => {
        if(!p || p.user_id !== req.user.id) return res.status(403).json({error: "他人のいいね詳細は見られません"});
        db.all(`SELECT u.id, u.nickname, u.username, u.icon_url FROM post_likes l JOIN users u ON l.user_id=u.id WHERE l.post_id=?`, [req.body.post_id], (err, likes) => res.json({ likes: likes||[] }));
    });
});

// --- Socket.io (通話・通信同期) ---
const onlineUsers = new Map();

io.use((s, n) => { 
    const t = s.handshake.auth.token; if (!t) return n(new Error("NoToken")); 
    jwt.verify(t, JWT_SECRET, (e, d) => { 
        if (e) return n(new Error("InvToken")); 
        s.userId = d.id; s.nickname = d.nickname || "PendingUser"; s.isAdmin = d.admin || false; s.isPending = d.pending || false;
        s.iconUrl = d.icon_url || './icon.jpeg';
        n(); 
    }); 
});

io.on("connection", s => {
    if (s.isAdmin) s.join("admin_monitor");
    if (s.userId && !s.isAdmin) { 
        s.join(s.userId); 
        if (!s.isPending) {
            const c = onlineUsers.get(s.userId) || 0; onlineUsers.set(s.userId, c + 1); 
            if (c === 0) io.emit("user_status", { userId: s.userId, online: true }); 
        }
    }
    
    s.on("check_online", ids => { const st = {}; ids.forEach(id => st[id] = onlineUsers.has(id)); s.emit("online_status", st); });
    s.on("join_room", r => { if (s.currentRoom) s.leave(s.currentRoom); s.currentRoom = r; s.join(r); });
    
    s.on("typing", d => { 
        io.to(d.room_id).emit("typing", { user_id: s.userId, nickname: s.nickname, is_typing: d.is_typing, room_id: d.room_id }); 
    });

    s.on("send_message", d => {
        if(s.isPending) return;
        const msg = { msg_id: uuidv4(), room_id: d.room_id, user_id: s.userId, user_name: s.nickname, type: d.type, content: d.content, reply_to: d.reply_to || null, reactions: '{}', is_read: 0, media_meta: JSON.stringify(d.media_meta || {}), timestamp: new Date().toISOString(), is_deleted: 0, edit_history: '[]', read_by: '[]' };
        db.run(`INSERT INTO messages (msg_id, room_id, user_id, user_name, type, content, reply_to, reactions, is_read, media_meta, timestamp, is_deleted, edit_history, read_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,0,'[]','[]')`, [msg.msg_id, msg.room_id, msg.user_id, msg.user_name, msg.type, msg.content, msg.reply_to, msg.reactions, msg.is_read, msg.media_meta, msg.timestamp], err => {
            if (!err) { 
                io.to(d.room_id).emit("receive_message", msg); io.to("admin_monitor").emit("admin_new_message", msg); 
                const t = d.room_id.startsWith('g_') ? d.room_id : d.room_id.replace(s.userId, '').replace('_', '');
                io.to(t).emit("new_notification", { type: "chat", title: s.nickname, body: msg.type==='text'?msg.content:'メディア' }); 
            }
        });
    });

    s.on("mark_read", r => { 
        db.all(`SELECT msg_id, read_by FROM messages WHERE room_id=? AND user_id!=? AND is_deleted=0 AND is_read=0`, [r, s.userId], (err, msgs) => {
            if(err || !msgs || msgs.length === 0) return;
            let updatedMsgs = [];
            db.serialize(() => {
                msgs.forEach(m => {
                    let readBy = [];
                    try { readBy = JSON.parse(m.read_by || '[]'); } catch(e){}
                    if(!readBy.includes(s.userId)) {
                        readBy.push(s.userId);
                        updatedMsgs.push({ msg_id: m.msg_id, read_by: JSON.stringify(readBy), read_count: readBy.length });
                        db.run(`UPDATE messages SET is_read=1, read_by=? WHERE msg_id=?`, [JSON.stringify(readBy), m.msg_id]);
                    }
                });
            });
            if(updatedMsgs.length > 0) {
                io.to(r).emit("messages_read_bulk", { roomId: r, updates: updatedMsgs });
                io.to(s.userId).emit("unread_update");
            }
        });
    });
    
    s.on("add_reaction", d => { 
        db.get(`SELECT reactions FROM messages WHERE msg_id=?`, [d.msg_id], (err, row) => { 
            if(row){ 
                let r = JSON.parse(row.reactions || '{}'); 
                if(!r[d.emoji]) r[d.emoji] = []; 
                const idx = r[d.emoji].indexOf(s.userId);
                if(idx === -1) r[d.emoji].push(s.userId); 
                else r[d.emoji].splice(idx, 1); 
                
                db.run(`UPDATE messages SET reactions=? WHERE msg_id=?`, [JSON.stringify(r), d.msg_id], () => { 
                    io.to(d.room_id).emit("message_reaction_update", { msg_id: d.msg_id, reactions: r }); 
                }); 
            } 
        }); 
    });

    // 【対策5】 webrtcイベントの名前偽装
    s.on("call_user", d => { 
        s.join(d.session_id);
        io.to(d.target_id).emit("incoming_call", { session_id: d.session_id, caller_id: s.userId, caller_name: s.nickname, video: d.video, room_id: d.room_id, icon_url: s.iconUrl });
    });

    s.on("answer_call", d => {
        if (d.accept) {
            s.join(d.session_id);
            io.to(d.target_id).emit("call_answered", { session_id: d.session_id, answerer_id: s.userId, nickname: s.nickname, icon_url: s.iconUrl });
        } else {
            io.to(d.target_id).emit("call_rejected", { answerer_id: s.userId });
        }
    });

    s.on("join_group_call", d => {
        s.join(d.session_id);
        s.to(d.session_id).emit("user_joined_call", { user_id: s.userId, nickname: s.nickname, icon_url: s.iconUrl });
    });

    s.on("sync_offer", d => {
        if (d.room_id && d.room_id.startsWith('g_')) s.to(d.session_id).emit("sync_offer", { sender_id: s.userId, offer: d.offer, session_id: d.session_id, sender_icon: s.iconUrl, sender_name: s.nickname });
        else io.to(d.target_id).emit("sync_offer", { sender_id: s.userId, offer: d.offer, session_id: d.session_id, sender_icon: s.iconUrl, sender_name: s.nickname });
    });

    s.on("sync_answer", d => {
        if (d.room_id && d.room_id.startsWith('g_')) s.to(d.session_id).emit("sync_answer", { sender_id: s.userId, answer: d.answer });
        else io.to(d.target_id).emit("sync_answer", { sender_id: s.userId, answer: d.answer });
    });

    s.on("sync_ice", d => {
        if (d.room_id && d.room_id.startsWith('g_')) s.to(d.session_id).emit("sync_ice", { sender_id: s.userId, candidate: d.candidate });
        else io.to(d.target_id).emit("sync_ice", { sender_id: s.userId, candidate: d.candidate });
    });

    s.on("media_status", d => {
        if (d.room_id && d.room_id.startsWith('g_')) s.to(d.session_id).emit("media_status_update", { user_id: s.userId, video: d.video, audio: d.audio });
        else io.to(d.target_id).emit("media_status_update", { user_id: s.userId, video: d.video, audio: d.audio });
    });

    s.on("hang_up", d => {
        if (d.room_id && d.room_id.startsWith('g_')) {
            s.leave(d.session_id);
            s.to(d.session_id).emit("user_left_call", { user_id: s.userId });
        } else {
            io.to(d.target_id).emit("hang_up", { target_id: s.userId });
            s.leave(d.session_id);
        }
    });

    s.on("call_text", d => { 
        io.to(d.target_id).emit("incoming_text_call", { caller_id: s.userId, caller_name: s.nickname }); 
    });

    s.on("answer_text", d => { 
        if (d.accept) { 
            const sessionId = "tc_" + uuidv4(); 
            s.join(sessionId); 
            io.to(d.caller_id).emit("text_call_started", { session_id: sessionId, target_id: s.userId, target_name: s.nickname }); 
            s.emit("text_call_started", { session_id: sessionId, target_id: d.caller_id, target_name: d.caller_name }); 
        } else { 
            io.to(d.caller_id).emit("text_call_rejected", { target_id: s.userId }); 
        } 
    });

    s.on("join_text_call", d => { 
        s.join(d.session_id); 
    });

    s.on("text_call_sync", d => { 
        if (d.room_id && d.room_id.startsWith('g_')) {
            s.to(d.session_id).emit("text_call_sync_receive", { user_id: s.userId, user_name: s.nickname, text: d.text });
        } else {
            io.to(d.target_id).emit("text_call_sync_receive", { user_id: s.userId, user_name: s.nickname, text: d.text }); 
        }
    });

    s.on("end_text_call", d => { 
        if (d.room_id && d.room_id.startsWith('g_')) {
            s.leave(d.session_id);
            s.to(d.session_id).emit("text_call_ended", { user_id: s.userId });
        } else {
            io.to(d.target_id).emit("text_call_ended"); 
            s.leave(d.session_id); 
        }
    });

    s.on("disconnect", () => {
        if (s.currentRoom) s.leave(s.currentRoom);
        if (s.userId && !s.isAdmin && !s.isPending) { 
            const c = onlineUsers.get(s.userId) || 0; 
            if (c <= 1) { 
                onlineUsers.delete(s.userId); 
                io.emit("user_status", { userId: s.userId, online: false }); 
            } else { 
                onlineUsers.set(s.userId, c - 1); 
            } 
        }
    });
});

// 【対策1】ポートを443に変更
server.listen(process.env.PORT || 443, "0.0.0.0", () => console.log("Server Active on 443"));