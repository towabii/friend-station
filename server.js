const fs = require('fs').promises;
const path = require('path');
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');

const ROOT = __dirname;
const DATA_FILE = path.join(ROOT, 'data.json');
const UPLOAD_DIR = path.join(ROOT, 'uploads');
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '0406';
const BACKEND_PATH = '/edu-backend';
const SYNC_PATH = `${BACKEND_PATH}/edu-sync`;
const PORT = process.env.PORT || 3000;

const db = {
  users: [],
  sessions: [],
  friendRequests: [],
  friendships: [],
  blocks: [],
  groups: [],
  messages: [],
  posts: [],
  comments: [],
  notes: [],
  notifications: [],
  backups: [],
  passwordRequests: []
};

const adminTokens = new Set();
const pendingSockets = new Map();
const userSockets = new Map();

const upload = multer({
  storage: multer.diskStorage({
    destination(req, file, cb) {
      cb(null, UPLOAD_DIR);
    },
    filename(req, file, cb) {
      const ext = path.extname(file.originalname).toLowerCase();
      cb(null, `${Date.now()}-${crypto.randomBytes(6).toString('hex')}${ext}`);
    }
  }),
  limits: { fileSize: 50 * 1024 * 1024 }
});

function safeString(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function now() {
  return new Date().toISOString();
}

function generateId() {
  return crypto.randomUUID();
}

function generateToken() {
  return crypto.randomBytes(24).toString('hex');
}

function generateBackupCode() {
  return Math.floor(100000 + Math.random() * 900000).toString();
}

function getBearer(req) {
  const raw = req.headers.authorization || '';
  if (!raw.startsWith('Bearer ')) return null;
  return raw.slice(7).trim();
}

function sanitizeUser(user) {
  return {
    id: user.id,
    username: user.username,
    nickname: user.nickname,
    full_name: user.full_name,
    first_name: user.first_name,
    last_name: user.last_name,
    school_name: user.school_name,
    grade: user.grade,
    class_num: user.class_num,
    attendance_num: user.attendance_num,
    birthdate: user.birthdate,
    icon_url: user.icon_url || '',
    status_message: user.status_message || '',
    status: user.status,
    is_banned: user.is_banned || 0,
    last_ip: user.last_ip || '',
    created_at: user.created_at || ''
  };
}

function getUserByUsername(username) {
  if (!username) return null;
  return db.users.find(u => u.username.toLowerCase() === username.toLowerCase());
}

function getUserByToken(token) {
  if (!token) return null;
  return db.users.find(u => u.token === token);
}

function getUserByPendingToken(token) {
  if (!token) return null;
  return db.users.find(u => u.pendingToken === token && u.status === 'pending');
}

function getFriendRecord(userId, friendId) {
  return db.friendships.find(f => f.user_id === userId && f.friend_id === friendId);
}

function getFriendList(userId) {
  return db.friendships.filter(f => f.user_id === userId).map(f => {
    const target = db.users.find(u => u.id === f.friend_id);
    if (!target) return null;
    return {
      id: target.id,
      username: target.username,
      nickname: target.nickname,
      icon_url: target.icon_url || '',
      status_message: target.status_message || '',
      is_close_friend: f.is_close_friend || 0
    };
  }).filter(Boolean);
}

function getBlockedList(userId) {
  return db.blocks.filter(rec => rec.blocker_id === userId).map(rec => {
    const target = db.users.find(u => u.id === rec.target_id);
    if (!target) return null;
    return {
      id: target.id,
      username: target.username,
      nickname: target.nickname,
      icon_url: target.icon_url || ''
    };
  }).filter(Boolean);
}

function areFriends(userId, targetId) {
  return !!getFriendRecord(userId, targetId);
}

function hasPendingFriendRequest(senderId, receiverId) {
  return db.friendRequests.some(r => ((r.from_id === senderId && r.to_id === receiverId) || (r.from_id === receiverId && r.to_id === senderId)) && r.status === 'pending');
}

function getPendingRequest(userId, requestId) {
  return db.friendRequests.find(r => r.id === requestId && r.to_id === userId && r.status === 'pending');
}

function saveDb() {
  return fs.writeFile(DATA_FILE, JSON.stringify(db, null, 2), 'utf8');
}

async function loadDb() {
  try {
    const content = await fs.readFile(DATA_FILE, 'utf8');
    const parsed = JSON.parse(content || '{}');
    Object.assign(db, {
      users: parsed.users || [],
      sessions: parsed.sessions || [],
      friendRequests: parsed.friendRequests || [],
      friendships: parsed.friendships || [],
      blocks: parsed.blocks || [],
      groups: parsed.groups || [],
      messages: parsed.messages || [],
      posts: parsed.posts || [],
      comments: parsed.comments || [],
      notes: parsed.notes || [],
      notifications: parsed.notifications || [],
      backups: parsed.backups || [],
      passwordRequests: parsed.passwordRequests || []
    });
  } catch (err) {
    if (err.code !== 'ENOENT') {
      console.error('Unable to load database:', err);
    }
    await saveDb();
  }
}

function addSocketForUser(userId, socket) {
  const set = userSockets.get(userId) || new Set();
  set.add(socket);
  userSockets.set(userId, set);
}

function removeSocketForUser(userId, socket) {
  const set = userSockets.get(userId);
  if (!set) return;
  set.delete(socket);
  if (set.size === 0) userSockets.delete(userId);
}

function emitToAdmin(event, payload) {
  io.sockets.sockets.forEach(socket => {
    if (socket.isAdmin) {
      socket.emit(event, payload);
    }
  });
}

function emitOnlineStatus(socket, uids) {
  const status = {};
  uids.forEach(uid => {
    status[uid] = userSockets.has(uid);
  });
  socket.emit('online_status', status);
}

function broadcastUserStatus(userId, online) {
  io.emit('user_status', { userId, online });
}

function broadcastUnreadCounts(userId) {
  const socketSet = userSockets.get(userId);
  if (!socketSet) return;
  socketSet.forEach(socket => {
    socket.emit('unread_update');
  });
}

function getRoomMembers(roomId) {
  return db.groups.find(g => g.id === roomId)?.members || [];
}

function getAccessibleMessagesForUser(user) {
  const friendIds = getFriendList(user.id).map(f => f.id);
  const groupIds = db.groups.filter(g => g.members.includes(user.id)).map(g => g.id);
  return db.messages.filter(msg => {
    if (msg.room_id.startsWith('g_')) {
      return groupIds.includes(msg.room_id);
    }
    const participants = msg.room_id.split('_');
    return participants.includes(user.id);
  });
}

function getVisiblePostsForUser(user) {
  const friends = getFriendList(user.id).map(f => f.id);
  return db.posts.filter(post => {
    if (post.visibility === 'public') return true;
    if (post.user_id === user.id) return true;
    if (post.visibility === 'friends' && friends.includes(post.user_id)) return true;
    if (post.visibility === 'close') {
      const relation = getFriendRecord(post.user_id, user.id);
      return relation && relation.is_close_friend === 1;
    }
    return false;
  });
}

function getVisibleNotesForUser(user) {
  const friends = getFriendList(user.id).map(f => f.id);
  return db.notes.filter(note => {
    if (note.user_id === user.id) return true;
    if (note.visibility === 'public') return true;
    if (note.visibility === 'friends' && friends.includes(note.user_id)) return true;
    if (note.visibility === 'close') {
      const relation = getFriendRecord(note.user_id, user.id);
      return relation && relation.is_close_friend === 1;
    }
    return false;
  });
}

function computeUnreadCounts(user) {
  const roomUnread = {};
  let directUnread = 0;

  db.messages.forEach(msg => {
    if (msg.room_id.startsWith('g_')) {
      const group = db.groups.find(g => g.id === msg.room_id);
      if (!group || !group.members.includes(user.id)) return;
      if (msg.user_id === user.id) return;
      const readBy = msg.read_by || [];
      if (!readBy.includes(user.id)) {
        roomUnread[msg.room_id] = (roomUnread[msg.room_id] || 0) + 1;
      }
      return;
    }
    const participants = msg.room_id.split('_');
    if (!participants.includes(user.id)) return;
    if (msg.user_id === user.id) return;
    if (!msg.is_read) {
      directUnread += 1;
      roomUnread[msg.room_id] = (roomUnread[msg.room_id] || 0) + 1;
    }
  });

  const notif_unread = db.notifications.filter(n => n.user_id === user.id && !n.is_read).length;
  return { unread: directUnread, notif_unread, room_unread: roomUnread };
}

function createNotification(userId, content, type = 'system', metadata = {}) {
  const note = {
    id: generateId(),
    user_id: userId,
    content: safeString(content),
    type,
    created_at: now(),
    is_read: false,
    ...metadata
  };
  db.notifications.push(note);
  return note;
}

function expandUser(user) {
  if (!user) return null;
  return sanitizeUser(user);
}

function findRoomParticipants(roomId) {
  if (roomId.startsWith('g_')) {
    return getRoomMembers(roomId);
  }
  return roomId.split('_');
}

function isBlocked(blockerId, targetId) {
  return db.blocks.some(rec => rec.blocker_id === blockerId && rec.target_id === targetId);
}

async function initialize() {
  await fs.mkdir(UPLOAD_DIR, { recursive: true });
  await loadDb();
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  path: SYNC_PATH,
  cors: { origin: true, methods: ['GET', 'POST'], credentials: true }
});

app.use(cors({ origin: true, credentials: true }));
app.use(express.json({ limit: '20mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(`${BACKEND_PATH}/uploads`, express.static(UPLOAD_DIR));

const router = express.Router();

router.get('/edu/ping', (req, res) => {
  return res.json({ ok: true, time: now() });
});

router.post('/edu/login', async (req, res) => {
  const username = safeString(req.body.username);
  const password = safeString(req.body.password);
  const user = getUserByUsername(username);
  if (!user) return res.status(404).json({ error: 'ユーザーが見つかりません' });
  if (user.password !== password) return res.status(401).json({ error: 'パスワードが違います' });
  if (user.is_banned === 1) return res.status(403).json({ error: 'このアカウントは停止されています' });
  if (user.status === 'pending') return res.status(403).json({ pendingToken: user.pendingToken });
  const token = generateToken();
  user.token = token;
  user.last_ip = req.ip;
  db.sessions = db.sessions.filter(s => s.user_id !== user.id);
  db.sessions.push({ token, user_id: user.id, created_at: now() });
  await saveDb();
  return res.json({ token, user: expandUser(user) });
});

router.post('/edu/register', async (req, res) => {
  const username = safeString(req.body.username);
  const nickname = safeString(req.body.nickname);
  const first_name = safeString(req.body.first_name);
  const last_name = safeString(req.body.last_name);
  const password = safeString(req.body.password);
  const school_name = safeString(req.body.school_name);
  const grade = safeString(req.body.grade);
  const class_num = safeString(req.body.class_num);
  const attendance_num = safeString(req.body.attendance_num);
  const birthdate = safeString(req.body.birthdate);

  if (!username || !nickname || !first_name || !last_name || !password) {
    return res.status(400).json({ error: '必須項目が不足しています' });
  }
  if (getUserByUsername(username)) {
    return res.status(409).json({ error: 'そのユーザー名は既に使用されています' });
  }
  const id = generateId();
  const pendingToken = generateToken();
  const user = {
    id,
    username,
    nickname,
    first_name,
    last_name,
    full_name: `${first_name} ${last_name}`,
    password,
    school_name,
    grade,
    class_num,
    attendance_num,
    birthdate,
    icon_url: '',
    status_message: '',
    status: 'pending',
    is_banned: 0,
    last_ip: req.ip,
    created_at: now(),
    pendingToken
  };
  db.users.push(user);
  await saveDb();
  return res.json({ pendingToken });
});

router.get('/edu/backup/pull', async (req, res) => {
  const username = safeString(req.query.username);
  const transfer_code = safeString(req.query.transfer_code);
  if (!username || !transfer_code) return res.status(400).json({ error: 'パラメータが不足しています' });
  const backup = db.backups.find(b => b.username === username && b.code === transfer_code);
  if (!backup) return res.status(404).json({ error: 'バックアップが見つかりません' });
  return res.json({ data: backup.data });
});

router.post('/edu/admin/login', async (req, res) => {
  const password = safeString(req.body.password);
  if (password !== ADMIN_PASSWORD) return res.status(401).json({ error: 'パスワードが違います' });
  const token = generateToken();
  adminTokens.add(token);
  return res.json({ token });
});

router.post('/edu/backup/push', authenticate, async (req, res) => {
  const data = req.body.data;
  const user = req.user;
  if (!data) return res.status(400).json({ error: 'データがありません' });
  const parsed = typeof data === 'string' ? JSON.parse(data || '[]') : data;
  const code = generateBackupCode();
  const backup = {
    id: generateId(),
    user_id: user.id,
    username: user.username,
    nickname: user.nickname,
    data: parsed,
    code,
    timestamp: now()
  };
  db.backups.push(backup);
  await saveDb();
  return res.json({ code });
});

router.get('/edu/messages/unread_count', authenticate, (req, res) => {
  return res.json(computeUnreadCounts(req.user));
});

router.get('/edu/sync/messages', authenticate, (req, res) => {
  const since = safeString(req.query.since);
  const fromTs = since ? new Date(since).getTime() : 0;
  const messages = getAccessibleMessagesForUser(req.user)
    .filter(m => new Date(m.timestamp).getTime() > fromTs)
    .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  return res.json({ messages });
});

router.get('/edu/notifications/list', authenticate, (req, res) => {
  const notifications = db.notifications.filter(n => n.user_id === req.user.id);
  return res.json(notifications.sort((a, b) => new Date(b.created_at) - new Date(a.created_at)));
});

router.post('/edu/notifications/read', authenticate, async (req, res) => {
  db.notifications.forEach(n => {
    if (n.user_id === req.user.id) n.is_read = true;
  });
  await saveDb();
  return res.json({ ok: true });
});

router.post('/edu/user/request_password_change', authenticate, async (req, res) => {
  const new_password = safeString(req.body.new_password);
  if (!new_password) return res.status(400).json({ error: '新しいパスワードを指定してください' });
  const request = {
    id: generateId(),
    user_id: req.user.id,
    username: req.user.username,
    full_name: req.user.full_name,
    new_password,
    timestamp: now(),
    status: 'pending'
  };
  db.passwordRequests.push(request);
  await saveDb();
  return res.json({ ok: true });
});

router.post('/edu/user/update_username', authenticate, async (req, res) => {
  const username = safeString(req.body.username);
  if (!username) return res.status(400).json({ error: 'ユーザーIDを指定してください' });
  if (getUserByUsername(username) && getUserByUsername(username).id !== req.user.id) {
    return res.status(409).json({ error: 'そのユーザーIDは既に使用されています' });
  }
  req.user.username = username;
  await saveDb();
  return res.json({ ok: true });
});

router.post('/edu/user/update_nickname', authenticate, async (req, res) => {
  const nickname = safeString(req.body.nickname);
  if (!nickname) return res.status(400).json({ error: 'ニックネームを指定してください' });
  req.user.nickname = nickname;
  await saveDb();
  return res.json({ ok: true });
});

router.post('/edu/user/update_status', authenticate, async (req, res) => {
  const status = safeString(req.body.status);
  req.user.status_message = status;
  await saveDb();
  return res.json({ ok: true });
});

router.post('/edu/user/update_icon', authenticate, upload.single('photo'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: '画像がアップロードされていません' });
  req.user.icon_url = `${BACKEND_PATH}/uploads/${req.file.filename}`;
  await saveDb();
  return res.json({ url: req.user.icon_url });
});

router.get('/edu/user/profile_status', authenticate, (req, res) => {
  const targetId = safeString(req.query.target_id);
  const target = db.users.find(u => u.id === targetId);
  if (!target) return res.status(404).json({ error: 'ユーザーが見つかりません' });
  if (target.id === req.user.id) return res.json({ status: 'self' });
  if (areFriends(req.user.id, targetId)) return res.json({ status: 'friend' });
  const pending = db.friendRequests.find(r => r.from_id === req.user.id && r.to_id === targetId && r.status === 'pending');
  if (pending) return res.json({ status: 'requesting' });
  const incoming = db.friendRequests.find(r => r.from_id === targetId && r.to_id === req.user.id && r.status === 'pending');
  if (incoming) return res.json({ status: 'pending_received', request_id: incoming.id });
  return res.json({ status: 'none' });
});

router.get('/edu/search/messages', authenticate, (req, res) => {
  const q = safeString(req.query.query).toLowerCase();
  const messages = getAccessibleMessagesForUser(req.user)
    .filter(m => m.content && m.content.toLowerCase().includes(q))
    .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
    .slice(0, 50)
    .map(m => ({ user_name: m.user_name, content: m.content, timestamp: m.timestamp }));
  return res.json({ messages });
});

router.get('/edu/search', authenticate, (req, res) => {
  const q = safeString(req.query.query).toLowerCase();
  const users = db.users.filter(u => u.status === 'approved' && !(u.is_banned === 1) && (u.username.toLowerCase().includes(q) || u.nickname.toLowerCase().includes(q)))
    .slice(0, 50)
    .map(u => sanitizeUser(u));
  return res.json({ users });
});

router.get('/edu/friends/list', authenticate, (req, res) => {
  return res.json(getFriendList(req.user.id));
});

router.post('/edu/friends/request', authenticate, async (req, res) => {
  const username = safeString(req.body.username);
  const target = getUserByUsername(username);
  if (!target) return res.status(404).json({ error: 'ユーザーが見つかりません' });
  if (target.id === req.user.id) return res.status(400).json({ error: '自分自身には申請できません' });
  if (areFriends(req.user.id, target.id)) return res.status(400).json({ error: '既にフレンドです' });
  if (hasPendingFriendRequest(req.user.id, target.id)) return res.status(400).json({ error: '申請は既に送信済みです' });
  db.friendRequests.push({ id: generateId(), from_id: req.user.id, to_id: target.id, status: 'pending', timestamp: now() });
  createNotification(target.id, `${req.user.nickname} さんからフレンド申請が届きました`, 'friend_request', { request_id: db.friendRequests[db.friendRequests.length - 1].id });
  await saveDb();
  emitToAdmin('admin_update');
  return res.json({ ok: true });
});

router.post('/edu/friends/accept', authenticate, async (req, res) => {
  const request_id = safeString(req.body.request_id);
  const request = db.friendRequests.find(r => r.id === request_id && r.to_id === req.user.id && r.status === 'pending');
  if (!request) return res.status(404).json({ error: '申請が見つかりません' });
  request.status = 'accepted';
  db.friendships.push({ user_id: req.user.id, friend_id: request.from_id, is_close_friend: 0 });
  db.friendships.push({ user_id: request.from_id, friend_id: req.user.id, is_close_friend: 0 });
  createNotification(request.from_id, `${req.user.nickname} さんがフレンド申請を承認しました`, 'system');
  await saveDb();
  io.emit('friend_update');
  return res.json({ ok: true });
});

router.post('/edu/friends/block', authenticate, async (req, res) => {
  const target_id = safeString(req.body.target_id);
  if (!target_id) return res.status(400).json({ error: '対象が指定されていません' });
  if (!db.blocks.some(b => b.blocker_id === req.user.id && b.target_id === target_id)) {
    db.blocks.push({ blocker_id: req.user.id, target_id, created_at: now() });
  }
  db.friendships = db.friendships.filter(f => !(f.user_id === req.user.id && f.friend_id === target_id) && !(f.user_id === target_id && f.friend_id === req.user.id));
  await saveDb();
  return res.json({ ok: true });
});

router.post('/edu/friends/unblock', authenticate, async (req, res) => {
  const target_id = safeString(req.body.target_id);
  db.blocks = db.blocks.filter(b => !(b.blocker_id === req.user.id && b.target_id === target_id));
  await saveDb();
  return res.json({ ok: true });
});

router.post('/edu/friends/block_list', authenticate, async (req, res) => {
  return res.json(getBlockedList(req.user.id));
});

router.post('/edu/friends/set_close_bulk', authenticate, async (req, res) => {
  const friend_ids = Array.isArray(req.body.friend_ids) ? req.body.friend_ids : [];
  db.friendships.forEach(f => {
    if (f.user_id === req.user.id) {
      f.is_close_friend = friend_ids.includes(f.friend_id) ? 1 : 0;
    }
  });
  await saveDb();
  return res.json({ ok: true });
});

router.get('/edu/groups/list', authenticate, (req, res) => {
  const groups = db.groups.filter(g => g.members.includes(req.user.id)).map(g => ({ id: g.id, name: g.name, members: g.members }));
  return res.json(groups);
});

router.post('/edu/groups/create', authenticate, async (req, res) => {
  const name = safeString(req.body.name);
  const members = Array.isArray(req.body.members) ? req.body.members.filter(Boolean) : [];
  if (!name) return res.status(400).json({ error: 'グループ名を指定してください' });
  const uniqueMembers = Array.from(new Set([req.user.id, ...members]));
  const group = { id: `g_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`, name, members: uniqueMembers, creator_id: req.user.id, created_at: now() };
  db.groups.push(group);
  await saveDb();
  io.emit('friend_update');
  return res.json({ ok: true, group });
});

router.post('/edu/groups/leave', authenticate, async (req, res) => {
  const group_id = safeString(req.body.group_id);
  const group = db.groups.find(g => g.id === group_id);
  if (!group) return res.status(404).json({ error: 'グループが見つかりません' });
  group.members = group.members.filter(id => id !== req.user.id);
  if (group.members.length === 0) {
    db.groups = db.groups.filter(g => g.id !== group_id);
  }
  await saveDb();
  io.emit('friend_update');
  return res.json({ ok: true });
});

router.post('/edu/groups/add_members', authenticate, async (req, res) => {
  const group_id = safeString(req.body.group_id);
  const members = Array.isArray(req.body.members) ? req.body.members.filter(Boolean) : [];
  const group = db.groups.find(g => g.id === group_id);
  if (!group) return res.status(404).json({ error: 'グループが見つかりません' });
  if (!group.members.includes(req.user.id)) return res.status(403).json({ error: '権限がありません' });
  group.members = Array.from(new Set([...group.members, ...members]));
  await saveDb();
  io.emit('friend_update');
  return res.json({ ok: true });
});

router.get('/edu/posts/list', authenticate, (req, res) => {
  const posts = getVisiblePostsForUser(req.user).map(post => ({
    ...post,
    like_count: post.likes?.length || 0,
    comment_count: db.comments.filter(c => c.post_id === post.id).length,
    is_liked: (post.likes || []).includes(req.user.id),
    icon_url: post.icon_url || '',
    media_url: post.media_url || ''
  })).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  return res.json(posts);
});

router.post('/edu/posts/create', authenticate, async (req, res) => {
  const content = safeString(req.body.content);
  const media_url = req.body.media_url ? safeString(req.body.media_url) : '';
  const visibility = safeString(req.body.visibility) || 'public';
  const post = {
    id: generateId(),
    user_id: req.user.id,
    username: req.user.username,
    nickname: req.user.nickname,
    icon_url: req.user.icon_url || '',
    content,
    media_url,
    visibility,
    timestamp: now(),
    likes: [],
    comment_count: 0
  };
  db.posts.push(post);
  await saveDb();
  io.emit('new_post_timeline');
  emitToAdmin('admin_new_post', post);
  return res.json({ ok: true });
});

router.post('/edu/posts/delete', authenticate, async (req, res) => {
  const post_id = safeString(req.body.post_id);
  const post = db.posts.find(p => p.id === post_id);
  if (!post) return res.status(404).json({ error: '投稿が見つかりません' });
  if (post.user_id !== req.user.id && !adminTokens.has(getBearer(req))) return res.status(403).json({ error: '権限がありません' });
  db.posts = db.posts.filter(p => p.id !== post_id);
  db.comments = db.comments.filter(c => c.post_id !== post_id);
  await saveDb();
  io.emit('new_post_timeline');
  emitToAdmin('admin_update');
  return res.json({ ok: true });
});

router.get('/edu/posts/comments/list', authenticate, (req, res) => {
  const post_id = safeString(req.query.post_id);
  if (!post_id) return res.status(400).json({ error: 'post_idが指定されていません' });
  const comments = db.comments.filter(c => c.post_id === post_id).map(c => ({
    ...c,
    icon_url: c.icon_url || ''
  }));
  return res.json(comments);
});

router.post('/edu/posts/comment', authenticate, async (req, res) => {
  const post_id = safeString(req.body.post_id);
  const content = safeString(req.body.content);
  const image_url = req.body.image_url ? safeString(req.body.image_url) : '';
  const post = db.posts.find(p => p.id === post_id);
  if (!post) return res.status(404).json({ error: '投稿が見つかりません' });
  const comment = {
    id: generateId(),
    post_id,
    user_id: req.user.id,
    username: req.user.username,
    nickname: req.user.nickname,
    icon_url: req.user.icon_url || '',
    content,
    image_url,
    created_at: now()
  };
  db.comments.push(comment);
  post.comment_count = db.comments.filter(c => c.post_id === post_id).length;
  createNotification(post.user_id, `${req.user.nickname} さんがあなたの投稿にコメントしました`, 'comment');
  await saveDb();
  return res.json({ ok: true });
});

router.get('/edu/posts/interactions', authenticate, (req, res) => {
  const post_id = safeString(req.query.post_id);
  const post = db.posts.find(p => p.id === post_id);
  if (!post) return res.status(404).json({ error: '投稿が見つかりません' });
  const likes = (post.likes || []).map(uid => {
    const user = db.users.find(u => u.id === uid);
    return user ? sanitizeUser(user) : null;
  }).filter(Boolean);
  return res.json({ likes });
});

router.post('/edu/posts/like', authenticate, async (req, res) => {
  const post_id = safeString(req.body.post_id);
  const post = db.posts.find(p => p.id === post_id);
  if (!post) return res.status(404).json({ error: '投稿が見つかりません' });
  post.likes = post.likes || [];
  if (post.likes.includes(req.user.id)) {
    post.likes = post.likes.filter(uid => uid !== req.user.id);
  } else {
    post.likes.push(req.user.id);
    if (post.user_id !== req.user.id) {
      createNotification(post.user_id, `${req.user.nickname} さんがあなたの投稿にいいねしました`, 'like');
    }
  }
  await saveDb();
  io.emit('new_notification');
  return res.json({ ok: true, liked: post.likes.includes(req.user.id) });
});

router.post('/edu/notes/post', authenticate, async (req, res) => {
  const content = safeString(req.body.content);
  const visibility = safeString(req.body.visibility) || 'public';
  const note = db.notes.find(n => n.user_id === req.user.id);
  if (!content || visibility === 'deleted') {
    db.notes = db.notes.filter(n => n.user_id !== req.user.id);
    await saveDb();
    io.emit('notes_update');
    return res.json({ ok: true });
  }
  if (note) {
    note.content = content;
    note.visibility = visibility;
    note.created_at = now();
  } else {
    db.notes.push({
      id: generateId(),
      user_id: req.user.id,
      username: req.user.username,
      nickname: req.user.nickname,
      icon_url: req.user.icon_url || '',
      content,
      visibility,
      created_at: now(),
      likes: []
    });
  }
  await saveDb();
  io.emit('notes_update');
  return res.json({ ok: true });
});

router.get('/edu/notes/list', authenticate, (req, res) => {
  const notes = getVisibleNotesForUser(req.user).map(note => ({
    ...note,
    like_count: note.likes?.length || 0,
    is_liked: (note.likes || []).includes(req.user.id),
    icon_url: note.icon_url || ''
  })).sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
  return res.json(notes);
});

router.post('/edu/notes/like', authenticate, async (req, res) => {
  const note_user_id = safeString(req.body.note_user_id);
  const note = db.notes.find(n => n.user_id === note_user_id);
  if (!note) return res.status(404).json({ error: 'ノートが見つかりません' });
  note.likes = note.likes || [];
  let liked;
  if (note.likes.includes(req.user.id)) {
    note.likes = note.likes.filter(uid => uid !== req.user.id);
    liked = false;
  } else {
    note.likes.push(req.user.id);
    liked = true;
    if (note.user_id !== req.user.id) {
      createNotification(note.user_id, `${req.user.nickname} さんがあなたのノートにいいねしました`, 'like');
    }
  }
  await saveDb();
  return res.json({ ok: true, liked, like_count: note.likes.length });
});

router.get('/edu/notes/interactions', authenticate, (req, res) => {
  const note_user_id = safeString(req.query.note_user_id);
  const note = db.notes.find(n => n.user_id === note_user_id);
  if (!note) return res.status(404).json({ error: 'ノートが見つかりません' });
  const likes = (note.likes || []).map(uid => sanitizeUser(db.users.find(u => u.id === uid))).filter(Boolean);
  return res.json({ likes });
});

router.get('/edu/admin/messages/list', adminAuth, (req, res) => {
  const list = db.messages.map(m => ({
    msg_id: m.msg_id,
    room_id: m.room_id,
    user_id: m.user_id,
    user_name: m.user_name,
    content: m.content,
    timestamp: m.timestamp,
    is_deleted: m.is_deleted || 0
  }));
  return res.json(list.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp)));
});

router.post('/edu/admin/messages/delete', adminAuth, async (req, res) => {
  const id = safeString(req.body.id);
  const message = db.messages.find(m => m.msg_id === id);
  if (!message) return res.status(404).json({ error: 'メッセージが見つかりません' });
  message.is_deleted = 1;
  await saveDb();
  io.to(message.room_id).emit('message_updated', { msg_id: message.msg_id, content: message.content, is_deleted: 1, edit_history: message.edit_history || '[]' });
  emitToAdmin('admin_update');
  return res.json({ ok: true });
});

router.get('/edu/admin/backups/list', adminAuth, (req, res) => {
  return res.json(db.backups.map(b => ({ id: b.id, username: b.username, nickname: b.nickname, timestamp: b.timestamp })));
});

router.post('/edu/admin/backups/view', adminAuth, (req, res) => {
  const id = safeString(req.body.id);
  const backup = db.backups.find(b => b.id === id);
  if (!backup) return res.status(404).json({ error: 'バックアップが見つかりません' });
  return res.json({ data: backup.data });
});

router.get('/edu/admin/posts/list', adminAuth, (req, res) => {
  return res.json(db.posts.map(p => ({
    ...p,
    like_count: p.likes?.length || 0,
    icon_url: p.icon_url || '',
    media_url: p.media_url || ''
  })).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp)));
});

router.get('/edu/admin/notes/list', adminAuth, (req, res) => {
  return res.json(db.notes.map(n => ({
    ...n,
    like_count: n.likes?.length || 0,
    icon_url: n.icon_url || ''
  })).sort((a, b) => new Date(b.created_at) - new Date(a.created_at)));
});

router.post('/edu/admin/posts/delete', adminAuth, async (req, res) => {
  const post_id = safeString(req.body.post_id || req.body.id);
  if (!post_id) return res.status(400).json({ error: 'post_idが指定されていません' });
  db.posts = db.posts.filter(p => p.id !== post_id);
  db.comments = db.comments.filter(c => c.post_id !== post_id);
  await saveDb();
  io.emit('new_post_timeline');
  emitToAdmin('admin_update');
  return res.json({ ok: true });
});

router.get('/edu/admin/likes/details', adminAuth, (req, res) => {
  const target_id = safeString(req.query.target_id);
  const type = safeString(req.query.type);
  if (type === 'post') {
    const post = db.posts.find(p => p.id === target_id);
    if (!post) return res.status(404).json({ error: '投稿が見つかりません' });
    const likes = (post.likes || []).map(uid => sanitizeUser(db.users.find(u => u.id === uid))).filter(Boolean);
    return res.json(likes);
  }
  if (type === 'note') {
    const note = db.notes.find(n => n.user_id === target_id);
    if (!note) return res.status(404).json({ error: 'ノートが見つかりません' });
    const likes = (note.likes || []).map(uid => sanitizeUser(db.users.find(u => u.id === uid))).filter(Boolean);
    return res.json(likes);
  }
  return res.status(400).json({ error: 'typeが不正です' });
});

router.get('/edu/admin/users/list', adminAuth, (req, res) => {
  return res.json(db.users.map(u => sanitizeUser(u)));
});

router.post('/edu/admin/users/:action', adminAuth, async (req, res) => {
  const action = req.params.action;
  const id = safeString(req.body.id);
  const user = db.users.find(u => u.id === id);
  if (!user) return res.status(404).json({ error: 'ユーザーが見つかりません' });

  if (action === 'approve') {
    const pendingToken = user.pendingToken;
    user.status = 'approved';
    user.pendingToken = null;
    const token = generateToken();
    user.token = token;
    db.sessions.push({ token, user_id: user.id, created_at: now() });
    if (pendingToken && pendingSockets.has(pendingToken)) {
      const socket = pendingSockets.get(pendingToken);
      socket.emit('registration_approved', { token, user: sanitizeUser(user) });
      pendingSockets.delete(pendingToken);
    }
    await saveDb();
    emitToAdmin('admin_update');
    io.emit('friend_update');
    return res.json({ ok: true });
  }
  if (action === 'ban') {
    user.is_banned = 1;
    await saveDb();
    emitToAdmin('admin_update');
    return res.json({ ok: true });
  }
  if (action === 'unban') {
    user.is_banned = 0;
    await saveDb();
    emitToAdmin('admin_update');
    return res.json({ ok: true });
  }
  if (action === 'delete') {
    db.users = db.users.filter(u => u.id !== id);
    db.sessions = db.sessions.filter(s => s.user_id !== id);
    db.friendRequests = db.friendRequests.filter(r => r.from_id !== id && r.to_id !== id);
    db.friendships = db.friendships.filter(f => f.user_id !== id && f.friend_id !== id);
    db.blocks = db.blocks.filter(b => b.blocker_id !== id && b.target_id !== id);
    db.groups = db.groups.map(g => ({ ...g, members: g.members.filter(mid => mid !== id) })).filter(g => g.members.length > 0);
    db.messages = db.messages.filter(m => m.user_id !== id);
    db.posts = db.posts.filter(p => p.user_id !== id);
    db.comments = db.comments.filter(c => c.user_id !== id);
    db.notes = db.notes.filter(n => n.user_id !== id);
    db.notifications = db.notifications.filter(n => n.user_id !== id);
    db.passwordRequests = db.passwordRequests.filter(r => r.user_id !== id);
    await saveDb();
    emitToAdmin('admin_update');
    return res.json({ ok: true });
  }
  return res.status(400).json({ error: '不正な操作です' });
});

router.get('/edu/admin/password_requests/list', adminAuth, (req, res) => {
  return res.json(db.passwordRequests.map(r => ({ id: r.id, username: r.username, full_name: r.full_name, timestamp: r.timestamp })));
});

router.post('/edu/admin/password_requests/:action', adminAuth, async (req, res) => {
  const action = req.params.action;
  const id = safeString(req.body.id);
  const request = db.passwordRequests.find(r => r.id === id);
  if (!request) return res.status(404).json({ error: '申請が見つかりません' });
  const user = db.users.find(u => u.id === request.user_id);
  if (action === 'approve') {
    if (user) user.password = request.new_password;
    db.passwordRequests = db.passwordRequests.filter(r => r.id !== id);
    await saveDb();
    return res.json({ ok: true });
  }
  if (action === 'reject') {
    db.passwordRequests = db.passwordRequests.filter(r => r.id !== id);
    await saveDb();
    return res.json({ ok: true });
  }
  return res.status(400).json({ error: '不正な操作です' });
});

router.post('/edu/admin/notifications/send', adminAuth, async (req, res) => {
  const content = safeString(req.body.content);
  if (!content) return res.status(400).json({ error: '内容を指定してください' });
  db.users.filter(u => u.status === 'approved' && u.is_banned === 0).forEach(user => {
    createNotification(user.id, content, 'system');
  });
  await saveDb();
  io.emit('new_notification');
  io.emit('unread_update');
  return res.json({ ok: true });
});

router.post('/edu/admin/database/cleanup', adminAuth, async (req, res) => {
  db.messages = [];
  const sevenDays = Date.now() - 7 * 24 * 60 * 60 * 1000;
  db.posts = db.posts.filter(post => new Date(post.timestamp).getTime() >= sevenDays);
  db.comments = db.comments.filter(comment => db.posts.some(post => post.id === comment.post_id));
  await saveDb();
  emitToAdmin('admin_update');
  return res.json({ ok: true });
});

router.post('/edu/messages/delete_msg', authenticate, async (req, res) => {
  const msg_id = safeString(req.body.msg_id);
  const message = db.messages.find(m => m.msg_id === msg_id);
  if (!message) return res.status(404).json({ error: 'メッセージが見つかりません' });
  if (message.user_id !== req.user.id) return res.status(403).json({ error: '権限がありません' });
  message.is_deleted = 1;
  await saveDb();
  io.to(message.room_id).emit('message_updated', { msg_id: message.msg_id, content: message.content, is_deleted: 1, edit_history: message.edit_history || '[]' });
  return res.json({ ok: true });
});

router.post('/edu/messages/edit_msg', authenticate, async (req, res) => {
  const msg_id = safeString(req.body.msg_id);
  const content = safeString(req.body.content);
  const message = db.messages.find(m => m.msg_id === msg_id);
  if (!message) return res.status(404).json({ error: 'メッセージが見つかりません' });
  if (message.user_id !== req.user.id) return res.status(403).json({ error: '権限がありません' });
  const history = JSON.parse(message.edit_history || '[]');
  history.push({ content: message.content, edited_at: message.edited_at || message.timestamp });
  message.content = content;
  message.edit_history = JSON.stringify(history);
  message.edited_at = now();
  await saveDb();
  io.to(message.room_id).emit('message_updated', { msg_id: message.msg_id, content: message.content, is_deleted: message.is_deleted || 0, edit_history: message.edit_history });
  return res.json({ ok: true });
});

router.post('/edu/upload', authenticate, upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'ファイルがありません' });
  return res.json({ url: `${BACKEND_PATH}/uploads/${req.file.filename}` });
});

app.use(BACKEND_PATH, router);

function authenticate(req, res, next) {
  const token = getBearer(req);
  if (!token) return res.status(401).json({ error: 'NoToken' });
  const user = getUserByToken(token);
  if (!user || user.status !== 'approved' || user.is_banned === 1) return res.status(401).json({ error: 'NoToken' });
  req.user = user;
  next();
}

function adminAuth(req, res, next) {
  const token = getBearer(req);
  if (!token || !adminTokens.has(token)) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

io.on('connection', socket => {
  const token = socket.handshake.auth?.token;
  if (!token) {
    socket.isAdmin = true;
    return;
  }
  if (adminTokens.has(token)) {
    socket.isAdmin = true;
    return;
  }
  const pendingUser = getUserByPendingToken(token);
  const authUser = getUserByToken(token);
  if (pendingUser) {
    socket.pendingUser = pendingUser;
    pendingSockets.set(token, socket);
    return;
  }
  if (!authUser || authUser.status !== 'approved' || authUser.is_banned === 1) {
    socket.emit('connect_error', new Error('NoToken')); 
    socket.disconnect(true);
    return;
  }

  socket.userId = authUser.id;
  addSocketForUser(authUser.id, socket);
  broadcastUserStatus(authUser.id, true);

  socket.on('disconnect', () => {
    if (socket.userId) {
      removeSocketForUser(socket.userId, socket);
      if (!userSockets.has(socket.userId)) {
        broadcastUserStatus(socket.userId, false);
      }
    }
    if (socket.pendingUser && socket.pendingUser.pendingToken) {
      pendingSockets.delete(socket.pendingUser.pendingToken);
    }
  });

  socket.on('check_online', uids => {
    if (!Array.isArray(uids)) return;
    emitOnlineStatus(socket, uids);
  });

  socket.on('join_room', roomId => {
    if (!roomId) return;
    socket.join(roomId);
  });

  socket.on('send_message', async message => {
    if (!socket.userId) return;
    const room_id = safeString(message.room_id);
    const type = safeString(message.type) || 'text';
    const content = safeString(message.content);
    if (!room_id) return;
    const msg = {
      msg_id: generateId(),
      room_id,
      user_id: socket.userId,
      user_name: authUser.nickname,
      content,
      type,
      reply_to: message.reply_to || null,
      timestamp: now(),
      is_deleted: 0,
      is_read: false,
      read_by: [],
      reactions: JSON.stringify({})
    };
    db.messages.push(msg);
    await saveDb();
    io.to(room_id).emit('receive_message', msg);
    emitToAdmin('admin_new_message', msg);
  });

  socket.on('mark_read', async roomId => {
    if (!socket.userId || !roomId) return;
    const isGroup = roomId.startsWith('g_');
    const updates = [];
    if (isGroup) {
      db.messages.forEach(msg => {
        if (msg.room_id !== roomId) return;
        if (msg.user_id === socket.userId) return;
        const read_by = Array.isArray(msg.read_by) ? msg.read_by : [];
        if (!read_by.includes(socket.userId)) {
          read_by.push(socket.userId);
          msg.read_by = read_by;
          updates.push({ msg_id: msg.msg_id, read_count: read_by.length });
        }
      });
      await saveDb();
      io.to(roomId).emit('messages_read_bulk', { roomId, updates });
    } else {
      db.messages.forEach(msg => {
        if (msg.room_id !== roomId) return;
        if (msg.user_id === socket.userId) return;
        if (!msg.is_read) {
          msg.is_read = true;
        }
      });
      await saveDb();
      io.to(roomId).emit('messages_read', { roomId });
    }
  });

  socket.on('add_reaction', async data => {
    if (!socket.userId) return;
    const msg_id = safeString(data.msg_id);
    const room_id = safeString(data.room_id);
    const emoji = safeString(data.emoji);
    const message = db.messages.find(m => m.msg_id === msg_id);
    if (!message) return;
    let reactions = {};
    try { reactions = JSON.parse(message.reactions || '{}'); } catch (e) { reactions = {}; }
    reactions[emoji] = reactions[emoji] || [];
    if (!reactions[emoji].includes(socket.userId)) {
      reactions[emoji].push(socket.userId);
    }
    message.reactions = JSON.stringify(reactions);
    await saveDb();
    io.to(room_id).emit('message_reaction_update', { msg_id, reactions });
  });

  socket.on('typing', data => {
    if (!socket.userId) return;
    const room_id = safeString(data.room_id);
    const is_typing = !!data.is_typing;
    if (!room_id) return;
    socket.to(room_id).emit('typing', { room_id, user_id: socket.userId, nickname: authUser.nickname, is_typing });
  });

  socket.on('call_user', data => {
    if (!socket.userId) return;
    const target_id = safeString(data.target_id);
    const room_id = safeString(data.room_id);
    const video = !!data.video;
    const session_id = safeString(data.session_id);
    const sockets = userSockets.get(target_id);
    if (!sockets || sockets.size === 0) {
      socket.emit('call_error', { message: '相手がオフラインです' });
      return;
    }
    sockets.forEach(s => {
      s.emit('incoming_call', { caller_id: socket.userId, caller_name: authUser.nickname, icon_url: authUser.icon_url || '', video, session_id, room_id });
    });
  });

  socket.on('answer_call', data => {
    if (!socket.userId) return;
    const target_id = safeString(data.target_id);
    const session_id = safeString(data.session_id);
    const accept = !!data.accept;
    const sockets = userSockets.get(target_id);
    if (!sockets) return;
    sockets.forEach(s => {
      s.emit(accept ? 'call_answered' : 'call_rejected', {
        session_id,
        caller_id: socket.userId,
        answerer_id: socket.userId,
        icon_url: authUser.icon_url || '',
        nickname: authUser.nickname,
        accept
      });
    });
  });

  socket.on('webrtc_offer', data => {
    const target_id = safeString(data.target_id);
    const sockets = userSockets.get(target_id);
    if (!sockets) return;
    sockets.forEach(s => {
      s.emit('webrtc_offer', { ...data, sender_id: socket.userId, sender_icon: authUser.icon_url || '', sender_name: authUser.nickname });
    });
  });

  socket.on('webrtc_answer', data => {
    const target_id = safeString(data.target_id);
    const sockets = userSockets.get(target_id);
    if (!sockets) return;
    sockets.forEach(s => {
      s.emit('webrtc_answer', { ...data, sender_id: socket.userId });
    });
  });

  socket.on('webrtc_ice', data => {
    const target_id = safeString(data.target_id);
    const sockets = userSockets.get(target_id);
    if (!sockets) return;
    sockets.forEach(s => {
      s.emit('webrtc_ice', { ...data, sender_id: socket.userId });
    });
  });

  socket.on('join_group_call', data => {
    const room_id = safeString(data.room_id);
    const session_id = safeString(data.session_id);
    if (!room_id || !session_id) return;
    socket.join(`group_call_${session_id}`);
    socket.to(`group_call_${session_id}`).emit('user_joined_call', { user_id: socket.userId, icon_url: authUser.icon_url || '', nickname: authUser.nickname });
  });

  socket.on('hang_up', data => {
    if (!socket.userId) return;
    const target_id = safeString(data.target_id);
    const room_id = safeString(data.room_id);
    const session_id = safeString(data.session_id);
    if (target_id) {
      const sockets = userSockets.get(target_id);
      if (sockets) sockets.forEach(s => s.emit('hang_up', { session_id, user_id: socket.userId }));
    }
    if (room_id && session_id) {
      io.to(`group_call_${session_id}`).emit('hang_up', { room_id, session_id, user_id: socket.userId });
    }
  });

  socket.on('call_text', data => {
    if (!socket.userId) return;
    const target_id = safeString(data.target_id);
    const sockets = userSockets.get(target_id);
    if (!sockets || sockets.size === 0) {
      socket.emit('call_error', { message: '相手がオフラインです' });
      return;
    }
    sockets.forEach(s => {
      s.emit('incoming_text_call', { caller_id: socket.userId, caller_name: authUser.nickname, icon_url: authUser.icon_url || '' });
    });
  });

  socket.on('answer_text', data => {
    const target_id = safeString(data.target_id || data.caller_id);
    const accept = !!data.accept;
    const caller_name = safeString(data.caller_name);
    const sockets = userSockets.get(target_id);
    if (!sockets) return;
    const session_id = `tc_${Date.now()}`;
    sockets.forEach(s => {
      if (accept) {
        s.emit('text_call_started', { session_id, target_name: authUser.nickname });
      } else {
        s.emit('text_call_rejected');
      }
    });
    if (accept) {
      socket.emit('text_call_started', { session_id, target_name: caller_name || '通話相手' });
    }
  });

  socket.on('join_text_call', data => {
    const session_id = safeString(data.session_id);
    if (!session_id) return;
    socket.join(`text_call_${session_id}`);
  });

  socket.on('text_call_sync', data => {
    const session_id = safeString(data.session_id);
    const room_id = safeString(data.room_id);
    const target_id = safeString(data.target_id);
    const text = safeString(data.text);
    if (room_id) {
      socket.to(`text_call_${session_id}`).emit('text_call_sync_receive', { user_id: socket.userId, user_name: authUser.nickname, text });
    } else if (target_id) {
      const sockets = userSockets.get(target_id);
      if (!sockets) return;
      sockets.forEach(s => s.emit('text_call_sync_receive', { user_id: socket.userId, user_name: authUser.nickname, text }));
    }
  });

  socket.on('end_text_call', data => {
    const target_id = safeString(data.target_id);
    const room_id = safeString(data.room_id);
    const session_id = safeString(data.session_id);
    if (target_id) {
      const sockets = userSockets.get(target_id);
      if (sockets) sockets.forEach(s => s.emit('text_call_ended', { user_id: socket.userId }));
    }
    if (room_id && session_id) {
      io.to(`text_call_${session_id}`).emit('text_call_ended', { room_id, session_id, user_id: socket.userId });
    }
  });

  socket.on('media_status', data => {
    const payload = {
      user_id: socket.userId,
      video: !!data.video,
      audio: !!data.audio
    };
    if (data.room_id) payload.room_id = data.room_id;
    if (data.target_id) payload.target_id = data.target_id;
    if (payload.room_id) {
      socket.to(payload.room_id).emit('media_status_update', payload);
    } else if (payload.target_id) {
      const sockets = userSockets.get(payload.target_id);
      if (sockets) sockets.forEach(s => s.emit('media_status_update', payload));
    }
  });
});

initialize().then(() => {
  server.listen(PORT, () => {
    console.log(`Friend Station backend running on http://localhost:${PORT}${BACKEND_PATH}`);
  });
}).catch(err => {
  console.error('Failed to initialize backend:', err);
  process.exit(1);
});
