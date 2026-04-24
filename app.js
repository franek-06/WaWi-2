'use strict';

/* â”€â”€ Firebase Konfiguration & Initialisierung â”€â”€ */
const firebaseConfig = {
  apiKey           : "AIzaSyB1CN9Wgmk7xYaDjxtt85j61rI4kjCCce0",
  authDomain       : "wawi-dfe57.firebaseapp.com",
  projectId        : "wawi-dfe57",
  storageBucket    : "wawi-dfe57.firebasestorage.app",
  messagingSenderId: "311289421867",
  appId            : "1:311289421867:web:e004d05334183f9b8d4a6e",
  measurementId    : "G-SNDZ2X7W92",
};
firebase.initializeApp(firebaseConfig);
const _auth           = firebase.auth();
const _googleProvider = new firebase.auth.GoogleAuthProvider();
const PUBLIC_QR_CONFIG = (() => {
  const cleanOrigin = String(window.location.origin ?? '').trim().replace(/\/+$/, '');
  const isUsableCurrentOrigin =
    /^https?:\/\//i.test(cleanOrigin) &&
    !/^https?:\/\/(?:localhost|127\.0\.0\.1)(?::\d+)?$/i.test(cleanOrigin) &&
    cleanOrigin !== 'null';
  const cleanPathname = String(window.location.pathname ?? '').trim() || '/';
  const appPathname = cleanPathname.endsWith('.html')
    ? cleanPathname.replace(/[^/]+$/, '')
    : cleanPathname;
  const normalizedAppPath = appPathname.endsWith('/') ? appPathname : `${appPathname}/`;
  const currentAppBaseUrl = isUsableCurrentOrigin
    ? `${cleanOrigin}${normalizedAppPath}`
    : '';
  const fallbackOrigin = `https://${firebaseConfig.authDomain}`.replace(/\/+$/, '');
  return {
    baseUrl: `${isUsableCurrentOrigin ? currentAppBaseUrl : `${fallbackOrigin}/`}#/a/`,
  };
})();
const INITIAL_PUBLIC_QR_ROUTE = (() => {
  try {
    const base = new URL(PUBLIC_QR_CONFIG.baseUrl);
    const baseHashPath = String(base.hash ?? '').replace(/^#/, '');
    const currentHashPath = String(window.location.hash ?? '').replace(/^#/, '');
    return window.location.origin === base.origin
      && !!baseHashPath
      && currentHashPath.startsWith(baseHashPath);
  } catch (_) {
    return false;
  }
})();

/* ============================================================
 1. WAWIDB â€” Datenbankschicht (Firestore)
============================================================ */
class WawiDB {
  constructor() {
    this._articles  = [];
    this._groups    = [];
    this._orders    = [];
    this._roles     = [];
    this._users     = [];
    this._db        = firebase.firestore();
    this._listeners = new Set();
    this._notifyQueued = false;
    this._ready     = this._load();
  }

  _notifyChange() {
    if (this._notifyQueued) return;
    this._notifyQueued = true;
    requestAnimationFrame(() => {
      this._notifyQueued = false;
      this._listeners.forEach(listener => {
        try {
          listener();
        } catch (e) {
          console.error('Realtime listener failed:', e);
        }
      });
    });
  }

  onChange(listener) {
    this._listeners.add(listener);
    return () => this._listeners.delete(listener);
  }

  _load() {
    return new Promise((resolve, reject) => {
      let pendingInitialSnapshots = 5;
      let settled = false;

      const handleSnapshot = (type, snap) => {
        if (type === 'articles') this._articles = snap.docs.map(d => d.data());
        if (type === 'groups')   this._groups   = snap.docs.map(d => d.data());
        if (type === 'orders')   this._orders   = snap.docs.map(d => d.data());
        if (type === 'roles')    this._roles    = snap.docs.map(d => d.data());
        if (type === 'users')    this._users    = snap.docs.map(d => d.data());

        if (pendingInitialSnapshots > 0) {
          pendingInitialSnapshots--;
          if (pendingInitialSnapshots === 0) {
            settled = true;
            resolve();
          }
          return;
        }

        this._notifyChange();
      };

      const handleError = err => {
        console.error('Firestore realtime load failed:', err);
        if (!settled) {
          settled = true;
          reject(err);
        }
      };

      this._db.collection('articles').onSnapshot(
        snap => handleSnapshot('articles', snap),
        handleError
      );
      this._db.collection('groups').onSnapshot(
        snap => handleSnapshot('groups', snap),
        handleError
      );
      this._db.collection('orders').onSnapshot(
        snap => handleSnapshot('orders', snap),
        handleError
      );
      this._db.collection('roles').onSnapshot(
        snap => handleSnapshot('roles', snap),
        handleError
      );
      this._db.collection('users').onSnapshot(
        snap => handleSnapshot('users', snap),
        handleError
      );
    });
  }

  _fsSet(col, id, data) {
    return this._db.collection(col).doc(id).set(data)
      .catch(e => console.error(`Firestore _fsSet ${col}/${id}:`, e));
  }

  _fsDelete(col, id) {
    return this._db.collection(col).doc(id).delete()
      .catch(e => console.error(`Firestore _fsDelete ${col}/${id}:`, e));
  }

  getArticles()           { return [...this._articles]; }
  getArticleById(id)      { return this._articles.find(a => a.id === id) ?? null; }
  getArticleByExternalQrCode(code, excludeArticleId = null) {
    const normalizedCode = String(code ?? '').trim();
    if (!normalizedCode) return null;
    return this._articles.find(article =>
      article.id !== excludeArticleId &&
      String(article.externalQrCode ?? '').trim() === normalizedCode
    ) ?? null;
  }
  getArticleByListingLink(link, excludeArticleId = null) {
    const normalizedLink = String(link ?? '').trim();
    if (!normalizedLink) return null;
    return this._articles.find(article =>
      article.id !== excludeArticleId &&
      String(article.listingLink ?? '').trim() === normalizedLink
    ) ?? null;
  }
  getArticleByPublicQrToken(token, excludeArticleId = null) {
    const normalizedToken = PublicQr.normalizeToken(token);
    if (!normalizedToken) return null;
    return this._articles.find(article =>
      article.id !== excludeArticleId &&
      PublicQr.normalizeToken(article.publicQrToken) === normalizedToken
    ) ?? null;
  }
  getArticlesByGroup(gid) { return this._articles.filter(a => a.groupId === gid); }

  _maxNumericId(items, prefix) {
    if (!items.length) return 0;
    const nums = items
      .map(item => {
        const id = item?.id ?? '';
        if (!id.startsWith(prefix)) return NaN;
        return parseInt(id.replace(prefix, ''), 10);
      })
      .filter(n => !isNaN(n));
    return nums.length ? Math.max(...nums) : 0;
  }

  async _reserveIds(counterField, prefix, quantity, fallbackMax) {
    const countersRef = this._db.collection('meta').doc('counters');
    return this._db.runTransaction(async tx => {
      const snap        = await tx.get(countersRef);
      const remoteValue = snap.exists ? parseInt(snap.data()?.[counterField], 10) || 0 : 0;
      const startNum    = Math.max(remoteValue, fallbackMax) + 1;
      const endNum      = startNum + quantity - 1;
      tx.set(countersRef, { [counterField]: endNum }, { merge: true });
      return Array.from({ length: quantity }, (_, index) =>
        prefix + String(startNum + index).padStart(4, '0')
      );
    });
  }

  async saveArticle(data) {
    const [id] = await this._reserveIds(
      'articleLastNumber',
      'A-',
      1,
      this._maxNumericId(this._articles, 'A-')
    );
    const now     = Date.now();
    const publicQrToken = this._ensurePublicQrToken(data);
    const article = {
      ...data,
      id,
      publicQrToken,
      createdAt: now,
      updatedAt: now,
    };
    this._articles.push(article);
    await this._fsSet('articles', article.id, article);
    return article;
  }

  async saveBulkArticles(data, quantity) {
    const ids = await this._reserveIds(
      'articleLastNumber',
      'A-',
      quantity,
      this._maxNumericId(this._articles, 'A-')
    );
    const now   = Date.now();
    const saved = [];
    const reservedTokens = this._collectPublicQrTokens();
    for (let i = 0; i < quantity; i++) {
      const publicQrToken = PublicQr.createToken(reservedTokens);
      reservedTokens.add(publicQrToken);
      const article = {
        ...data,
        quantity : 1,
        id       : ids[i],
        publicQrToken,
        createdAt: now,
        updatedAt: now,
      };
      this._articles.push(article);
      await this._fsSet('articles', article.id, article);
      saved.push(article);
    }
    return saved;
  }

  updateArticle(id, data) {
    const idx = this._articles.findIndex(a => a.id === id);
    if (idx === -1) return null;
    const publicQrToken = this._ensurePublicQrToken(
      { ...this._articles[idx], ...data },
      id
    );
    this._articles[idx] = {
      ...this._articles[idx],
      ...data,
      publicQrToken,
      updatedAt: Date.now(),
    };
    this._fsSet('articles', id, this._articles[idx]);
    return this._articles[idx];
  }

  _commitArticleBatch(articles) {
    const chunkSize = 400;
    for (let i = 0; i < articles.length; i += chunkSize) {
      const batch = this._db.batch();
      articles.slice(i, i + chunkSize).forEach(article => {
        batch.set(this._db.collection('articles').doc(article.id), article);
      });
      batch.commit().catch(e => console.error('Firestore article batch commit:', e));
    }
  }

  updateArticles(ids, data) {
    const now     = Date.now();
    const updated = [];
    ids.forEach(id => {
      const idx = this._articles.findIndex(a => a.id === id);
      if (idx === -1) return;
      this._articles[idx] = { ...this._articles[idx], ...data, updatedAt: now };
      updated.push(this._articles[idx]);
    });
    if (updated.length) this._commitArticleBatch(updated);
    return updated;
  }

  updateArticlesBulk(updates) {
    const now     = Date.now();
    const updated = [];
    updates.forEach(({ id, data }) => {
      const idx = this._articles.findIndex(a => a.id === id);
      if (idx === -1) return;
      this._articles[idx] = { ...this._articles[idx], ...data, updatedAt: now };
      updated.push(this._articles[idx]);
    });
    if (updated.length) this._commitArticleBatch(updated);
    return updated;
  }

  importArticle(id, data) {
    const now      = Date.now();
    const existing = this._articles.findIndex(a => a.id === id);
    if (existing !== -1) {
      const publicQrToken = this._ensurePublicQrToken(
        { ...this._articles[existing], ...data },
        id
      );
      this._articles[existing] = {
        ...this._articles[existing],
        ...data,
        publicQrToken,
        updatedAt: now,
      };
      this._fsSet('articles', id, this._articles[existing]);
    } else {
      const publicQrToken = this._ensurePublicQrToken(data);
      const article = { ...data, id, publicQrToken, createdAt: now, updatedAt: now };
      this._articles.push(article);
      this._fsSet('articles', id, article);
    }
  }

  deleteArticle(id)     { return this.updateArticle(id, { status: 'Entsorgt' }); }

  hardDeleteArticle(id) {
    const idx = this._articles.findIndex(a => a.id === id);
    if (idx !== -1) this._articles.splice(idx, 1);
    this._fsDelete('articles', id);
  }

  getGroups()      { return [...this._groups]; }
  getGroupById(id) { return this._groups.find(g => g.id === id) ?? null; }

  getOrders()      { return [...this._orders]; }
  getOrderById(id) { return this._orders.find(order => order.id === id) ?? null; }

  async saveOrder(data) {
    const [id] = await this._reserveIds(
      'orderLastNumber',
      'O-',
      1,
      this._maxNumericId(this._orders, 'O-')
    );
    const now   = Date.now();
    const order = {
      ...data,
      id,
      createdAt: now,
      updatedAt: now,
    };
    this._orders.push(order);
    await this._fsSet('orders', order.id, order);
    return order;
  }

  updateOrder(id, data) {
    const idx = this._orders.findIndex(order => order.id === id);
    if (idx === -1) return null;
    this._orders[idx] = {
      ...this._orders[idx],
      ...data,
      updatedAt: Date.now(),
    };
    this._fsSet('orders', id, this._orders[idx]);
    return this._orders[idx];
  }

  getRoles()      { return [...this._roles]; }
  getRoleById(id) { return this._roles.find(role => role.id === id) ?? null; }

  async saveRole(data) {
    const [id] = await this._reserveIds(
      'roleLastNumber',
      'R-',
      1,
      this._maxNumericId(this._roles, 'R-')
    );
    const now  = Date.now();
    const role = {
      ...data,
      id,
      createdAt: now,
      updatedAt: now,
    };
    this._roles.push(role);
    await this._fsSet('roles', role.id, role);
    return role;
  }

  updateRole(id, data) {
    const idx = this._roles.findIndex(role => role.id === id);
    if (idx === -1) return null;
    this._roles[idx] = {
      ...this._roles[idx],
      ...data,
      updatedAt: Date.now(),
    };
    this._fsSet('roles', id, this._roles[idx]);
    return this._roles[idx];
  }

  async ensureSystemRoles() {
    const existingIds = new Set(this._roles.map(role => role.id));
    const now = Date.now();
    const missingRoles = RoleSecurity.getSystemRoles().filter(role => !existingIds.has(role.id));
    for (const role of missingRoles) {
      const fullRole = {
        ...role,
        createdAt: now,
        updatedAt: now,
      };
      this._roles.push(fullRole);
      await this._fsSet('roles', fullRole.id, fullRole);
    }
  }

  getUsers()      { return [...this._users]; }
  getUserById(id) { return this._users.find(user => user.id === id) ?? null; }

  getUserByEmail(email) {
    return this.getUserById(RoleSecurity.normalizeEmail(email));
  }

  async saveUser(data) {
    const id = RoleSecurity.normalizeEmail(data.email);
    if (!id) throw new Error('USER_EMAIL_REQUIRED');
    const existing = this.getUserById(id);
    const now = Date.now();
    const user = {
      ...(existing ?? {}),
      ...data,
      id,
      email: String(data.email ?? existing?.email ?? '').trim(),
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
    };
    if (existing) {
      const idx = this._users.findIndex(entry => entry.id === id);
      this._users[idx] = user;
    } else {
      this._users.push(user);
    }
    await this._fsSet('users', id, user);
    return user;
  }

  updateUser(id, data) {
    const idx = this._users.findIndex(user => user.id === id);
    if (idx === -1) return null;
    this._users[idx] = {
      ...this._users[idx],
      ...data,
      updatedAt: Date.now(),
    };
    this._fsSet('users', id, this._users[idx]);
    return this._users[idx];
  }

  async saveGroup(data) {
    const [id] = await this._reserveIds(
      'groupLastNumber',
      'G-',
      1,
      this._maxNumericId(this._groups, 'G-')
    );
    const now   = Date.now();
    const group = {
      ...data,
      id,
      priceHistory: data.priceNet
        ? [{ price: parseFloat(data.priceNet), priceGross: parseFloat(data.priceGross) || parseFloat(data.priceNet) * 1.19, date: Utils.formatDate(now) }]
        : [],
      createdAt: now,
      updatedAt: now,
    };
    this._groups.push(group);
    await this._fsSet('groups', group.id, group);
    return group;
  }

  updateGroup(id, data) {
    const idx = this._groups.findIndex(g => g.id === id);
    if (idx === -1) return null;
    const existing   = this._groups[idx];
    let priceHistory = existing.priceHistory ?? [];
    if (data.priceNet !== undefined && parseFloat(data.priceNet) !== parseFloat(existing.priceNet)) {
      priceHistory = [
        ...priceHistory,
        {
          price     : parseFloat(data.priceNet),
          priceGross: parseFloat(data.priceGross) || parseFloat(data.priceNet) * 1.19,
          date      : Utils.formatDate(Date.now()),
        },
      ];
    }
    this._groups[idx] = { ...existing, ...data, priceHistory, updatedAt: Date.now() };
    this._fsSet('groups', id, this._groups[idx]);
    return this._groups[idx];
  }

  deleteGroup(id) { return this.updateGroup(id, { status: 'Entsorgt' }); }

  hardDeleteGroup(id) {
    const idx = this._groups.findIndex(g => g.id === id);
    if (idx !== -1) this._groups.splice(idx, 1);
    this._fsDelete('groups', id);
  }

  resetAll() {
    this._articles = [];
    this._groups   = [];
    this._db.collection('articles').get().then(snap => {
      const batch = this._db.batch();
      snap.docs.forEach(d => batch.delete(d.ref));
      return batch.commit();
    }).catch(e => console.error('Firestore resetAll articles:', e));
    this._db.collection('groups').get().then(snap => {
      const batch = this._db.batch();
      snap.docs.forEach(d => batch.delete(d.ref));
      return batch.commit();
    }).catch(e => console.error('Firestore resetAll groups:', e));
  }

  getStats() {
    const a = this.getArticles();
    return {
      total    : a.length,
      available: a.filter(x => Utils.normalizeStatus(x.status) === 'Verf\u00fcgbar').length,
      reserved : a.filter(x => Utils.normalizeStatus(x.status) === 'Reserviert').length,
      sold     : a.filter(x => Utils.normalizeStatus(x.status) === 'Verkauft').length,
      revenue  : a.filter(x => Utils.normalizeStatus(x.status) === 'Verkauft' && x.soldPrice)
                  .reduce((s, x) => s + parseFloat(x.soldPrice), 0),
    };
  }

  static articleKey(data) {
    const mfr   = (data.manufacturer ?? '').trim().toLowerCase();
    const model = (data.model ?? '').trim().toLowerCase();
    if (mfr && model) return mfr + model;
    return (data.category ?? '').trim().toLowerCase();
  }

  findMatchingGroup(articleData) {
    const targetKey = WawiDB.articleKey(articleData);
    if (!targetKey) return null;
    for (const group of this._groups.filter(g => g.status !== 'Entsorgt')) {
      const members = this.getArticlesByGroup(group.id);
      if (members.some(m => WawiDB.articleKey(m) === targetKey)) return group;
    }
    return null;
  }

  async autoAssignGroup(articleIds, sampleData) {
    if (sampleData.groupId) {
      this.updateArticles(articleIds, { groupId: sampleData.groupId });
      return this.getGroupById(sampleData.groupId);
    }
    let group = this.findMatchingGroup(sampleData);
    if (!group) {
      const mfr  = (sampleData.manufacturer ?? '').trim();
      const model = (sampleData.model ?? '').trim();
      const category = (sampleData.category ?? '').trim();
      const hasImportedInventoryId =
        sampleData.id !== undefined &&
        sampleData.id !== null &&
        String(sampleData.id).trim() !== '' &&
        !String(sampleData.id).startsWith('A-');
      const name = hasImportedInventoryId
        ? [String(sampleData.id).trim(), [mfr, model, category].filter(Boolean).join(' ')].filter(Boolean).join(' - ')
        : ([mfr, model].filter(Boolean).join(' ') || category);
      group = await this.saveGroup({
        name,
        status           : sampleData.status === 'Entsorgt' ? 'Entsorgt' : 'Verf\u00fcgbar',
        quantity         : articleIds.length,
        location         : sampleData.location ?? '',
        priceNet         : null,
        priceGross       : null,
        conditionOverview: '',
        notes            : '',
        image            : sampleData.photos?.[0] ?? null,
      });
    }
    this.updateArticles(articleIds, { groupId: group.id });
    return group;
  }

  async autoAssignAllUngrouped() {
    const unassigned = this.getArticles()
      .filter(a => !a.groupId && a.status !== 'Entsorgt');
    if (!unassigned.length) return { assigned: 0, groupsCreated: 0, groupsReused: 0 };
    const buckets = {};
    unassigned.forEach(a => {
      const key = WawiDB.articleKey(a);
      if (!buckets[key]) buckets[key] = [];
      buckets[key].push(a);
    });
    let assigned = 0, groupsCreated = 0, groupsReused = 0;
    for (const bucket of Object.values(buckets)) {
      const sample   = bucket[0];
      const ids      = bucket.map(a => a.id);
      const existing = this.findMatchingGroup(sample);
      if (existing) groupsReused++; else groupsCreated++;
      await this.autoAssignGroup(ids, sample);
      assigned += ids.length;
    }
    return { assigned, groupsCreated, groupsReused };
  }

  _collectPublicQrTokens(excludeArticleId = null) {
    const tokens = new Set();
    this._articles.forEach(article => {
      if (article.id === excludeArticleId) return;
      const token = PublicQr.normalizeToken(article.publicQrToken);
      if (token) tokens.add(token);
    });
    return tokens;
  }

  _ensurePublicQrToken(data, excludeArticleId = null) {
    const existingTokens = this._collectPublicQrTokens(excludeArticleId);
    const requestedToken = PublicQr.normalizeToken(data?.publicQrToken);
    if (requestedToken) {
      if (existingTokens.has(requestedToken)) {
        throw new Error(`Der öffentliche QR-Token ${requestedToken} ist bereits vergeben.`);
      }
      return requestedToken;
    }
    return PublicQr.createToken(existingTokens);
  }

  ensurePublicQrTokens() {
    const seenTokens = new Set();
    const updates = [];
    this._articles.forEach(article => {
      const normalizedToken = PublicQr.normalizeToken(article.publicQrToken);
      const nextToken = normalizedToken && !seenTokens.has(normalizedToken)
        ? normalizedToken
        : PublicQr.createToken(seenTokens);
      seenTokens.add(nextToken);
      if (String(article.publicQrToken ?? '').trim() !== nextToken) {
        updates.push({ id: article.id, data: { publicQrToken: nextToken } });
      }
    });
    if (updates.length) this.updateArticlesBulk(updates);
    return updates.length;
  }
}

const DB = INITIAL_PUBLIC_QR_ROUTE ? null : new WawiDB();

/* ============================================================
   2. GLOBALER STATE
============================================================ */
const State = {
  currentView      : 'dashboard',
  editingArticleId : null,
  editingGroupId   : null,
  articleReturnGroupId: null,
  articleGroupSelectionTouched: false,
  articlePhotos    : [],
  groupImageBase64 : null,
  inventoryViewMode: 'grid',
  encSortKey       : 'updatedAt',
  encSortDir       : 'desc',
  selectedOrderId  : null,
  selectedWarehouseOrderId: null,
  selectedSoldOrderId: null,
  warehouseLastScan: null,
  adminTab         : 'users',
  authUser         : null,
  appUser          : null,
  activeRole       : null,
};

/* ============================================================
   2B. ROLLEN / BERECHTIGUNGEN
============================================================ */
const RoleSecurity = {
  systemAdminRoleId: 'ROLE_ADMIN',

  permissionGroups: [
    {
      id: 'dashboard',
      label: 'Dashboard',
      description: 'Erfassung und Startbereich',
      permissions: [
        { id: 'dashboard.view', label: 'Dashboard anzeigen', description: 'Dashboard und Erfassung öffnen' },
        { id: 'articles.create', label: 'Artikel erfassen', description: 'Neue Artikel im Dashboard speichern' },
        { id: 'groups.create', label: 'Gruppen anlegen', description: 'Neue Gruppen im Dashboard speichern' },
      ],
    },
    {
      id: 'groups',
      label: 'Gruppen',
      description: 'Artikelgruppen und Gruppendetails',
      permissions: [
        { id: 'groups.view', label: 'Gruppen anzeigen', description: 'Gruppenübersicht öffnen' },
        { id: 'groups.edit', label: 'Gruppen bearbeiten', description: 'Gruppendetails ändern' },
      ],
    },
    {
      id: 'inventory',
      label: 'Bestand',
      description: 'Bestandsübersicht und Sammelaktionen',
      permissions: [
        { id: 'inventory.view', label: 'Bestand anzeigen', description: 'Bestandsübersicht öffnen' },
        { id: 'inventory.edit', label: 'Bestand bearbeiten', description: 'Einzelne Bestandsaktionen nutzen' },
        { id: 'inventory.bulk', label: 'Sammelaktionen nutzen', description: 'Mehrfachauswahl und Sammelverkauf nutzen' },
      ],
    },
    {
      id: 'sold',
      label: 'Verkauft',
      description: 'Verkaufte Artikel und Auswertung',
      permissions: [
        { id: 'sold.view', label: 'Verkauft anzeigen', description: 'Verkauft-Ansicht öffnen' },
      ],
    },
    {
      id: 'encyclopedia',
      label: 'Enzyklopädie',
      description: 'Komplette Artikeltabelle',
      permissions: [
        { id: 'encyclopedia.view', label: 'Enzyklopädie anzeigen', description: 'Gesamttabelle öffnen' },
      ],
    },
    {
      id: 'tools',
      label: 'Tools',
      description: 'Import, Export und Hilfsfunktionen',
      permissions: [
        { id: 'tools.view', label: 'Tools anzeigen', description: 'Toolbereich öffnen' },
      ],
    },
    {
      id: 'scanner',
      label: 'Scanner',
      description: 'QR-Scanner und Umlagerung',
      permissions: [
        { id: 'scanner.view', label: 'Scanner anzeigen', description: 'Scannerbereich öffnen' },
      ],
    },
    {
      id: 'orders',
      label: 'Aufträge',
      description: 'Aufträge, Status und Zahlungsübersicht',
      permissions: [
        { id: 'orders.view', label: 'Aufträge anzeigen', description: 'Auftragsübersicht öffnen' },
        { id: 'orders.create', label: 'Aufträge anlegen', description: 'Neue Aufträge speichern' },
        { id: 'orders.edit', label: 'Aufträge bearbeiten', description: 'Vorhandene Aufträge ändern' },
        { id: 'orders.release', label: 'Aufträge freigeben', description: 'Aufträge an den Warenausgang übergeben' },
        { id: 'orders.payment', label: 'Zahlungsstatus ändern', description: 'Zahlungsstatus pflegen' },
      ],
    },
    {
      id: 'warehouse',
      label: 'Warenausgang',
      description: 'Kommissionierung und Scanbereich',
      permissions: [
        { id: 'warehouse.view', label: 'Warenausgang anzeigen', description: 'Mitarbeiteransicht öffnen' },
        { id: 'warehouse.scan', label: 'Artikel scannen', description: 'Artikel im Auftrag buchen' },
        { id: 'warehouse.ready', label: 'Als bereit markieren', description: 'Auftrag als bereit kennzeichnen' },
        { id: 'warehouse.handover', label: 'Als übergeben markieren', description: 'Auftrag als übergeben kennzeichnen' },
      ],
    },
    {
      id: 'adminUsers',
      label: 'Nutzer',
      description: 'Nutzer anlegen und verwalten',
      permissions: [
        { id: 'admin.users.view', label: 'Nutzerbereich anzeigen', description: 'Nutzerverwaltung öffnen' },
        { id: 'admin.users.manage', label: 'Nutzer verwalten', description: 'Nutzer anlegen und bearbeiten' },
      ],
    },
    {
      id: 'adminRoles',
      label: 'Rollen',
      description: 'Rollen und Berechtigungen verwalten',
      permissions: [
        { id: 'admin.roles.view', label: 'Rollenbereich anzeigen', description: 'Rollenverwaltung öffnen' },
        { id: 'admin.roles.manage', label: 'Rollen verwalten', description: 'Rollen anlegen und bearbeiten' },
      ],
    },
  ],

  normalizeEmail(email) {
    return String(email ?? '').trim().toLowerCase();
  },

  getAllPermissions() {
    return this.permissionGroups.flatMap(group => group.permissions.map(permission => permission.id));
  },

  getSystemRoles() {
    return [
      {
        id: this.systemAdminRoleId,
        name: 'Admin',
        description: 'Vollzugriff auf das gesamte Warenwirtschaftssystem.',
        permissions: this.getAllPermissions(),
        isSystemRole: true,
        locked: true,
      },
    ];
  },

  getViewPermission(viewId) {
    return ({
      dashboard   : 'dashboard.view',
      groups      : 'groups.view',
      inventory   : 'inventory.view',
      orders      : 'orders.view',
      warehouse   : 'warehouse.view',
      sold        : 'sold.view',
      encyclopedia: 'encyclopedia.view',
      tools       : 'tools.view',
      scanner     : 'scanner.view',
      admin       : ['admin.users.view', 'admin.roles.view'],
    })[viewId] ?? null;
  },
};

/* ============================================================
   3. UTILS â€” Hilfsfunktionen
============================================================ */
const Utils = {

  formatDate(ts) {
    const d = new Date(ts);
    return [
      String(d.getDate()).padStart(2, '0'),
      String(d.getMonth() + 1).padStart(2, '0'),
      d.getFullYear(),
    ].join('.');
  },

  formatDateTime(ts) {
    const d = new Date(ts);
    return `${this.formatDate(ts)} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
  },

  formatDateInput(ts) {
    const d = new Date(ts);
    return [
      d.getFullYear(),
      String(d.getMonth() + 1).padStart(2, '0'),
      String(d.getDate()).padStart(2, '0'),
    ].join('-');
  },

  formatEuro(val) {
    const n = parseFloat(val);
    return isNaN(n)
      ? '-'
      : n.toLocaleString('de-DE', { style: 'currency', currency: 'EUR' });
  },

  condSlug(c) {
    return (c ?? '')
      .toLowerCase()
      .replace(/Ã¤/g, 'ae')
      .replace(/Ã¶/g, 'oe')
      .replace(/Ã¼/g, 'ue')
      .replace(/\s+/g, '-');
  },

  condColor(c) {
    return ({
      'Neuwertig'               : '#1d4ed8',
      'Leichte Gebrauchsspuren' : '#16a34a',
      'Mittlere Gebrauchsspuren': '#ea580c',
      'Starke Gebrauchsspuren'  : '#dc2626',
      'Defekt'                  : '#1e1e1e',
    })[c] ?? '#94a3b8';
  },

  statusBadge(status) {
    const normalizedStatus = this.normalizeStatus(status);
    const slug = normalizedStatus
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/\u00df/g, 'ss')
      .replace(/\s+/g, '-');
    const icon = ({
      'Verf\u00fcgbar': 'fa-circle-check',
      'Reserviert': 'fa-clock',
      'Verkauft'  : 'fa-handshake',
      'Entsorgt'  : 'fa-trash',
    })[normalizedStatus] ?? 'fa-circle';
    return `<span class="badge badge-status-${slug}">
              <i class="fa-solid ${icon}"></i> ${normalizedStatus}
            </span>`;
  },

  isNewArticle(article) {
    if (!article?.createdAt) return false;
    return Date.now() - article.createdAt <= 7 * 24 * 60 * 60 * 1000;
  },

  newBadge() {
    return `<span class="badge badge-new">
              <i class="fa-solid fa-sparkles"></i> Neu
            </span>`;
  },

  condBadge(condition) {
    const slug  = this.condSlug(condition);
    const color = this.condColor(condition);
    return `<span style="display:inline-flex;align-items:center;gap:5px;">
              <span class="cond-dot cond-dot-${slug}"></span>
              <span style="font-size:var(--font-size-xs)">${condition ?? '-'}</span>
            </span>`;
  },

  fileToBase64(file) {
    return new Promise((resolve, reject) => {
      const r    = new FileReader();
      r.onload   = e => resolve(e.target.result);
      r.onerror  = reject;
      r.readAsDataURL(file);
    });
  },

  resizeImage(file, maxPx = 900, quality = 0.82) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onerror = reject;
      reader.onload  = ev => {
        const img   = new Image();
        img.onerror = reject;
        img.onload  = () => {
          const scale   = Math.min(1, maxPx / Math.max(img.width, img.height));
          const canvas  = document.createElement('canvas');
          canvas.width  = Math.round(img.width  * scale);
          canvas.height = Math.round(img.height * scale);
          canvas.getContext('2d').drawImage(img, 0, 0, canvas.width, canvas.height);
          resolve(canvas.toDataURL('image/jpeg', quality));
        };
        img.src = ev.target.result;
      };
      reader.readAsDataURL(file);
    });
  },

  escHtml(str) {
    return String(str ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  },

  autoGroupName(article) {
    return [article?.manufacturer, article?.model].filter(Boolean).join(' ') || article?.category || '';
  },

  displayInventoryId(article) {
    if (!article) return '';
    const storedId = String(article.displayInventoryId ?? '').trim();
    if (storedId) return storedId;
    const ownId = String(article.id ?? '').trim();
    if (ownId && !ownId.startsWith('A-')) return ownId;
    if (!article.groupId) return '';
    const sourceArticle = DB.getArticlesByGroup(article.groupId)
      .find(item => {
        const candidateId = String(item?.displayInventoryId ?? item?.id ?? '').trim();
        return candidateId && !String(item?.id ?? '').trim().startsWith('A-');
      });
    const sourceId = String(sourceArticle?.displayInventoryId ?? sourceArticle?.id ?? '').trim();
    if (sourceId) return sourceId;
    const groupName = String(DB.getGroupById(article.groupId)?.name ?? '').trim();
    const groupInventoryId = groupName.split(' - ')[0]?.trim() ?? '';
    return /^\d+$/.test(groupInventoryId) ? groupInventoryId : '';
  },

  articleDisplayName(article, fallback = '-') {
    if (!article) return fallback;
    const inventoryId = this.displayInventoryId(article);
    const baseName = [article.manufacturer, article.model, article.category].filter(Boolean).join(' ').trim();
    const composed = inventoryId
      ? [inventoryId, baseName].filter(Boolean).join(' - ')
      : ([article.manufacturer, article.model].filter(Boolean).join(' ') || article.category || '');
    return composed || article.id || fallback;
  },

  groupDisplayName(group, articles, fallback = '') {
    const firstArticle = articles?.[0] ?? null;
    const currentName  = String(group?.name ?? '').trim();
    if (firstArticle && currentName && currentName === this.autoGroupName(firstArticle)) {
      return this.articleDisplayName(firstArticle, fallback);
    }
    return currentName || fallback;
  },

  normalizeStatus(status) {
    const raw = String(status ?? '').trim();
    const repaired = raw
      .replace(/Ã¼/g, '\u00fc')
      .replace(/¼/g, '\u00fc')
      .replace(/Ã¶/g, '\u00f6')
      .replace(/Ã¤/g, '\u00e4')
      .replace(/ÃŸ/g, '\u00df');
    const normalized = repaired
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/\u00df/g, 'ss')
      .replace(/[^a-z]/g, '');
    if (!normalized) return raw;
    if (
      normalized.startsWith('verfugbar') ||
      normalized.startsWith('verfuegbar') ||
      normalized.startsWith('verfgbar')
    ) return 'Verf\u00fcgbar';
    if (normalized.startsWith('reserviert')) return 'Reserviert';
    if (normalized.startsWith('verkauft')) return 'Verkauft';
    if (normalized.startsWith('entsorgt')) return 'Entsorgt';
    return repaired || raw;
  },

  articleMatchesSearch(a, q) {
    if (!q) return true;
    const ql = q.toLowerCase();
    return [
      a.id, a.manufacturer, a.model, a.category, this.articleDisplayName(a, ''),
      a.location, a.material, a.style, a.notes,
      a.listingLink, a.groupId,
    ].some(v => (v ?? '').toLowerCase().includes(ql));
  },

  csvCell(val) {
    const s = String(val ?? '');
    return s.includes(';') || s.includes('"') || s.includes('\n')
      ? `"${s.replace(/"/g, '""')}"`
      : s;
  },

  debounce(fn, wait = 120) {
    let timeoutId = null;
    return (...args) => {
      clearTimeout(timeoutId);
      timeoutId = window.setTimeout(() => fn(...args), wait);
    };
  },

  repairVisibleString(value) {
    const input = String(value ?? '');
    if (!/[ÃÂâð�]/.test(input) &&
        !/\b(?:oeffnen|Oeffnen|naechst\w*|rueckg\w*|Rueckg\w*|dafuer|Dafuer|fuer|Fuer|moeglich|Moeglich)\b/.test(input)) {
      return input;
    }
    let repaired = input
      .replace(/\bOeffnen\b/g, 'Öffnen')
      .replace(/\boeffnen\b/g, 'öffnen')
      .replace(/\bnaechste\b/g, 'nächste')
      .replace(/\bnaechsten\b/g, 'nächsten')
      .replace(/\bnaechster\b/g, 'nächster')
      .replace(/\bnaechstes\b/g, 'nächstes')
      .replace(/\bNaechste\b/g, 'Nächste')
      .replace(/\bNaechsten\b/g, 'Nächsten')
      .replace(/\bRueckgaengig\b/g, 'Rückgängig')
      .replace(/\brueckgaengig\b/g, 'rückgängig')
      .replace(/\bDafuer\b/g, 'Dafür')
      .replace(/\bdafuer\b/g, 'dafür')
      .replace(/\bMoeglich\b/g, 'Möglich')
      .replace(/\bmoeglich\b/g, 'möglich')
      .replace(/\bFuer\b/g, 'Für')
      .replace(/\bfuer\b/g, 'für')
      .replace(/fÃƒÂ¼r/g, 'für')
      .replace(/spÃƒÂ¤ter/g, 'später')
      .replace(/ÃƒÂ¼bernehmen/g, 'übernehmen')
      .replace(/Bestands�bersicht/g, 'Bestandsübersicht')
      .replace(/Gruppen�bersicht/g, 'Gruppenübersicht')
      .replace(/Enzyklop�die/g, 'Enzyklopädie')
      .replace(/Verf�gbar/g, 'Verfügbar')
      .replace(/K�nig/g, 'König')
      .replace(/f�r/g, 'für')
      .replace(/sp�ter/g, 'später')
      .replace(/�bernehmen/g, 'übernehmen')
      .replace(/m�glich/g, 'möglich')
      .replace(/l�schen/g, 'löschen')
      .replace(/L�schen/g, 'Löschen')
      .replace(/ausgew�hlt/g, 'ausgewählt')
      .replace(/best�tigen/g, 'bestätigen')
      .replace(/Best�tigen/g, 'Bestätigen')
      .replace(/Ã¤ndern/g, 'ändern')
      .replace(/Ã„ndern/g, 'Ändern')
      .replace(/Ã„nderungen/g, 'Änderungen')
      .replace(/wÃ¤hlen/g, 'wählen')
      .replace(/WÃ¤hlen/g, 'Wählen')
      .replace(/ZustÃ¤nde/g, 'Zustände')
      .replace(/ZeitrÃ¤ume/g, 'Zeiträume')
      .replace(/ZurÃ¼ck/g, 'Zurück')
      .replace(/zurÃ¼ck/g, 'zurück')
      .replace(/SchlieÃŸen/g, 'Schließen')
      .replace(/schlieÃŸen/g, 'schließen')
      .replace(/gÃ¼ltig/g, 'gültig')
      .replace(/gÃ¼ltigen/g, 'gültigen')
      .replace(/vollstÃ¤ndig/g, 'vollständig')
      .replace(/vollstÃ¤ndiges/g, 'vollständiges')
      .replace(/unberÃ¼hrt/g, 'unberührt')
      .replace(/rÃ¼ckgÃ¤ngig/g, 'rückgängig')
      .replace(/rÃ¼cksetzen/g, 'rücksetzen')
      .replace(/gel�st/g, 'gelöst')
      .replace(/vollst�ndig/g, 'vollständig')
      .replace(/r�ckg�ngig/g, 'rückgängig')
      .replace(/endg�ltig/g, 'endgültig')
      .replace(/St�ckzahl/g, 'Stückzahl')
      .replace(/Ma�e/g, 'Maße')
      .replace(/H�he/g, 'Höhe')
      .replace(/Wei�/g, 'Weiß')
      .replace(/Ã¼/g, 'ü')
      .replace(/Ã¶/g, 'ö')
      .replace(/Ã¤/g, 'ä')
      .replace(/ÃŸ/g, 'ß')
      .replace(/Ãœ/g, 'Ü')
      .replace(/Ã–/g, 'Ö')
      .replace(/Ã„/g, 'Ä')
      .replace(/Â·/g, '·')
      .replace(/Â /g, ' ')
      .replace(/Ã—/g, '×')
      .replace(/â‚¬/g, '€')
      .replace(/â€“/g, '–')
      .replace(/â€”/g, '—')
      .replace(/â€¦/g, '…')
      .replace(/â†’/g, '→')
      .replace(/â€œ/g, '“')
      .replace(/â€ž/g, '„')
      .replace(/â€/g, '”')
      .replace(/âœ“/g, '✓');
    for (let i = 0; i < 3; i++) {
      try {
        const bytes = Uint8Array.from(Array.from(repaired, ch => ch.charCodeAt(0) & 0xff));
        const decoded = new TextDecoder('utf-8').decode(bytes);
        if (!decoded || decoded === repaired) break;
        repaired = decoded;
      } catch (_) {
        break;
      }
    }
    return repaired
      .replace(/\bOeffnen\b/g, 'Öffnen')
      .replace(/\boeffnen\b/g, 'öffnen')
      .replace(/\bnaechste\b/g, 'nächste')
      .replace(/\bnaechsten\b/g, 'nächsten')
      .replace(/\bnaechster\b/g, 'nächster')
      .replace(/\bnaechstes\b/g, 'nächstes')
      .replace(/\bNaechste\b/g, 'Nächste')
      .replace(/\bNaechsten\b/g, 'Nächsten')
      .replace(/\bRueckgaengig\b/g, 'Rückgängig')
      .replace(/\brueckgaengig\b/g, 'rückgängig')
      .replace(/\bDafuer\b/g, 'Dafür')
      .replace(/\bdafuer\b/g, 'dafür')
      .replace(/\bMoeglich\b/g, 'Möglich')
      .replace(/\bmoeglich\b/g, 'möglich')
      .replace(/\bFuer\b/g, 'Für')
      .replace(/\bfuer\b/g, 'für')
      .replace(/fÃƒÂ¼r/g, 'für')
      .replace(/spÃƒÂ¤ter/g, 'später')
      .replace(/ÃƒÂ¼bernehmen/g, 'übernehmen')
      .replace(/Bestands�bersicht/g, 'Bestandsübersicht')
      .replace(/Gruppen�bersicht/g, 'Gruppenübersicht')
      .replace(/Enzyklop�die/g, 'Enzyklopädie')
      .replace(/Verf�gbar/g, 'Verfügbar')
      .replace(/K�nig/g, 'König')
      .replace(/f�r/g, 'für')
      .replace(/sp�ter/g, 'später')
      .replace(/�bernehmen/g, 'übernehmen')
      .replace(/m�glich/g, 'möglich')
      .replace(/l�schen/g, 'löschen')
      .replace(/L�schen/g, 'Löschen')
      .replace(/ausgew�hlt/g, 'ausgewählt')
      .replace(/best�tigen/g, 'bestätigen')
      .replace(/Best�tigen/g, 'Bestätigen')
      .replace(/Ã¤ndern/g, 'ändern')
      .replace(/Ã„ndern/g, 'Ändern')
      .replace(/Ã„nderungen/g, 'Änderungen')
      .replace(/wÃ¤hlen/g, 'wählen')
      .replace(/WÃ¤hlen/g, 'Wählen')
      .replace(/ZustÃ¤nde/g, 'Zustände')
      .replace(/ZeitrÃ¤ume/g, 'Zeiträume')
      .replace(/ZurÃ¼ck/g, 'Zurück')
      .replace(/zurÃ¼ck/g, 'zurück')
      .replace(/SchlieÃŸen/g, 'Schließen')
      .replace(/schlieÃŸen/g, 'schließen')
      .replace(/gÃ¼ltig/g, 'gültig')
      .replace(/gÃ¼ltigen/g, 'gültigen')
      .replace(/vollstÃ¤ndig/g, 'vollständig')
      .replace(/vollstÃ¤ndiges/g, 'vollständiges')
      .replace(/unberÃ¼hrt/g, 'unberührt')
      .replace(/rÃ¼ckgÃ¤ngig/g, 'rückgängig')
      .replace(/rÃ¼cksetzen/g, 'rücksetzen')
      .replace(/gel�st/g, 'gelöst')
      .replace(/vollst�ndig/g, 'vollständig')
      .replace(/r�ckg�ngig/g, 'rückgängig')
      .replace(/endg�ltig/g, 'endgültig')
      .replace(/St�ckzahl/g, 'Stückzahl')
      .replace(/Ma�e/g, 'Maße')
      .replace(/H�he/g, 'Höhe')
      .replace(/Wei�/g, 'Weiß')
      .replace(/Ã¼/g, 'ü')
      .replace(/Ã¶/g, 'ö')
      .replace(/Ã¤/g, 'ä')
      .replace(/ÃŸ/g, 'ß')
      .replace(/Ãœ/g, 'Ü')
      .replace(/Ã–/g, 'Ö')
      .replace(/Ã„/g, 'Ä')
      .replace(/Â·/g, '·')
      .replace(/Â /g, ' ')
      .replace(/Ã—/g, '×')
      .replace(/â‚¬/g, '€')
      .replace(/â€“/g, '–')
      .replace(/â€”/g, '—')
      .replace(/â€¦/g, '…')
      .replace(/â†’/g, '→')
      .replace(/â€œ/g, '“')
      .replace(/â€ž/g, '„')
      .replace(/â€/g, '”')
      .replace(/âœ“/g, '✓')
      .replace(/�/g, '');
  },

  repairVisibleDom(root = document.body) {
    if (!root) return;
    if (this._shouldSkipVisibleRepair(root)) return;
    const textTargets = [];
    if (root.nodeType === Node.TEXT_NODE) {
      textTargets.push(root);
    } else if (root.querySelectorAll) {
      textTargets.push(root);
      textTargets.push(...Array.from(root.querySelectorAll('*')).filter(node => !this._shouldSkipVisibleRepair(node)));
    }

    textTargets.forEach(node => {
      if (node.nodeType === Node.TEXT_NODE) {
        const fixed = this.repairVisibleString(node.textContent);
        if (fixed !== node.textContent) node.textContent = fixed;
        return;
      }

      if (node.matches('script, style')) return;

      Array.from(node.childNodes).forEach(child => {
        if (child.nodeType !== Node.TEXT_NODE) return;
        const fixed = this.repairVisibleString(child.textContent);
        if (fixed !== child.textContent) child.textContent = fixed;
      });

      ['placeholder', 'title', 'aria-label', 'data-tooltip'].forEach(attr => {
        const current = node.getAttribute(attr);
        if (current == null) return;
        const fixed = this.repairVisibleString(current);
        if (fixed !== current) node.setAttribute(attr, fixed);
      });
    });

    document.title = this.repairVisibleString(document.title);
  },

  _shouldSkipVisibleRepair(node) {
    const element = node?.nodeType === Node.TEXT_NODE ? node.parentElement : node;
    if (!element || element === document.body || element === document.documentElement) return false;
    if (!element.closest) return false;
    return !!element.closest('#art-qr-preview, #location-qr-preview, #qr-reader');
  },

  queueVisibleDomRepair(node) {
    if (!node) return;
    if (this._shouldSkipVisibleRepair(node)) return;
    if (!this._visibleTextRepairQueue) this._visibleTextRepairQueue = new Set();
    this._visibleTextRepairQueue.add(node);
    if (this._visibleTextRepairFrame) return;
    this._visibleTextRepairFrame = window.requestAnimationFrame(() => {
      const queue = [...(this._visibleTextRepairQueue ?? [])];
      this._visibleTextRepairQueue?.clear();
      this._visibleTextRepairFrame = null;
      queue.forEach(target => this.repairVisibleDom(target));
    });
  },

  observeVisibleTextRepair() {
    if (this._visibleTextRepairObserver) return;
    this.repairVisibleDom(document.body);
    this._visibleTextRepairObserver = new MutationObserver(mutations => {
      mutations.forEach(mutation => {
        if (mutation.type === 'characterData') {
          this.queueVisibleDomRepair(mutation.target);
          return;
        }
        mutation.addedNodes.forEach(node => this.queueVisibleDomRepair(node));
        if (mutation.type === 'attributes' && mutation.target) {
          this.queueVisibleDomRepair(mutation.target);
        }
      });
    });
    this._visibleTextRepairObserver.observe(document.body, {
      childList: true,
      subtree: true,
      characterData: true,
      attributes: true,
      attributeFilter: ['placeholder', 'title', 'aria-label', 'data-tooltip'],
    });
  },
};

/* ============================================================
   4. TOAST â€” Benachrichtigungen
============================================================ */
const Toast = {

  show(message, type = 'default', duration = 3500) {
    const c = document.getElementById('toast-container');
    const t = document.createElement('div');
    const repairedMessage = Utils.repairVisibleString(message);
    const icons = {
      success: 'fa-circle-check',
      error  : 'fa-circle-xmark',
      warning: 'fa-triangle-exclamation',
      default: 'fa-circle-info',
    };
    t.className = `toast toast-${type}`;
    t.innerHTML = `<i class="fa-solid ${icons[type] ?? icons.default}"></i>
                   ${Utils.escHtml(repairedMessage)}`;
    c.appendChild(t);
    setTimeout(() => {
      t.classList.add('toast-out');
      t.addEventListener('animationend', () => t.remove(), { once: true });
    }, duration);
  },

  success(m) { this.show(m, 'success');  },
  error(m)   { this.show(m, 'error');    },
  warning(m) { this.show(m, 'warning');  },
};

/* ============================================================
   5. MODAL
============================================================ */
const Modal = {

  init() {
    this.overlay  = document.getElementById('modal-overlay');
    this.content  = document.getElementById('modal-content');
    this.closeBtn = document.getElementById('modal-close-btn');

    this.closeBtn.addEventListener('click', () => this.close());
    this.overlay.addEventListener('click', e => {
      if (e.target === this.overlay) this.close();
    });
    document.addEventListener('keydown', e => {
      if (e.key === 'Escape') this.close();
    });
  },

  open(html, onOpen) {
    this.content.innerHTML = Utils.repairVisibleString(html);
    this.overlay.classList.remove('hidden');
    document.body.style.overflow = 'hidden';
    Utils.repairVisibleDom(this.content);
    if (typeof onOpen === 'function') onOpen(this.content);
  },

  close() {
    this.overlay.classList.add('hidden');
    this.content.innerHTML       = '';
    document.body.style.overflow = '';
  },
};

/* ============================================================
   6. ROUTER
============================================================ */
const Router = {

  views: ['dashboard', 'groups', 'inventory', 'orders', 'warehouse', 'sold', 'encyclopedia', 'admin', 'tools', 'scanner'],

  titles: {
    dashboard  : 'Dashboard &amp; Erfassung',
    groups     : 'Artikelgruppen',
    inventory  : 'Bestandsübersicht',
    orders     : 'Aufträge &amp; Status',
    warehouse  : 'Warenausgang &amp; Scan',
    sold       : 'Verkäufe &amp; Abschluss',
    encyclopedia: 'Enzyklopädie',
    admin      : 'Nutzer &amp; Rollen',
    tools      : 'Tools &amp; Import/Export',
    scanner    : '<i class="fa-solid fa-qrcode"></i> QR-Scanner',
  },

  init() {
    document.querySelectorAll('[data-view]').forEach(el => {
      el.addEventListener('click', e => {
        e.preventDefault();
        this.navigate(el.dataset.view);
      });
    });
  },

  navigate(viewId) {
    if (!this.views.includes(viewId)) return;
    if (typeof AccessControl !== 'undefined' && !AccessControl.canAccessView(viewId)) {
      const fallbackView = AccessControl.getFirstAvailableView();
      if (!fallbackView) return;
      viewId = fallbackView;
    }
    State.currentView = viewId;

    this.views.forEach(v => {
      const el = document.getElementById(`view-${v}`);
      if (el) el.classList.toggle('hidden', v !== viewId);
    });

    document.querySelectorAll('[data-view]').forEach(el => {
      el.classList.toggle('active', el.dataset.view === viewId);
    });

    document.getElementById('topbar-title').innerHTML = this.titles[viewId] ?? viewId;

    Sidebar.close();

    const renderers = {
      dashboard  : () => Dashboard.renderStats(),
      groups     : () => Groups.render(),
      inventory  : () => Inventory.render(),
      orders     : () => Orders.render(),
      warehouse  : () => Warehouse.render(),
      sold       : () => Sold.render(),
      encyclopedia: () => Encyclopedia.render(),
      admin      : () => AdminPanel.render(),
      tools      : () => {},
      scanner    : () => QRScanner.start(),
    };
    if (State.currentView !== 'scanner') QRScanner.stop();
    renderers[viewId]?.();
    if (typeof AppChrome !== 'undefined') AppChrome.update();
  },
};

/* ============================================================
   7. SIDEBAR
============================================================ */
const Sidebar = {

  init() {
    this.sidebar   = document.getElementById('sidebar');
    this.hamburger = document.getElementById('hamburger-btn');

    this.overlay           = document.createElement('div');
    this.overlay.className = 'sidebar-overlay';
    document.body.appendChild(this.overlay);

    this.hamburger.addEventListener('click', () => this.toggle());
    this.overlay.addEventListener('click',   () => this.close());
  },

  toggle() { this.sidebar.classList.contains('open') ? this.close() : this.open(); },

  open() {
    this.sidebar.classList.add('open');
    this.overlay.classList.add('visible');
    this.hamburger.setAttribute('aria-expanded', 'true');
  },

  close() {
    this.sidebar.classList.remove('open');
    this.overlay.classList.remove('visible');
    this.hamburger.setAttribute('aria-expanded', 'false');
  },
};

/* ============================================================
   8.1. ZUGRIFF / ROLLEN
============================================================ */
const AccessControl = {
  getCurrentRole() {
    return State.activeRole;
  },

  getCurrentUser() {
    return State.appUser;
  },

  getPermissionSet() {
    return new Set(this.getCurrentRole()?.permissions ?? []);
  },

  can(permission) {
    if (!permission) return true;
    const permissions = this.getPermissionSet();
    return permissions.has(permission);
  },

  canAny(permissions) {
    if (!permissions) return true;
    if (!Array.isArray(permissions)) return this.can(permissions);
    return permissions.some(permission => this.can(permission));
  },

  canAccessView(viewId) {
    if (
      viewId === 'scanner'
      && typeof QRScanner !== 'undefined'
      && QRScanner.hasWarehouseSession?.()
      && this.can('warehouse.scan')
    ) {
      return true;
    }
    const requiredPermission = RoleSecurity.getViewPermission(viewId);
    return this.canAny(requiredPermission);
  },

  getFirstAvailableView() {
    return Router.views.find(viewId => this.canAccessView(viewId)) ?? null;
  },

  async bootstrapIfNeeded(authUser) {
    await DB.ensureSystemRoles();
    if (DB.getUsers().length || !authUser?.email) return;
    await DB.saveUser({
      email: authUser.email,
      displayName: authUser.displayName || authUser.email,
      roleId: RoleSecurity.systemAdminRoleId,
      active: true,
      createdBy: 'system-bootstrap',
    });
  },

  async syncAuthUser(authUser) {
    if (!authUser?.email) {
      return {
        allowed: false,
        message: 'Für dieses Google-Konto konnte keine E-Mail-Adresse ermittelt werden.',
      };
    }

    await this.bootstrapIfNeeded(authUser);

    const existingUser = DB.getUserByEmail(authUser.email);
    if (!existingUser) {
      return {
        allowed: false,
        message: 'Für diese E-Mail-Adresse ist noch kein Nutzer angelegt.',
      };
    }

    if (existingUser.active === false) {
      return {
        allowed: false,
        message: 'Dieser Nutzer wurde deaktiviert und hat aktuell keinen Zugriff.',
      };
    }

    const role = DB.getRoleById(existingUser.roleId);
    if (!role) {
      return {
        allowed: false,
        message: 'Dem Nutzer ist keine gültige Rolle zugewiesen.',
      };
    }

    const syncedUser = await DB.saveUser({
      ...existingUser,
      email: existingUser.email || authUser.email,
      displayName: existingUser.displayName || authUser.displayName || authUser.email,
      uid: authUser.uid,
      photoURL: authUser.photoURL || existingUser.photoURL || '',
      lastLoginAt: Date.now(),
      active: true,
    });

    this.applySession(authUser, syncedUser, role);
    return { allowed: true };
  },

  applySession(authUser, appUser, role) {
    State.authUser = authUser;
    State.appUser = appUser;
    State.activeRole = role;
    this.refreshNavigation();
    this.refreshBodyState();
    if (!this.canAccessView(State.currentView)) {
      const fallbackView = this.getFirstAvailableView();
      if (fallbackView) Router.navigate(fallbackView);
    } else if (typeof AppChrome !== 'undefined') {
      AppChrome.update();
    }
  },

  clearSession() {
    State.authUser = null;
    State.appUser = null;
    State.activeRole = null;
    this.refreshNavigation();
    this.refreshBodyState();
  },

  refreshSessionFromStore() {
    if (!State.appUser?.id) return;
    const liveUser = DB.getUserById(State.appUser.id);
    if (liveUser) State.appUser = liveUser;
    const liveRole = DB.getRoleById((liveUser ?? State.appUser)?.roleId);
    if (liveRole) State.activeRole = liveRole;
  },

  refreshBodyState() {
    document.body.classList.toggle(
      'permission-warehouse',
      this.can('warehouse.view') && !this.can('dashboard.view')
    );
  },

  refreshNavigation() {
    document.querySelectorAll('[data-view]').forEach(link => {
      const isVisible = this.canAccessView(link.dataset.view);
      link.classList.toggle('hidden', !isVisible);
    });
  },
};

/* ============================================================
   8.2. APP-CHROME
============================================================ */
const AppChrome = {
  init() {
    const newArticleBtn = document.getElementById('topbar-new-article-btn');
    if (newArticleBtn) {
      newArticleBtn.addEventListener('click', () => {
        if (!AccessControl.can('articles.create')) return;
        Router.navigate('dashboard');
        Dashboard.resetArticleForm();
      });
    }
  },

  update() {
    const newArticleBtn = document.getElementById('topbar-new-article-btn');
    if (!newArticleBtn) return;
    const canShow = State.currentView === 'dashboard' && AccessControl.can('articles.create');
    newArticleBtn.classList.toggle('hidden', !canShow);
  },
};

/* ============================================================
   8. PUBLIC QR
============================================================ */
const PublicQr = {
  TOKEN_LENGTH: 8,
  TOKEN_ALPHABET: 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',

  getBaseUrl() {
    return PUBLIC_QR_CONFIG.baseUrl.endsWith('/')
      ? PUBLIC_QR_CONFIG.baseUrl
      : `${PUBLIC_QR_CONFIG.baseUrl}/`;
  },

  getBase() {
    return new URL(this.getBaseUrl());
  },

  getBasePath() {
    const baseHashPath = this.getBaseHashPath();
    if (baseHashPath) {
      return baseHashPath.startsWith('/') ? baseHashPath : `/${baseHashPath}`;
    }
    const basePath = this.getBase().pathname;
    return basePath.endsWith('/') ? basePath : `${basePath}/`;
  },

  getBaseHashPath() {
    const baseHashPath = String(this.getBase().hash ?? '').replace(/^#/, '');
    if (!baseHashPath) return '';
    return baseHashPath.endsWith('/') ? baseHashPath : `${baseHashPath}/`;
  },

  normalizeToken(rawValue) {
    const value = String(rawValue ?? '').trim().toUpperCase();
    return /^[A-Z0-9]+$/.test(value) ? value : '';
  },

  createToken(existingTokens = new Set()) {
    let token = '';
    do {
      token = this._randomToken();
    } while (!token || existingTokens.has(token));
    return token;
  },

  _randomToken() {
    const chars = [];
    const values = new Uint32Array(this.TOKEN_LENGTH);
    if (window.crypto?.getRandomValues) {
      window.crypto.getRandomValues(values);
    } else {
      for (let i = 0; i < values.length; i++) {
        values[i] = Math.floor(Math.random() * 0xffffffff);
      }
    }
    for (let i = 0; i < values.length; i++) {
      chars.push(this.TOKEN_ALPHABET[values[i] % this.TOKEN_ALPHABET.length]);
    }
    return chars.join('');
  },

  buildUrl(token) {
    const normalizedToken = this.normalizeToken(token);
    return normalizedToken ? `${this.getBaseUrl()}${normalizedToken}` : '';
  },

  getArticleToken(article) {
    return this.normalizeToken(article?.publicQrToken);
  },

  getArticleUrl(article) {
    return this.buildUrl(this.getArticleToken(article));
  },

  parseUrl(rawValue) {
    const value = String(rawValue ?? '').trim();
    if (!value) return null;
    try {
      const url = new URL(value);
      const base = this.getBase();
      if (url.origin !== base.origin) return null;

      const baseHashPath = this.getBaseHashPath();
      const currentHashPath = String(url.hash ?? '').replace(/^#/, '');
      let restPath = '';

      if (baseHashPath && currentHashPath.startsWith(baseHashPath)) {
        restPath = currentHashPath.slice(baseHashPath.length).replace(/^\/+/, '');
      } else {
        const basePath = this.getBasePath();
        if (!url.pathname.startsWith(basePath)) return null;
        restPath = url.pathname.slice(basePath.length).replace(/^\/+/, '');
      }

      const rawToken = restPath.split('/')[0] ?? '';
      const token = this.normalizeToken(rawToken);
      if (!token) return null;
      return { token, url: url.toString() };
    } catch (_) {
      return null;
    }
  },

  getCurrentRouteToken() {
    return this.parseUrl(window.location.href)?.token ?? '';
  },
};

/* ============================================================
   8. QR MANAGER
============================================================ */
const QRManager = {
  LOCATION_PREFIX: 'LOC:',

  generate(containerId, text, size = 128) {
    const el = document.getElementById(containerId);
    if (!el || !text) return;
    el.innerHTML = '';
    try {
      new QRCode(el, {
        text,
        width        : size,
        height       : size,
        colorDark    : '#000000',
        colorLight   : '#ffffff',
        correctLevel : QRCode.CorrectLevel.M,
      });
    } catch (e) {
      el.innerHTML = `<span style="font-size:var(--font-size-xs);color:var(--color-muted)">QR n/v</span>`;
    }
  },

  makeLocationCode(location) {
    return this.LOCATION_PREFIX + String(location ?? '').trim();
  },

  parseLocationCode(rawValue) {
    const raw = String(rawValue ?? '').trim();
    if (!raw.startsWith(this.LOCATION_PREFIX)) return null;
    const location = raw.slice(this.LOCATION_PREFIX.length).trim();
    return location || null;
  },

  hasConfiguredPublicBaseUrl() {
    try {
      const base = new URL(PUBLIC_QR_CONFIG.baseUrl);
      return /^https?:$/i.test(base.protocol)
        && !/deine-domain\.de$/i.test(base.hostname)
        && !/^(localhost|127\.0\.0\.1)$/i.test(base.hostname);
    } catch (_) {
      return false;
    }
  },

  getListingQrText(article) {
    const value = String(article?.listingLink ?? '').trim();
    if (!value) return '';
    try {
      const url = new URL(value);
      return /^https?:$/i.test(url.protocol) ? url.toString() : '';
    } catch (_) {
      return '';
    }
  },

  getArticleQrText(article) {
    const publicQrUrl = PublicQr.getArticleUrl(article);
    if (this.hasConfiguredPublicBaseUrl() && publicQrUrl) return publicQrUrl;
    const listingQrText = this.getListingQrText(article);
    if (listingQrText) return listingQrText;
    return String(article?.id ?? '').trim();
  },

  _openPrintWindow({ title, label, qrText, subtitle = '' }) {
    const w = window.open('', '_blank', 'width=420,height=480');
    if (!w) {
      Toast.error('Popup blockiert. Bitte Popups fÃ¼r den QR-Druck erlauben.');
      return;
    }
    w.document.write(`<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="UTF-8"/>
  <title>${Utils.escHtml(title)}</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"><\/script>
  <style>
    body { font-family:sans-serif; display:flex; flex-direction:column;
           align-items:center; justify-content:center; height:100vh; margin:0; padding:24px; text-align:center; }
    #qr  { margin-bottom:14px; }
    p    { font-size:16px; margin-top:10px; font-weight:bold; }
    small{ color:#666; font-size:12px; margin-top:4px; display:block; }
    button { margin-top:18px; padding:10px 16px; border:0; border-radius:8px; background:#2563eb; color:#fff; font-weight:600; cursor:pointer; }
  </style>
</head>
<body>
  <div id="qr"></div>
  <p>${Utils.escHtml(label)}</p>
  ${subtitle ? `<small>${Utils.escHtml(subtitle)}</small>` : ''}
  <button id="print-btn" type="button">Drucken</button>
  <script>
    const qrText = ${JSON.stringify(qrText)};
    new QRCode(document.getElementById('qr'), {
      text   : qrText,
      width  : 200,
      height : 200,
      colorDark : '#000000',
      colorLight: '#ffffff',
      correctLevel: QRCode.CorrectLevel.M,
    });
    document.getElementById('print-btn').addEventListener('click', function () {
      window.print();
    });
  <\/script>
</body>
</html>`);
    w.document.close();
    w.focus();
  },

  printQR(articleId, articleOverride = null) {
    const a = articleOverride ?? DB.getArticleById(articleId);
    if (!a) return;
    const qrText = this.getArticleQrText(a);
    if (!qrText) {
      Toast.error('Für diesen Artikel ist noch keine öffentliche QR-URL verfügbar.');
      return;
    }
    this._openPrintWindow({
      title   : `QR ${a.id}`,
      label   : a.id,
      qrText,
      subtitle: Utils.articleDisplayName(a, ''),
    });
  },

  printLocationQR(location) {
    const cleanLocation = String(location ?? '').trim();
    if (!cleanLocation) {
      Toast.error('Bitte zuerst einen Standort eingeben.');
      return;
    }
    this._openPrintWindow({
      title   : `Standort ${cleanLocation}`,
      label   : cleanLocation,
      qrText  : this.makeLocationCode(cleanLocation),
      subtitle: 'Standort-QR fÃ¼r Umlagerung und Einlagerung',
    });
  },
};

const DymoManager = {
  _initialized: false,

  init() {
    const framework = window.dymo?.label?.framework;
    if (!framework) return false;
    if (this._initialized) return true;
    try {
      framework.init();
      this._initialized = true;
      return true;
    } catch (error) {
      console.error('DYMO init failed:', error);
      return false;
    }
  },

  _getFramework() {
    const framework = window.dymo?.label?.framework;
    if (!framework) {
      throw new Error('DYMO Connect Framework ist nicht verfuegbar.');
    }
    if (!this.init()) {
      throw new Error('DYMO Connect konnte nicht initialisiert werden.');
    }
    return framework;
  },

  _getPrinter(framework) {
    const printers = framework.getPrinters();
    const labelWriters = Array.isArray(printers)
      ? printers.filter(printer => printer?.printerType === 'LabelWriterPrinter')
      : [];
    const connectedPrinter = labelWriters.find(printer => printer?.isConnected !== false);
    const printer = connectedPrinter || labelWriters[0] || null;
    if (!printer?.name) {
      throw new Error('Kein verbundener DYMO LabelWriter gefunden.');
    }
    return printer;
  },

  _buildSecondaryLine(article) {
    return [article?.manufacturer, article?.model]
      .map(value => String(value ?? '').trim())
      .filter(Boolean)
      .join(' ');
  },

  _buildPrintParamsXml() {
    const framework = window.dymo?.label?.framework;
    if (typeof framework?.createLabelWriterPrintParamsXml === 'function') {
      return framework.createLabelWriterPrintParamsXml({ copies: 1 });
    }
    return `<?xml version="1.0" encoding="utf-8"?>
<LabelWriterPrintParams>
  <Copies>1</Copies>
</LabelWriterPrintParams>`;
  },

  async _createQrImageBase64(qrText, size = 240) {
    if (!window.QRCode || !document?.body) {
      throw new Error('QRCode.js ist nicht verfuegbar.');
    }
    const value = String(qrText ?? '').trim();
    if (!value) {
      throw new Error('QR-Inhalt ist leer.');
    }

    const host = document.createElement('div');
    host.style.position = 'fixed';
    host.style.left = '-99999px';
    host.style.top = '0';
    host.style.width = `${size}px`;
    host.style.height = `${size}px`;
    host.style.pointerEvents = 'none';
    host.style.opacity = '0';
    document.body.appendChild(host);

    try {
      new QRCode(host, {
        text         : value,
        width        : size,
        height       : size,
        colorDark    : '#000000',
        colorLight   : '#ffffff',
        correctLevel : QRCode.CorrectLevel.M,
      });

      await new Promise(resolve => setTimeout(resolve, 30));

      const canvas = host.querySelector('canvas');
      const img    = host.querySelector('img');
      const dataUrl = canvas?.toDataURL('image/png') || img?.src || '';
      const base64 = String(dataUrl).split(',')[1] || '';

      if (!base64) {
        throw new Error('QR-Bild konnte nicht erzeugt werden.');
      }

      return base64;
    } finally {
      host.remove();
    }
  },

  _buildLabelXml() {
    return `<?xml version="1.0" encoding="utf-8"?>
<DesktopLabel Version="1">
  <DYMOLabel Version="4">
    <Description>DYMO Label</Description>
    <Orientation>Landscape</Orientation>
    <LabelName>StorageS0722440</LabelName>
    <InitialLength>0</InitialLength>
    <BorderStyle>SolidLine</BorderStyle>
    <DYMORect>
      <DYMOPoint>
        <X>0.22</X>
        <Y>0.05666666</Y>
      </DYMOPoint>
      <Size>
        <Width>2.47</Width>
        <Height>2.03</Height>
      </Size>
    </DYMORect>
    <BorderColor>
      <SolidColorBrush>
        <Color A="1" R="0" G="0" B="0"></Color>
      </SolidColorBrush>
    </BorderColor>
    <BorderThickness>1</BorderThickness>
    <Show_Border>False</Show_Border>
    <HasFixedLength>False</HasFixedLength>
    <FixedLengthValue>0</FixedLengthValue>
    <DynamicLayoutManager>
      <RotationBehavior>ClearObjects</RotationBehavior>
      <LabelObjects>
        <TextObject>
          <Name>ITextObject0</Name>
          <Brushes>
            <BackgroundBrush>
              <SolidColorBrush>
                <Color A="0" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </BackgroundBrush>
            <BorderBrush>
              <SolidColorBrush>
                <Color A="1" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </BorderBrush>
            <StrokeBrush>
              <SolidColorBrush>
                <Color A="1" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </StrokeBrush>
            <FillBrush>
              <SolidColorBrush>
                <Color A="0" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </FillBrush>
          </Brushes>
          <Rotation>Rotation270</Rotation>
          <OutlineThickness>1</OutlineThickness>
          <IsOutlined>False</IsOutlined>
          <BorderStyle>SolidLine</BorderStyle>
          <Margin>
            <DYMOThickness Left="0" Top="0" Right="0" Bottom="0" />
          </Margin>
          <HorizontalAlignment>Center</HorizontalAlignment>
          <VerticalAlignment>Middle</VerticalAlignment>
          <FitMode>AlwaysFit</FitMode>
          <IsVertical>False</IsVertical>
          <FormattedText>
            <FitMode>AlwaysFit</FitMode>
            <HorizontalAlignment>Center</HorizontalAlignment>
            <VerticalAlignment>Middle</VerticalAlignment>
            <IsVertical>False</IsVertical>
            <LineTextSpan>
              <TextSpan>
                <Text>A-0000</Text>
                <FontInfo>
                  <FontName>Segoe UI Symbol</FontName>
                  <FontSize>34</FontSize>
                  <IsBold>True</IsBold>
                  <IsItalic>False</IsItalic>
                  <IsUnderline>False</IsUnderline>
                  <FontBrush>
                    <SolidColorBrush>
                      <Color A="1" R="0" G="0" B="0"></Color>
                    </SolidColorBrush>
                  </FontBrush>
                </FontInfo>
              </TextSpan>
            </LineTextSpan>
          </FormattedText>
          <ObjectLayout>
            <DYMOPoint>
              <X>0.18</X>
              <Y>0.12</Y>
            </DYMOPoint>
            <Size>
              <Width>0.82</Width>
              <Height>1.78</Height>
            </Size>
          </ObjectLayout>
        </TextObject>
        <LineObject>
          <Name>ILineObject0</Name>
          <Brushes>
            <BackgroundBrush>
              <SolidColorBrush>
                <Color A="0" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </BackgroundBrush>
            <BorderBrush>
              <SolidColorBrush>
                <Color A="1" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </BorderBrush>
            <StrokeBrush>
              <SolidColorBrush>
                <Color A="1" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </StrokeBrush>
            <FillBrush>
              <SolidColorBrush>
                <Color A="0" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </FillBrush>
          </Brushes>
          <Rotation>Rotation0</Rotation>
          <OutlineThickness>1</OutlineThickness>
          <IsOutlined>False</IsOutlined>
          <BorderStyle>SolidLine</BorderStyle>
          <Margin>
            <DYMOThickness Left="0" Top="0" Right="0" Bottom="0" />
          </Margin>
          <StrokeWidth>1</StrokeWidth>
          <DashPattern>SolidLine</DashPattern>
          <LineType>Vertical</LineType>
          <ObjectLayout>
            <DYMOPoint>
              <X>0.93</X>
              <Y>0.12</Y>
            </DYMOPoint>
            <Size>
              <Width>0.06</Width>
              <Height>1.78</Height>
            </Size>
          </ObjectLayout>
        </LineObject>
        <QRCodeObject>
          <Name>IQRCodeObject0</Name>
          <Brushes>
            <BackgroundBrush>
              <SolidColorBrush>
                <Color A="1" R="1" G="1" B="1"></Color>
              </SolidColorBrush>
            </BackgroundBrush>
            <BorderBrush>
              <SolidColorBrush>
                <Color A="1" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </BorderBrush>
            <StrokeBrush>
              <SolidColorBrush>
                <Color A="1" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </StrokeBrush>
            <FillBrush>
              <SolidColorBrush>
                <Color A="1" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </FillBrush>
          </Brushes>
          <Rotation>Rotation0</Rotation>
          <OutlineThickness>1</OutlineThickness>
          <IsOutlined>False</IsOutlined>
          <BorderStyle>SolidLine</BorderStyle>
          <Margin>
            <DYMOThickness Left="0" Top="0" Right="0" Bottom="0" />
          </Margin>
          <BarcodeFormat>QRCode</BarcodeFormat>
          <Data>
            <DataString>A-0000</DataString>
          </Data>
          <HorizontalAlignment>Center</HorizontalAlignment>
          <VerticalAlignment>Middle</VerticalAlignment>
          <Size>AutoFit</Size>
          <EQRCodeType>QRCodeText</EQRCodeType>
          <ObjectLayout>
            <DYMOPoint>
              <X>1.12</X>
              <Y>0.16</Y>
            </DYMOPoint>
            <Size>
              <Width>1.12</Width>
              <Height>1.12</Height>
            </Size>
          </ObjectLayout>
        </QRCodeObject>
        <TextObject>
          <Name>ITextObject1</Name>
          <Brushes>
            <BackgroundBrush>
              <SolidColorBrush>
                <Color A="0" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </BackgroundBrush>
            <BorderBrush>
              <SolidColorBrush>
                <Color A="1" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </BorderBrush>
            <StrokeBrush>
              <SolidColorBrush>
                <Color A="1" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </StrokeBrush>
            <FillBrush>
              <SolidColorBrush>
                <Color A="0" R="0" G="0" B="0"></Color>
              </SolidColorBrush>
            </FillBrush>
          </Brushes>
          <Rotation>Rotation0</Rotation>
          <OutlineThickness>1</OutlineThickness>
          <IsOutlined>False</IsOutlined>
          <BorderStyle>SolidLine</BorderStyle>
          <Margin>
            <DYMOThickness Left="0" Top="0" Right="0" Bottom="0" />
          </Margin>
          <HorizontalAlignment>Center</HorizontalAlignment>
          <VerticalAlignment>Middle</VerticalAlignment>
          <FitMode>AlwaysFit</FitMode>
          <IsVertical>False</IsVertical>
          <FormattedText>
            <FitMode>AlwaysFit</FitMode>
            <HorizontalAlignment>Center</HorizontalAlignment>
            <VerticalAlignment>Middle</VerticalAlignment>
            <IsVertical>False</IsVertical>
            <LineTextSpan>
              <TextSpan>
                <Text>Hersteller Modell</Text>
                <FontInfo>
                  <FontName>Segoe UI Symbol</FontName>
                  <FontSize>10.5</FontSize>
                  <IsBold>False</IsBold>
                  <IsItalic>False</IsItalic>
                  <IsUnderline>False</IsUnderline>
                  <FontBrush>
                    <SolidColorBrush>
                      <Color A="1" R="0" G="0" B="0"></Color>
                    </SolidColorBrush>
                  </FontBrush>
                </FontInfo>
              </TextSpan>
            </LineTextSpan>
          </FormattedText>
          <ObjectLayout>
            <DYMOPoint>
              <X>1.02</X>
              <Y>1.40</Y>
            </DYMOPoint>
            <Size>
              <Width>1.32</Width>
              <Height>0.40</Height>
            </Size>
          </ObjectLayout>
        </TextObject>
      </LabelObjects>
    </DynamicLayoutManager>
  </DYMOLabel>
  <LabelApplication>Blank</LabelApplication>
  <DataTable>
    <Columns></Columns>
    <Rows></Rows>
  </DataTable>
</DesktopLabel>`;
  },

  _applyLabelValues(label, article, qrText) {
    if (typeof label?.setObjectText !== 'function') {
      throw new Error('DYMO-Labelobjekt unterstützt kein setObjectText().');
    }
    label.setObjectText('ITextObject0', String(article?.id ?? '').trim() || '-');
    label.setObjectText('ITextObject1', this._buildSecondaryLine(article) || ' ');
    label.setObjectText('IQRCodeObject0', String(qrText ?? '').trim());
  },

  _extractErrorMessage(error) {
    const rawMessage = String(error?.message ?? error ?? '').trim();
    return rawMessage || 'Unbekannter DYMO-Fehler.';
  },

  async printArticles(articles) {
    const list = Array.isArray(articles) ? articles.filter(Boolean) : [];
    if (!list.length) return { ok: true, count: 0, printerName: '' };
    let printedCount = 0;
    let printerName = '';
    try {
      const framework = this._getFramework();
      const printer = this._getPrinter(framework);
      printerName = printer.name;
      for (const article of list) {
        const qrText = QRManager.getArticleQrText(article);
        if (!qrText) {
          throw new Error(`Fuer Artikel ${article.id} ist kein QR-Inhalt verfuegbar.`);
        }
        const labelXml = this._buildLabelXml();
        const label = framework.openLabelXml(labelXml);
        this._applyLabelValues(label, article, qrText);
        if (typeof label?.isValidLabel === 'function' && !label.isValidLabel()) {
          throw new Error(`DYMO-Label fuer Artikel ${article.id} ist ungueltig.`);
        }
        if (typeof label?.print === 'function') {
          label.print(printer.name, this._buildPrintParamsXml(), '');
        } else {
          framework.printLabel(
            printer.name,
            this._buildPrintParamsXml(),
            typeof label?.getLabelXml === 'function' ? label.getLabelXml() : labelXml,
            ''
          );
        }
        printedCount++;
      }
      return { ok: true, count: printedCount, printerName };
    } catch (error) {
      console.error('DYMO print failed:', error);
      return {
        ok         : false,
        printedCount,
        printerName,
        message    : this._extractErrorMessage(error),
      };
    }
  },
};

const ScanResolver = {

  resolve(rawValue) {
    const value = String(rawValue ?? '').trim();
    if (!value) return { type: 'unknown', value: '' };

    const location = QRManager.parseLocationCode(value);
    if (location) {
      return { type: 'location', value, location };
    }

    const internalArticle = DB.getArticleById(value);
    if (internalArticle) {
      return { type: 'article-internal', value, article: internalArticle };
    }

    const externalArticle = DB.getArticleByExternalQrCode(value);
    if (externalArticle) {
      return { type: 'article-external', value, article: externalArticle };
    }

    const publicQrUrl = PublicQr.parseUrl(value);
    if (publicQrUrl) {
      const publicArticle = DB.getArticleByPublicQrToken(publicQrUrl.token);
      if (publicArticle) {
        return {
          type: 'article-public-url',
          value,
          article: publicArticle,
          publicQrToken: publicQrUrl.token,
          publicQrUrl: publicQrUrl.url,
        };
      }
      return {
        type: 'unknown',
        value,
        reason: 'public-url-not-found',
        publicQrToken: publicQrUrl.token,
        publicQrUrl: publicQrUrl.url,
      };
    }

    const listingArticle = DB.getArticleByListingLink(value);
    if (listingArticle) {
      return { type: 'article-listing', value, article: listingArticle };
    }

    if (this.isLikelyListingCode(value)) {
      return { type: 'unknown', value, reason: 'listing' };
    }

    return { type: 'unknown', value };
  },

  isLikelyListingCode(rawValue, excludeArticleId = null) {
    const value = String(rawValue ?? '').trim();
    if (!value) return false;
    if (PublicQr.parseUrl(value)) return false;
    const lowerValue = value.toLowerCase();
    if (
      lowerValue.includes('kleinanzeigen.de') ||
      lowerValue.includes('ebay-kleinanzeigen.de')
    ) {
      return true;
    }
    return !!DB.getArticleByListingLink(value, excludeArticleId);
  },

  articleReference(article) {
    if (!article) return '';
    const name = Utils.articleDisplayName(article, article.id);
    return `${article.id} (${name})`;
  },

  articleSourceLabel(type) {
    return ({
      'article-internal' : 'Erkannt über interne Artikel-ID',
      'article-external' : 'Erkannt über Fremd-QR-Code',
      'article-public-url': 'Erkannt über öffentlichen 2-in-1-QR-Code',
      'article-listing'  : 'Erkannt über Kleinanzeigen-Link',
    })[type] ?? 'Artikel erkannt';
  },

  unknownMessage(rawValue, resolution = null, context = 'default') {
    const value = String(rawValue ?? '').trim();
    const resolved = resolution ?? this.resolve(value);
    if (resolved.reason === 'public-url-not-found') {
      return `Öffentliche QR-URL erkannt, aber kein Artikel mit Token ${resolved.publicQrToken} gefunden.`;
    }
    if (resolved.reason === 'listing') {
      if (context === 'relocate') {
        return 'Kleinanzeigen-QR-Codes sind hier kein gültiger Artikel- oder Standort-Scan.';
      }
      if (context === 'scanner') {
        return 'Kleinanzeigen-QR-Codes werden im Scanner nicht als Artikel erkannt.';
      }
      return 'Kleinanzeigen-QR-Codes werden nicht als Artikelkennung verwendet.';
    }
    if (context === 'relocate') {
      return 'Code „' + value + '" ist weder ein Artikel noch ein Standort-QR.';
    }
    return 'Code „' + value + '" wurde nicht erkannt.';
  },

  validateExternalQrCode(rawValue, currentArticleId = null) {
    const value = String(rawValue ?? '').trim();
    if (!value) return { ok: true, value: '' };

    const currentArticle = currentArticleId ? DB.getArticleById(currentArticleId) : null;
    if (currentArticle && String(currentArticle.externalQrCode ?? '').trim() === value) {
      return { ok: true, value };
    }

    if (PublicQr.parseUrl(value)) {
      return {
        ok: false,
        message: 'Öffentliche 2-in-1-QR-URLs können nicht als Fremd-QR-Code gespeichert werden.',
      };
    }

    const resolution = this.resolve(value);
    if (resolution.type === 'location') {
      return {
        ok: false,
        message: 'Standort-QRs können nicht als Fremd-QR-Code gespeichert werden.',
      };
    }

    if (resolution.type === 'article-internal') {
      return {
        ok: false,
        message: 'Interne MöbelWawi-Codes können nicht als Fremd-QR-Code gespeichert werden.',
      };
    }

    if (resolution.type === 'article-public-url') {
      return {
        ok: false,
        message: 'Öffentliche 2-in-1-QR-URLs können nicht als Fremd-QR-Code gespeichert werden.',
      };
    }

    if (resolution.type === 'article-listing') {
      return {
        ok: false,
        message: 'Kleinanzeigen-QR-Codes oder Kleinanzeigen-Links können hier nicht verwendet werden.',
      };
    }

    if (resolution.type === 'article-external' && resolution.article.id !== currentArticleId) {
      return {
        ok: false,
        duplicateArticle: resolution.article,
        message:
          'Dieser Fremd-QR-Code ist bereits bei Artikel '
          + this.articleReference(resolution.article)
          + ' hinterlegt.',
      };
    }

    if (this.isLikelyListingCode(value, currentArticleId)) {
      return {
        ok: false,
        message: 'Kleinanzeigen-QR-Codes oder Kleinanzeigen-Links können hier nicht verwendet werden.',
      };
    }

    return { ok: true, value };
  },
};

/* ============================================================
   9. DASHBOARD
============================================================ */
const Dashboard = {

  init() {
    this.initFormTabs();
    this.initArticleForm();
    this.initGroupForm();
    this.populateArticleCategoryDropdown();
    this.populateGroupDropdown();
  },

  populateArticleCategoryDropdown(selectedValue = '') {
    const sel = document.getElementById('art-category');
    if (!sel) return;

    sel.querySelector('optgroup[data-dynamic-categories="1"]')?.remove();

    const existingValues = new Set(
      Array.from(sel.querySelectorAll('option'))
        .map(option => String(option.value ?? '').trim())
        .filter(Boolean)
    );

    const desiredValue = String(selectedValue ?? '').trim();
    const dynamicValues = [...new Set(
      DB.getArticles()
        .map(article => String(article.category ?? '').trim())
        .filter(Boolean)
    )]
      .filter(value => !existingValues.has(value));

    if (desiredValue && !existingValues.has(desiredValue) && !dynamicValues.includes(desiredValue)) {
      dynamicValues.push(desiredValue);
    }

    if (!dynamicValues.length) return;

    dynamicValues.sort((a, b) => a.localeCompare(b, 'de-DE'));

    const group = document.createElement('optgroup');
    group.label = 'Weitere Kategorien';
    group.dataset.dynamicCategories = '1';

    dynamicValues.forEach(value => {
      const option = document.createElement('option');
      option.value = value;
      option.textContent = value;
      group.appendChild(option);
    });

    sel.appendChild(group);
  },

  populateGroupDropdown(selectedValue = null) {
    const sel    = document.getElementById('art-group-assign');
    if (!sel) return;
    const fallbackValue = document.getElementById('article-edit-group-id')?.value ?? '';
    const desiredValue = selectedValue ?? sel.value ?? fallbackValue ?? '';
    const groups = DB.getGroups().filter(g => g.status !== 'Entsorgt');
    sel.innerHTML =
      `<option value="">â€“ Automatisch zuordnen â€“</option>` +
      groups.map(g => {
        const label = g.name
          ? `${g.id} Â· ${g.name.substring(0, 35)}${g.name.length > 35 ? 'â€¦' : ''}`
          : g.id;
        return `<option value="${g.id}">${Utils.escHtml(label)}</option>`;
      }).join('');
    sel.value = groups.some(group => group.id === desiredValue) ? desiredValue : '';
  },

  renderStats() {
    const s = DB.getStats();
    document.getElementById('stat-total').textContent     = s.total;
    document.getElementById('stat-available').textContent = s.available;
    document.getElementById('stat-reserved').textContent  = s.reserved;
    document.getElementById('stat-revenue').textContent   = Utils.formatEuro(s.revenue);
    document.getElementById('db-stats').textContent       =
      `${s.total} Artikel Â· ${DB.getGroups().length} Gruppen`;
    this.populateArticleCategoryDropdown();
    this.populateGroupDropdown();
  },

  initFormTabs() {
    document.querySelectorAll('.form-tab').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.form-tab').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById(btn.dataset.tab).classList.add('active');
      });
    });
  },

  initArticleForm() {
    document.querySelectorAll('.condition-btn').forEach(label => {
      label.addEventListener('click', () => {
        document.querySelectorAll('.condition-btn')
          .forEach(l => l.classList.remove('selected'));
        label.classList.add('selected');
      });
    });

    document.getElementById('art-status')
      .addEventListener('change', e => this.toggleSoldFieldsArticle(e.target.value));

    document.getElementById('art-shipping')
      .addEventListener('change', e => {
        document.getElementById('shipping-cost-wrapper').style.display =
          e.target.checked ? 'flex' : 'none';
      });

    document.getElementById('art-purchase-price')
      .addEventListener('input', e => {
        const n = parseFloat(e.target.value);
        document.getElementById('art-purchase-price-gross').value = isNaN(n) ? '' : (n * 1.19).toFixed(2);
      });
    document.getElementById('art-purchase-price-gross')
      .addEventListener('input', e => {
        const g = parseFloat(e.target.value);
        document.getElementById('art-purchase-price').value = isNaN(g) ? '' : (g / 1.19).toFixed(2);
      });

    document.getElementById('art-original-price')
      .addEventListener('input', e => {
        const n = parseFloat(e.target.value);
        document.getElementById('art-original-price-gross').value = isNaN(n) ? '' : (n * 1.19).toFixed(2);
      });
    document.getElementById('art-original-price-gross')
      .addEventListener('input', e => {
        const g = parseFloat(e.target.value);
        document.getElementById('art-original-price').value = isNaN(g) ? '' : (g / 1.19).toFixed(2);
      });

    document.getElementById('art-sold-price')
      .addEventListener('input', e => {
        const n = parseFloat(e.target.value);
        document.getElementById('art-sold-price-gross').value = isNaN(n) ? '' : (n * 1.19).toFixed(2);
      });
    document.getElementById('art-sold-price-gross')
      .addEventListener('input', e => {
        const g = parseFloat(e.target.value);
        document.getElementById('art-sold-price').value = isNaN(g) ? '' : (g / 1.19).toFixed(2);
      });

    document.getElementById('art-quantity')
      .addEventListener('input', e => this.updateQtyHint(parseInt(e.target.value) || 1));

    document.getElementById('art-listing-link')
      .addEventListener('input', () => this.refreshArticleQrPreview());

    document.getElementById('art-group-assign')
      .addEventListener('change', () => {
        State.articleGroupSelectionTouched = true;
      });

    document.getElementById('art-external-qr-code')
      .addEventListener('keydown', e => {
        if (e.key === 'Enter') e.preventDefault();
      });

    document.getElementById('art-photos')
      .addEventListener('change', async e => {
        await this.handlePhotoUpload(e.target.files);
        e.target.value = '';
      });

    this.bindFileDropzone('art-photos-dropzone', 'art-photos', files => this.handlePhotoUpload(files));

    document.getElementById('article-form')
      .addEventListener('submit', e => { e.preventDefault(); this.saveArticle(); });

    document.getElementById('btn-reset-article')
      .addEventListener('click', () => this.resetArticleForm());

    document.getElementById('btn-apply-article-listing-to-group')
      .addEventListener('click', () => this.applyArticleListingLinkToGroup());

    document.getElementById('btn-print-qr')
      .addEventListener('click', () => {
        const qrArticle = this.getCurrentArticleQrData();
        if (State.editingArticleId && qrArticle) QRManager.printQR(State.editingArticleId, qrArticle);
      });
    const cancelArtBtn = document.getElementById('btn-cancel-article');
    if (cancelArtBtn) {
      cancelArtBtn.addEventListener('click', () => {
        const articleId = State.editingArticleId;
        const returnGroupId = State.articleReturnGroupId;
        this.resetArticleForm();
        if (returnGroupId) {
          Router.navigate('groups');
          setTimeout(() => Groups.openDetail(returnGroupId), 80);
        } else if (articleId) {
          Router.navigate('inventory');
        }
      });
    }
  },

  updateQtyHint(qty) {
    let hint = document.getElementById('qty-hint-banner');

    if (qty <= 1) {
      if (hint) hint.remove();
      return;
    }

    if (!hint) {
      hint    = document.createElement('div');
      hint.id = 'qty-hint-banner';
      hint.style.cssText = `
        margin-top:6px; padding:8px 12px;
        background:var(--color-primary-light);
        border:1px solid var(--color-primary);
        border-radius:var(--border-radius-sm);
        font-size:var(--font-size-xs);
        color:var(--color-primary);
        font-weight:600;
        display:flex; align-items:center; gap:7px;
      `;
      const qtyGroup = document.getElementById('art-quantity').closest('.form-group');
      qtyGroup.insertAdjacentElement('afterend', hint);
    }

    if (State.editingArticleId) {
      hint.innerHTML = `
        <i class="fa-solid fa-circle-info"></i>
        Es werden <strong>${qty - 1} Duplikat(e)</strong> mit neuen fortlaufenden IDs angelegt.
        Der Originalartikel (${Utils.escHtml(State.editingArticleId)}) bleibt erhalten.
      `;
    } else {
      hint.innerHTML = `
        <i class="fa-solid fa-circle-info"></i>
        Es werden <strong>${qty} separate Artikel</strong> mit fortlaufenden IDs angelegt
        (je StÃ¼ckzahl 1). Alle Kopien erhalten gleiche Daten und werden automatisch
        derselben Gruppe zugeordnet.
      `;
    }
  },

  toggleSoldFieldsArticle(status) {
    const sf = document.getElementById('sold-fields-article');
    sf.style.display = status === 'Verkauft' ? 'block' : 'none';
    document.getElementById('art-sold-price').required = status === 'Verkauft';
    document.getElementById('art-sold-date').required  = status === 'Verkauft';
  },

  async handlePhotoUpload(files) {
    const preview = document.getElementById('art-photo-preview');
    for (const file of Array.from(files)) {
      if (State.articlePhotos.length >= 5) {
        Toast.warning('Maximal 5 Fotos erlaubt.');
        break;
      }
      if (!file.type.startsWith('image/')) continue;
      try {
        let base64 = await Utils.resizeImage(file, 1400, 0.88);
        if (base64.length > 400000) base64 = await Utils.resizeImage(file, 1100, 0.80);
        if (base64.length > 250000) base64 = await Utils.resizeImage(file,  800, 0.70);
        State.articlePhotos.push(base64);
      } catch (err) {
        Toast.error('Foto konnte nicht geladen werden.');
      }
    }
    this.renderPhotoPreviews(preview, State.articlePhotos, 'articlePhotos');
  },

  bindFileDropzone(dropzoneId, inputId, onFiles) {
    const dropzone = document.getElementById(dropzoneId);
    const input    = document.getElementById(inputId);
    if (!dropzone || !input || typeof onFiles !== 'function') return;

    const setActive = isActive => dropzone.classList.toggle('drag-active', isActive);

    ['dragenter', 'dragover'].forEach(eventName => {
      dropzone.addEventListener(eventName, e => {
        e.preventDefault();
        e.stopPropagation();
        setActive(true);
      });
    });

    ['dragleave', 'dragend', 'drop'].forEach(eventName => {
      dropzone.addEventListener(eventName, e => {
        e.preventDefault();
        e.stopPropagation();
        if (eventName === 'dragleave' && dropzone.contains(e.relatedTarget)) return;
        setActive(false);
      });
    });

    dropzone.addEventListener('drop', async e => {
      const files = e.dataTransfer?.files;
      if (!files?.length) return;
      await onFiles(files);
      input.value = '';
    });
  },

  renderPhotoPreviews(container, arr, stateKey) {
    container.innerHTML = '';
    arr.forEach((src, idx) => {
      const thumb       = document.createElement('div');
      thumb.className   = 'photo-thumb';
      thumb.innerHTML   = `
        <img src="${src}" alt="Foto ${idx + 1}" loading="lazy"/>
        <button type="button" class="photo-thumb-remove"
                data-idx="${idx}" aria-label="Foto entfernen">
          <i class="fa-solid fa-xmark"></i>
        </button>`;
      thumb.querySelector('.photo-thumb-remove')
        .addEventListener('click', () => {
          State[stateKey].splice(idx, 1);
          this.renderPhotoPreviews(container, State[stateKey], stateKey);
        });
      container.appendChild(thumb);
    });
  },

  validateArticle() {
    if (!document.getElementById('art-category').value) {
      Toast.error('Bitte eine Kategorie wÃ¤hlen.');
      return false;
    }
    if (!document.querySelector('input[name="art-condition"]:checked')) {
      Toast.error('Bitte den Zustand angeben.');
      return false;
    }
    if (document.getElementById('art-status').value === 'Verkauft') {
      if (
        !document.getElementById('art-sold-price').value ||
        !document.getElementById('art-sold-date').value
      ) {
        Toast.error('Bitte Verkaufspreis und Verkaufsdatum angeben.');
        return false;
      }
    }
    return true;
  },

  syncPublicQrFields(article = null) {
    const tokenEl = document.getElementById('art-public-qr-token');
    const urlEl   = document.getElementById('art-public-qr-url');
    if (!tokenEl || !urlEl) return;
    tokenEl.value = article ? PublicQr.getArticleToken(article) : '';
    urlEl.value   = article && QRManager.hasConfiguredPublicBaseUrl()
      ? PublicQr.getArticleUrl(article)
      : '';
  },

  getCurrentArticleQrData(article = null) {
    const baseArticle = article ?? (State.editingArticleId ? DB.getArticleById(State.editingArticleId) : null);
    if (!baseArticle) return null;
    return {
      ...baseArticle,
      listingLink: document.getElementById('art-listing-link').value.trim(),
    };
  },

  refreshArticleQrPreview(article = null) {
    const qrArticle = this.getCurrentArticleQrData(article);
    this.syncPublicQrFields(qrArticle);
    const previewEl = document.getElementById('art-qr-preview');
    if (!previewEl) return;
    previewEl.innerHTML = '';
    if (qrArticle) QRManager.generate('art-qr-preview', QRManager.getArticleQrText(qrArticle), 128);
  },

  isDymoAutoPrintEnabled() {
    const checkbox = document.getElementById('art-dymo-auto-print');
    return checkbox ? checkbox.checked : true;
  },

  resetDymoAutoPrintOption() {
    const checkbox = document.getElementById('art-dymo-auto-print');
    if (checkbox) checkbox.checked = true;
  },

  async autoPrintArticlesOnDymo(articles) {
    const list = Array.isArray(articles) ? articles.filter(Boolean) : [];
    if (!list.length) return;
    const result = await DymoManager.printArticles(list);
    if (!result.ok) {
      const prefix = result.printedCount
        ? `${result.printedCount} DYMO-Etikett(en) wurden gesendet, danach trat ein Fehler auf`
        : 'DYMO-Autodruck nicht moeglich';
      Toast.warning(result.message ? `${prefix}: ${result.message}` : prefix + '.');
      return;
    }
    Toast.success(`${result.count} DYMO-Etikett(e) an ${result.printerName} gesendet.`);
  },

  applyArticleListingLinkToGroup() {
    const listingInput = document.getElementById('art-listing-link');
    const listingLink  = listingInput.value.trim();
    if (!listingLink) {
      Toast.warning('Bitte zuerst einen Kleinanzeigen-Link eingeben.');
      listingInput.focus();
      return;
    }

    const groupSelect = document.getElementById('art-group-assign');
    const groupId = groupSelect.value
      || document.getElementById('article-edit-group-id').value
      || DB.getArticleById(State.editingArticleId)?.groupId
      || null;

    if (!groupId) {
      Toast.warning('Bitte zuerst eine Gruppe auswählen oder einen Gruppenartikel öffnen.');
      groupSelect.focus();
      return;
    }

    const group = DB.getGroupById(groupId);
    if (!group) {
      Toast.error('Gruppe wurde nicht gefunden.');
      return;
    }

    const articles = DB.getArticlesByGroup(groupId);
    if (!articles.length) {
      Toast.warning('Keine Artikel in dieser Gruppe.');
      return;
    }

    DB.updateGroup(groupId, { listingLink });
    DB.updateArticles(articles.map(article => article.id), { listingLink });
    this.refreshArticleQrPreview();
    Toast.success('Kleinanzeigen-Link bei ' + articles.length + ' Artikel(n) der Gruppe übernommen.');
  },

  async saveArticle() {
    if (!this.validateArticle()) return;
    try {
    const condEl = document.querySelector('input[name="art-condition"]:checked');
    const editId = State.editingArticleId;
    const qty    = parseInt(document.getElementById('art-quantity').value) || 1;
    const dymoAutoPrintEnabled = this.isDymoAutoPrintEnabled();
    const selectedGroupId = String(document.getElementById('art-group-assign').value ?? '').trim();
    const storedEditGroupId = String(document.getElementById('article-edit-group-id').value ?? '').trim();
    const currentArticleGroupId = editId
      ? String(DB.getArticleById(editId)?.groupId ?? '').trim()
      : '';
    const effectiveGroupId = editId
      ? (selectedGroupId || currentArticleGroupId || storedEditGroupId || null)
      : (selectedGroupId || null);
    const externalQrValidation = ScanResolver.validateExternalQrCode(
      document.getElementById('art-external-qr-code').value,
      editId
    );
    if (!externalQrValidation.ok) {
      Toast.error(externalQrValidation.message);
      document.getElementById('art-external-qr-code').focus();
      return;
    }
    if (qty > 1 && externalQrValidation.value) {
      Toast.error('Ein Fremd-QR-Code kann nur einem einzelnen Artikel zugeordnet werden. Bitte StÃ¼ckzahl 1 verwenden oder die Serienzuordnung in der Gruppe nutzen.');
      document.getElementById('art-external-qr-code').focus();
      return;
    }
    const data = {
      status        : document.getElementById('art-status').value,
      category      : document.getElementById('art-category').value,
      manufacturer  : document.getElementById('art-manufacturer').value.trim(),
      model         : document.getElementById('art-model').value.trim(),
      location      : document.getElementById('art-location').value.trim(),
      externalQrCode: externalQrValidation.value || null,
      condition     : condEl?.value ?? '',
      material      : document.getElementById('art-material').value.trim(),
      style         : document.getElementById('art-style').value.trim(),
      color         : document.getElementById('art-color').value.trim(),
      width         : parseFloat(document.getElementById('art-width').value)  || null,
      depth         : parseFloat(document.getElementById('art-depth').value)  || null,
      height        : parseFloat(document.getElementById('art-height').value) || null,
      purchasePrice : parseFloat(document.getElementById('art-purchase-price').value)       || null,
      purchasePriceGross: parseFloat(document.getElementById('art-purchase-price-gross').value) || null,
      originalPrice : parseFloat(document.getElementById('art-original-price').value)       || null,
      originalPriceGross: parseFloat(document.getElementById('art-original-price-gross').value) || null,
      publicQrToken : document.getElementById('art-public-qr-token').value.trim() || null,
      listingLink   : document.getElementById('art-listing-link').value.trim(),
      pickupZip     : document.getElementById('art-pickup-zip').value.trim(),
      shipping      : document.getElementById('art-shipping').checked,
      shippingCost  : parseFloat(document.getElementById('art-shipping-cost').value)  || null,
      photos        : [...State.articlePhotos],
      notes         : document.getElementById('art-notes').value.trim(),
      soldPrice     : parseFloat(document.getElementById('art-sold-price').value)          || null,
      soldPriceGross: parseFloat(document.getElementById('art-sold-price-gross').value)    || null,
      soldDate      : document.getElementById('art-sold-date').value                  || null,
      groupId       : effectiveGroupId,
    };

    if (editId) {
      const dataForOriginal = { ...data, quantity: 1 };
      const saved = DB.updateArticle(editId, dataForOriginal);
      if (!saved) return;
      const returnGroupId = State.articleReturnGroupId;
      let targetGroupId = saved.groupId || returnGroupId || null;

      let dupIds = [];
      if (qty > 1) {
        const dupData = { ...dataForOriginal, groupId: saved.groupId };
        const dups = await DB.saveBulkArticles(dupData, qty - 1);
        dupIds = dups.map(d => d.id);
      }

      const allIds = [editId, ...dupIds];
      if (!saved.groupId) {
        const group = await DB.autoAssignGroup(allIds, saved);
        targetGroupId = group?.id || targetGroupId;
        if (qty > 1) {
          Toast.success('Artikel ' + editId + ' aktualisiert + ' + (qty - 1) + ' Duplikat(e) angelegt Â· Gruppe "' + Utils.escHtml(group.name) + '".');
        } else {
          Toast.success('Artikel ' + editId + ' aktualisiert Â· Gruppe "' + Utils.escHtml(group.name) + '".');
        }
      } else {
        if (qty > 1) {
          Toast.success('Artikel ' + editId + ' aktualisiert + ' + (qty - 1) + ' Duplikat(e) angelegt.');
        } else {
          Toast.success('Artikel ' + editId + ' aktualisiert.');
        }
      }
      if (dymoAutoPrintEnabled && dupIds.length) {
        await this.autoPrintArticlesOnDymo(
          dupIds.map(id => DB.getArticleById(id)).filter(Boolean)
        );
      }
      document.getElementById('art-id-display').value         = saved.id;
      document.getElementById('art-qr-section').style.display = 'block';
      State.editingArticleId = saved.id;
      this.refreshArticleQrPreview(saved);
      this.resetDymoAutoPrintOption();
      this.renderStats();
      const savedId = saved.id;
      setTimeout(() => {
        if (returnGroupId) {
          Router.navigate('groups');
          setTimeout(() => {
            Groups.openDetail(targetGroupId || returnGroupId);
            setTimeout(() => {
              const card = document.querySelector('[data-article-id="' + CSS.escape(savedId) + '"]');
              if (card) {
                card.scrollIntoView({ behavior: 'smooth', block: 'center' });
                card.classList.add('highlight-new');
                setTimeout(() => card.classList.remove('highlight-new'), 1800);
              }
            }, 160);
          }, 80);
        } else {
          Router.navigate('inventory');
          setTimeout(() => {
            const card = document.querySelector('[data-id="' + CSS.escape(savedId) + '"]');
            if (card) {
              card.scrollIntoView({ behavior: 'smooth', block: 'center' });
              card.classList.add('highlight-new');
              setTimeout(() => card.classList.remove('highlight-new'), 1800);
            }
          }, 200);
        }
      }, 400);
      return;
    }

    const savedArticles = qty > 1
      ? await DB.saveBulkArticles(data, qty)
      : [await DB.saveArticle({ ...data, quantity: 1 })];
    const articleIds = savedArticles.map(a => a.id);
    const group      = await DB.autoAssignGroup(articleIds, data);
    const firstId    = articleIds[0];
    const firstArticle = savedArticles[0] ?? DB.getArticleById(firstId);
    document.getElementById('art-id-display').value         = firstId;
    document.getElementById('art-qr-section').style.display = 'block';
    State.editingArticleId = firstId;
    this.refreshArticleQrPreview(firstArticle);
    document.getElementById('qty-hint-banner')?.remove();
    if (qty > 1) {
      Toast.success(qty + ' Artikel angelegt (' + articleIds[0] + '-' + articleIds[articleIds.length - 1] + ') Â· Gruppe "' + Utils.escHtml(group.name) + '".');
    } else {
      Toast.success('Artikel ' + firstId + ' gespeichert Â· Gruppe "' + Utils.escHtml(group.name) + '".');
    }
    if (dymoAutoPrintEnabled) {
      await this.autoPrintArticlesOnDymo(
        articleIds.map(id => DB.getArticleById(id)).filter(Boolean)
      );
    }
    this.resetDymoAutoPrintOption();
    this.renderStats();
    } catch (err) {
      console.error('saveArticle failed:', err);
      Toast.error(err?.message || 'Speichern fehlgeschlagen. Bitte erneut versuchen.');
    }
  },

  resetArticleForm() {
    document.getElementById('article-form').reset();
    document.getElementById('art-id-display').value         = '';
    document.getElementById('art-qr-section').style.display = 'none';
    document.getElementById('art-photo-preview').innerHTML  = '';
    document.getElementById('sold-fields-article').style.display    = 'none';
    document.getElementById('shipping-cost-wrapper').style.display  = 'none';
    document.getElementById('art-qr-preview').innerHTML     = '';
    this.resetDymoAutoPrintOption();
    document.getElementById('article-edit-group-id').value  = '';
    document.querySelectorAll('.condition-btn')
      .forEach(l => l.classList.remove('selected'));
    document.getElementById('qty-hint-banner')?.remove();
    this.syncPublicQrFields(null);
    State.editingArticleId = null;
    State.articleReturnGroupId = null;
    State.articleGroupSelectionTouched = false;
    State.articlePhotos    = [];
  },

  loadArticleIntoForm(id) {
    const a = DB.getArticleById(id);
    if (!a) return;
    const returnGroupId = State.currentView === 'groups'
      && !document.getElementById('group-detail-view').classList.contains('hidden')
      ? Groups._currentGroupId
      : null;

    this.resetArticleForm();
    this.populateArticleCategoryDropdown(a.category);
    this.populateGroupDropdown(a.groupId ?? '');
    State.editingArticleId = id;
    State.articleReturnGroupId = returnGroupId;
    State.articleGroupSelectionTouched = false;

    document.querySelectorAll('.form-tab').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    document.querySelector('[data-tab="tab-article"]').classList.add('active');
    document.getElementById('tab-article').classList.add('active');

    document.getElementById('art-id-display').value    = a.id;
    document.getElementById('art-status').value        = Utils.normalizeStatus(a.status) || 'Verf\u00fcgbar';
    document.getElementById('art-category').value      = a.category  ?? '';
    document.getElementById('art-manufacturer').value  = a.manufacturer ?? '';
    document.getElementById('art-model').value         = a.model       ?? '';
    document.getElementById('art-location').value      = a.location    ?? '';
    document.getElementById('art-external-qr-code').value = a.externalQrCode ?? '';
    document.getElementById('art-quantity').value      = 1;
    document.getElementById('art-material').value      = a.material    ?? '';
    document.getElementById('art-style').value         = a.style       ?? '';
    document.getElementById('art-color').value         = a.color       ?? '';
    document.getElementById('art-width').value         = a.width       ?? '';
    document.getElementById('art-depth').value         = a.depth       ?? '';
    document.getElementById('art-height').value        = a.height      ?? '';
    document.getElementById('art-purchase-price').value       = a.purchasePrice      ?? '';
    document.getElementById('art-purchase-price-gross').value = a.purchasePriceGross ?? '';
    document.getElementById('art-original-price').value       = a.originalPrice      ?? '';
    document.getElementById('art-original-price-gross').value = a.originalPriceGross ?? '';
    document.getElementById('art-listing-link').value  = a.listingLink  ?? '';
    document.getElementById('art-pickup-zip').value    = a.pickupZip    ?? '';
    document.getElementById('art-notes').value         = a.notes        ?? '';
    document.getElementById('article-edit-group-id').value = a.groupId ?? '';

    if (a.groupId) {
      document.getElementById('art-group-assign').value = a.groupId;
    }

    if (a.shipping) {
      document.getElementById('art-shipping').checked = true;
      document.getElementById('shipping-cost-wrapper').style.display = 'flex';
      document.getElementById('art-shipping-cost').value = a.shippingCost ?? '';
    }

    if (a.condition) {
      const radio = document.querySelector(
        `input[name="art-condition"][value="${a.condition}"]`
      );
      if (radio) {
        radio.checked = true;
        radio.closest('.condition-btn').classList.add('selected');
      }
    }

    this.toggleSoldFieldsArticle(a.status);
    if (a.soldPrice)      document.getElementById('art-sold-price').value       = a.soldPrice;
    if (a.soldPriceGross) document.getElementById('art-sold-price-gross').value = a.soldPriceGross;
    if (a.soldDate)  document.getElementById('art-sold-date').value  = a.soldDate;

    State.articlePhotos = [...(a.photos ?? [])];
    this.renderPhotoPreviews(
      document.getElementById('art-photo-preview'),
      State.articlePhotos,
      'articlePhotos'
    );

    document.getElementById('art-qr-section').style.display = 'block';
    this.refreshArticleQrPreview(a);

    Router.navigate('dashboard');
    window.scrollTo({ top: 0, behavior: 'smooth' });
  },

  initGroupForm() {
    document.getElementById('grp-price-net')
      .addEventListener('input', e => {
        const n = parseFloat(e.target.value);
        document.getElementById('grp-price-gross').value =
          isNaN(n) ? '' : (n * 1.19).toFixed(2);
      });

    document.getElementById('grp-price-gross')
      .addEventListener('input', e => {
        const g = parseFloat(e.target.value);
        document.getElementById('grp-price-net').value =
          isNaN(g) ? '' : (g / 1.19).toFixed(2);
      });

    document.getElementById('grp-status')
      .addEventListener('change', e => {
        document.getElementById('sold-fields-group').style.display =
          e.target.value === 'Verkauft' ? 'block' : 'none';
      });

    document.getElementById('grp-image')
      .addEventListener('change', async e => {
        const file = e.target.files[0];
        if (!file) return;
        State.groupImageBase64 = await Utils.resizeImage(file, 1400, 0.88);
        document.getElementById('grp-image-preview').innerHTML =
          `<div class="photo-thumb">
             <img src="${State.groupImageBase64}" alt="Gruppenbild"/>
           </div>`;
        e.target.value = '';
      });

    this.bindFileDropzone('grp-image-dropzone', 'grp-image', async files => {
      const file = Array.from(files).find(entry => entry.type.startsWith('image/'));
      if (!file) return;
      State.groupImageBase64 = await Utils.resizeImage(file, 1400, 0.88);
      document.getElementById('grp-image-preview').innerHTML =
        `<div class="photo-thumb">
           <img src="${State.groupImageBase64}" alt="Gruppenbild"/>
         </div>`;
    });

    document.getElementById('btn-price-history')
      .addEventListener('click', () => {
        if (!State.editingGroupId) {
          Toast.warning('Bitte zuerst eine Gruppe speichern.');
          return;
        }
        const group = DB.getGroupById(State.editingGroupId);
        if (group) this.showPriceHistoryModal(group);
      });

    document.getElementById('group-form')
      .addEventListener('submit', e => { e.preventDefault(); this.saveGroup(); });

    document.getElementById('btn-reset-group')
      .addEventListener('click', () => this.resetGroupForm());

    const cancelGrpBtn = document.getElementById('btn-cancel-group');
    if (cancelGrpBtn) {
      cancelGrpBtn.addEventListener('click', () => {
        this.resetGroupForm();
        Router.navigate('groups');
      });
    }
  },

  async saveGroup() {
    const name     = document.getElementById('grp-name').value.trim();
    const priceNet = parseFloat(document.getElementById('grp-price-net').value) || null;
    const editId   = State.editingGroupId;

    if (!name) {
      Toast.error('Bitte einen Gruppennamen eingeben.');
      document.getElementById('grp-name').focus();
      return;
    }

    try {
    const data = {
      name,
      status           : document.getElementById('grp-status').value,
      quantity         : parseInt(document.getElementById('grp-quantity').value) || 1,
      location         : document.getElementById('grp-location').value.trim(),
      listingLink      : document.getElementById('grp-listing-link').value.trim(),
      priceNet,
      priceGross       : parseFloat(document.getElementById('grp-price-gross').value) || null,
      soldPrice        : parseFloat(document.getElementById('grp-sold-price').value)  || null,
      image            : State.groupImageBase64,
      conditionOverview: document.getElementById('grp-condition-overview').value.trim(),
      notes            : document.getElementById('grp-notes').value.trim(),
    };

    let saved;
    if (editId) {
      saved = DB.updateGroup(editId, data);
      Toast.success(`Gruppe ${editId} â€ž${Utils.escHtml(name)}" aktualisiert.`);
    } else {
      saved = await DB.saveGroup(data);
      Toast.success(`Gruppe ${saved.id} â€ž${Utils.escHtml(name)}" gespeichert!`);
    }

    document.getElementById('grp-id-display').value = saved.id;
    State.editingGroupId = saved.id;
    this.renderStats();
    const targetGroupId = saved.id;
    setTimeout(() => { Router.navigate('groups'); setTimeout(() => Groups.openDetail(targetGroupId), 80); }, 300);
    } catch (err) {
      console.error('saveGroup failed:', err);
      Toast.error('Gruppe konnte nicht gespeichert werden.');
    }
  },

  resetGroupForm() {
    document.getElementById('group-form').reset();
    document.getElementById('grp-id-display').value        = '';
    document.getElementById('grp-name').value              = '';
    document.getElementById('grp-listing-link').value      = '';
    document.getElementById('grp-image-preview').innerHTML = '';
    document.getElementById('sold-fields-group').style.display = 'none';
    State.editingGroupId   = null;
    State.groupImageBase64 = null;
  },

  showPriceHistoryModal(group) {
    const history = group.priceHistory ?? [];
    const rows    = history.length
      ? history.map((h, i) => `
          <tr>
            <td>${i + 1}</td>
            <td>${Utils.escHtml(h.date)}</td>
            <td style="font-weight:600">${Utils.formatEuro(h.price)}</td>
            <td style="color:var(--color-primary);font-weight:600;">
              ${h.priceGross ? Utils.formatEuro(h.priceGross) : Utils.formatEuro((h.price || 0) * 1.19)}
            </td>
          </tr>`).join('')
      : `<tr>
           <td colspan="4" class="text-center text-muted">
             Noch keine Preishistorie vorhanden.
           </td>
         </tr>`;

    Modal.open(`
      <h2 class="modal-title">
        <i class="fa-solid fa-clock-rotate-left"></i>
        Preishistorie ${Utils.escHtml(group.name)} (${group.id})
      </h2>
      <table class="data-table">
        <thead>
          <tr><th>#</th><th>Datum</th><th>Netto</th><th style="color:var(--color-primary);">Brutto</th></tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
      <div class="modal-actions">
        <button class="btn btn-ghost" onclick="Modal.close()">
          <i class="fa-solid fa-xmark"></i> SchlieÃŸen
        </button>
      </div>
    `);
  },
};
/* ============================================================
   10. INVENTORY â€” BestandsÃ¼bersicht
============================================================ */
const InventorySelection = {
  _active     : false,
  _selectedIds: new Set(),

  enter() {
    this._active = true;
    this._selectedIds.clear();
    document.getElementById('inventory-container')?.classList.add('selection-mode');
    const btn = document.getElementById('btn-inv-toggle-selection');
    if (btn) {
      btn.classList.add('is-active');
      btn.innerHTML = '<i class="fa-solid fa-xmark"></i> Auswahl beenden';
    }
    this._updateBar();
  },

  leave() {
    this._active = false;
    this._selectedIds.clear();
    const c = document.getElementById('inventory-container');
    if (c) {
      c.classList.remove('selection-mode');
      c.querySelectorAll('.article-card.is-selected').forEach(x => x.classList.remove('is-selected'));
      c.querySelectorAll('tr.is-selected').forEach(x => x.classList.remove('is-selected'));
    }
    const btn = document.getElementById('btn-inv-toggle-selection');
    if (btn) {
      btn.classList.remove('is-active');
      btn.innerHTML = '<i class="fa-solid fa-check-square"></i> AuswÃ¤hlen';
    }
    this._hideBar();
  },

  toggleMode() { this._active ? this.leave() : this.enter(); },

  toggleArticle(articleId, force) {
    if (!this._active) return;
    const sel  = force !== undefined ? force : !this._selectedIds.has(articleId);
    sel ? this._selectedIds.add(articleId) : this._selectedIds.delete(articleId);
    const card = document.querySelector('.article-card[data-id="' + CSS.escape(articleId) + '"]');
    if (card) card.classList.toggle('is-selected', sel);
    const row = document.querySelector('#inventory-container tr[data-id="' + CSS.escape(articleId) + '"]');
    if (row) row.classList.toggle('is-selected', sel);
    this._updateBar();
  },

  selectAll() {
    document.querySelectorAll('#inventory-container .article-card[data-id]')
      .forEach(c => this.toggleArticle(c.dataset.id, true));
    document.querySelectorAll('#inventory-container tr[data-id]')
      .forEach(r => this.toggleArticle(r.dataset.id, true));
  },

  deselectAll() {
    [...this._selectedIds].forEach(id => this.toggleArticle(id, false));
  },

  _updateBar() {
    const n   = this._selectedIds.size;
    const bar = this._getOrCreateBar();
    if (n === 0) { this._hideBar(); return; }
    bar.querySelector('.bulk-action-bar__count').textContent = n + ' Artikel ausgewÃ¤hlt';
    bar.classList.add('is-visible');
  },

  _hideBar() {
    document.getElementById('inv-bulk-action-bar')?.classList.remove('is-visible');
  },

  _getOrCreateBar() {
    let bar = document.getElementById('inv-bulk-action-bar');
    if (bar) return bar;
    bar = document.createElement('div');
    bar.id        = 'inv-bulk-action-bar';
    bar.className = 'bulk-action-bar';
    bar.innerHTML =
      '<div class="bulk-action-bar__info">'
      + '<span class="bulk-action-bar__count">0 ausgewÃ¤hlt</span>'
      + '<button class="bulk-action-bar__link-btn" id="inv-bulk-select-all" type="button">Alle</button>'
      + '<button class="bulk-action-bar__link-btn muted" id="inv-bulk-deselect" type="button">Aufheben</button>'
      + '</div>'
      + '<div class="bulk-action-bar__actions">'
      + '<button class="btn btn-primary btn-sm" id="inv-bulk-sell" type="button">'
      +   '<i class="fa-solid fa-handshake"></i> Verkauft</button>'
      + '<button class="btn btn-outline btn-sm" id="inv-bulk-reserve" type="button">'
      +   '<i class="fa-solid fa-clock"></i> Reservieren</button>'
      + '<button class="btn btn-ghost btn-sm" id="inv-bulk-available" type="button">'
      +   '<i class="fa-solid fa-circle-check"></i> Verf\u00fcgbar</button>'
      + '<button class="btn btn-danger btn-sm" id="inv-bulk-dispose" type="button">'
      +   '<i class="fa-solid fa-box-archive"></i> Entsorgen</button>'
      + '<button class="btn btn-danger btn-sm" id="inv-bulk-delete" type="button">'
      +   '<i class="fa-solid fa-trash-can"></i> L\u00f6schen</button>'
      + '</div>';
    document.body.appendChild(bar);
    bar.querySelector('#inv-bulk-select-all').addEventListener('click', () => this.selectAll());
    bar.querySelector('#inv-bulk-deselect').addEventListener('click', () => this.deselectAll());
    bar.querySelector('#inv-bulk-sell').addEventListener('click', () => this.openSellModal());
    bar.querySelector('#inv-bulk-reserve').addEventListener('click', () => this._bulkSetStatus('Reserviert'));
    bar.querySelector('#inv-bulk-available').addEventListener('click', () => this._bulkSetStatus('Verf\u00fcgbar'));
    bar.querySelector('#inv-bulk-dispose').addEventListener('click', () => this._bulkDispose());
    bar.querySelector('#inv-bulk-delete').addEventListener('click', () => this._bulkDelete());
    return bar;
  },

  _bulkSetStatus(newStatus) {
    const ids = [...this._selectedIds];
    if (!ids.length) return;
    DB.updateArticles(ids, { status: newStatus });
    const n = ids.length;
    this.leave();
    Inventory.render();
    Dashboard.renderStats();
    Toast.success(n + ' Artikel auf â€ž' + newStatus + '" gesetzt.');
  },

  _bulkDispose() {
    const ids = [...this._selectedIds];
    if (!ids.length) return;
    const n = ids.length;
    Modal.open(
      '<h2 class="modal-title" style="color:var(--color-warning);">'
      + '<i class="fa-solid fa-box-archive"></i> ' + n + ' Artikel entsorgen?</h2>'
      + '<p>' + n + ' Artikel werden auf Status <strong>Entsorgt</strong> gesetzt '
      + 'und aus ihren Gruppen gelÃ¶st. Sie bleiben in der EnzyklopÃ¤die.</p>'
      + '<div class="modal-actions">'
      + '<button class="btn btn-ghost" onclick="Modal.close()"><i class="fa-solid fa-xmark"></i> Abbrechen</button>'
      + '<button class="btn btn-danger" id="inv-bulk-dispose-confirm">'
      + '<i class="fa-solid fa-box-archive"></i> Alle entsorgen</button></div>',
      content => {
        content.querySelector('#inv-bulk-dispose-confirm').addEventListener('click', () => {
          DB.updateArticles(ids, { status: 'Entsorgt', groupId: null });
          Modal.close();
          this.leave();
          Inventory.render();
          Dashboard.renderStats();
          Toast.success(n + ' Artikel entsorgt.');
        });
      }
    );
  },

  _bulkDelete() {
    const ids = [...this._selectedIds];
    if (!ids.length) return;
    const n = ids.length;
    Modal.open(
      '<h2 class="modal-title" style="color:var(--color-danger);">'
      + '<i class="fa-solid fa-trash-can"></i> ' + n + ' Artikel l\u00f6schen</h2>'
      + '<p>Was soll mit den <strong>' + n + ' ausgew\u00e4hlten Artikeln</strong> passieren?</p>'
      + '<div class="delete-option-card" id="inv-bulk-del-opt-soft">'
      +   '<div class="delete-option-card__header">'
      +     '<i class="fa-solid fa-box-archive" style="color:var(--color-warning);"></i>'
      +     '<strong>Entsorgen</strong>'
      +     '<span class="badge badge-status-entsorgt" style="margin-left:auto;">In Enzyklop\u00e4die behalten</span>'
      +   '</div>'
      +   '<p class="delete-option-card__desc">Alle ausgew\u00e4hlten Artikel bekommen Status <em>Entsorgt</em>, werden aus Gruppen gel\u00f6st und bleiben in der Enzyklop\u00e4die auffindbar.</p>'
      + '</div>'
      + '<div class="delete-option-card" id="inv-bulk-del-opt-hard">'
      +   '<div class="delete-option-card__header">'
      +     '<i class="fa-solid fa-fire-flame-curved" style="color:var(--color-danger);"></i>'
      +     '<strong>Dauerhaft l\u00f6schen</strong>'
      +     '<span style="margin-left:auto;font-size:var(--font-size-xs);background:var(--color-danger-light);color:var(--color-danger);padding:2px 8px;border-radius:99px;font-weight:700;">Endg\u00fcltig</span>'
      +   '</div>'
      +   '<p class="delete-option-card__desc">Alle ausgew\u00e4hlten Artikel werden <strong>vollst\u00e4ndig und unwiderruflich</strong> entfernt.</p>'
      + '</div>'
      + '<div class="modal-actions" style="margin-top:20px;">'
      +   '<button class="btn btn-ghost" onclick="Modal.close()"><i class="fa-solid fa-xmark"></i> Abbrechen</button>'
      +   '<button class="btn btn-danger" id="inv-bulk-delete-confirm" disabled>'
      +     '<i class="fa-solid fa-trash-can"></i> Best\u00e4tigen'
      +   '</button>'
      + '</div>',
      content => {
        const softCard   = content.querySelector('#inv-bulk-del-opt-soft');
        const hardCard   = content.querySelector('#inv-bulk-del-opt-hard');
        const confirmBtn = content.querySelector('#inv-bulk-delete-confirm');
        let mode = null;
        const selectCard = (sel, other, m) => {
          mode = m;
          sel.classList.add('is-selected');
          other.classList.remove('is-selected');
          confirmBtn.disabled = false;
        };
        softCard.addEventListener('click', () => selectCard(softCard, hardCard, 'soft'));
        hardCard.addEventListener('click', () => selectCard(hardCard, softCard, 'hard'));
        confirmBtn.addEventListener('click', () => {
          if (!mode) return;
          if (mode === 'soft') {
            DB.updateArticles(ids, { status: 'Entsorgt', groupId: null });
            Modal.close();
            Toast.success(n + ' Artikel entsorgt — bleiben in der Enzyklopädie.');
          } else {
            ids.forEach(id => DB.hardDeleteArticle(id));
            Modal.close();
            Toast.success(n + ' Artikel dauerhaft gelöscht.');
          }
          this.leave();
          Inventory.render();
          Dashboard.renderStats();
        });
      }
    );
  },

  openSellModal() {
    const ids      = [...this._selectedIds];
    if (!ids.length) return;
    const articles = ids.map(id => DB.getArticleById(id)).filter(Boolean);
    if (!articles.length) return;
    const today = new Date().toISOString().split('T')[0];
    let rows = '';
    articles.forEach(a => {
      const name = Utils.escHtml(Utils.articleDisplayName(a, '-'));
      rows += '<div class="bulk-sell-price-row" data-article-id="' + Utils.escHtml(a.id) + '">'
        + '<span class="bulk-sell-price-row__id">' + Utils.escHtml(a.id) + '</span>'
        + '<span class="bulk-sell-price-row__name">' + name + '</span>'
        + '<input type="number" class="bulk-sell-price-input" min="0" step="0.01" placeholder="0,00" value="' + Utils.escHtml(String(a.soldPrice ?? '')) + '"/>'
        + '<input type="date" class="bulk-sell-date-input" '
        +   'value="' + Utils.escHtml(a.soldDate ?? today) + '"/>'
        + '</div>';
    });
    Modal.open(
      '<h2 class="modal-title"><i class="fa-solid fa-handshake" style="color:var(--color-success)"></i> '
      + articles.length + ' Artikel als verkauft markieren</h2>'
      + '<div id="inv-bulk-sell-list" style="max-height:340px;overflow-y:auto;'
      +   'border:1px solid var(--color-border);border-radius:var(--border-radius-sm);padding:6px;">'
      + rows + '</div>'
      + '<div class="bulk-sell-total">'
      +   '<span class="bulk-sell-total__label">GesamterlÃ¶s</span>'
      +   '<span class="bulk-sell-total__value" id="inv-bulk-sell-total">0,00 â‚¬</span>'
      + '</div>'
      + '<div class="modal-actions" style="margin-top:16px;">'
      +   '<button class="btn btn-ghost" onclick="Modal.close()">'
      +     '<i class="fa-solid fa-xmark"></i> Abbrechen</button>'
      +   '<button class="btn btn-primary" id="inv-confirm-bulk-sell">'
      +     '<i class="fa-solid fa-floppy-disk"></i> Speichern</button>'
      + '</div>',
      content => {
        const recalc = () => {
          let t = 0;
          content.querySelectorAll('.bulk-sell-price-input').forEach(inp => {
            const v = parseFloat(inp.value);
            if (!isNaN(v)) t += v;
          });
          const el = content.querySelector('#inv-bulk-sell-total');
          if (el) el.textContent = t.toLocaleString('de-DE', { style: 'currency', currency: 'EUR' });
        };
        content.querySelectorAll('.bulk-sell-price-input').forEach(i => i.addEventListener('input', recalc));
        recalc();
        content.querySelector('#inv-confirm-bulk-sell').addEventListener('click', () => {
          const updates = [];
          content.querySelectorAll('.bulk-sell-price-row[data-article-id]').forEach(row => {
            const id = row.dataset.articleId;
            if (!id) return;
            const soldPrice = parseFloat(row.querySelector('.bulk-sell-price-input')?.value) || null;
            const soldDate  = row.querySelector('.bulk-sell-date-input')?.value || today;
            updates.push({ id, data: { status: 'Verkauft', soldPrice, soldDate } });
          });
          const n = DB.updateArticlesBulk(updates).length;
          Modal.close();
          this.leave();
          Inventory.render();
          Dashboard.renderStats();
          Toast.success(n + ' Artikel als verkauft gespeichert.');
        });
      }
    );
  },
};

const Inventory = {

  _renderQueued: false,
  _searchRender: null,

  init() {
    this._searchRender = Utils.debounce(() => this.queueRender());
    document.getElementById('inv-search')
      .addEventListener('input', () => this._searchRender());
    document.getElementById('inv-filter-category')
      .addEventListener('change', () => this.queueRender());
    document.getElementById('inv-filter-condition')
      .addEventListener('change', () => this.queueRender());
    document.getElementById('inv-sort')
      .addEventListener('change', () => this.queueRender());
    document.getElementById('inv-view-grid')
      .addEventListener('click', () => {
        State.inventoryViewMode = 'grid';
        document.getElementById('inv-view-grid').classList.add('active');
        document.getElementById('inv-view-table').classList.remove('active');
        this.queueRender();
      });
    document.getElementById('inv-view-table')
      .addEventListener('click', () => {
        State.inventoryViewMode = 'table';
        document.getElementById('inv-view-table').classList.add('active');
        document.getElementById('inv-view-grid').classList.remove('active');
        this.queueRender();
      });
    const invSelBtn = document.getElementById('btn-inv-toggle-selection');
    if (invSelBtn) {
      invSelBtn.addEventListener('click', () => InventorySelection.toggleMode());
    }
  },

  queueRender() {
    if (this._renderQueued) return;
    this._renderQueued = true;
    requestAnimationFrame(() => {
      this._renderQueued = false;
      this.render();
    });
  },

  /* â”€â”€ Gefilterte & sortierte Artikel abrufen â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  getFiltered() {
    const search    = document.getElementById('inv-search').value.trim();
    const category  = document.getElementById('inv-filter-category').value;
    const condition = document.getElementById('inv-filter-condition').value;
    const sortVal   = document.getElementById('inv-sort').value;

    let articles = DB.getArticles().filter(
      a => ['Verf\u00fcgbar', 'Reserviert'].includes(Utils.normalizeStatus(a.status))
    );

    if (search)    articles = articles.filter(a => Utils.articleMatchesSearch(a, search));
    if (category)  articles = articles.filter(a => a.category  === category);
    if (condition) articles = articles.filter(a => a.condition === condition);

    articles.sort((a, b) => {
      if (sortVal === 'date-desc')  return b.updatedAt      - a.updatedAt;
      if (sortVal === 'date-asc')   return a.updatedAt      - b.updatedAt;
      if (sortVal === 'price-asc')  return (a.purchasePrice || 0) - (b.purchasePrice || 0);
      if (sortVal === 'price-desc') return (b.purchasePrice || 0) - (a.purchasePrice || 0);
      return 0;
    });

    return articles;
  },

  /* â”€â”€ Haupt-Render â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  render() {
    const container = document.getElementById('inventory-container');
    const articles  = this.getFiltered();

    if (!articles.length) {
      container.className = '';
      container.innerHTML = `
        <div class="empty-state" style="grid-column:1/-1;">
          <i class="fa-solid fa-box-open"></i>
          <p>Keine Artikel gefunden.</p>
        </div>`;
      return;
    }

    if (State.inventoryViewMode === 'grid') {
      container.className = 'cards-grid';
      container.innerHTML = articles.map(a => this.renderCard(a)).join('');
    } else {
      container.className = '';
      container.innerHTML = this.renderTable(articles);
    }

    // Event-Listener fÃ¼r Aktions-Buttons
    container.querySelectorAll('[data-action]').forEach(btn => {
      btn.addEventListener('click', ev => {
        if (InventorySelection._active) { ev.stopPropagation(); return; }
        const { action, id } = btn.dataset;
        if (action === 'edit')   Dashboard.loadArticleIntoForm(id);
        if (action === 'sell')   this._openSellModal(id);
        if (action === 'qr')     QRManager.printQR(id);
        if (action === 'delete') this._confirmDelete(id);
        if (action === 'group')  this._openGroupModal(id);
      });
    });

    // Klick auf Karte im Auswahl-Modus
    container.querySelectorAll('.article-card[data-id]').forEach(card => {
      card.addEventListener('click', ev => {
        if (!InventorySelection._active) return;
        if (ev.target.closest('[data-action]')) return;
        InventorySelection.toggleArticle(card.dataset.id);
      });
    });

    // Klick auf Tabellenzeile im Auswahl-Modus
    container.querySelectorAll('tr[data-id]').forEach(row => {
      row.addEventListener('click', ev => {
        if (!InventorySelection._active) return;
        if (ev.target.closest('[data-action]')) return;
        InventorySelection.toggleArticle(row.dataset.id);
      });
    });
  },

  /* â”€â”€ Karten-Render (Grid-Ansicht) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  renderCard(a) {
    const img = a.photos?.[0]
      ? `<img src="${a.photos[0]}" alt="${Utils.escHtml(a.model || a.category)}" loading="lazy"/>`
      : `<div class="card-image-placeholder"><i class="fa-solid fa-couch"></i></div>`;

    const dims = [a.width, a.depth, a.height]
      .filter(Boolean).map(v => v + 'cm').join(' x ');

    const groupBadge = a.groupId
      ? (() => {
          const g     = DB.getGroupById(a.groupId);
          const label = g?.name
            ? `${Utils.escHtml(a.groupId)} Â· ${Utils.escHtml(g.name.substring(0, 20))}${g.name.length > 20 ? 'â€¦' : ''}`
            : Utils.escHtml(a.groupId);
          return `<div style="margin-top:6px;">
                    <span style="font-size:var(--font-size-xs);color:var(--color-primary);font-weight:600;">
                      <i class="fa-solid fa-layer-group"></i> ${label}
                    </span>
                  </div>`;
        })()
      : '';

    return `
      <article class="article-card" data-id="${Utils.escHtml(a.id)}">
        <div class="card-image">
          ${img}
          <div class="card-badges">${Utils.statusBadge(a.status)}${Utils.isNewArticle(a) ? Utils.newBadge() : ''}</div>
          ${a.quantity > 1
            ? `<div class="article-count-badge">
                 <i class="fa-solid fa-cube"></i> ${a.quantity}Ã—
               </div>`
            : ''}
        </div>
        <div class="card-body">
          <span class="card-id">${Utils.escHtml(a.id)}</span>
          <div class="card-title">
            ${Utils.escHtml(Utils.articleDisplayName(a))}
          </div>
          <div class="card-meta">
            ${Utils.condBadge(a.condition)}
            ${a.category
              ? `<span style="font-size:var(--font-size-xs);color:var(--color-muted);">
                   ${Utils.escHtml(a.category)}
                 </span>`
              : ''}
            ${dims
              ? `<span style="font-size:var(--font-size-xs);color:var(--color-muted);">
                   <i class="fa-solid fa-ruler-combined"></i> ${dims}
                 </span>`
              : ''}
            ${a.location
              ? `<span style="font-size:var(--font-size-xs);color:var(--color-muted);">
                   <i class="fa-solid fa-location-dot"></i> ${Utils.escHtml(a.location)}
                 </span>`
              : ''}
          </div>
          ${groupBadge}
          ${a.purchasePrice
            ? `<div style="margin-top:8px;font-size:var(--font-size-sm);
                           font-weight:600;color:var(--color-primary);">
                 EK: ${Utils.formatEuro(a.purchasePrice)}
               </div>`
            : ''}
        </div>
        <div class="card-footer">
          <div class="action-btn-group">
            <button class="btn btn-primary btn-sm" data-action="edit" data-id="${a.id}"
                    data-tooltip="Artikel bearbeiten">
              <i class="fa-solid fa-pen-to-square"></i> Bearbeiten
            </button>
            <button class="btn btn-outline btn-sm" data-action="group" data-id="${a.id}"
                    data-tooltip="Gruppe zuweisen">
              <i class="fa-solid fa-layer-group"></i>
            </button>
            <button class="btn btn-ghost btn-sm" data-action="qr" data-id="${a.id}"
                    data-tooltip="QR-Code drucken">
              <i class="fa-solid fa-qrcode"></i>
            </button>
            <button class="btn btn-success btn-sm" data-action="sell" data-id="${a.id}"
                    data-tooltip="Als verkauft markieren">
              <i class="fa-solid fa-handshake"></i>
            </button>
            <button class="btn btn-danger btn-sm" data-action="delete" data-id="${a.id}"
                    data-tooltip="Artikel lÃ¶schen">
              <i class="fa-solid fa-trash-can"></i>
            </button>
          </div>
        </div>
      </article>`;
  },

  /* â”€â”€ Tabellen-Render â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  renderTable(articles) {
    const rows = articles.map(a => {
      const dims = [a.width, a.depth, a.height]
        .filter(Boolean).map(v => v + 'cm').join(' x ');
      const g          = a.groupId ? DB.getGroupById(a.groupId) : null;
      const groupLabel = g?.name
        ? `${Utils.escHtml(a.groupId)} Â· ${Utils.escHtml(g.name.substring(0, 16))}${g.name.length > 16 ? 'â€¦' : ''}`
        : (a.groupId ? Utils.escHtml(a.groupId) : 'â€“');
      return `
        <tr data-id="${Utils.escHtml(a.id)}">
          <td>
            <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
              <strong>${Utils.escHtml(a.id)}</strong>
              ${Utils.isNewArticle(a) ? Utils.newBadge() : ''}
            </div>
          </td>
<td>${Utils.escHtml(Utils.articleDisplayName(a))}</td>
          <td>${Utils.condBadge(a.condition)}</td>
          <td>${Utils.statusBadge(a.status)}</td>
          <td>${Utils.escHtml(dims || 'â€“')}</td>
          <td>${Utils.escHtml(a.location ?? 'â€“')}</td>
          <td style="color:var(--color-primary);font-size:var(--font-size-xs);font-weight:600;">
            ${groupLabel}
          </td>
          <td>${a.purchasePrice ? Utils.formatEuro(a.purchasePrice) : 'â€“'}</td>
          <td>
            <div class="table-actions">
              <button class="btn btn-ghost btn-sm" data-action="edit"   data-id="${a.id}" data-tooltip="Bearbeiten"><i class="fa-solid fa-pen-to-square"></i></button>
              <button class="btn btn-ghost btn-sm" data-action="group"  data-id="${a.id}" data-tooltip="Gruppe"><i class="fa-solid fa-layer-group"></i></button>
              <button class="btn btn-ghost btn-sm" data-action="qr"     data-id="${a.id}" data-tooltip="QR"><i class="fa-solid fa-qrcode"></i></button>
              <button class="btn btn-ghost btn-sm" data-action="sell"   data-id="${a.id}" data-tooltip="Verkauft"><i class="fa-solid fa-handshake"></i></button>
              <button class="btn btn-danger btn-sm" data-action="delete" data-id="${a.id}" data-tooltip="LÃ¶schen"><i class="fa-solid fa-trash-can"></i></button>
            </div>
          </td>
        </tr>`;
    }).join('');
    return `
      <div class="table-wrapper">
        <table class="inventory-table">
          <thead>
            <tr>
              <th>ID</th>
              <th>Artikel</th>
              <th>Zustand</th>
              <th>Status</th>
              <th>MaÃŸe</th>
              <th>Standort</th>
              <th>Gruppe</th>
              <th>EK</th>
              <th>Aktionen</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;
  },

  /* â”€â”€ Verkaufs-Modal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  _openSellModal(articleId) {
    const article = DB.getArticleById(articleId);
    if (!article) return;
    const today = new Date().toISOString().split('T')[0];
    Modal.open(`
      <h2 class="modal-title">
        <i class="fa-solid fa-handshake" style="color:var(--color-success)"></i>
        Artikel verkauft
      </h2>
      <p style="margin-bottom:16px;color:var(--color-text-secondary);">
        <strong>${Utils.escHtml(Utils.articleDisplayName(article, articleId))}</strong>
        <span style="color:var(--color-muted);font-size:var(--font-size-xs);">(${Utils.escHtml(articleId)})</span>
      </p>
      <div class="form-group" style="margin-bottom:14px;">
        <label for="sell-price-input">
          Verkaufspreis (â‚¬) <span class="required">*</span>
        </label>
        <input type="number" id="sell-price-input"
               placeholder="0,00" min="0" step="0.01"
               value="${article.soldPrice ?? ''}"
               style="width:100%;"/>
      </div>
      <div class="form-group">
        <label for="sell-date-input">
          Verkaufsdatum <span class="required">*</span>
        </label>
        <input type="date" id="sell-date-input"
               value="${article.soldDate ?? today}"
               style="width:100%;"/>
      </div>
      <div class="modal-actions">
        <button class="btn btn-ghost" onclick="Modal.close()">
          <i class="fa-solid fa-xmark"></i> Abbrechen
        </button>
        <button class="btn btn-success" id="confirm-sell-btn">
          <i class="fa-solid fa-check"></i> Als verkauft speichern
        </button>
      </div>`, content => {
      content.querySelector('#confirm-sell-btn').addEventListener('click', () => {
        const soldPrice = parseFloat(content.querySelector('#sell-price-input').value);
        const soldDate  = content.querySelector('#sell-date-input').value;
        if (!soldPrice || !soldDate) {
          Toast.error('Bitte Preis und Datum angeben.');
          return;
        }
        DB.updateArticle(articleId, { status: 'Verkauft', soldPrice, soldDate });
        Modal.close();
        this.render();
        Dashboard.renderStats();
        Toast.success(`Artikel ${articleId} als verkauft gespeichert.`);
      });
    });
  },

  /* â”€â”€ LÃ¶sch-BestÃ¤tigung â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  _confirmDelete(articleId) {
    const article = DB.getArticleById(articleId);
    if (!article) return;
    const name = Utils.escHtml(Utils.articleDisplayName(article, articleId));
    Modal.open(
      '<h2 class="modal-title" style="color:var(--color-danger);">'
      + '<i class="fa-solid fa-trash-can"></i> Artikel lÃ¶schen</h2>'
      + '<p>Was soll mit <strong>' + name + '</strong> <span style="color:var(--color-muted);font-size:var(--font-size-xs);">(' + Utils.escHtml(articleId) + ')</span> passieren?</p>'
      + '<div class="delete-option-card" id="inv-del-opt-soft">'
      +   '<div class="delete-option-card__header">'
      +     '<i class="fa-solid fa-box-archive" style="color:var(--color-warning);"></i>'
      +     '<strong>Entsorgen</strong>'
      +     '<span class="badge badge-status-entsorgt" style="margin-left:auto;">In EnzyklopÃ¤die behalten</span>'
      +   '</div>'
      +   '<p class="delete-option-card__desc">Artikel bekommt Status <em>Entsorgt</em>. Bleibt in der EnzyklopÃ¤die auffindbar.</p>'
      + '</div>'
      + '<div class="delete-option-card" id="inv-del-opt-hard">'
      +   '<div class="delete-option-card__header">'
      +     '<i class="fa-solid fa-fire-flame-curved" style="color:var(--color-danger);"></i>'
      +     '<strong>Dauerhaft lÃ¶schen</strong>'
      +     '<span style="margin-left:auto;font-size:var(--font-size-xs);background:var(--color-danger-light);color:var(--color-danger);padding:2px 8px;border-radius:99px;font-weight:700;">EndgÃ¼ltig</span>'
      +   '</div>'
      +   '<p class="delete-option-card__desc">Artikel wird <strong>vollstÃ¤ndig und unwiderruflich</strong> entfernt.</p>'
      + '</div>'
      + '<div class="modal-actions" style="margin-top:20px;">'
      +   '<button class="btn btn-ghost" onclick="Modal.close()"><i class="fa-solid fa-xmark"></i> Abbrechen</button>'
      +   '<button class="btn btn-danger" id="inv-confirm-del-btn" disabled>'
      +     '<i class="fa-solid fa-trash-can"></i> BestÃ¤tigen'
      +   '</button>'
      + '</div>',
      content => {
        const softCard   = content.querySelector('#inv-del-opt-soft');
        const hardCard   = content.querySelector('#inv-del-opt-hard');
        const confirmBtn = content.querySelector('#inv-confirm-del-btn');
        let   mode       = null;
        const selectCard = (sel, other, m) => {
          mode = m;
          sel.classList.add('is-selected');
          other.classList.remove('is-selected');
          confirmBtn.disabled = false;
        };
        softCard.addEventListener('click', () => selectCard(softCard, hardCard, 'soft'));
        hardCard.addEventListener('click', () => selectCard(hardCard, softCard, 'hard'));
        confirmBtn.addEventListener('click', () => {
          if (!mode) return;
          if (mode === 'soft') {
            DB.updateArticle(articleId, { status: 'Entsorgt', groupId: null });
            Modal.close();
            Toast.success('Artikel ' + articleId + ' entsorgt â€” bleibt in der EnzyklopÃ¤die.');
          } else {
            DB.hardDeleteArticle(articleId);
            Modal.close();
            Toast.success('Artikel ' + articleId + ' dauerhaft gelÃ¶scht.');
          }
          this.render();
          Dashboard.renderStats();
        });
      }
    );
  },

  /* â”€â”€ Gruppen-Zuweisung Modal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  _openGroupModal(articleId) {
    const article = DB.getArticleById(articleId);
    if (!article) return;
    const groups  = DB.getGroups().filter(g => g.status !== 'Entsorgt');
    const current = article.groupId
      ? DB.getGroupById(article.groupId)
      : null;

    let options = `<option value="">â€” Keine Gruppe â€”</option>`;
    groups.forEach(g => {
      const label    = g.name
        ? `${g.id} Â· ${g.name.substring(0, 30)}${g.name.length > 30 ? 'â€¦' : ''}`
        : g.id;
      const selected = g.id === article.groupId ? 'selected' : '';
      options += `<option value="${Utils.escHtml(g.id)}" ${selected}>${Utils.escHtml(label)}</option>`;
    });

    Modal.open(`
      <h2 class="modal-title">
        <i class="fa-solid fa-layer-group"></i> Gruppe zuweisen
      </h2>
      <p style="margin-bottom:16px;color:var(--color-text-secondary);">
        Artikel <strong>${Utils.escHtml(articleId)}</strong>
        ${current
          ? `ist aktuell in Gruppe <strong>${Utils.escHtml(current.name || current.id)}</strong>`
          : 'ist keiner Gruppe zugeordnet'}.
      </p>
      <div class="form-group">
        <label for="group-assign-select">Gruppe ausw\u00e4hlen</label>
        <select id="group-assign-select" style="width:100%;">
          ${options}
        </select>
      </div>
      <div class="modal-actions">
        <button class="btn btn-ghost" onclick="Modal.close()">
          <i class="fa-solid fa-xmark"></i> Abbrechen
        </button>
        <button class="btn btn-primary" id="group-assign-confirm">
          <i class="fa-solid fa-floppy-disk"></i> Speichern
        </button>
      </div>`, content => {
      content.querySelector('#group-assign-confirm').addEventListener('click', () => {
        const newGroupId = content.querySelector('#group-assign-select').value || null;
        DB.updateArticle(articleId, { groupId: newGroupId });
        Modal.close();
        this.render();
        const g = newGroupId ? DB.getGroupById(newGroupId) : null;
        Toast.success(
          newGroupId
            ? `Artikel ${articleId} â†’ â€ž${g?.name ?? newGroupId}" zugeordnet.`
            : `Artikel ${articleId} aus Gruppe entfernt.`
        );
        this.render();
        Dashboard.renderStats();
      });
    });
   },

  getFiltered() {
    const search    = document.getElementById('inv-search').value.trim();
    const category  = document.getElementById('inv-filter-category').value;
    const condition = document.getElementById('inv-filter-condition').value;
    const sortVal   = document.getElementById('inv-sort').value;
    let articles = DB.getArticles().filter(
      a => ['Verf\u00fcgbar', 'Reserviert'].includes(Utils.normalizeStatus(a.status))
    );
    if (search)    articles = articles.filter(a => Utils.articleMatchesSearch(a, search));
    if (category)  articles = articles.filter(a => a.category  === category);
    if (condition) articles = articles.filter(a => a.condition === condition);
    articles.sort((a, b) => {
      if (sortVal === 'date-desc')  return b.updatedAt      - a.updatedAt;
      if (sortVal === 'date-asc')   return a.updatedAt      - b.updatedAt;
      if (sortVal === 'price-asc')  return (a.purchasePrice || 0) - (b.purchasePrice || 0);
      if (sortVal === 'price-desc') return (b.purchasePrice || 0) - (a.purchasePrice || 0);
      return 0;
    });
    return articles;
  },

  render() {
    const container = document.getElementById('inventory-container');
    const articles  = this.getFiltered();
    if (!articles.length) {
      container.className = '';
      container.innerHTML = `
        <div class="empty-state" style="grid-column:1/-1;">
          <i class="fa-solid fa-box-open"></i>
          <p>Keine Artikel gefunden.</p>
        </div>`;
      return;
    }
    if (State.inventoryViewMode === 'grid') {
      container.className = 'cards-grid';
      container.innerHTML = articles.map(a => this.renderCard(a)).join('');
    } else {
      container.className = '';
      container.innerHTML = this.renderTable(articles);
    }
    container.querySelectorAll('[data-action]').forEach(btn => {
      btn.addEventListener('click', ev => {
        if (InventorySelection._active) { ev.stopPropagation(); return; }
        const { action, id } = btn.dataset;
        if (action === 'edit')   Dashboard.loadArticleIntoForm(id);
        if (action === 'delete') this.confirmDelete(id);
        if (action === 'sell')   this.quickSellModal(id);
        if (action === 'qr')     QRManager.printQR(id);
      });
    });
    container.querySelectorAll('.article-card[data-id]').forEach(card => {
      card.addEventListener('click', ev => {
        if (!InventorySelection._active) return;
        if (ev.target.closest('button')) return;
        InventorySelection.toggleArticle(card.dataset.id);
      });
    });
    container.querySelectorAll('tr[data-id]').forEach(row => {
      row.addEventListener('click', ev => {
        if (!InventorySelection._active) return;
        if (ev.target.closest('button')) return;
        InventorySelection.toggleArticle(row.dataset.id);
      });
    });
    container.querySelectorAll('.inv-assign-group-btn').forEach(btn => {
      btn.addEventListener('click', () => this.assignGroupModal(btn.dataset.id));
    });
    container.querySelectorAll('.inv-group-link').forEach(link => {
      link.addEventListener('click', () => {
        Router.navigate('groups');
        setTimeout(() => Groups.openDetail(link.dataset.group), 100);
      });
    });
  },

  renderCard(a) {
    const img = a.photos?.[0]
      ? `<img src="${a.photos[0]}" alt="${Utils.escHtml(a.model || a.category)}" loading="lazy"/>`
      : `<div class="card-image-placeholder"><i class="fa-solid fa-couch"></i></div>`;
    const dims = [a.width, a.depth, a.height].filter(Boolean).map(v => `${v}cm`).join(' Ã— ');
    const groupRow = a.groupId
      ? (() => {
          const g     = DB.getGroupById(a.groupId);
          const label = g?.name
            ? `${Utils.escHtml(a.groupId)} Â· ${Utils.escHtml(g.name.substring(0, 22))}${g.name.length > 22 ? 'â€¦' : ''}`
            : Utils.escHtml(a.groupId);
          return `<div style="margin-top:6px;display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
                    <span class="inv-group-link" data-group="${Utils.escHtml(a.groupId)}" title="Zur Gruppe wechseln"
                          style="display:inline-flex;align-items:center;gap:5px;font-size:var(--font-size-xs);color:var(--color-primary);font-weight:600;cursor:pointer;">
                      <i class="fa-solid fa-layer-group"></i> ${label}
                    </span>
                    <button class="btn btn-ghost btn-sm inv-assign-group-btn" data-id="${a.id}"
                            style="padding:3px 9px;font-size:var(--font-size-xs);">
                      <i class="fa-solid fa-layer-group"></i> Gruppe zuordnen
                    </button>
                  </div>`;
        })()
      : `<div style="margin-top:6px;">
           <button class="btn btn-ghost btn-sm inv-assign-group-btn" data-id="${a.id}"
                   style="padding:3px 9px;font-size:var(--font-size-xs);">
             <i class="fa-solid fa-layer-group"></i> Gruppe zuordnen
           </button>
         </div>`;
    return `
      <article class="article-card" data-id="${Utils.escHtml(a.id)}">
        <div class="card-image">${img}<div class="card-badges">${Utils.statusBadge(a.status)}</div></div>
        <div class="card-body">
          <span class="card-id">${Utils.escHtml(a.id)}</span>
          <div class="card-title">${Utils.escHtml(Utils.articleDisplayName(a))}</div>
          <div class="card-meta">${Utils.condBadge(a.condition)}${dims ? ` <span style="font-size:var(--font-size-xs);color:var(--color-muted);">${dims}</span>` : ''}</div>
          ${groupRow}
          <div class="card-price">${a.purchasePrice ? Utils.formatEuro(a.purchasePrice) : '<span class="text-muted">Kein EK</span>'}</div>
        </div>
        <div class="card-footer">
          <button class="btn btn-outline btn-sm" data-action="qr" data-id="${a.id}" data-tooltip="QR-Code drucken">
            <i class="fa-solid fa-qrcode"></i>
          </button>
          <button class="btn btn-ghost btn-sm" data-action="edit" data-id="${a.id}" data-tooltip="Artikel bearbeiten">
            <i class="fa-solid fa-pen-to-square"></i> Bearbeiten
          </button>
          <button class="btn btn-success btn-sm" data-action="sell" data-id="${a.id}" data-tooltip="Als verkauft markieren">
            <i class="fa-solid fa-handshake"></i>
          </button>
          <button class="btn btn-danger btn-sm" data-action="delete" data-id="${a.id}" data-tooltip="Artikel lÃ¶schen">
            <i class="fa-solid fa-trash-can"></i>
          </button>
        </div>
      </article>`;
  },

  renderTable(articles) {
    const rows = articles.map(a => {
      const g          = a.groupId ? DB.getGroupById(a.groupId) : null;
      const groupLabel = g?.name
        ? `${Utils.escHtml(a.groupId)} Â· ${Utils.escHtml(g.name.substring(0, 18))}${g.name.length > 18 ? 'â€¦' : ''}`
        : (a.groupId ? Utils.escHtml(a.groupId) : 'â€“');
      const groupCell = a.groupId
        ? `<span class="inv-group-link" data-group="${a.groupId}"
                 style="color:var(--color-primary);font-size:var(--font-size-xs);font-weight:600;cursor:pointer;white-space:nowrap;">
             <i class="fa-solid fa-layer-group"></i> ${groupLabel}
           </span>
           <button class="btn btn-ghost btn-sm inv-assign-group-btn" data-id="${a.id}"
                   style="padding:2px 7px;font-size:var(--font-size-xs);">
             <i class="fa-solid fa-layer-group"></i> Zuordnen
           </button>`
        : `<button class="btn btn-ghost btn-sm inv-assign-group-btn" data-id="${a.id}"
                   style="padding:2px 7px;font-size:var(--font-size-xs);">
             <i class="fa-solid fa-layer-group"></i> Zuordnen
           </button>`;
      return `
        <tr data-id="${Utils.escHtml(a.id)}">
          <td><strong>${Utils.escHtml(a.id)}</strong></td>
          <td>${Utils.escHtml(a.category ?? 'â€“')}</td>
            <td>${Utils.escHtml(Utils.articleDisplayName(a))}${a.color ? `<br><small style="color:var(--color-muted);font-size:var(--font-size-xs);"><i class="fa-solid fa-palette"></i> ${Utils.escHtml(a.color)}</small>` : ''}</td>
          <td>${Utils.condBadge(a.condition)}</td>
          <td>${Utils.statusBadge(a.status)}</td>
          <td>${groupCell}</td>
          <td>${Utils.formatEuro(a.purchasePrice)}</td>
          <td>${Utils.escHtml(a.location ?? 'â€“')}</td>
          <td>${Utils.formatDate(a.updatedAt)}</td>
          <td>
            <div class="table-actions">
              <button class="btn btn-ghost btn-sm" data-action="edit" data-id="${a.id}" data-tooltip="Bearbeiten">
                <i class="fa-solid fa-pen-to-square"></i>
              </button>
              <button class="btn btn-danger btn-sm" data-action="delete" data-id="${a.id}" data-tooltip="Entsorgen">
                <i class="fa-solid fa-trash-can"></i>
              </button>
            </div>
          </td>
        </tr>`;
    }).join('');
    return `
      <div class="table-scroll-wrapper">
        <table class="data-table">
          <thead>
            <tr>
              <th>ID</th><th>Kategorie</th><th>Hersteller / Modell</th>
              <th>Zustand</th><th>Status</th><th>Gruppe</th>
              <th>EK (â‚¬)</th><th>Standort</th><th>Aktualisiert</th><th>Aktionen</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;
  },

  confirmDelete(id) {
    const article = DB.getArticleById(id);
    if (!article) return;
    const name   = Utils.escHtml(Utils.articleDisplayName(article, id));
    const inGrp  = article.groupId ? DB.getGroupById(article.groupId) : null;
    const grpHint = inGrp
      ? '<p style="margin-top:8px;font-size:var(--font-size-sm);color:var(--color-muted);">'
        + '<i class="fa-solid fa-layer-group"></i> Artikel ist Teil von Gruppe <strong>'
        + Utils.escHtml(inGrp.name || article.groupId) + '</strong> â€” Zuweisung wird aufgehoben.</p>'
      : '';
    Modal.open(
      '<h2 class="modal-title" style="color:var(--color-danger);">'
      + '<i class="fa-solid fa-trash-can"></i> Artikel lÃ¶schen</h2>'
      + '<p>Was soll mit <strong>' + name + '</strong> <span style="color:var(--color-muted);font-size:var(--font-size-xs);">(' + Utils.escHtml(id) + ')</span> passieren?</p>'
      + grpHint
      + '<div class="delete-option-card" id="del-opt-soft">'
      +   '<div class="delete-option-card__header">'
      +     '<i class="fa-solid fa-box-archive" style="color:var(--color-warning);"></i>'
      +     '<strong>Entsorgen</strong>'
      +     '<span class="badge badge-status-entsorgt" style="margin-left:auto;">In EnzyklopÃ¤die behalten</span>'
      +   '</div>'
      +   '<p class="delete-option-card__desc">Artikel bekommt Status <em>Entsorgt</em> und verschwindet aus dem Bestand. Er bleibt in der EnzyklopÃ¤die auffindbar.</p>'
      + '</div>'
      + '<div class="delete-option-card" id="del-opt-hard">'
      +   '<div class="delete-option-card__header">'
      +     '<i class="fa-solid fa-fire-flame-curved" style="color:var(--color-danger);"></i>'
      +     '<strong>Dauerhaft lÃ¶schen</strong>'
      +     '<span style="margin-left:auto;font-size:var(--font-size-xs);background:var(--color-danger-light);color:var(--color-danger);padding:2px 8px;border-radius:99px;font-weight:700;">EndgÃ¼ltig</span>'
      +   '</div>'
      +   '<p class="delete-option-card__desc">Artikel wird <strong>vollstÃ¤ndig und unwiderruflich</strong> aus allen Daten entfernt. Nicht rÃ¼ckgÃ¤ngig machbar!</p>'
      + '</div>'
      + '<div class="modal-actions" style="margin-top:20px;">'
      +   '<button class="btn btn-ghost" onclick="Modal.close()"><i class="fa-solid fa-xmark"></i> Abbrechen</button>'
      +   '<button class="btn btn-danger" id="confirm-del-article-btn" disabled>'
      +     '<i class="fa-solid fa-trash-can"></i> BestÃ¤tigen'
      +   '</button>'
      + '</div>',
      content => {
        const softCard   = content.querySelector('#del-opt-soft');
        const hardCard   = content.querySelector('#del-opt-hard');
        const confirmBtn = content.querySelector('#confirm-del-article-btn');
        let   mode       = null;
        const selectCard = (sel, other, m) => {
          mode = m;
          sel.classList.add('is-selected');
          other.classList.remove('is-selected');
          confirmBtn.disabled = false;
        };
        softCard.addEventListener('click', () => selectCard(softCard, hardCard, 'soft'));
        hardCard.addEventListener('click', () => selectCard(hardCard, softCard, 'hard'));
        confirmBtn.addEventListener('click', () => {
          if (!mode) return;
          if (mode === 'soft') {
            DB.updateArticle(id, { status: 'Entsorgt', groupId: null });
            Modal.close();
            Toast.success('Artikel ' + id + ' entsorgt â€” bleibt in der EnzyklopÃ¤die.');
          } else {
            DB.hardDeleteArticle(id);
            Modal.close();
            Toast.success('Artikel ' + id + ' dauerhaft gelÃ¶scht.');
          }
          this.render();
          Dashboard.renderStats();
        });
      }
    );
  },

  quickSellModal(id) {
    const article = DB.getArticleById(id);
    if (!article) return;
    const name  = Utils.escHtml(Utils.articleDisplayName(article, id));
    const today = new Date().toISOString().split('T')[0];
    Modal.open(
      '<h2 class="modal-title" style="color:var(--color-success);">'
      + '<i class="fa-solid fa-handshake"></i> Verkauft markieren</h2>'
      + '<p style="margin-bottom:16px;"><strong>' + name + '</strong>'
      + ' <span style="color:var(--color-muted);font-size:var(--font-size-xs);">(' + Utils.escHtml(id) + ')</span></p>'
      + '<div class="form-group" style="margin-bottom:12px;">'
      +   '<label style="font-size:var(--font-size-sm);font-weight:600;">Verkaufspreis (â‚¬)</label>'
      +   '<input type="number" id="qs-sold-price" class="form-control" min="0" step="0.01" '
      +     'placeholder="0,00" value="' + Utils.escHtml(String(article.soldPrice ?? '')) + '"/>'
      + '</div>'
      + '<div class="form-group" style="margin-bottom:20px;">'
      +   '<label style="font-size:var(--font-size-sm);font-weight:600;">Verkaufsdatum</label>'
      +   '<input type="date" id="qs-sold-date" class="form-control" value="' + today + '"/>'
      + '</div>'
      + '<div class="modal-actions">'
      +   '<button class="btn btn-ghost" onclick="Modal.close()"><i class="fa-solid fa-xmark"></i> Abbrechen</button>'
      +   '<button class="btn btn-success" id="qs-confirm-btn"><i class="fa-solid fa-handshake"></i> Als verkauft speichern</button>'
      + '</div>',
      content => {
        content.querySelector('#qs-confirm-btn').addEventListener('click', () => {
          const soldPrice = parseFloat(content.querySelector('#qs-sold-price').value) || null;
          const soldDate  = content.querySelector('#qs-sold-date').value || today;
          DB.updateArticle(id, { status: 'Verkauft', soldPrice, soldDate });
          Modal.close();
          const priceStr = soldPrice
            ? ' fÃ¼r ' + soldPrice.toLocaleString('de-DE', { style: 'currency', currency: 'EUR' })
            : '';
          Toast.success('Artikel ' + id + ' als verkauft markiert' + priceStr + '.');
          this.render();
          Dashboard.renderStats();
        });
      }
    );
  },

  assignGroupModal(articleId) {
    const groups  = DB.getGroups().filter(g => g.status !== 'Entsorgt');
    const article = DB.getArticleById(articleId);
    if (!article) return;
    if (!groups.length) { Toast.warning('Noch keine Gruppen vorhanden.'); return; }
    const rows = groups.map(g => {
      const count = DB.getArticlesByGroup(g.id).length;
      return `
        <label class="assign-row"
               style="display:flex;align-items:center;gap:12px;padding:10px;border-radius:var(--border-radius-sm);cursor:pointer;transition:background var(--transition);"
               onmouseover="this.style.background='var(--color-bg)'"
               onmouseout="this.style.background=''">
          <input type="radio" name="group-pick" value="${Utils.escHtml(g.id)}"
                 style="width:16px;height:16px;accent-color:var(--color-primary);flex-shrink:0;"
                 ${article.groupId === g.id ? 'checked' : ''}/>
          ${g.image
            ? `<img src="${g.image}" style="width:44px;height:44px;object-fit:cover;border-radius:var(--border-radius-sm);flex-shrink:0;" alt="Bild"/>`
            : `<div style="width:44px;height:44px;background:var(--color-border);border-radius:var(--border-radius-sm);display:flex;align-items:center;justify-content:center;color:var(--color-muted);flex-shrink:0;">
                 <i class="fa-solid fa-layer-group"></i>
               </div>`}
          <div style="flex:1;min-width:0;">
            <div style="font-weight:700;font-size:var(--font-size-sm);">
              ${g.name ? Utils.escHtml(g.name) : '<em style="color:var(--color-muted)">Kein Name</em>'}
            </div>
            <div style="font-size:var(--font-size-xs);color:var(--color-muted);margin-top:1px;">${Utils.escHtml(g.id)}</div>
            <div style="display:flex;gap:8px;margin-top:4px;flex-wrap:wrap;">
              ${Utils.statusBadge(g.status)}
              <span style="font-size:var(--font-size-xs);color:var(--color-muted);"><i class="fa-solid fa-cube"></i> ${count} Artikel</span>
            </div>
          </div>
        </label>`;
    }).join('');
    const detachOption = article.groupId
      ? `<div style="margin-top:10px;">
           <label class="assign-row" style="display:flex;align-items:center;gap:10px;padding:10px;border-radius:var(--border-radius-sm);cursor:pointer;border:1px dashed var(--color-border);"
                  onmouseover="this.style.background='var(--color-bg)'" onmouseout="this.style.background=''">
             <input type="radio" name="group-pick" value="" style="width:16px;height:16px;accent-color:var(--color-danger);flex-shrink:0;"/>
             <i class="fa-solid fa-link-slash" style="color:var(--color-danger);"></i>
             <span style="font-size:var(--font-size-sm);color:var(--color-danger);font-weight:600;">Aus Gruppe entfernen</span>
           </label>
         </div>`
      : '';
    Modal.open(`
      <h2 class="modal-title"><i class="fa-solid fa-layer-group"></i> Gruppe manuell zuordnen</h2>
      <p style="color:var(--color-text-secondary);font-size:var(--font-size-sm);margin-bottom:12px;">
        Artikel <strong>${Utils.escHtml(articleId)}</strong>
        ${article.groupId
          ? `ist aktuell Gruppe <strong>${Utils.escHtml(DB.getGroupById(article.groupId)?.name ?? article.groupId)}</strong> zugeordnet.`
          : 'ist noch keiner Gruppe zugeordnet.'}
      </p>
      <div style="position:relative;margin-bottom:10px;">
        <i class="fa-solid fa-magnifying-glass" style="position:absolute;left:10px;top:50%;transform:translateY(-50%);color:var(--color-muted);pointer-events:none;"></i>
        <input type="search" id="group-pick-search" placeholder="Gruppe suchen â€¦" style="padding-left:34px;width:100%;"/>
      </div>
      <div id="group-pick-list" style="max-height:320px;overflow-y:auto;border:1px solid var(--color-border);border-radius:var(--border-radius-sm);">
        ${rows}
      </div>
      ${detachOption}
      <div class="modal-actions">
        <button class="btn btn-ghost" onclick="Modal.close()">Abbrechen</button>
        <button class="btn btn-primary" id="confirm-group-pick-btn"><i class="fa-solid fa-floppy-disk"></i> Speichern</button>
      </div>`,
      content => {
        content.querySelector('#group-pick-search').addEventListener('input', e => {
          const q = e.target.value.toLowerCase();
          content.querySelectorAll('.assign-row').forEach(row => {
            row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
          });
        });
        content.querySelector('#confirm-group-pick-btn').addEventListener('click', () => {
          const selected = content.querySelector('input[name="group-pick"]:checked');
          if (!selected) { Toast.warning('Bitte eine Gruppe ausw\u00e4hlen.'); return; }
          const newGroupId = selected.value || null;
          DB.updateArticle(articleId, { groupId: newGroupId });
          Modal.close();
          const g = newGroupId ? DB.getGroupById(newGroupId) : null;
          Toast.success(newGroupId
            ? `Artikel ${articleId} â†’ â€ž${g?.name ?? newGroupId}" zugeordnet.`
            : `Artikel ${articleId} aus Gruppe entfernt.`
          );
          this.render();
          Dashboard.renderStats();
        });
      }
    );
  },
};
/* ============================================================
   11. SOLD â€” Verkaufte Artikel
============================================================ */
const Sold = {

  _renderQueued: false,
  _searchRender: null,

  init() {
    this._searchRender = Utils.debounce(() => this.queueRender());
    document.getElementById('sold-search')
      .addEventListener('input', () => this._searchRender());
    document.getElementById('sold-filter-payment-status')
      .addEventListener('change', () => this.queueRender());
    document.getElementById('sold-filter-invoice-status')
      .addEventListener('change', () => this.queueRender());
    document.getElementById('sold-period-preset')
      .addEventListener('change', () => {
        const val = document.getElementById('sold-period-preset').value;
        const wrapper = document.getElementById('sold-date-range-wrapper');
        if (val === 'custom') {
          wrapper.style.display = 'flex';
        } else {
          wrapper.style.display = 'none';
          const { from, to } = this._presetRange(val);
          document.getElementById('sold-date-from').value = from;
          document.getElementById('sold-date-to').value = to;
        }
        this.queueRender();
      });
    document.getElementById('sold-date-from')
      .addEventListener('change', () => this.queueRender());
    document.getElementById('sold-date-to')
      .addEventListener('change', () => this.queueRender());
    document.getElementById('btn-sold-back')
      .addEventListener('click', () => this.closeDetail());
    document.getElementById('btn-sold-open-order')
      .addEventListener('click', () => this.openSelectedOrder());
  },

  queueRender() {
    if (this._renderQueued) return;
    this._renderQueued = true;
    requestAnimationFrame(() => {
      this._renderQueued = false;
      this.render();
    });
  },

  _presetRange(preset) {
    const now = new Date();
    const pad = n => String(n).padStart(2, '0');
    const fmt = d => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
    const today = fmt(now);
    if (preset === 'week') {
      const day = now.getDay() || 7;
      const mon = new Date(now);
      mon.setDate(now.getDate() - day + 1);
      return { from: fmt(mon), to: today };
    }
    if (preset === 'month') return { from: `${now.getFullYear()}-${pad(now.getMonth() + 1)}-01`, to: today };
    if (preset === 'quarter') {
      const q = Math.floor(now.getMonth() / 3);
      const from = new Date(now.getFullYear(), q * 3, 1);
      return { from: fmt(from), to: today };
    }
    if (preset === 'year') return { from: `${now.getFullYear()}-01-01`, to: today };
    return { from: '', to: '' };
  },

  getAllOrders() {
    return DB.getOrders()
      .map(order => OrderLogic.decorate(order))
      .filter(order => OrderLogic.isVisibleInSold(order))
      .sort((left, right) => (OrderLogic.getCompletionTimestamp(right) || 0) - (OrderLogic.getCompletionTimestamp(left) || 0));
  },

  getFilteredOrders() {
    const search = document.getElementById('sold-search').value.trim();
    const dateFrom = document.getElementById('sold-date-from').value;
    const dateTo = document.getElementById('sold-date-to').value;
    const paymentStatus = document.getElementById('sold-filter-payment-status').value;
    const invoiceStatus = document.getElementById('sold-filter-invoice-status').value;

    return this.getAllOrders().filter(order => {
      const completionTs = OrderLogic.getCompletionTimestamp(order);
      const completionDate = completionTs ? Utils.formatDateInput(completionTs) : '';
      return OrderLogic.matchesSearch(order, search)
        && (!paymentStatus || order.paymentStatus === paymentStatus)
        && (!invoiceStatus || order.invoiceStatus === invoiceStatus)
        && (!dateFrom || (completionDate && completionDate >= dateFrom))
        && (!dateTo || (completionDate && completionDate <= dateTo));
    });
  },

  updateLayoutState() {
    const layout = document.querySelector('#view-sold .sold-layout');
    if (!layout) return;
    layout.classList.toggle('show-detail-mobile', !!State.selectedSoldOrderId);
  },

  renderStats(orders) {
    const totalOrders = orders.length;
    const totalRevenue = orders.reduce((sum, order) => sum + (order.pricing?.total || 0), 0);
    const paidCount = orders.filter(order => order.paymentStatus === 'Bezahlt').length;
    const invoicedCount = orders.filter(order => order.invoiceStatus === 'Erstellt').length;

    document.getElementById('sold-stat-orders').textContent = String(totalOrders);
    document.getElementById('sold-stat-revenue').textContent = Utils.formatEuro(totalRevenue);
    document.getElementById('sold-stat-paid').textContent = String(paidCount);
    document.getElementById('sold-stat-invoiced').textContent = String(invoicedCount);
  },

  renderList(orders = this.getFilteredOrders()) {
    const container = document.getElementById('sold-orders-list');

    if (!orders.length) {
      State.selectedSoldOrderId = null;
      container.innerHTML = `<div class="empty-state">
        <i class="fa-solid fa-file-invoice-dollar"></i>
        <p>Noch keine abgeschlossenen Aufträge im gewählten Zeitraum.</p>
      </div>`;
      this.updateLayoutState();
      return;
    }

    if (!orders.some(order => order.id === State.selectedSoldOrderId)) {
      State.selectedSoldOrderId = null;
    }

    container.className = 'sold-order-list';
    container.innerHTML = orders.map(order => {
      const isActive = State.selectedSoldOrderId === order.id ? ' is-active' : '';
      const completionTs = OrderLogic.getCompletionTimestamp(order);
      const completionDate = completionTs ? Utils.formatDate(completionTs) : '-';
      return `<article class="sold-order-card${isActive}" data-sold-order-id="${Utils.escHtml(order.id)}">
        <div class="sold-order-card__top">
          <div>
            <span class="order-card__eyebrow">${Utils.escHtml(order.id)}</span>
            <div class="order-card__title">${Utils.escHtml(order.customerName || 'Ohne Namen')}</div>
            <div class="sold-order-card__meta">
              <span><i class="fa-solid fa-calendar-check"></i> ${Utils.escHtml(completionDate)}</span>
              <span><i class="fa-solid fa-truck-ramp-box"></i> ${Utils.escHtml(order.fulfillmentType || 'Abholung')}</span>
              <span><i class="fa-solid fa-credit-card"></i> ${Utils.escHtml(order.paymentMethod || 'Noch offen')}</span>
            </div>
          </div>
          <div class="sold-order-card__total">
            <span class="sold-order-card__total-label">Verkauf</span>
            <span class="sold-order-card__total-value">${Utils.formatEuro(order.pricing?.total || 0)}</span>
          </div>
        </div>
        <div class="sold-order-card__footer">
          <div class="order-card__badges">
            ${OrderLogic.renderStatusPill(order.paymentStatus)}
            ${OrderLogic.renderStatusPill(order.invoiceStatus)}
          </div>
          <div class="sold-order-card__date">${order.progress.total} Stück</div>
        </div>
      </article>`;
    }).join('');

    container.querySelectorAll('[data-sold-order-id]').forEach(card => {
      card.addEventListener('click', () => this.open(card.dataset.soldOrderId));
    });
    this.updateLayoutState();
  },

  open(orderId) {
    State.selectedSoldOrderId = orderId;
    this.renderList();
    this.renderDetail();
    this.updateLayoutState();
  },

  closeDetail() {
    State.selectedSoldOrderId = null;
    this.render();
  },

  getSelectedOrder() {
    if (!State.selectedSoldOrderId) return null;
    return OrderLogic.decorate(DB.getOrderById(State.selectedSoldOrderId));
  },

  openSelectedOrder() {
    const order = this.getSelectedOrder();
    if (!order?.id || !AccessControl.can('orders.view')) return;
    State.selectedOrderId = order.id;
    Router.navigate('orders');
  },

  renderDetail() {
    const emptyState = document.getElementById('sold-empty-state');
    const shell = document.getElementById('sold-detail-shell');
    const order = this.getSelectedOrder();

    if (!order?.id) {
      emptyState.classList.remove('hidden');
      shell.classList.add('hidden');
      this.updateLayoutState();
      return;
    }

    const completionTs = OrderLogic.getCompletionTimestamp(order);
    const completionDate = completionTs ? Utils.formatDateTime(completionTs) : '-';

    emptyState.classList.add('hidden');
    shell.classList.remove('hidden');

    document.getElementById('sold-detail-order-id').textContent = order.id;
    document.getElementById('sold-detail-customer').textContent = order.customerName || 'Ohne Namen';
    document.getElementById('sold-detail-statuses').innerHTML = [
      OrderLogic.renderStatusPill(order.orderStatus),
      OrderLogic.renderStatusPill(order.warehouseStatus),
      OrderLogic.renderStatusPill(order.paymentStatus),
      OrderLogic.renderStatusPill(order.invoiceStatus),
    ].join('');
    document.getElementById('btn-sold-open-order').disabled = !AccessControl.can('orders.view');

    document.getElementById('sold-detail-meta').innerHTML = [
      `<article class="sold-meta-card">
        <span class="sold-meta-card__label">Auftragsdatum</span>
        <div class="sold-meta-card__value">${Utils.escHtml(order.orderDate || '-')}</div>
        <div class="sold-meta-card__sub">Ursprünglich angelegt im Auftrag.</div>
      </article>`,
      `<article class="sold-meta-card">
        <span class="sold-meta-card__label">Abschluss</span>
        <div class="sold-meta-card__value">${Utils.escHtml(completionDate)}</div>
        <div class="sold-meta-card__sub">Sichtbar, sobald der Auftrag übergeben und abgeschlossen wurde.</div>
      </article>`,
      `<article class="sold-meta-card">
        <span class="sold-meta-card__label">Art</span>
        <div class="sold-meta-card__value">${Utils.escHtml(order.fulfillmentType || 'Abholung')}</div>
        <div class="sold-meta-card__sub">${Utils.escHtml(order.pickupDate || 'Kein Termin hinterlegt')}</div>
      </article>`,
      `<article class="sold-meta-card">
        <span class="sold-meta-card__label">Zahlungsart</span>
        <div class="sold-meta-card__value">${Utils.escHtml(order.paymentMethod || 'Noch offen')}</div>
        <div class="sold-meta-card__sub">${Utils.escHtml(order.customerPhone || 'Keine Telefonnummer hinterlegt')}</div>
      </article>`,
    ].join('');

    document.getElementById('sold-detail-summary').innerHTML = [
      `<article class="sold-summary-card">
        <span class="sold-summary-card__label">Listenwert</span>
        <div class="sold-summary-card__value sold-summary-card__value--large">${Utils.formatEuro(order.pricing?.listTotal || 0)}</div>
        <div class="sold-summary-card__sub">Aus den hinterlegten Gruppenpreisen berechnet.</div>
      </article>`,
      `<article class="sold-summary-card">
        <span class="sold-summary-card__label">Verkauf gesamt</span>
        <div class="sold-summary-card__value sold-summary-card__value--large">${Utils.formatEuro(order.pricing?.total || 0)}</div>
        <div class="sold-summary-card__sub">Finaler Auftragswert aus den im Auftrag gespeicherten Verkaufspreisen.</div>
      </article>`,
      `<article class="sold-summary-card">
        <span class="sold-summary-card__label">Rabatt / Abweichung</span>
        <div class="sold-summary-card__value sold-summary-card__value--large">${Utils.formatEuro(order.pricing?.discount || 0)}</div>
        <div class="sold-summary-card__sub">${(order.pricing?.discount || 0) > 0 ? 'Unter dem Listenwert verkauft.' : (order.pricing?.discount || 0) < 0 ? 'Über dem Listenwert verkauft.' : 'Entspricht dem Listenwert.'}</div>
      </article>`,
      `<article class="sold-summary-card">
        <span class="sold-summary-card__label">Ohne Preis</span>
        <div class="sold-summary-card__value sold-summary-card__value--large">${order.pricing?.unpricedPositions || 0}</div>
        <div class="sold-summary-card__sub">Positionen ohne Gruppen- oder Verkaufspreis.</div>
      </article>`,
    ].join('');

    document.getElementById('sold-detail-positions').innerHTML = `
      <section class="sold-detail-section">
        <div class="sold-detail-section__header">
          <h3>Auftragspositionen</h3>
          <p>Hier stehen die finalen Verkaufspreise aus dem Auftrag. Diese Informationen bleiben vom Warenausgang getrennt.</p>
        </div>
        <div class="sold-position-list">
          ${order.positions.map(position => {
            const quantity = parseInt(position.quantity, 10) || 0;
            const standardUnit = position.defaultUnitPrice !== null ? Utils.formatEuro(position.defaultUnitPrice) : '–';
            const actualUnit = position.effectiveUnitPrice !== null ? Utils.formatEuro(position.effectiveUnitPrice) : '–';
            const total = position.effectiveUnitPrice !== null ? Utils.formatEuro((position.effectiveUnitPrice || 0) * quantity) : '–';
            return `<article class="sold-position-row">
              <div>
                <div class="sold-position-row__title">${Utils.escHtml(OrderLogic.getGroupLabel(position.groupId))}</div>
                <div class="sold-position-row__sub">${Utils.escHtml(position.groupId)} · ${quantity} Stück</div>
              </div>
              <div>
                <span class="sold-position-row__metric-label">Menge</span>
                <div class="sold-position-row__metric-value">${quantity}</div>
              </div>
              <div>
                <span class="sold-position-row__metric-label">Standard</span>
                <div class="sold-position-row__metric-value">${standardUnit}</div>
              </div>
              <div>
                <span class="sold-position-row__metric-label">Verkauf</span>
                <div class="sold-position-row__metric-value">${actualUnit}</div>
              </div>
              <div>
                <span class="sold-position-row__metric-label">Position</span>
                <div class="sold-position-row__metric-value">${total}</div>
              </div>
            </article>`;
          }).join('')}
        </div>
      </section>`;

    document.getElementById('sold-sumup-card').innerHTML = `
      <span class="sold-interface-card__label">SumUp später</span>
      <strong>Zahlungsseite vorbereitet</strong>
      <p>Wichtige Basis dafür sind hier schon vorhanden: Zahlungsstatus <strong>${Utils.escHtml(order.paymentStatus)}</strong> und Zahlungsart <strong>${Utils.escHtml(order.paymentMethod || 'offen')}</strong>.</p>`;

    document.getElementById('sold-easybill-card').innerHTML = `
      <span class="sold-interface-card__label">easybill später</span>
      <strong>Rechnungsseite vorbereitet</strong>
      <p>Die spätere Anbindung kann auf dem Auftragswert <strong>${Utils.formatEuro(order.pricing?.total || 0)}</strong> und dem Rechnungsstatus <strong>${Utils.escHtml(order.invoiceStatus)}</strong> aufsetzen.</p>`;

    this.updateLayoutState();
  },

  render() {
    if (!AccessControl.can('sold.view')) return;
    const orders = this.getFilteredOrders();
    this.renderStats(orders);
    this.renderList(orders);
    this.renderDetail();
    this.updateLayoutState();
  },
};

/* ============================================================
   12. ENCYCLOPEDIA
============================================================ */
const Encyclopedia = {

  _renderQueued: false,
  _searchRender: null,

  init() {
    this._searchRender = Utils.debounce(() => this.queueRender());
    document.getElementById('enc-search')
      .addEventListener('input',  () => this._searchRender());
    document.getElementById('enc-filter-status')
      .addEventListener('change', () => this.queueRender());
    document.getElementById('enc-filter-category')
      .addEventListener('change', () => this.queueRender());
    document.querySelectorAll('#encyclopedia-table th[data-sort]').forEach(th => {
      th.addEventListener('click', () => {
        const key = th.dataset.sort;
        if (State.encSortKey === key) {
          State.encSortDir = State.encSortDir === 'asc' ? 'desc' : 'asc';
        } else {
          State.encSortKey = key;
          State.encSortDir = 'desc';
        }
        this.queueRender();
      });
    });
  },

  queueRender() {
    if (this._renderQueued) return;
    this._renderQueued = true;
    requestAnimationFrame(() => {
      this._renderQueued = false;
      this.render();
    });
  },

  getFiltered() {
    const search   = document.getElementById('enc-search').value.trim();
    const status   = document.getElementById('enc-filter-status').value;
    const category = document.getElementById('enc-filter-category').value;
    let articles = DB.getArticles();
    if (search)   articles = articles.filter(a => Utils.articleMatchesSearch(a, search));
    if (status)   articles = articles.filter(a => a.status   === status);
    if (category) articles = articles.filter(a => a.category === category);
    const key = State.encSortKey;
    const dir = State.encSortDir === 'asc' ? 1 : -1;
    articles.sort((a, b) => {
      const av = a[key] ?? '';
      const bv = b[key] ?? '';
      if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * dir;
      return String(av).localeCompare(String(bv), 'de') * dir;
    });
    return articles;
  },

  render() {
    const tbody    = document.getElementById('encyclopedia-tbody');
    const articles = this.getFiltered();
    document.querySelectorAll('#encyclopedia-table th[data-sort]').forEach(th => {
      const icon = th.querySelector('i');
      if (!icon) return;
      if (th.dataset.sort === State.encSortKey) {
        icon.className = `fa-solid ${State.encSortDir === 'asc' ? 'fa-sort-up' : 'fa-sort-down'}`;
      } else {
        icon.className = 'fa-solid fa-sort';
      }
    });
    if (!articles.length) {
      tbody.innerHTML = `
        <tr><td colspan="11">
          <div class="empty-state">
            <i class="fa-solid fa-book-open"></i>
            <p>Keine Artikel gefunden.</p>
          </div>
        </td></tr>`;
      return;
    }
    tbody.innerHTML = articles.map(a => {
      const g          = a.groupId ? DB.getGroupById(a.groupId) : null;
      const groupLabel = g?.name
        ? `${Utils.escHtml(a.groupId)} Â· ${Utils.escHtml(g.name.substring(0, 18))}${g.name.length > 18 ? 'â€¦' : ''}`
        : (a.groupId ? Utils.escHtml(a.groupId) : 'â€“');
      const groupCell = a.groupId
        ? `<span class="enc-group-link" data-group="${Utils.escHtml(a.groupId)}"
                 style="color:var(--color-primary);font-size:var(--font-size-xs);font-weight:600;cursor:pointer;white-space:nowrap;">
             <i class="fa-solid fa-layer-group"></i> ${groupLabel}
           </span>`
        : 'â€“';
      return `
        <tr>
          <td>
            <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
              <strong>${Utils.escHtml(a.id)}</strong>
              ${Utils.isNewArticle(a) ? Utils.newBadge() : ''}
            </div>
          </td>
          <td>${Utils.escHtml(a.category  ?? 'â€“')}</td>
            <td>${Utils.escHtml(Utils.articleDisplayName(a))}</td>
          <td>${Utils.condBadge(a.condition)}</td>
          <td>${Utils.statusBadge(a.status)}</td>
          <td>${groupCell}</td>
          <td>${Utils.formatEuro(a.purchasePrice)}</td>
          <td>${Utils.formatEuro(a.soldPrice)}</td>
          <td>${Utils.escHtml(a.location ?? 'â€“')}</td>
          <td>${Utils.formatDate(a.updatedAt)}</td>
          <td>
            <div class="table-actions">
              <button class="btn btn-ghost btn-sm enc-edit-btn" data-id="${a.id}" data-tooltip="Bearbeiten">
                <i class="fa-solid fa-pen-to-square"></i>
              </button>
            </div>
          </td>
        </tr>`;
    }).join('');
    tbody.querySelectorAll('.enc-edit-btn').forEach(btn => {
      btn.addEventListener('click', () => Dashboard.loadArticleIntoForm(btn.dataset.id));
    });
    tbody.querySelectorAll('.enc-group-link').forEach(link => {
      link.addEventListener('click', () => {
        Router.navigate('groups');
        setTimeout(() => Groups.openDetail(link.dataset.group), 100);
      });
    });
  },
};

/* ============================================================
   13. GROUPS â€” GruppenÃ¼bersicht & Detailansicht
============================================================ */
const GroupSelection = {
  _active: false, _selectedIds: new Set(), _groupId: null,

  enter(groupId) {
    this._active = true; this._groupId = groupId; this._selectedIds.clear();
    document.getElementById('group-articles-container')?.classList.add('selection-mode');
    const btn = document.getElementById('btn-toggle-selection');
    if (btn) { btn.classList.add('is-active'); btn.innerHTML = '<i class="fa-solid fa-xmark"></i> Auswahl beenden'; }
    this._updateBar();
  },

  leave() {
    this._active = false; this._selectedIds.clear(); this._groupId = null;
    const c = document.getElementById('group-articles-container');
    if (c) {
      c.classList.remove('selection-mode');
      c.querySelectorAll('.group-article-card.is-selected').forEach(x => x.classList.remove('is-selected'));
      c.querySelectorAll('.article-select-cb').forEach(cb => { cb.checked = false; });
    }
    const btn = document.getElementById('btn-toggle-selection');
    if (btn) { btn.classList.remove('is-active'); btn.innerHTML = '<i class="fa-solid fa-check-square"></i> AuswÃ¤hlen'; }
    this._hideBar();
  },

  toggleMode(groupId) { this._active ? this.leave() : this.enter(groupId); },

  toggleArticle(articleId, force) {
    if (!this._active) return;
    const sel = force !== undefined ? force : !this._selectedIds.has(articleId);
    sel ? this._selectedIds.add(articleId) : this._selectedIds.delete(articleId);
    const card = document.querySelector('.group-article-card[data-article-id="' + CSS.escape(articleId) + '"]');
    if (card) card.classList.toggle('is-selected', sel);
    const cb = document.querySelector('.article-select-cb[data-article-id="' + CSS.escape(articleId) + '"]');
    if (cb) cb.checked = sel;
    this._updateBar();
  },

  selectAll() {
    document.querySelectorAll('#group-articles-container .group-article-card[data-article-id]')
      .forEach(c => this.toggleArticle(c.dataset.articleId, true));
  },

  deselectAll() {
    [...this._selectedIds].forEach(id => this.toggleArticle(id, false));
  },

  _updateBar() {
    const n   = this._selectedIds.size;
    const bar = this._getOrCreateBar();
    if (n === 0) { this._hideBar(); return; }
    bar.querySelector('.bulk-action-bar__count').textContent = n + ' Artikel ausgewÃ¤hlt';
    bar.classList.add('is-visible');
  },

  _hideBar() { document.getElementById('bulk-action-bar')?.classList.remove('is-visible'); },

  _getOrCreateBar() {
    let bar = document.getElementById('bulk-action-bar');
    if (bar) return bar;
    bar = document.createElement('div');
    bar.id = 'bulk-action-bar'; bar.className = 'bulk-action-bar';
    bar.innerHTML =
      '<div class="bulk-action-bar__info"><span class="bulk-action-bar__count">0 ausgewÃ¤hlt</span>'
      + '<button class="bulk-action-bar__link-btn" id="bulk-btn-select-all" type="button">Alle</button>'
      + '<button class="bulk-action-bar__link-btn muted" id="bulk-btn-deselect-all" type="button">Aufheben</button>'
      + '</div><div class="bulk-action-bar__actions">'
      + '<button class="btn btn-primary btn-sm" id="bulk-btn-sell" type="button"><i class="fa-solid fa-handshake"></i> Verkauft</button>'
      + '<button class="btn btn-outline btn-sm" id="bulk-btn-reserve" type="button"><i class="fa-solid fa-clock"></i> Reservieren</button>'
      + '<button class="btn btn-ghost btn-sm" id="bulk-btn-available" type="button"><i class="fa-solid fa-circle-check"></i> Verf\u00fcgbar</button>'
      + '<button class="btn btn-outline btn-sm" id="bulk-btn-edit" type="button"><i class="fa-solid fa-pen-to-square"></i> Bearbeiten</button>'
      + '<button class="btn btn-danger btn-sm" id="bulk-btn-delete" type="button"><i class="fa-solid fa-trash-can"></i> L\u00f6schen</button>'
      + '</div>';
    document.body.appendChild(bar);
    bar.querySelector('#bulk-btn-select-all').addEventListener('click', () => this.selectAll());
    bar.querySelector('#bulk-btn-deselect-all').addEventListener('click', () => this.deselectAll());
    bar.querySelector('#bulk-btn-edit').addEventListener('click', () => this.openBulkEditModal());
    bar.querySelector('#bulk-btn-sell').addEventListener('click', () => this.openSellModal());
    bar.querySelector('#bulk-btn-reserve').addEventListener('click', () => this._bulkSetStatus('Reserviert'));
    bar.querySelector('#bulk-btn-available').addEventListener('click', () => this._bulkSetStatus('Verf\u00fcgbar'));
    bar.querySelector('#bulk-btn-delete').addEventListener('click', () => this.openBulkDeleteModal());
    return bar;
  },

  _bulkSetStatus(newStatus) {
    const ids = [...this._selectedIds]; if (!ids.length) return;
    DB.updateArticles(ids, { status: newStatus });
    const gid = this._groupId; const n = ids.length;
    this.leave(); Groups._renderGroupArticles(gid); Dashboard.renderStats();
    Toast.success(n + ' Artikel auf "' + newStatus + '" gesetzt.');
  },

  openSellModal() {
    const ids = [...this._selectedIds]; if (!ids.length) return;
    const articles = ids.map(id => DB.getArticleById(id)).filter(Boolean);
    if (!articles.length) return;
    const today = new Date().toISOString().split('T')[0];
    let rows = '';
    articles.forEach(a => {
    const name = Utils.escHtml(Utils.articleDisplayName(a, '-'));
      rows += '<div class="bulk-sell-price-row" data-article-id="' + Utils.escHtml(a.id) + '">'
        + '<span class="bulk-sell-price-row__id">' + Utils.escHtml(a.id) + '</span>'
        + '<span class="bulk-sell-price-row__name">' + name + '</span>'
        + '<input type="number" class="bulk-sell-price-input" min="0" step="0.01" placeholder="0,00" value="' + Utils.escHtml(String(a.soldPrice ?? '')) + '"/>'
        + '<input type="date" class="bulk-sell-date-input" value="' + Utils.escHtml(a.soldDate ?? today) + '"/>'
        + '</div>';
    });
    Modal.open(
      '<h2 class="modal-title"><i class="fa-solid fa-handshake" style="color:var(--color-success)"></i> '
      + articles.length + ' Artikel als verkauft markieren</h2>'
      + '<div id="bulk-sell-price-list" style="max-height:340px;overflow-y:auto;border:1px solid var(--color-border);border-radius:var(--border-radius-sm);padding:6px;">' + rows + '</div>'
      + '<div class="bulk-sell-total"><span class="bulk-sell-total__label">GesamterlÃ¶s</span><span class="bulk-sell-total__value" id="bulk-sell-total-value">0,00 â‚¬</span></div>'
      + '<div class="modal-actions" style="margin-top:16px;"><button class="btn btn-ghost" onclick="Modal.close()"><i class="fa-solid fa-xmark"></i> Abbrechen</button>'
      + '<button class="btn btn-primary" id="confirm-bulk-sell-btn"><i class="fa-solid fa-floppy-disk"></i> Speichern</button></div>',
      content => {
        const recalc = () => {
          let t = 0;
          content.querySelectorAll('.bulk-sell-price-input').forEach(i => { const v = parseFloat(i.value); if (!isNaN(v)) t += v; });
          const el = content.querySelector('#bulk-sell-total-value');
          if (el) el.textContent = t.toLocaleString('de-DE', { style: 'currency', currency: 'EUR' });
        };
        content.querySelectorAll('.bulk-sell-price-input').forEach(i => i.addEventListener('input', recalc));
        recalc();
        content.querySelector('#confirm-bulk-sell-btn').addEventListener('click', () => this._confirmSell(content));
      }
    );
  },

  _confirmSell(mc) {
    const updates = [];
    mc.querySelectorAll('.bulk-sell-price-row[data-article-id]').forEach(row => {
      const id = row.dataset.articleId; if (!id) return;
      const soldPrice = parseFloat(row.querySelector('.bulk-sell-price-input')?.value) || null;
      const soldDate  = row.querySelector('.bulk-sell-date-input')?.value || new Date().toISOString().split('T')[0];
      updates.push({ id, data: { status: 'Verkauft', soldPrice, soldDate } });
    });
    const n = DB.updateArticlesBulk(updates).length;
    Modal.close(); const gid = this._groupId; this.leave();
    Groups._renderGroupArticles(gid); Dashboard.renderStats();
    Toast.success(n + ' Artikel als verkauft gespeichert.');
  },

  openBulkDeleteModal() {
    const ids = [...this._selectedIds];
    if (!ids.length) return;
    const n = ids.length;
    const gid = this._groupId;
    Modal.open(
      '<h2 class="modal-title" style="color:var(--color-danger);"><i class="fa-solid fa-trash-can"></i> '
      + n + ' Artikel l\u00f6schen</h2>'
      + '<p>Was soll mit den <strong>' + n + ' ausgew\u00e4hlten Artikeln</strong> passieren?</p>'
      + '<div class="delete-option-card" id="grp-bulk-del-opt-soft">'
      +   '<div class="delete-option-card__header"><i class="fa-solid fa-box-archive" style="color:var(--color-warning);"></i><strong>Entsorgen</strong>'
      +   '<span class="badge badge-status-entsorgt" style="margin-left:auto;">In Enzyklop\u00e4die behalten</span></div>'
      +   '<p class="delete-option-card__desc">Alle ausgew\u00e4hlten Artikel bekommen Status <em>Entsorgt</em>, werden aus der Gruppe gel\u00f6st und bleiben in der Enzyklop\u00e4die auffindbar.</p>'
      + '</div>'
      + '<div class="delete-option-card" id="grp-bulk-del-opt-hard">'
      +   '<div class="delete-option-card__header"><i class="fa-solid fa-fire-flame-curved" style="color:var(--color-danger);"></i><strong>Dauerhaft l\u00f6schen</strong>'
      +   '<span style="margin-left:auto;font-size:var(--font-size-xs);background:var(--color-danger-light);color:var(--color-danger);padding:2px 8px;border-radius:99px;font-weight:700;">Endg\u00fcltig</span></div>'
      +   '<p class="delete-option-card__desc">Alle ausgew\u00e4hlten Artikel werden <strong>vollst\u00e4ndig und unwiderruflich</strong> entfernt.</p>'
      + '</div>'
      + '<div class="modal-actions" style="margin-top:20px;"><button class="btn btn-ghost" onclick="Modal.close()"><i class="fa-solid fa-xmark"></i> Abbrechen</button>'
      +   '<button class="btn btn-danger" id="grp-bulk-delete-confirm" disabled><i class="fa-solid fa-trash-can"></i> Best\u00e4tigen</button></div>',
      content => {
        const softCard   = content.querySelector('#grp-bulk-del-opt-soft');
        const hardCard   = content.querySelector('#grp-bulk-del-opt-hard');
        const confirmBtn = content.querySelector('#grp-bulk-delete-confirm');
        let mode = null;
        const selectCard = (sel, other, m) => {
          mode = m;
          sel.classList.add('is-selected');
          other.classList.remove('is-selected');
          confirmBtn.disabled = false;
        };
        softCard.addEventListener('click', () => selectCard(softCard, hardCard, 'soft'));
        hardCard.addEventListener('click', () => selectCard(hardCard, softCard, 'hard'));
        confirmBtn.addEventListener('click', () => {
          if (!mode) return;
          if (mode === 'soft') {
            DB.updateArticles(ids, { status: 'Entsorgt', groupId: null });
            Modal.close();
            Toast.success(n + ' Artikel entsorgt — bleiben in der Enzyklopädie.');
          } else {
            ids.forEach(id => DB.hardDeleteArticle(id));
            Modal.close();
            Toast.success(n + ' Artikel dauerhaft gelöscht.');
          }
          this.leave();
          if (gid) {
            Groups._renderGroupArticles(gid);
            Groups._renderGroupInfoCard(DB.getGroupById(gid));
          }
          Dashboard.renderStats();
        });
      }
    );
  },

  openBulkEditModal() {
    const ids = [...this._selectedIds]; if (!ids.length) return;
    const n   = ids.length;
    const gid = this._groupId;
    Modal.open(
      '<h2 class="modal-title"><i class="fa-solid fa-pen-to-square"></i> ' + n + ' Artikel bearbeiten</h2>'
      + '<p style="color:var(--color-muted);font-size:var(--font-size-sm);margin-bottom:16px;">Leere Felder werden <strong>nicht</strong> überschrieben.</p>'
      + '<div class="form-group" style="margin-bottom:12px;"><label style="font-weight:600;">Status</label>'
      +   '<select id="be-status" style="width:100%;"><option value="">- nicht ändern -</option>'
      +   '<option value="Verf\u00fcgbar">Verf\u00fcgbar</option><option value="Reserviert">Reserviert</option>'
      +   '<option value="Verkauft">Verkauft</option><option value="Entsorgt">Entsorgt</option></select></div>'
      + '<div class="form-group" style="margin-bottom:12px;"><label style="font-weight:600;">Standort</label>'
      +   '<input type="text" id="be-location" placeholder="- nicht ändern -" style="width:100%;"/></div>'
      + '<div class="form-group" style="margin-bottom:12px;"><label style="font-weight:600;">Zustand</label>'
      +   '<select id="be-condition" style="width:100%;"><option value="">- nicht ändern -</option>'
      +   '<option value="Neuwertig">Neuwertig</option>'
      +   '<option value="Leichte Gebrauchsspuren">Leichte Gebrauchsspuren</option>'
      +   '<option value="Mittlere Gebrauchsspuren">Mittlere Gebrauchsspuren</option>'
      +   '<option value="Starke Gebrauchsspuren">Starke Gebrauchsspuren</option>'
      +   '<option value="Defekt">Defekt</option></select></div>'
      + '<div class="form-group" style="margin-bottom:20px;"><label style="font-weight:600;">Bemerkungen</label>'
      +   '<input type="text" id="be-notes" placeholder="- nicht ändern -" style="width:100%;"/></div>'
      + '<div class="modal-actions"><button class="btn btn-ghost" onclick="Modal.close()"><i class="fa-solid fa-xmark"></i> Abbrechen</button>'
      +   '<button class="btn btn-primary" id="be-confirm"><i class="fa-solid fa-floppy-disk"></i> Auf alle anwenden</button></div>',
      content => {
        content.querySelector('#be-confirm').addEventListener('click', () => {
          const upd       = {};
          const status    = content.querySelector('#be-status').value;
          const location  = content.querySelector('#be-location').value.trim();
          const condition = content.querySelector('#be-condition').value;
          const notes     = content.querySelector('#be-notes').value.trim();
          if (status)    upd.status    = status;
          if (location)  upd.location  = location;
          if (condition) upd.condition = condition;
          if (notes)     upd.notes     = notes;
          if (!Object.keys(upd).length) { Toast.warning('Keine Ã„nderungen eingegeben.'); return; }
          DB.updateArticles(ids, upd);
          Modal.close(); this.leave();
          if (gid) { Groups._renderGroupArticles(gid); Groups._renderGroupInfoCard(DB.getGroupById(gid)); }
          if (State.currentView === 'inventory') Inventory.render();
          Dashboard.renderStats();
          Toast.success(n + ' Artikel aktualisiert.');
        });
      }
    );
  },
};

const Groups = {

  _currentGroupId: null,
  _renderQueued: false,
  _groupArticlesRenderQueued: false,
  _searchRender: null,
  _groupArticleSearchRender: null,
  _externalQrMode: false,
  _externalQrQueue: [],
  _externalQrHistory: [],
  _externalQrRecentAssignments: [],
  _externalQrLastAssignment: null,

  init() {
    this._searchRender = Utils.debounce(() => this.queueRender());
    this._groupArticleSearchRender = Utils.debounce(() => this.queueRenderGroupArticles());
    document.getElementById('groups-search')
      .addEventListener('input',  () => this._searchRender());
    document.getElementById('groups-filter-status')
      .addEventListener('change', () => this.queueRender());
    document.getElementById('btn-new-group-shortcut')
      .addEventListener('click', () => {
        Router.navigate('dashboard');
        document.querySelectorAll('.form-tab').forEach(b  => b.classList.remove('active'));
        document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
        document.querySelector('[data-tab="tab-group"]').classList.add('active');
        document.getElementById('tab-group').classList.add('active');
        Dashboard.resetGroupForm();
      });
    document.getElementById('btn-back-to-groups')
      .addEventListener('click', () => { GroupSelection.leave(); this._showOverview(); this.render(); });
    document.getElementById('btn-group-external-qr-mode')
      .addEventListener('click', () => this.startExternalQrMode());
    document.getElementById('btn-group-external-qr-open-scanner')
      .addEventListener('click', () => this.openExternalQrScanner());
    document.getElementById('group-external-qr-input')
      .addEventListener('keydown', e => {
        if (e.key !== 'Enter') return;
        e.preventDefault();
        this.assignExternalQrToCurrentArticle(e.target.value);
      });
    document.getElementById('btn-group-external-qr-undo')
      .addEventListener('click', () => this.undoLastExternalQrAssignment());
    document.getElementById('btn-group-external-qr-skip')
      .addEventListener('click', () => this.skipExternalQrTarget());
    document.getElementById('btn-group-external-qr-end')
      .addEventListener('click', () => this.stopExternalQrMode());
  },

  queueRender() {
    if (this._renderQueued) return;
    this._renderQueued = true;
    requestAnimationFrame(() => {
      this._renderQueued = false;
      this.render();
    });
  },

  queueRenderGroupArticles() {
    if (this._groupArticlesRenderQueued) return;
    this._groupArticlesRenderQueued = true;
    requestAnimationFrame(() => {
      this._groupArticlesRenderQueued = false;
      if (this._currentGroupId) this._renderGroupArticles(this._currentGroupId);
    });
  },

  openDetail(groupId) { this._openDetail(groupId); },

  render() {
    const search = document.getElementById('groups-search').value.trim();
    const status = document.getElementById('groups-filter-status').value;
    let groups   = DB.getGroups().filter(g =>
      !search || [g.id, g.name, g.location, g.notes]
        .some(v => (v ?? '').toLowerCase().includes(search.toLowerCase()))
    );
    if (status) groups = groups.filter(g => g.status === status);
    groups.sort((a, b) => b.updatedAt - a.updatedAt);

    const container = document.getElementById('groups-container');
    if (!groups.length) {
      container.innerHTML = `<div class="empty-state" style="grid-column:1/-1;">
        <i class="fa-solid fa-layer-group"></i>
        <p>Noch keine Gruppen vorhanden.<br/>Erstelle die erste Gruppe im Dashboard.</p>
      </div>`;
      return;
    }
    container.innerHTML = groups.map(g => this._renderGroupCard(g)).join('');
    container.querySelectorAll('.group-card[data-group-id]').forEach(card => {
      card.addEventListener('click', e => {
        if (e.target.closest('[data-action]')) return;
        this._openDetail(card.dataset.groupId);
      });
      card.addEventListener('keydown', e => {
        if (e.key === 'Enter' || e.key === ' ') this._openDetail(card.dataset.groupId);
      });
    });
    container.querySelectorAll('[data-action]').forEach(btn => {
      btn.addEventListener('click', e => {
        e.stopPropagation();
        const { action, id } = btn.dataset;
        if (action === 'edit-group')   this._editGroup(id);
        if (action === 'delete-group') this._confirmDeleteGroup(id);
      });
    });
  },

  _renderGroupCard(group) {
    const articles    = DB.getArticlesByGroup(group.id);
    const activeCount = articles.filter(a => Utils.normalizeStatus(a.status) !== 'Entsorgt').length;
    const soldCount   = articles.filter(a => Utils.normalizeStatus(a.status) === 'Verkauft').length;
    const totalQty    = articles.reduce((s, a) => s + (parseInt(a.quantity) || 1), 0);
    const firstPhoto  = group.image || (articles.find(a => a.photos && a.photos[0])?.photos[0]) || null;
    const imgHtml     = firstPhoto
      ? `<img src="${firstPhoto}" alt="Gruppenbild" loading="lazy"/>`
      : `<div class="group-card-image-placeholder"><i class="fa-solid fa-layer-group"></i></div>`;
    const condMap = {};
    articles.forEach(a => { if (a.condition) condMap[a.condition] = (condMap[a.condition] || 0) + 1; });
    const condChips = Object.entries(condMap).map(([c, n]) =>
      `<span class="cond-chip" style="background:${Utils.condColor(c)};">${n}x ${c.replace(' Gebrauchsspuren', '')}</span>`
    ).join('');
    const priceDisplay = group.priceGross
      ? Utils.formatEuro(group.priceGross)
      : (group.priceNet ? Utils.formatEuro(group.priceNet) + ' netto' : '-');
    const derivedGroupName = Utils.groupDisplayName(group, articles);
    const displayName = derivedGroupName
      ? Utils.escHtml(derivedGroupName)
      : `<span style="color:var(--color-muted);font-style:italic;">Kein Name vergeben</span>`;
    return `
      <div class="group-card" data-group-id="${Utils.escHtml(group.id)}" role="button" tabindex="0"
           aria-label="Gruppe ${Utils.escHtml(derivedGroupName || group.name || group.id)} \u00f6ffnen">
        <div class="group-card-image">
          ${imgHtml}
          <div class="article-count-badge"><i class="fa-solid fa-cube"></i> ${totalQty} St\u00fcck</div>
          <div class="card-badges" style="top:8px;right:8px;">${Utils.statusBadge(group.status)}</div>
        </div>
        <div class="group-card-body">
          <span class="group-card-id">${Utils.escHtml(group.id)}</span>
          <div class="group-card-name">${displayName}</div>
          ${group.notes ? `<div class="group-card-subtitle">${Utils.escHtml(group.notes.substring(0, 60))}${group.notes.length > 60 ? '...' : ''}</div>` : ''}
          <div class="group-card-stats">
            <div class="group-stat"><span class="group-stat-value">${priceDisplay}</span><span class="group-stat-label">Zielpreis</span></div>
            <div class="group-stat"><span class="group-stat-value">${totalQty}</span><span class="group-stat-label"><i class="fa-solid fa-cube"></i> St\u00fcck gesamt</span></div>
            <div class="group-stat"><span class="group-stat-value">${soldCount}/${activeCount}</span><span class="group-stat-label">Verkauft</span></div>
            ${group.location ? `<div class="group-stat"><span class="group-stat-value" style="font-size:var(--font-size-sm);">${Utils.escHtml(group.location)}</span><span class="group-stat-label"><i class="fa-solid fa-location-dot"></i> Standort</span></div>` : ''}
          </div>
          ${condChips ? `<div class="group-condition-strip">${condChips}</div>` : ''}
        </div>
        <div class="group-card-footer">
          <div class="action-btn-group">
            <button class="btn-icon-label" data-action="edit-group" data-id="${Utils.escHtml(group.id)}" data-tooltip="Gruppendetails bearbeiten">
              <i class="fa-solid fa-pen-to-square"></i> Bearbeiten
            </button>
            <button class="btn-icon-label danger" data-action="delete-group" data-id="${Utils.escHtml(group.id)}" data-tooltip="Gruppe als entsorgt markieren">
              <i class="fa-solid fa-trash-can"></i> Entsorgen
            </button>
          </div>
          <span class="group-card-arrow"><i class="fa-solid fa-magnifying-glass"></i> Details <i class="fa-solid fa-arrow-right"></i></span>
        </div>
      </div>`;
  },

  _openDetail(groupId) {
    const group = DB.getGroupById(groupId);
    if (!group) return;
    this._resetExternalQrState();
    this._currentGroupId = groupId;
    document.getElementById('groups-overview').classList.add('hidden');
    document.getElementById('group-detail-view').classList.remove('hidden');
    const _t = document.getElementById('group-articles-toolbar');
    if (_t) {
      const _s = document.getElementById('ga-search');        if (_s) _s.value = '';
      const _f = document.getElementById('ga-filter-status'); if (_f) _f.value = '';
      const _o = document.getElementById('ga-sort');          if (_o) _o.value = 'id-asc';
    }
    this._renderDetailMeta(group);
    this._renderGroupInfoCard(group);
    this._renderGroupArticles(groupId);
  },

  _showOverview() {
    this._resetExternalQrState();
    this._currentGroupId = null;
    document.getElementById('groups-overview').classList.remove('hidden');
    document.getElementById('group-detail-view').classList.add('hidden');
  },

  _resetExternalQrState() {
    this._externalQrMode = false;
    this._externalQrQueue = [];
    this._externalQrHistory = [];
    this._externalQrRecentAssignments = [];
    this._externalQrLastAssignment = null;
    const input = document.getElementById('group-external-qr-input');
    if (input) input.value = '';
  },

  _buildExternalQrQueue(groupId) {
    return DB.getArticlesByGroup(groupId)
      .filter(article => !String(article.externalQrCode ?? '').trim())
      .sort((a, b) => a.id.localeCompare(b.id))
      .map(article => article.id);
  },

  _syncExternalQrQueue(groupId = this._currentGroupId) {
    if (!this._externalQrMode || !groupId) return;
    const missingIds = this._buildExternalQrQueue(groupId);
    const missingIdSet = new Set(missingIds);
    this._externalQrQueue = this._externalQrQueue.filter(id => missingIdSet.has(id));
    missingIds.forEach(id => {
      if (!this._externalQrQueue.includes(id)) this._externalQrQueue.push(id);
    });
  },

  _getExternalQrTargetArticle(groupId = this._currentGroupId) {
    this._syncExternalQrQueue(groupId);
    const targetId = this._externalQrQueue[0];
    return targetId ? DB.getArticleById(targetId) : null;
  },

  _focusExternalQrInput() {
    if (!this._externalQrMode) return;
    const input = document.getElementById('group-external-qr-input');
    if (!input || input.disabled) return;
    window.setTimeout(() => {
      input.focus();
      input.select();
    }, 0);
  },

  _refreshExternalQrScannerContext() {
    if (State.currentView === 'scanner' && typeof QRScanner !== 'undefined') {
      QRScanner._renderExternalQrContext();
    }
  },

  isExternalQrModeActive() {
    return !!(this._externalQrMode && this._currentGroupId);
  },

  getExternalQrScannerContext() {
    if (!this.isExternalQrModeActive()) return null;
    const group = DB.getGroupById(this._currentGroupId);
    if (!group) return null;
    const articles = DB.getArticlesByGroup(this._currentGroupId)
      .sort((a, b) => a.id.localeCompare(b.id));
    return {
      group,
      groupId: group.id,
      groupName: Utils.groupDisplayName(group, articles, group.id) || group.id,
      totalCount: articles.length,
      assignedCount: articles.filter(article => String(article.externalQrCode ?? '').trim()).length,
      targetArticle: this._getExternalQrTargetArticle(this._currentGroupId),
    };
  },

  openExternalQrScanner() {
    if (!this._currentGroupId) return;
    if (!this._externalQrMode) this.startExternalQrMode();
    Router.navigate('scanner');
    window.setTimeout(() => {
      QRScanner.setMode('single');
      QRScanner._renderExternalQrContext();
    }, 60);
  },

  _getExternalQrDuplicateTemplate(groupId = this._currentGroupId) {
    const articles = DB.getArticlesByGroup(groupId)
      .sort((a, b) => a.id.localeCompare(b.id));
    if (!articles.length) return null;
    const lastAssignedArticle = this._externalQrLastAssignment?.articleId
      ? DB.getArticleById(this._externalQrLastAssignment.articleId)
      : null;
    if (lastAssignedArticle && lastAssignedArticle.groupId === groupId) {
      return lastAssignedArticle;
    }
    return articles[articles.length - 1] ?? null;
  },

  async _createExternalQrDuplicate(groupId = this._currentGroupId) {
    const templateArticle = this._getExternalQrDuplicateTemplate(groupId);
    if (!templateArticle) {
      Toast.error('In dieser Gruppe gibt es keinen Artikel zum Duplizieren.');
      return null;
    }

    const duplicateData = {
      ...templateArticle,
      photos: Array.isArray(templateArticle.photos) ? [...templateArticle.photos] : [],
      quantity: 1,
      externalQrCode: null,
      externalQrAssignedAt: null,
      externalQrAssignedBy: null,
    };
    delete duplicateData.id;
    delete duplicateData.createdAt;
    delete duplicateData.updatedAt;
    delete duplicateData.publicQrToken;

    const duplicateArticle = await DB.saveArticle(duplicateData);
    return { duplicateArticle, templateArticle };
  },

  startExternalQrMode() {
    if (!this._currentGroupId) return;
    const searchInput = document.getElementById('ga-search');
    const statusInput = document.getElementById('ga-filter-status');
    const sortInput = document.getElementById('ga-sort');
    if (searchInput) searchInput.value = '';
    if (statusInput) statusInput.value = '';
    if (sortInput) sortInput.value = 'id-asc';
    this._externalQrMode = true;
    this._externalQrQueue = this._buildExternalQrQueue(this._currentGroupId);
    this._externalQrHistory = [];
    this._externalQrRecentAssignments = [];
    this._externalQrLastAssignment = null;
    this._renderExternalQrPanel(this._currentGroupId);
    this._renderGroupArticles(this._currentGroupId);
    this._focusExternalQrInput();
    this._refreshExternalQrScannerContext();
  },

  stopExternalQrMode() {
    if (!this._externalQrMode) return;
    this._resetExternalQrState();
    if (this._currentGroupId) this._renderGroupArticles(this._currentGroupId);
    this._refreshExternalQrScannerContext();
  },

  skipExternalQrTarget() {
    if (!this._externalQrMode || !this._currentGroupId) return;
    this._syncExternalQrQueue(this._currentGroupId);
    if (!this._externalQrQueue.length) {
      Toast.warning('Es gibt keinen offenen Artikel mehr für die Serienzuordnung.');
      return;
    }
    if (this._externalQrQueue.length === 1) {
      Toast.warning('Es gibt nur noch einen offenen Artikel in dieser Gruppe.');
      return;
    }
    const [currentId, ...rest] = this._externalQrQueue;
    this._externalQrQueue = [...rest, currentId];
    this._renderExternalQrPanel(this._currentGroupId);
    this._renderGroupArticles(this._currentGroupId);
    this._focusExternalQrInput();
    this._refreshExternalQrScannerContext();
  },

  async assignExternalQrToCurrentArticle(rawValue) {
    if (!this._externalQrMode || !this._currentGroupId) return false;
    const input = document.getElementById('group-external-qr-input');
    const value = String(rawValue ?? '').trim();
    if (!value) {
      this._focusExternalQrInput();
      return false;
    }

    try {
      const queueBefore = [...this._externalQrQueue];
      let targetArticle = this._getExternalQrTargetArticle(this._currentGroupId);
      const validation = ScanResolver.validateExternalQrCode(value, targetArticle?.id ?? null);
      if (!validation.ok) {
        Toast.error(validation.message);
        if (input) {
          input.focus();
          input.select();
        }
        return false;
      }

      let duplicateMeta = null;
      if (!targetArticle) {
        duplicateMeta = await this._createExternalQrDuplicate(this._currentGroupId);
        if (!duplicateMeta) {
          if (input) input.value = '';
          this._renderExternalQrPanel(this._currentGroupId);
          this._renderGroupArticles(this._currentGroupId);
          this._refreshExternalQrScannerContext();
          return false;
        }
        targetArticle = duplicateMeta.duplicateArticle;
      }

      if (String(targetArticle.externalQrCode ?? '').trim()) {
        Toast.error(`Artikel ${targetArticle.id} hat bereits einen Fremd-QR-Code. Bitte zuerst entfernen.`);
        this._renderExternalQrPanel(this._currentGroupId);
        this._renderGroupArticles(this._currentGroupId);
        this._focusExternalQrInput();
        this._refreshExternalQrScannerContext();
        return false;
      }

      const assignmentId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const articleName = Utils.articleDisplayName(targetArticle, targetArticle.id);

      DB.updateArticle(targetArticle.id, { externalQrCode: validation.value });
      this._externalQrQueue = queueBefore.filter(id => id !== targetArticle.id);
      this._externalQrHistory.push({
        assignmentId,
        articleId: targetArticle.id,
        previousCode: targetArticle.externalQrCode || null,
        newCode: validation.value,
        queueBefore,
        createdArticleId: duplicateMeta?.duplicateArticle?.id ?? null,
        duplicatedFromArticleId: duplicateMeta?.templateArticle?.id ?? null,
      });
      this._externalQrRecentAssignments = [
        {
          assignmentId,
          articleId: targetArticle.id,
          articleName,
          code: validation.value,
        },
        ...this._externalQrRecentAssignments,
      ].slice(0, 12);
      this._externalQrLastAssignment = {
        articleId: targetArticle.id,
        articleName,
        code: validation.value,
      };

      if (input) input.value = '';

      if (duplicateMeta?.duplicateArticle) {
        const group = DB.getGroupById(this._currentGroupId);
        if (group) this._renderGroupInfoCard(group);
        Toast.success(`Fremd-QR ${validation.value} wurde ${targetArticle.id} zugeordnet. ${targetArticle.id} wurde dafür automatisch aus ${duplicateMeta.templateArticle.id} dupliziert.`);
      } else {
        Toast.success(`Fremd-QR ${validation.value} wurde ${targetArticle.id} zugeordnet.`);
      }
      this._renderExternalQrPanel(this._currentGroupId);
      this._renderGroupArticles(this._currentGroupId);
      this._focusExternalQrInput();
      this._refreshExternalQrScannerContext();
      return true;
    } catch (err) {
      console.error('assignExternalQrToCurrentArticle failed:', err);
      Toast.error('Fremd-QR-Zuordnung fehlgeschlagen.');
      this._refreshExternalQrScannerContext();
      return false;
    }
  },

  undoLastExternalQrAssignment() {
    if (!this._externalQrMode || !this._currentGroupId) return;
    const lastAssignment = this._externalQrHistory.pop();
    if (!lastAssignment) {
      Toast.warning('Es gibt keinen Scan zum Rückgängig machen.');
      return;
    }

    if (lastAssignment.createdArticleId && lastAssignment.createdArticleId === lastAssignment.articleId) {
      DB.hardDeleteArticle(lastAssignment.articleId);
      const group = DB.getGroupById(this._currentGroupId);
      if (group) this._renderGroupInfoCard(group);
    } else {
      DB.updateArticle(lastAssignment.articleId, {
        externalQrCode: lastAssignment.previousCode || null,
      });
    }
    this._externalQrQueue = [...lastAssignment.queueBefore];
    this._externalQrRecentAssignments = this._externalQrRecentAssignments
      .filter(entry => entry.assignmentId !== lastAssignment.assignmentId);

    const latest = this._externalQrRecentAssignments[0] ?? null;
    this._externalQrLastAssignment = latest
      ? {
          articleId: latest.articleId,
          articleName: latest.articleName,
          code: latest.code,
        }
      : null;

    Toast.success(
      lastAssignment.createdArticleId && lastAssignment.createdArticleId === lastAssignment.articleId
        ? `Der letzte Fremd-QR-Scan für ${lastAssignment.articleId} wurde rückgängig gemacht und das automatische Duplikat entfernt.`
        : `Der letzte Fremd-QR-Scan für ${lastAssignment.articleId} wurde rückgängig gemacht.`
    );
    this._renderExternalQrPanel(this._currentGroupId);
    this._renderGroupArticles(this._currentGroupId);
    this._focusExternalQrInput();
    this._refreshExternalQrScannerContext();
  },

  _enqueueExternalQrArticle(articleId, addToFront = false) {
    if (!this._externalQrMode) return;
    const article = DB.getArticleById(articleId);
    if (!article || article.groupId !== this._currentGroupId) return;
    if (String(article.externalQrCode ?? '').trim()) return;
    this._externalQrQueue = this._externalQrQueue.filter(id => id !== articleId);
    if (addToFront) this._externalQrQueue.unshift(articleId);
    else this._externalQrQueue.push(articleId);
  },

  _renderExternalQrPanel(groupId) {
    const panel = document.getElementById('group-external-qr-panel');
    const progressEl = document.getElementById('group-external-qr-progress');
    const targetEl = document.getElementById('group-external-qr-target');
    const historyEl = document.getElementById('group-external-qr-history');
    const lastEl = document.getElementById('group-external-qr-last-assignment');
    const input = document.getElementById('group-external-qr-input');
    const undoBtn = document.getElementById('btn-group-external-qr-undo');
    const skipBtn = document.getElementById('btn-group-external-qr-skip');
    const openScannerBtn = document.getElementById('btn-group-external-qr-open-scanner');
    if (!panel || !progressEl || !targetEl || !historyEl || !lastEl || !input || !undoBtn || !skipBtn || !openScannerBtn) return;

    if (!this._externalQrMode || !groupId) {
      panel.classList.add('hidden');
      progressEl.textContent = '';
      targetEl.innerHTML = '';
      historyEl.innerHTML = '';
      lastEl.innerHTML = '';
      input.value = '';
      input.disabled = true;
      undoBtn.disabled = true;
      skipBtn.disabled = true;
      openScannerBtn.disabled = true;
      return;
    }

    this._syncExternalQrQueue(groupId);
    const articles = DB.getArticlesByGroup(groupId).sort((a, b) => a.id.localeCompare(b.id));
    const assignedCount = articles.filter(article => String(article.externalQrCode ?? '').trim()).length;
    const targetArticle = this._getExternalQrTargetArticle(groupId);
    const canAssign = articles.length > 0;

    panel.classList.remove('hidden');
    progressEl.textContent = `${assignedCount} von ${articles.length} zugeordnet`;
    input.disabled = !canAssign;
    input.placeholder = targetArticle || canAssign
      ? 'Fremd-QR scannen und mit Enter bestätigen'
      : 'Keine Artikel in dieser Gruppe';
    undoBtn.disabled = !this._externalQrHistory.length;
    skipBtn.disabled = !targetArticle || this._externalQrQueue.length <= 1;
    openScannerBtn.disabled = !canAssign;

    targetEl.innerHTML = targetArticle
      ? `
        <div class="external-qr-batch__target-card">
          <span class="external-qr-batch__target-id">${Utils.escHtml(targetArticle.id)}</span>
          <strong>${Utils.escHtml(Utils.articleDisplayName(targetArticle, targetArticle.id))}</strong>
          <div class="external-qr-batch__target-meta">
            ${Utils.statusBadge(targetArticle.status)}
            <span class="external-qr-chip external-qr-chip--missing">
              <i class="fa-solid fa-qrcode"></i> Fremd-QR fehlt
            </span>
          </div>
        </div>`
      : canAssign
      ? `
        <div class="external-qr-batch__target-empty">
          <i class="fa-solid fa-copy"></i>
          <span>Alle vorhandenen Artikel haben bereits einen Fremd-QR-Code. Der nächste Scan legt automatisch ein weiteres Duplikat in dieser Gruppe an.</span>
        </div>`
      : `
        <div class="external-qr-batch__target-empty">
          <i class="fa-solid fa-circle-xmark"></i>
          <span>In dieser Gruppe sind noch keine Artikel vorhanden.</span>
        </div>`;

    lastEl.innerHTML = this._externalQrLastAssignment
      ? `
        <div class="external-qr-batch__last-card">
          <strong>Zuletzt zugeordnet:</strong>
          <span>${Utils.escHtml(this._externalQrLastAssignment.articleId)} · ${Utils.escHtml(this._externalQrLastAssignment.articleName)}</span>
          <code>${Utils.escHtml(this._externalQrLastAssignment.code)}</code>
        </div>`
      : '';

    historyEl.innerHTML = this._externalQrRecentAssignments.length
      ? this._externalQrRecentAssignments.map(entry => `
          <div class="external-qr-batch__history-item">
            <strong>${Utils.escHtml(entry.articleId)}</strong>
            <span>${Utils.escHtml(entry.articleName)}</span>
            <code>${Utils.escHtml(entry.code)}</code>
          </div>`).join('')
      : `
        <div class="external-qr-batch__history-empty">
          Noch keine Fremd-QR-Scans in dieser Serienzuordnung.
        </div>`;
  },

  _clearExternalQr(articleId, groupId) {
    const article = DB.getArticleById(articleId);
    if (!article || !String(article.externalQrCode ?? '').trim()) return;
    Modal.open(`
      <h2 class="modal-title">
        <i class="fa-solid fa-qrcode"></i>
        Fremd-QR-Code entfernen
      </h2>
      <p>Der Fremd-QR-Code von <strong>${Utils.escHtml(articleId)}</strong> wird entfernt.</p>
      <p style="margin-top:8px;color:var(--color-muted);font-size:var(--font-size-sm);">
        Artikel: ${Utils.escHtml(Utils.articleDisplayName(article, articleId))}
      </p>
      <div style="margin-top:12px;padding:12px;background:var(--color-bg);border-radius:var(--border-radius-sm);word-break:break-word;">
        <code>${Utils.escHtml(article.externalQrCode)}</code>
      </div>
      <div class="modal-actions">
        <button class="btn btn-ghost" onclick="Modal.close()">Abbrechen</button>
        <button class="btn btn-danger" id="confirm-clear-external-qr-btn">
          <i class="fa-solid fa-link-slash"></i> Entfernen
        </button>
      </div>`, content => {
      content.querySelector('#confirm-clear-external-qr-btn').addEventListener('click', () => {
        DB.updateArticle(articleId, { externalQrCode: null });
        this._enqueueExternalQrArticle(articleId, true);
        Modal.close();
        Toast.success(`Fremd-QR-Code von ${articleId} wurde entfernt.`);
        this._renderGroupArticles(groupId);
        this._focusExternalQrInput();
        this._refreshExternalQrScannerContext();
      });
    });
  },

  _renderDetailMeta(group) {
    const metaEl = document.getElementById('group-detail-meta');
    const displayName = Utils.groupDisplayName(group, DB.getArticlesByGroup(group.id));
    let h = '<div style="display:flex;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;gap:12px;">'
      + '<div style="flex:1;min-width:0;">'
      + '<div style="margin-bottom:4px;"><span style="font-size:var(--font-size-xs);font-weight:700;color:var(--color-muted);letter-spacing:0.06em;text-transform:uppercase;">' + Utils.escHtml(group.id) + '</span></div>'
      + '<div class="group-detail-name">' + (displayName ? Utils.escHtml(displayName) : '<span style="color:var(--color-muted);font-style:italic;">Kein Name</span>') + '</div>'
      + '<div style="display:flex;align-items:center;gap:10px;margin-top:8px;flex-wrap:wrap;">' + Utils.statusBadge(group.status);
    if (group.location) h += '<span style="font-size:var(--font-size-sm);color:var(--color-muted);"><i class="fa-solid fa-location-dot"></i> ' + Utils.escHtml(group.location) + '</span>';
    h += '</div>';
    if (group.notes) h += '<p style="color:var(--color-text-secondary);font-size:var(--font-size-sm);margin-top:8px;line-height:1.5;max-width:600px;">' + Utils.escHtml(group.notes.substring(0, 200)) + '</p>';
    h += '</div><button class="btn btn-outline btn-sm" id="btn-toggle-selection" type="button"><i class="fa-solid fa-check-square"></i> Ausw\u00e4hlen</button></div>';
    metaEl.innerHTML = h;
    metaEl.querySelector('#btn-toggle-selection').addEventListener('click', () => GroupSelection.toggleMode(group.id));
  },

  _renderGroupInfoCard(group) {
    const articles = DB.getArticlesByGroup(group.id);
    const displayName = Utils.groupDisplayName(group, articles);
    const totalQty = articles.reduce((s, a) => s + (parseInt(a.quantity) || 1), 0);
    const groupListingLink = String(group.listingLink ?? '').trim();
    const imgHtml  = group.image
      ? `<div class="group-info-image"><img src="${group.image}" alt="Gruppenbild"/></div>`
      : `<div class="group-info-image"><div class="group-info-image-placeholder"><i class="fa-solid fa-layer-group"></i></div></div>`;
    const infoRows = [
      ['Gruppen-ID',  Utils.escHtml(group.id)],
      ['Name', displayName ? `<strong>${Utils.escHtml(displayName)}</strong>` : '<span style="color:var(--color-muted);font-style:italic;">-</span>'],
      ['Status',           Utils.statusBadge(group.status)],
      ['Zielpreis Netto',  Utils.formatEuro(group.priceNet)],
      ['Zielpreis Brutto', `<span style="color:var(--color-primary);font-size:var(--font-size-lg);font-weight:700;">${Utils.formatEuro(group.priceGross)}</span>`],
      ...(group.soldPrice ? [['Erzielt', `<span style="color:var(--color-success);">${Utils.formatEuro(group.soldPrice)}</span>`]] : []),
      ['Standort',     Utils.escHtml(group.location || '-')],
      ['Kleinanzeigen', groupListingLink
        ? `<a href="${Utils.escHtml(groupListingLink)}" target="_blank" rel="noopener" style="color:var(--color-primary);word-break:break-all;">Link öffnen</a>`
        : '-'],
      ['St\u00fcck gesamt', `<strong>${totalQty}</strong>`],
      ['Datens\u00e4tze',   `${articles.length} Artikel`],
      ['Aktualisiert', Utils.formatDate(group.updatedAt)],
    ].map(([label, val]) => `
      <div class="group-info-row">
        <span class="group-info-label">${label}</span>
        <span class="group-info-value">${val}</span>
      </div>`).join('');
    document.getElementById('group-info-card').innerHTML = `
      ${imgHtml}
      <div class="group-info-table">${infoRows}</div>
      ${group.conditionOverview ? `<div style="margin-top:12px;padding:10px;background:var(--color-bg);border-radius:var(--border-radius-sm);font-size:var(--font-size-sm);color:var(--color-text-secondary);">${Utils.escHtml(group.conditionOverview)}</div>` : ''}
      <div class="group-info-actions">
        <button class="btn btn-ghost btn-sm" id="detail-price-history-btn">
          <i class="fa-solid fa-clock-rotate-left"></i> Preishistorie
        </button>
        <button class="btn btn-outline btn-sm" style="flex:1;" id="detail-edit-group-btn">
          <i class="fa-solid fa-pen-to-square"></i> Bearbeiten
        </button>
        <button class="btn btn-danger btn-sm" id="detail-delete-group-btn" data-tooltip="Gruppe entsorgen">
          <i class="fa-solid fa-trash-can"></i>
        </button>
      </div>
      ${group.image ? `
      <div class="group-info-actions" style="padding-top:0;">
        <button class="btn btn-outline btn-sm" style="width:100%;" id="detail-apply-group-image-btn">
          <i class="fa-solid fa-images"></i> Gruppenbild f\u00fcr alle Artikel \u00fcbernehmen
        </button>
      </div>` : ''}
      ${groupListingLink ? `
      <div class="group-info-actions" style="padding-top:0;">
        <button class="btn btn-outline btn-sm" style="width:100%;" id="detail-apply-group-listing-link-btn">
          <i class="fa-solid fa-link"></i> Kleinanzeigen-Link für alle Artikel übernehmen
        </button>
      </div>` : ''}`;
    document.getElementById('detail-price-history-btn').addEventListener('click', () => Dashboard.showPriceHistoryModal(group));
    document.getElementById('detail-edit-group-btn').addEventListener('click', () => this._editGroup(group.id));
    document.getElementById('detail-delete-group-btn').addEventListener('click', () => this._confirmDeleteGroup(group.id));
    const applyImgBtn = document.getElementById('detail-apply-group-image-btn');
    if (applyImgBtn) { applyImgBtn.addEventListener('click', () => this._applyGroupImageToArticles(group.id)); }
    const applyListingBtn = document.getElementById('detail-apply-group-listing-link-btn');
    if (applyListingBtn) { applyListingBtn.addEventListener('click', () => this._applyGroupListingLinkToArticles(group.id)); }
  },

  _renderGroupArticles(groupId) {
    const container   = document.getElementById('group-articles-container');
    const toolbar     = document.getElementById('group-articles-toolbar');
    const allArticles = DB.getArticlesByGroup(groupId);
    this._renderExternalQrPanel(groupId);
    const currentTargetId = this._externalQrMode
      ? (this._getExternalQrTargetArticle(groupId)?.id ?? null)
      : null;
    if (toolbar) {
      toolbar.style.display = allArticles.length ? 'block' : 'none';
      if (!toolbar.dataset.bound) {
        toolbar.dataset.bound = '1';
        ['ga-search','ga-filter-status','ga-sort'].forEach(id => {
          const el = document.getElementById(id);
          if (el) {
            el.addEventListener(
              el.tagName === 'INPUT' ? 'input' : 'change',
              () => el.tagName === 'INPUT' ? this._groupArticleSearchRender() : this.queueRenderGroupArticles()
            );
          }
        });
      }
    }
    const search   = (document.getElementById('ga-search')?.value        ?? '').toLowerCase();
    const filterSt =  document.getElementById('ga-filter-status')?.value ?? '';
    const sortVal  =  document.getElementById('ga-sort')?.value          ?? 'id-asc';
    let articles = allArticles;
    if (search)   articles = articles.filter(a => [a.id, a.manufacturer, a.model, a.category, a.location, Utils.articleDisplayName(a, '')].some(v => (v ?? '').toLowerCase().includes(search)));
    if (filterSt) articles = articles.filter(a => Utils.normalizeStatus(a.status) === filterSt);
    articles = [...articles].sort((a, b) => {
      if (sortVal === 'id-desc')    return b.id.localeCompare(a.id);
      if (sortVal === 'status')     return Utils.normalizeStatus(a.status).localeCompare(Utils.normalizeStatus(b.status));
      if (sortVal === 'condition')  return (a.condition ?? '').localeCompare(b.condition ?? '');
      if (sortVal === 'price-asc')  return (a.purchasePrice || 0) - (b.purchasePrice || 0);
      if (sortVal === 'price-desc') return (b.purchasePrice || 0) - (a.purchasePrice || 0);
      return a.id.localeCompare(b.id);
    });
    const totalQty = allArticles.reduce((s, a) => s + (parseInt(a.quantity) || 1), 0);
    document.getElementById('group-articles-title').textContent =
      'Zugeh\u00f6rige Artikel (' + allArticles.length + ' Datens\u00e4tze · ' + totalQty + ' St\u00fcck)'
      + (articles.length < allArticles.length ? ' - ' + articles.length + ' gefiltert' : '');
    if (!articles.length) {
      container.style.cssText = ''; container.classList.remove('selection-mode');
      container.innerHTML = '<div class="empty-state"><i class="fa-solid fa-cube"></i><p>Keine Artikel entsprechen dem Filter.</p></div>';
      return;
    }
    container.style.cssText = 'display:flex;flex-direction:column;gap:10px;';
    let html = '';
    articles.forEach(a => {
      const externalQrCode = String(a.externalQrCode ?? '').trim();
      const thumb = (a.photos && a.photos[0])
        ? '<div class="group-article-thumb"><img src="' + a.photos[0] + '" alt="Foto" loading="lazy"/></div>'
        : '<div class="group-article-thumb"><i class="fa-solid fa-couch"></i></div>';
    const name = Utils.escHtml(Utils.articleDisplayName(a, '-'));
      const dims = [a.width, a.depth, a.height].filter(Boolean).map(v => v + 'cm').join(' x ');
      let meta = Utils.statusBadge(a.status) + Utils.condBadge(a.condition);
      meta += externalQrCode
        ? '<span class="external-qr-chip external-qr-chip--assigned"><i class="fa-solid fa-qrcode"></i> Fremd-QR vorhanden</span>'
        : '<span class="external-qr-chip external-qr-chip--missing"><i class="fa-solid fa-qrcode"></i> Fremd-QR fehlt</span>';
      if (a.category) meta += '<span style="font-size:var(--font-size-xs);color:var(--color-muted);">' + Utils.escHtml(a.category) + '</span>';
      if (dims)       meta += '<span style="font-size:var(--font-size-xs);color:var(--color-muted);"><i class="fa-solid fa-ruler-combined"></i> ' + dims + '</span>';
      if (a.location) meta += '<span style="font-size:var(--font-size-xs);color:var(--color-muted);"><i class="fa-solid fa-location-dot"></i> ' + Utils.escHtml(a.location) + '</span>';
      if (a.color)    meta += '<span style="font-size:var(--font-size-xs);color:var(--color-muted);"><i class="fa-solid fa-palette"></i> ' + Utils.escHtml(a.color) + '</span>';
      let price = '';
      if (a.purchasePrice) price += '<span style="font-size:var(--font-size-sm);font-weight:600;color:var(--color-primary);">EK: ' + Utils.formatEuro(a.purchasePrice) + '</span>';
      if (a.soldPrice)     price += '<span style="font-size:var(--font-size-sm);font-weight:600;color:var(--color-success);">VK: ' + Utils.formatEuro(a.soldPrice) + '</span>';
      html += '<div class="group-article-card' + (currentTargetId === a.id ? ' is-external-qr-target' : '') + '" data-article-id="' + Utils.escHtml(a.id) + '">'
        + '<div class="article-select-checkbox-wrap"><input type="checkbox" class="article-select-cb" data-article-id="' + Utils.escHtml(a.id) + '"/></div>'
        + thumb + '<div class="group-article-info">'
        + '<span class="group-article-id">' + Utils.escHtml(a.id) + '</span>'
        + '<span class="group-article-name">' + name + '</span>'
        + '<div class="group-article-meta">' + meta + '</div>'
        + '<div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:4px;">' + price + '</div>'
        + (externalQrCode
          ? '<div class="group-article-external-qr"><span>Fremd-QR</span><code title="' + Utils.escHtml(externalQrCode) + '">' + Utils.escHtml(externalQrCode) + '</code></div>'
          : '')
        + '</div><div class="group-article-actions">'
        + '<button class="btn btn-outline btn-sm" data-action="edit-article" data-id="' + a.id + '"><i class="fa-solid fa-pen-to-square"></i> Bearbeiten</button>'
        + '<button class="btn btn-ghost btn-sm" data-action="qr-article" data-id="' + a.id + '"><i class="fa-solid fa-qrcode"></i> QR</button>'
        + (externalQrCode
          ? '<button class="btn btn-ghost btn-sm" data-action="clear-external-qr" data-id="' + a.id + '"><i class="fa-solid fa-link-slash"></i> Fremd-QR löschen</button>'
          : '')
        + '<button class="btn btn-danger btn-sm" data-action="detach-article" data-id="' + a.id + '"><i class="fa-solid fa-link-slash"></i> Entfernen</button>'
        + '<button class="btn btn-danger btn-sm" data-action="delete-article-in-group" data-id="' + a.id + '"><i class="fa-solid fa-trash-can"></i> L\u00f6schen</button>'
        + '</div></div>';
    });
    container.innerHTML = html;
    container.querySelectorAll('[data-action]').forEach(btn => {
      btn.addEventListener('click', ev => {
        if (GroupSelection._active) { ev.stopPropagation(); return; }
        const { action, id } = btn.dataset;
        if (action === 'edit-article')            Dashboard.loadArticleIntoForm(id);
        if (action === 'qr-article')              QRManager.printQR(id);
        if (action === 'clear-external-qr')       this._clearExternalQr(id, groupId);
        if (action === 'detach-article')          this._detachArticle(id, groupId);
        if (action === 'delete-article-in-group') this._deleteArticleInGroup(id, groupId);
      });
    });
    container.querySelectorAll('.article-select-cb').forEach(cb => {
      cb.addEventListener('change', ev => { ev.stopPropagation(); GroupSelection.toggleArticle(cb.dataset.articleId, cb.checked); });
    });
    container.querySelectorAll('.group-article-card').forEach(card => {
      card.addEventListener('click', ev => {
        if (!GroupSelection._active) return;
        if (ev.target.closest('button') || ev.target.classList.contains('article-select-cb')) return;
        GroupSelection.toggleArticle(card.dataset.articleId);
      });
    });
  },

  _detachArticle(articleId, groupId) {
    Modal.open(`
      <h2 class="modal-title"><i class="fa-solid fa-link-slash"></i> Artikel aus Gruppe entfernen</h2>
      <p>Soll Artikel <strong>${Utils.escHtml(articleId)}</strong> aus Gruppe <strong>${Utils.escHtml(groupId)}</strong> entfernt werden?</p>
      <p style="margin-top:8px;color:var(--color-muted);font-size:var(--font-size-sm);">Der Artikel selbst wird nicht gelÃ¶scht.</p>
      <div class="modal-actions">
        <button class="btn btn-ghost" onclick="Modal.close()">Abbrechen</button>
        <button class="btn btn-danger" id="confirm-detach-btn"><i class="fa-solid fa-link-slash"></i> Entfernen</button>
      </div>`, content => {
      content.querySelector('#confirm-detach-btn').addEventListener('click', () => {
        DB.updateArticle(articleId, { groupId: null });
        Modal.close();
        Toast.success(`Artikel ${articleId} aus Gruppe entfernt.`);
        this._renderGroupArticles(groupId);
        this._renderGroupInfoCard(DB.getGroupById(groupId));
      });
    });
  },

  _deleteArticleInGroup(articleId, groupId) {
    const article = DB.getArticleById(articleId);
    if (!article) return;
    const name = Utils.escHtml(Utils.articleDisplayName(article, articleId));
    Modal.open(
      '<h2 class="modal-title" style="color:var(--color-danger);"><i class="fa-solid fa-trash-can"></i> Artikel lÃ¶schen</h2>'
      + '<p>Was soll mit <strong>' + name + '</strong> <span style="color:var(--color-muted);font-size:var(--font-size-xs);">(' + Utils.escHtml(articleId) + ')</span> passieren?</p>'
      + '<div class="delete-option-card" id="grp-del-opt-soft">'
      +   '<div class="delete-option-card__header"><i class="fa-solid fa-box-archive" style="color:var(--color-warning);"></i><strong>Entsorgen</strong>'
      +   '<span class="badge badge-status-entsorgt" style="margin-left:auto;">In EnzyklopÃ¤die behalten</span></div>'
      +   '<p class="delete-option-card__desc">Artikel bekommt Status <em>Entsorgt</em>. Bleibt in der EnzyklopÃ¤die auffindbar.</p>'
      + '</div>'
      + '<div class="delete-option-card" id="grp-del-opt-hard">'
      +   '<div class="delete-option-card__header"><i class="fa-solid fa-fire-flame-curved" style="color:var(--color-danger);"></i><strong>Dauerhaft lÃ¶schen</strong>'
      +   '<span style="margin-left:auto;font-size:var(--font-size-xs);background:var(--color-danger-light);color:var(--color-danger);padding:2px 8px;border-radius:99px;font-weight:700;">EndgÃ¼ltig</span></div>'
      +   '<p class="delete-option-card__desc">Artikel wird <strong>vollstÃ¤ndig und unwiderruflich</strong> entfernt.</p>'
      + '</div>'
      + '<div class="modal-actions" style="margin-top:20px;"><button class="btn btn-ghost" onclick="Modal.close()"><i class="fa-solid fa-xmark"></i> Abbrechen</button>'
      +   '<button class="btn btn-danger" id="grp-confirm-del-btn" disabled><i class="fa-solid fa-trash-can"></i> BestÃ¤tigen</button></div>',
      content => {
        const softCard   = content.querySelector('#grp-del-opt-soft');
        const hardCard   = content.querySelector('#grp-del-opt-hard');
        const confirmBtn = content.querySelector('#grp-confirm-del-btn');
        let   mode       = null;
        const selectCard = (sel, other, m) => { mode = m; sel.classList.add('is-selected'); other.classList.remove('is-selected'); confirmBtn.disabled = false; };
        softCard.addEventListener('click', () => selectCard(softCard, hardCard, 'soft'));
        hardCard.addEventListener('click', () => selectCard(hardCard, softCard, 'hard'));
        confirmBtn.addEventListener('click', () => {
          if (!mode) return;
          if (mode === 'soft') {
            DB.updateArticle(articleId, { status: 'Entsorgt', groupId: null });
            Modal.close();
            Toast.success('Artikel ' + articleId + ' entsorgt â€” bleibt in der EnzyklopÃ¤die.');
          } else {
            DB.hardDeleteArticle(articleId);
            Modal.close();
            Toast.success('Artikel ' + articleId + ' dauerhaft gelÃ¶scht.');
          }
          Groups._renderGroupArticles(groupId);
          Groups._renderGroupInfoCard(DB.getGroupById(groupId));
          Dashboard.renderStats();
        });
      }
    );
  },

  _applyGroupImageToArticles(groupId) {
    const group = DB.getGroupById(groupId);
    if (!group || !group.image) { Toast.warning('Kein Gruppenbild vorhanden.'); return; }
    const articles = DB.getArticlesByGroup(groupId);
    if (!articles.length) { Toast.warning('Keine Artikel in dieser Gruppe.'); return; }
    let updated = 0;
    articles.forEach(a => {
      const photos = [...(a.photos ?? [])];
      if (photos.length === 0) { photos.push(group.image); } else { photos[0] = group.image; }
      try { DB.updateArticle(a.id, { photos }); updated++; }
      catch (e) { if (e.name !== 'QuotaExceededError' && !(e instanceof DOMException && e.code === 22)) throw e; }
    });
    this._renderGroupArticles(groupId);
    this._renderGroupInfoCard(DB.getGroupById(groupId));
    Dashboard.renderStats();
    if (updated === 0)                       Toast.warning('Speicher voll â€” Bild konnte nicht Ã¼bernommen werden.');
    else if (updated < articles.length)      Toast.warning('Gruppenbild bei ' + updated + ' von ' + articles.length + ' Artikeln gesetzt (Speicher voll).');
    else                                     Toast.success('Gruppenbild bei ' + updated + ' Artikel(n) als erstes Bild gesetzt.');
  },

  _applyGroupListingLinkToArticles(groupId) {
    const group = DB.getGroupById(groupId);
    const listingLink = String(group?.listingLink ?? '').trim();
    if (!group || !listingLink) {
      Toast.warning('Kein Kleinanzeigen-Link in der Gruppe hinterlegt.');
      return;
    }
    const articles = DB.getArticlesByGroup(groupId);
    if (!articles.length) {
      Toast.warning('Keine Artikel in dieser Gruppe.');
      return;
    }
    DB.updateArticles(articles.map(article => article.id), { listingLink });
    this._renderGroupArticles(groupId);
    this._renderGroupInfoCard(DB.getGroupById(groupId));
    Dashboard.renderStats();
    Toast.success('Kleinanzeigen-Link bei ' + articles.length + ' Artikel(n) übernommen.');
  },

  _editGroup(groupId) {
    const group = DB.getGroupById(groupId);
    if (!group) return;
    State.editingGroupId   = groupId;
    State.groupImageBase64 = group.image || null;
    document.querySelectorAll('.form-tab').forEach(b  => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    document.querySelector('[data-tab="tab-group"]').classList.add('active');
    document.getElementById('tab-group').classList.add('active');
    document.getElementById('grp-id-display').value         = group.id;
    document.getElementById('grp-name').value               = group.name || '';
    document.getElementById('grp-status').value             = Utils.normalizeStatus(group.status) || 'Verf\u00fcgbar';
    document.getElementById('grp-quantity').value           = group.quantity    || 1;
    document.getElementById('grp-location').value           = group.location    || '';
    document.getElementById('grp-price-net').value          = group.priceNet    || '';
    document.getElementById('grp-price-gross').value        = group.priceGross  || '';
    document.getElementById('grp-condition-overview').value = group.conditionOverview || '';
    document.getElementById('grp-listing-link').value       = group.listingLink || '';
    document.getElementById('grp-notes').value              = group.notes       || '';
    if (group.status === 'Verkauft') {
      document.getElementById('sold-fields-group').style.display = 'block';
      document.getElementById('grp-sold-price').value = group.soldPrice || '';
    }
    if (group.image) {
      document.getElementById('grp-image-preview').innerHTML =
        `<div class="photo-thumb"><img src="${group.image}" alt="Gruppenbild"/></div>`;
    }
    Router.navigate('dashboard');
    window.scrollTo({ top: 0, behavior: 'smooth' });
  },

  _confirmDeleteGroup(groupId) {
    const group = DB.getGroupById(groupId); if (!group) return;
    const name  = group.name ? Utils.escHtml(group.name) : groupId;
    const articles = DB.getArticlesByGroup(groupId); const count = articles.length;
    Modal.open(
      '<h2 class="modal-title" style="color:var(--color-danger);"><i class="fa-solid fa-trash-can"></i> Gruppe lÃ¶schen</h2>'
      + '<p>Was soll mit <strong>' + name + '</strong> (' + Utils.escHtml(groupId) + ') passieren?</p>'
      + '<div class="delete-option-card" id="delete-opt-soft"><div class="delete-option-card__header"><i class="fa-solid fa-box-archive" style="color:var(--color-warning);"></i><strong>Entsorgen</strong><span style="margin-left:auto;" class="badge badge-status-entsorgt">Soft-Delete</span></div>'
      + '<p class="delete-option-card__desc">Gruppe bekommt Status <em>Entsorgt</em>. ' + count + ' Artikel werden gelÃ¶st, aber <strong>nicht gelÃ¶scht</strong>.</p></div>'
      + '<div class="delete-option-card" id="delete-opt-hard"><div class="delete-option-card__header"><i class="fa-solid fa-fire-flame-curved" style="color:var(--color-danger);"></i><strong>EndgÃ¼ltig lÃ¶schen</strong><span style="margin-left:auto;font-size:var(--font-size-xs);background:var(--color-danger-light);color:var(--color-danger);padding:2px 8px;border-radius:99px;font-weight:700;">Hard-Delete</span></div>'
      + '<p class="delete-option-card__desc">Gruppe wird <strong>dauerhaft entfernt</strong>. Nicht rÃ¼ckgÃ¤ngig!</p>'
      + (count > 0 ? '<div class="delete-option-articles"><label class="delete-option-radio"><input type="radio" name="article-fate" value="keep" checked/><span><i class="fa-solid fa-link-slash"></i> ' + count + ' Artikel <strong>behalten</strong></span></label><label class="delete-option-radio"><input type="radio" name="article-fate" value="delete"/><span style="color:var(--color-danger);"><i class="fa-solid fa-trash-can"></i> ' + count + ' Artikel <strong>ebenfalls lÃ¶schen</strong></span></label></div>' : '')
      + '</div><div class="modal-actions" style="margin-top:20px;"><button class="btn btn-ghost" onclick="Modal.close()"><i class="fa-solid fa-xmark"></i> Abbrechen</button><button class="btn btn-danger" id="confirm-delete-group-btn" disabled><i class="fa-solid fa-trash-can"></i> LÃ¶schen bestÃ¤tigen</button></div>',
      content => {
        const softCard   = content.querySelector('#delete-opt-soft');
        const hardCard   = content.querySelector('#delete-opt-hard');
        const confirmBtn = content.querySelector('#confirm-delete-group-btn');
        let mode = null;
        const selectCard = (sel, other, m) => { mode = m; sel.classList.add('is-selected'); other.classList.remove('is-selected'); confirmBtn.disabled = false; };
        softCard.addEventListener('click', () => selectCard(softCard, hardCard, 'soft'));
        hardCard.addEventListener('click', () => selectCard(hardCard, softCard, 'hard'));
        confirmBtn.addEventListener('click', () => {
          if (!mode) return;
          if (mode === 'soft') {
            DB.getArticlesByGroup(groupId).forEach(a => DB.updateArticle(a.id, { groupId: null }));
            DB.deleteGroup(groupId); Modal.close(); Toast.success('Gruppe ' + groupId + ' entsorgt.');
          } else {
            const delArt = content.querySelector('input[name="article-fate"]:checked')?.value === 'delete';
            DB.getArticlesByGroup(groupId).forEach(a => delArt ? DB.hardDeleteArticle(a.id) : DB.updateArticle(a.id, { groupId: null }));
            DB.hardDeleteGroup(groupId); Modal.close();
            Toast.success('Gruppe ' + groupId + (delArt ? ' und ' + count + ' Artikel' : '') + ' dauerhaft gelÃ¶scht.');
          }
          this._showOverview(); this.render(); Dashboard.renderStats();
        });
      }
    );
  },
};
/* ============================================================
   14. TOOLS â€” Import / Export / Scanner / Reset
============================================================ */
const Tools = {
  _lastLocationQrValue: '',
  CSV_HEADERS: [
    'ID', 'Status', 'Kategorie', 'Hersteller', 'Modell', 'Zustand',
    'Standort', 'StÃ¼ckzahl', 'Breite_cm', 'Tiefe_cm', 'HÃ¶he_cm',
    'Einkaufspreis_EUR', 'Originalpreis_EUR', 'Material', 'Stil',
    'Versand', 'Versandkosten_EUR', 'Abhol_PLZ', 'Inserat_Link', 'Public_QR_Token',
    'Gruppe_ID', 'Verkaufspreis_EUR', 'Verkaufsdatum',
    'Bemerkungen', 'Erstellt', 'Aktualisiert',
  ],
  IMPORT_FIELD_ALIASES: {
    id                : ['id', 'inventarnummer', 'inventarnr', 'inventarnummernr', 'artikelid', 'artikel-id', 'artikelnummer', 'artikelnr', 'artnr', 'art-nr', 'artikel nr', 'nummer', 'nr', 'sku', 'artikelnummershopventory', 'artikelnummershopventroy', 'shopventoryartikelnummer', 'shopventroyartikelnummer'],
    status            : ['status', 'bestand', 'bestandstatus', 'lagerstatus'],
    category          : ['kategorie', 'warengruppe', 'produktgruppe', 'artikelgruppe', 'gruppe', 'kategoriepfad', 'warengruppepfad', 'hauptkategorie'],
    manufacturer      : ['hersteller', 'herstellername', 'marke', 'brand', 'herstellermarke', 'lieferant'],
    model             : ['modell', 'name', 'artikelname', 'artikelbezeichnung', 'bezeichnung', 'produktname', 'titel', 'name1'],
    condition         : ['zustand', 'condition'],
    location          : ['standort', 'lager', 'lagerort', 'lagerplatz', 'regal', 'fach', 'ort'],
    locationSlot      : ['stellplatz', 'stell platz', 'platz'],
    reservedInfo      : ['reserviertanwenanzahlpreis', 'reserviertanwen', 'reserviertinfo', 'reservierungsinfo', 'reserviert'],
    quantity          : ['stuckzahl', 'stÃ¼ckzahl', 'menge', 'anzahl', 'qty', 'lagerbestand', 'bestandsmenge'],
    width             : ['breite', 'breiteincm', 'breitecm', 'breite_cm', 'breite(cm)', 'b', 'bcm'],
    depth             : ['tiefe', 'tiefeincm', 'tiefecm', 'tiefe_cm', 'tiefe(cm)', 'laenge', 'lÃ¤nge', 'laengecm', 'lÃ¤ngencm', 'laenge_cm', 'lÃ¤nge_cm', 'l', 'lcm'],
    height            : ['hoehe', 'hÃ¶he', 'hoeheincm', 'hÃ¶heincm', 'hoehecm', 'hÃ¶hecm', 'hoehe_cm', 'hÃ¶he_cm', 'hoehe(cm)', 'hÃ¶he(cm)', 'h', 'hcm'],
    dimensions        : ['masse', 'maÃŸe', 'abmessung', 'abmessungen', 'dimensionen', 'groesse', 'grÃ¶ÃŸe'],
    purchasePrice     : ['preisjestk', 'preisprostk', 'preisjeeinheit', 'einkaufspreis', 'einkaufspreis_eur', 'einkaufspreisnetto', 'einkaufspreis_netto', 'ek', 'eknetto', 'einkaufnetto', 'nettoek'],
    purchasePriceGross: ['einkaufspreisbrutto', 'einkaufspreis_brutto', 'ekbrutto', 'einkaufbrutto'],
    originalPrice     : ['zielverkaufspreis1', 'zielverkaufspreis', 'originalpreis', 'originalpreis_eur', 'originalpreisnetto', 'originalpreis_netto', 'uvp', 'uvpnetto', 'listenpreis'],
    originalPriceGross: ['originalpreisbrutto', 'originalpreis_brutto', 'uvpbrutto', 'listenpreisbrutto'],
    material          : ['material', 'materialart'],
    style             : ['stil', 'designstil'],
    color             : ['farbe', 'farbton', 'dekor'],
    shipping          : ['versand', 'versandmoglich', 'versandmÃ¶glich', 'lieferung', 'lieferungmoglich', 'lieferungmÃ¶glich'],
    shippingCost      : ['versandkosten', 'versandkosten_eur', 'lieferkosten', 'lieferungskosten'],
    pickupZip         : ['abholplz', 'abhol_plz', 'abholungplz', 'plz', 'postleitzahl'],
    listingLink       : ['inseratlink', 'inserat_link', 'listinglink', 'link', 'url', 'produkturl', 'artikelurl'],
    publicQrToken     : ['publicqrtoken', 'public_qr_token', 'oeffentlicherqrtoken', 'oeffentlicher_qr_token'],
    groupId           : ['gruppeid', 'gruppe_id', 'gruppenid'],
    soldPrice         : ['verkaufspreis', 'verkaufspreis_eur', 'verkaufspreisnetto', 'verkaufspreis_netto', 'vk', 'vknetto', 'verkaufnetto'],
    soldPriceGross    : ['verkaufspreisbrutto', 'verkaufspreis_brutto', 'vkbrutto', 'verkaufbrutto'],
    soldDate          : ['verkaufsdatum', 'verkauftam', 'verkaufsdate', 'verkaufs_datum'],
    notes             : ['bemerkung', 'bemerkungen', 'notizen', 'notiz', 'beschreibung', 'kommentar', 'info', 'hinweis'],
  },
  STATUS_VALUES: {
    verfugbar : 'Verf\u00fcgbar',
    verfuegbar: 'Verf\u00fcgbar',
    available : 'Verf\u00fcgbar',
    instock   : 'Verf\u00fcgbar',
    lagernd   : 'Verf\u00fcgbar',
    reserviert: 'Reserviert',
    reserved  : 'Reserviert',
    onhold    : 'Reserviert',
    hold      : 'Reserviert',
    verkauft  : 'Verkauft',
    sold      : 'Verkauft',
    entsorgt  : 'Entsorgt',
    disposed  : 'Entsorgt',
    deleted   : 'Entsorgt',
    geloescht : 'Entsorgt',
    geloscht  : 'Entsorgt',
  },
  CONDITION_VALUES: {
    neuwertig              : 'Neuwertig',
    neu                    : 'Neuwertig',
    neuware                : 'Neuwertig',
    sehrgut                : 'Neuwertig',
    leichtegebrauchsspuren : 'Leichte Gebrauchsspuren',
    leichtgebraucht        : 'Leichte Gebrauchsspuren',
    gut                    : 'Leichte Gebrauchsspuren',
    mittleregebrauchsspuren: 'Mittlere Gebrauchsspuren',
    mittelgebraucht        : 'Mittlere Gebrauchsspuren',
    gebraucht              : 'Mittlere Gebrauchsspuren',
    starkegebrauchsspuren  : 'Starke Gebrauchsspuren',
    starkgebraucht         : 'Starke Gebrauchsspuren',
    beschadigt             : 'Starke Gebrauchsspuren',
    beschaedigt            : 'Starke Gebrauchsspuren',
    schlecht               : 'Starke Gebrauchsspuren',
    defekt                 : 'Defekt',
    kaputt                 : 'Defekt',
  },
  TRUE_VALUES: ['1', 'ja', 'yes', 'true', 'wahr', 'x', 'y'],
  FALSE_VALUES: ['0', 'nein', 'no', 'false', 'falsch', 'n'],

  init() {
    document.getElementById('btn-export-csv')
      .addEventListener('click', () => this.exportCSV());
    document.getElementById('btn-import-csv-input')
      .addEventListener('change', e => this.importCSV(e));
    document.getElementById('btn-export-json')
      .addEventListener('click', () => this.exportJSON());
    document.getElementById('btn-reset-db')
      .addEventListener('click', () => this.confirmResetDB());
    document.getElementById('scanner-input')
      .addEventListener('keydown', e => {
        if (e.key === 'Enter') this.handleScan(e.target.value.trim());
      });
    document.getElementById('location-qr-input')
      .addEventListener('keydown', e => {
        if (e.key === 'Enter') this.generateLocationQR();
      });
    document.getElementById('btn-generate-location-qr')
      .addEventListener('click', () => this.generateLocationQR());
    document.getElementById('btn-print-location-qr')
      .addEventListener('click', () => this.printLocationQR());
    document.getElementById('btn-auto-group-all')
      .addEventListener('click', () => this.autoGroupAll());
  },

  generateLocationQR() {
    const input    = document.getElementById('location-qr-input');
    const location = input.value.trim();
    if (!location) {
      Toast.error('Bitte einen Standort eingeben.');
      input.focus();
      return;
    }
    this._lastLocationQrValue = location;
    QRManager.generate('location-qr-preview', QRManager.makeLocationCode(location), 176);
    document.getElementById('location-qr-preview-label').textContent = location;
    document.getElementById('location-qr-preview-box').style.display = 'flex';
    document.getElementById('btn-print-location-qr').disabled = false;
    Toast.success(`Standort-QR fÃ¼r â€ž${location}â€œ erstellt.`);
  },

  printLocationQR() {
    if (!this._lastLocationQrValue) {
      this.generateLocationQR();
      if (!this._lastLocationQrValue) return;
    }
    QRManager.printLocationQR(this._lastLocationQrValue);
  },

  relocateArticles(articleIds, location) {
    const cleanLocation = String(location ?? '').trim();
    const ids = [...new Set(articleIds)].filter(id => DB.getArticleById(id));
    if (!ids.length) {
      Toast.error('Keine gÃ¼ltigen Artikel zum Umlagern ausgewÃ¤hlt.');
      return false;
    }
    if (!cleanLocation) {
      Toast.error('Bitte einen Ziel-Standort angeben.');
      return false;
    }
    DB.updateArticles(ids, { location: cleanLocation });
    Dashboard.renderStats();
    if (State.currentView === 'inventory')    Inventory.render();
    if (State.currentView === 'groups') {
      const detailVisible = !document.getElementById('group-detail-view').classList.contains('hidden');
      if (detailVisible && Groups._currentGroupId) {
        const group = DB.getGroupById(Groups._currentGroupId);
        if (group) {
          Groups._renderDetailMeta(group);
          Groups._renderGroupInfoCard(group);
          Groups._renderGroupArticles(Groups._currentGroupId);
        } else {
          Groups._showOverview();
          Groups.render();
        }
      } else {
        Groups.render();
      }
    }
    if (State.currentView === 'encyclopedia') Encyclopedia.render();
    if (State.currentView === 'sold')         Sold.render();
    Toast.success(`${ids.length} Artikel nach â€ž${cleanLocation}â€œ umgelagert.`);
    return true;
  },

  autoGroupAll() {
    const ungroupedCount = DB.getArticles().filter(
      a => !a.groupId && a.status !== 'Entsorgt'
    ).length;
    if (!ungroupedCount) {
      Toast.success('Alle Artikel sind bereits einer Gruppe zugeordnet. âœ“');
      return;
    }
    const articles   = DB.getArticles().filter(a => !a.groupId && a.status !== 'Entsorgt');
    const uniqueKeys = new Set(articles.map(a => WawiDB.articleKey(a) || `id:${a.id}`)).size;
    Modal.open(`
      <h2 class="modal-title">
        <i class="fa-solid fa-wand-magic-sparkles"></i>
        Bestand automatisch gruppieren
      </h2>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:20px;">
        <div style="padding:14px;background:var(--color-bg);border-radius:var(--border-radius-sm);text-align:center;">
          <div style="font-size:var(--font-size-2xl);font-weight:800;color:var(--color-primary);">${ungroupedCount}</div>
          <div style="font-size:var(--font-size-xs);color:var(--color-muted);margin-top:2px;">Artikel ohne Gruppe</div>
        </div>
        <div style="padding:14px;background:var(--color-bg);border-radius:var(--border-radius-sm);text-align:center;">
          <div style="font-size:var(--font-size-2xl);font-weight:800;color:var(--color-success);">~${uniqueKeys}</div>
          <div style="font-size:var(--font-size-xs);color:var(--color-muted);margin-top:2px;">Gruppen werden benÃ¶tigt</div>
        </div>
      </div>
      <div style="padding:14px;background:var(--color-bg);border-radius:var(--border-radius-sm);border-left:4px solid var(--color-primary);font-size:var(--font-size-sm);color:var(--color-text-secondary);margin-bottom:20px;">
        <strong style="color:var(--color-text);">
          <i class="fa-solid fa-circle-info" style="color:var(--color-primary);"></i>
          So funktioniert die automatische Zuweisung:
        </strong>
        <ul style="margin:8px 0 0 16px;line-height:1.8;">
          <li>Gleiche Artikel (Hersteller + Modell) â†’ gleiche Gruppe</li>
          <li>Gibt es bereits eine passende Gruppe â†’ wird wiederverwendet</li>
          <li>Gibt es keine passende Gruppe â†’ neue wird angelegt</li>
          <li>Gruppenname = Hersteller + Modell (oder Kategorie)</li>
        </ul>
      </div>
      <div class="modal-actions">
        <button class="btn btn-ghost" onclick="Modal.close()">Abbrechen</button>
        <button class="btn btn-primary" id="confirm-auto-group-btn">
          <i class="fa-solid fa-wand-magic-sparkles"></i> Jetzt ausfÃ¼hren
        </button>
      </div>`, content => {
      content.querySelector('#confirm-auto-group-btn').addEventListener('click', async () => {
        try {
          const result = await DB.autoAssignAllUngrouped();
          Modal.close();
          Modal.open(`
          <h2 class="modal-title" style="color:var(--color-success);">
            <i class="fa-solid fa-circle-check"></i> Gruppierung abgeschlossen
          </h2>
          <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:20px;">
            <div style="padding:14px;background:var(--color-bg);border-radius:var(--border-radius-sm);text-align:center;">
              <div style="font-size:var(--font-size-2xl);font-weight:800;color:var(--color-success);">${result.assigned}</div>
              <div style="font-size:var(--font-size-xs);color:var(--color-muted);margin-top:2px;">Artikel zugeordnet</div>
            </div>
            <div style="padding:14px;background:var(--color-bg);border-radius:var(--border-radius-sm);text-align:center;">
              <div style="font-size:var(--font-size-2xl);font-weight:800;color:var(--color-primary);">${result.groupsCreated}</div>
              <div style="font-size:var(--font-size-xs);color:var(--color-muted);margin-top:2px;">Neue Gruppen</div>
            </div>
            <div style="padding:14px;background:var(--color-bg);border-radius:var(--border-radius-sm);text-align:center;">
              <div style="font-size:var(--font-size-2xl);font-weight:800;color:var(--color-warning);">${result.groupsReused}</div>
              <div style="font-size:var(--font-size-xs);color:var(--color-muted);margin-top:2px;">Bestehende genutzt</div>
            </div>
          </div>
          <div class="modal-actions">
            <button class="btn btn-ghost" onclick="Modal.close()">SchlieÃŸen</button>
            <button class="btn btn-primary" id="goto-groups-btn">
              <i class="fa-solid fa-layer-group"></i> Gruppen ansehen
            </button>
          </div>`, inner => {
          inner.querySelector('#goto-groups-btn').addEventListener('click', () => {
            Modal.close();
            Router.navigate('groups');
          });
        });
          Dashboard.renderStats();
          if (State.currentView === 'inventory')    Inventory.render();
          if (State.currentView === 'groups')       Groups.render();
          if (State.currentView === 'encyclopedia') Encyclopedia.render();
        } catch (err) {
          console.error('autoAssignAllUngrouped failed:', err);
          Toast.error('Automatische Gruppierung fehlgeschlagen.');
        }
      });
    });
  },

  exportCSV() {
    const articles = DB.getArticles();
    if (!articles.length) { Toast.warning('Keine Artikel zum Exportieren.'); return; }
    const rows = articles.map(a => [
      a.id, a.status, a.category, a.manufacturer, a.model, a.condition,
      a.location, a.quantity, a.width, a.depth, a.height,
      a.purchasePrice, a.originalPrice, a.material, a.style,
      a.shipping ? 'Ja' : 'Nein', a.shippingCost,
      a.pickupZip, a.listingLink, a.publicQrToken || '', a.groupId || '',
      a.soldPrice, a.soldDate, a.notes,
      Utils.formatDateTime(a.createdAt),
      Utils.formatDateTime(a.updatedAt),
    ].map(Utils.csvCell));
    const csv = [
      this.CSV_HEADERS.map(Utils.csvCell).join(';'),
      ...rows.map(r => r.join(';')),
    ].join('\r\n');
    this._download(
      new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' }),
      `MÃ¶belWawi_Export_${this._stamp()}.csv`
    );
    Toast.success(`${articles.length} Artikel exportiert.`);
  },

  async importCSV(event) {
    const input = event.target;
    const file  = input.files[0];
    if (!file) return;

    try {
      const rows   = await this._readImportFile(file);
      const result = await this._importRows(rows);

      Dashboard.renderStats();
      if (State.currentView === 'inventory')    Inventory.render();
      if (State.currentView === 'groups')       Groups.render();
      if (State.currentView === 'encyclopedia') Encyclopedia.render();
      if (State.currentView === 'sold')         Sold.render();

      Toast.success(
        `Import: ${result.imported} verarbeitet, ${result.created} neu, ${result.updated} aktualisiert, ${result.skipped} Ã¼bersprungen.`
      );
    } catch (err) {
      Toast.error(`Import fehlgeschlagen: ${err.message}`);
    } finally {
      input.value = '';
    }
  },

  _readImportFile(file) {
    const name = String(file?.name ?? '').toLowerCase();
    if (name.endsWith('.xlsx') || name.endsWith('.xls')) {
      return this._readExcelFile(file);
    }
    return this._readCsvFile(file);
  },

  _readCsvFile(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = e => {
        try {
          resolve(this._extractRowsFromCsv(e.target.result));
        } catch (err) {
          reject(err);
        }
      };
      reader.onerror = () => reject(new Error('CSV-Datei konnte nicht gelesen werden.'));
      reader.readAsText(file, 'UTF-8');
    });
  },

  _readExcelFile(file) {
    return new Promise((resolve, reject) => {
      if (typeof XLSX === 'undefined') {
        reject(new Error('Excel-UnterstÃ¼tzung konnte nicht geladen werden.'));
        return;
      }
      const reader = new FileReader();
      reader.onload = e => {
        try {
          resolve(this._extractRowsFromWorkbook(e.target.result));
        } catch (err) {
          reject(err);
        }
      };
      reader.onerror = () => reject(new Error('Excel-Datei konnte nicht gelesen werden.'));
      reader.readAsArrayBuffer(file);
    });
  },

  _extractRowsFromCsv(text) {
    const cleanText = String(text ?? '').replace(/^\uFEFF/, '');
    const lines     = cleanText.split(/\r?\n/).filter(line => line.trim());
    if (lines.length < 2) throw new Error('CSV leer oder ungÃ¼ltig.');
    const sep = this._detectSeparator(lines[0]);
    return lines.map(line => this._parseLine(line, sep));
  },

  _extractRowsFromWorkbook(buffer) {
    const workbook = XLSX.read(buffer, { type: 'array', cellDates: true });
    for (const sheetName of workbook.SheetNames) {
      const sheet = workbook.Sheets[sheetName];
      const rows  = XLSX.utils.sheet_to_json(sheet, {
        header   : 1,
        defval   : '',
        raw      : false,
        blankrows: false,
      });
      if (rows.some(row => Array.isArray(row) && row.some(cell => this._hasValue(cell)))) {
        return rows;
      }
    }
    throw new Error('Excel-Datei leer oder ungÃ¼ltig.');
  },

  async _importRows(rawRows) {
    const rows = rawRows.filter(row => Array.isArray(row));
    if (rows.length < 2) throw new Error('Datei leer oder ungÃ¼ltig.');

    const headerRowIndex = this._findHeaderRow(rows);
    if (headerRowIndex === -1) {
      throw new Error('SpaltenÃ¼berschriften konnten nicht erkannt werden.');
    }

    const headers          = rows[headerRowIndex].map(value => this._cleanText(value));
    const headerIndex      = this._buildHeaderIndex(headers);
    const dataRows         = rows.slice(headerRowIndex + 1);
    const existingIds      = new Set(DB.getArticles().map(article => article.id));
    const autoGroupBuckets = new Map();
    let imported = 0;
    let created  = 0;
    let updated  = 0;
    let skipped  = 0;

    for (const row of dataRows) {
      if (!Array.isArray(row) || this._isRowEmpty(row)) continue;

      const payload = this._rowToImportPayload(row, headerIndex);
      if (!payload) {
        skipped++;
        continue;
      }

      const importedIds = [];

      if (payload.id) {
        const exists = existingIds.has(payload.id);
        const dataToSave = exists
          ? this._mergeImportData(DB.getArticleById(payload.id), payload.data)
          : this._finalizeImportedArticle(payload.data);
        DB.importArticle(payload.id, dataToSave);
        existingIds.add(payload.id);
        importedIds.push(payload.id);
        imported++;

        if (exists) updated++;
        else created++;

        if (!exists && payload.quantity > 1) {
          const duplicates = await DB.saveBulkArticles(this._finalizeImportedArticle(payload.data), payload.quantity - 1);
          duplicates.forEach(article => {
            existingIds.add(article.id);
            importedIds.push(article.id);
          });
          imported += duplicates.length;
          created  += duplicates.length;
        }

        if (!dataToSave.groupId && importedIds.length) {
          this._queueAutoGroup(autoGroupBuckets, importedIds, dataToSave);
        }
      } else {
        const dataToSave = this._finalizeImportedArticle(payload.data);
        const savedArticles = payload.quantity > 1
          ? await DB.saveBulkArticles(dataToSave, payload.quantity)
          : [await DB.saveArticle(dataToSave)];

        savedArticles.forEach(article => {
          existingIds.add(article.id);
          importedIds.push(article.id);
        });
        imported += savedArticles.length;
        created  += savedArticles.length;

        if (!dataToSave.groupId && importedIds.length) {
          this._queueAutoGroup(autoGroupBuckets, importedIds, dataToSave);
        }
      }
    }

    if (!imported) {
      throw new Error('Keine importierbaren Zeilen gefunden.');
    }

    for (const bucket of autoGroupBuckets.values()) {
      await DB.autoAssignGroup(bucket.ids, bucket.sample);
    }

    return { imported, created, updated, skipped };
  },

  _findHeaderRow(rows) {
    const knownHeaders = new Set(
      Object.values(this.IMPORT_FIELD_ALIASES)
        .flat()
        .map(alias => this._normalizeKey(alias))
    );

    let bestIndex = -1;
    let bestScore = 0;

    rows.slice(0, 25).forEach((row, index) => {
      const score = row.reduce((sum, cell) => {
        return sum + (knownHeaders.has(this._normalizeKey(cell)) ? 1 : 0);
      }, 0);
      if (score > bestScore) {
        bestScore = score;
        bestIndex = index;
      }
    });

    return bestScore > 0 ? bestIndex : -1;
  },

  _buildHeaderIndex(headers) {
    const map = new Map();
    headers.forEach((header, index) => {
      const normalized = this._normalizeKey(header);
      if (normalized && !map.has(normalized)) map.set(normalized, index);
    });
    return map;
  },

  _readField(row, headerIndex, key) {
    const aliases = this.IMPORT_FIELD_ALIASES[key] ?? [key];
    for (const alias of aliases) {
      const idx = headerIndex.get(this._normalizeKey(alias));
      if (idx !== undefined) return row[idx];
    }
    return '';
  },

  _rowToImportPayload(row, headerIndex) {
    const id          = this._cleanText(this._readField(row, headerIndex, 'id'));
    const quantityRaw = this._parseNumber(this._readField(row, headerIndex, 'quantity'));
    const quantity    = Math.max(1, Math.round(quantityRaw || 1));
    const dimensions  = this._parseDimensions(this._readField(row, headerIndex, 'dimensions'));
    const locationBase = this._cleanText(this._readField(row, headerIndex, 'location'));
    const locationSlot = this._cleanText(this._readField(row, headerIndex, 'locationSlot'));
    const reservedInfo = this._cleanText(this._readField(row, headerIndex, 'reservedInfo'));
    const location = [locationBase, locationSlot].filter(Boolean).join(' / ');

    const purchasePriceGrossRaw = this._parseNumber(this._readField(row, headerIndex, 'purchasePriceGross'));
    const originalPriceGrossRaw = this._parseNumber(this._readField(row, headerIndex, 'originalPriceGross'));
    const soldPriceGrossRaw     = this._parseNumber(this._readField(row, headerIndex, 'soldPriceGross'));

    const purchasePrice = this._parseNumber(this._readField(row, headerIndex, 'purchasePrice'))
      ?? this._netFromGross(purchasePriceGrossRaw);
    const originalPrice = this._parseNumber(this._readField(row, headerIndex, 'originalPrice'))
      ?? this._netFromGross(originalPriceGrossRaw);
    const soldPrice = this._parseNumber(this._readField(row, headerIndex, 'soldPrice'))
      ?? this._netFromGross(soldPriceGrossRaw);

    const soldDate     = this._parseDateValue(this._readField(row, headerIndex, 'soldDate'));
    const rawStatus    = this._readField(row, headerIndex, 'status');
    const rawShipping  = this._readField(row, headerIndex, 'shipping');
    const shippingFlag = this._parseBoolean(rawShipping);
    const shippingCost = this._parseNumber(this._readField(row, headerIndex, 'shippingCost'));

    let status = this._hasValue(rawStatus) ? this._parseStatus(rawStatus) : '';
    if (!this._hasValue(rawStatus) && reservedInfo) {
      status = 'Reserviert';
    }
    if (!this._hasValue(rawStatus) && (soldPrice !== null || soldDate)) {
      status = 'Verkauft';
    }

    const baseNotes = this._cleanText(this._readField(row, headerIndex, 'notes'));
    const notes = reservedInfo
      ? [baseNotes, `Reserviert: ${reservedInfo}`].filter(Boolean).join('\n')
      : baseNotes;

    const data = {
      status,
      category          : this._cleanText(this._readField(row, headerIndex, 'category')),
      manufacturer      : this._cleanText(this._readField(row, headerIndex, 'manufacturer')),
      model             : this._cleanText(this._readField(row, headerIndex, 'model')),
      condition         : this._parseCondition(this._readField(row, headerIndex, 'condition')),
      location,
      quantity          : 1,
      width             : this._parseNumber(this._readField(row, headerIndex, 'width')) ?? dimensions.width,
      depth             : this._parseNumber(this._readField(row, headerIndex, 'depth')) ?? dimensions.depth,
      height            : this._parseNumber(this._readField(row, headerIndex, 'height')) ?? dimensions.height,
      purchasePrice,
      purchasePriceGross: purchasePriceGrossRaw ?? this._grossFromNet(purchasePrice),
      originalPrice,
      originalPriceGross: originalPriceGrossRaw ?? this._grossFromNet(originalPrice),
      material          : this._cleanText(this._readField(row, headerIndex, 'material')),
      style             : this._cleanText(this._readField(row, headerIndex, 'style')),
      color             : this._cleanText(this._readField(row, headerIndex, 'color')),
      shipping          : this._hasValue(rawShipping)
        ? shippingFlag
        : (shippingCost !== null ? true : null),
      shippingCost,
      pickupZip         : this._cleanText(this._readField(row, headerIndex, 'pickupZip')),
      listingLink       : this._cleanText(this._readField(row, headerIndex, 'listingLink')),
      publicQrToken     : this._cleanText(this._readField(row, headerIndex, 'publicQrToken')),
      groupId           : this._cleanText(this._readField(row, headerIndex, 'groupId')) || null,
      soldPrice,
      soldPriceGross    : soldPriceGrossRaw ?? this._grossFromNet(soldPrice),
      soldDate,
      notes,
      photos            : [],
    };

    const meaningfulValues = [
      id,
      quantityRaw,
      rawStatus,
      reservedInfo,
      this._readField(row, headerIndex, 'category'),
      this._readField(row, headerIndex, 'manufacturer'),
      this._readField(row, headerIndex, 'model'),
      this._readField(row, headerIndex, 'condition'),
      locationBase,
      locationSlot,
      this._readField(row, headerIndex, 'width'),
      this._readField(row, headerIndex, 'depth'),
      this._readField(row, headerIndex, 'height'),
      this._readField(row, headerIndex, 'dimensions'),
      this._readField(row, headerIndex, 'purchasePrice'),
      this._readField(row, headerIndex, 'purchasePriceGross'),
      this._readField(row, headerIndex, 'originalPrice'),
      this._readField(row, headerIndex, 'originalPriceGross'),
      this._readField(row, headerIndex, 'material'),
      this._readField(row, headerIndex, 'style'),
      this._readField(row, headerIndex, 'color'),
      this._readField(row, headerIndex, 'shipping'),
      this._readField(row, headerIndex, 'shippingCost'),
      this._readField(row, headerIndex, 'pickupZip'),
      this._readField(row, headerIndex, 'listingLink'),
      this._readField(row, headerIndex, 'publicQrToken'),
      this._readField(row, headerIndex, 'groupId'),
      this._readField(row, headerIndex, 'soldPrice'),
      this._readField(row, headerIndex, 'soldPriceGross'),
      this._readField(row, headerIndex, 'soldDate'),
      this._readField(row, headerIndex, 'notes'),
    ];

    if (!meaningfulValues.some(value => this._hasValue(value))) return null;

    return { id, quantity, data };
  },

  _queueAutoGroup(buckets, articleIds, sampleData) {
    const key = WawiDB.articleKey(sampleData) || `import:${articleIds[0]}`;
    if (!buckets.has(key)) {
      buckets.set(key, { ids: [], sample: { ...sampleData } });
    }
    buckets.get(key).ids.push(...articleIds);
  },

  _finalizeImportedArticle(data) {
    const displayInventoryId =
      data.displayInventoryId
      || (
        data.id !== undefined &&
        data.id !== null &&
        String(data.id).trim() !== '' &&
        !String(data.id).trim().startsWith('A-')
          ? String(data.id).trim()
          : ''
      );
    return {
      ...data,
      displayInventoryId,
      status           : data.status || 'Verf\u00fcgbar',
      shipping         : data.shipping ?? false,
      quantity         : 1,
    };
  },

  _mergeImportData(existing, imported) {
    const merged = { ...(existing ?? {}) };
    Object.entries(imported).forEach(([key, value]) => {
      if (Array.isArray(value)) {
        if (value.length) merged[key] = [...value];
        return;
      }
      if (this._hasValue(value)) merged[key] = value;
    });
    merged.quantity = 1;
    return merged;
  },

  _normalizeKey(value) {
    return String(value ?? '')
      .trim()
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/\u00df/g, 'ss')
      .replace(/[^a-z0-9]+/g, '');
  },

  _cleanText(value) {
    if (value === null || value === undefined) return '';
    if (value instanceof Date && !Number.isNaN(value.getTime())) {
      return Utils.formatDateInput(value.getTime());
    }
    return String(value).trim();
  },

  _hasValue(value) {
    if (value === null || value === undefined) return false;
    if (typeof value === 'number') return !Number.isNaN(value);
    if (value instanceof Date) return !Number.isNaN(value.getTime());
    return String(value).trim() !== '';
  },

  _isRowEmpty(row) {
    return !row.some(cell => this._hasValue(cell));
  },

  _detectSeparator(line) {
    const semicolons = (line.match(/;/g) || []).length;
    const commas     = (line.match(/,/g) || []).length;
    const tabs       = (line.match(/\t/g) || []).length;
    if (tabs > semicolons && tabs > commas) return '\t';
    return semicolons >= commas ? ';' : ',';
  },

  _parseNumber(value) {
    if (value === null || value === undefined || value === '') return null;
    if (typeof value === 'number') return Number.isFinite(value) ? value : null;

    let text = String(value).trim();
    if (!text) return null;

    text = text.replace(/\s+/g, '').replace(/[\u20ac$]/g, '');

    if (/^\d{1,3}(\.\d{3})+$/.test(text)) {
      text = text.replace(/\./g, '');
    } else if (text.includes(',') && text.includes('.')) {
      text = text.lastIndexOf(',') > text.lastIndexOf('.')
        ? text.replace(/\./g, '').replace(',', '.')
        : text.replace(/,/g, '');
    } else if (text.includes(',')) {
      text = text.replace(',', '.');
    }

    text = text.replace(/[^0-9.\-]/g, '');
    const parsed = parseFloat(text);
    return Number.isFinite(parsed) ? parsed : null;
  },

  _parseBoolean(value) {
    if (value === null || value === undefined || value === '') return null;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'number') return value !== 0;

    const normalized = this._normalizeKey(value);
    if (!normalized) return null;
    if (this.TRUE_VALUES.includes(normalized))  return true;
    if (this.FALSE_VALUES.includes(normalized)) return false;
    return null;
  },

  _parseStatus(value) {
    const normalized = this._normalizeKey(value);
    if (!normalized) return 'Verf\u00fcgbar';
    return this.STATUS_VALUES[normalized] ?? 'Verf\u00fcgbar';
  },

  _parseCondition(value) {
    const raw = this._cleanText(value);
    if (!raw) return '';
    return this.CONDITION_VALUES[this._normalizeKey(raw)] ?? raw;
  },

  _parseDateValue(value) {
    if (value === null || value === undefined || value === '') return null;
    if (value instanceof Date && !Number.isNaN(value.getTime())) {
      return Utils.formatDateInput(value.getTime());
    }
    if (typeof value === 'number' && Number.isFinite(value)) {
      const ts = Date.UTC(1899, 11, 30) + Math.round(value * 86400000);
      return Utils.formatDateInput(ts);
    }

    const raw = this._cleanText(value);
    if (!raw) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;

    const match = raw.match(/^(\d{1,2})[.\/-](\d{1,2})[.\/-](\d{2,4})/);
    if (match) {
      let year = parseInt(match[3], 10);
      if (year < 100) year += 2000;
      return [
        String(year).padStart(4, '0'),
        String(parseInt(match[2], 10)).padStart(2, '0'),
        String(parseInt(match[1], 10)).padStart(2, '0'),
      ].join('-');
    }

    const parsed = Date.parse(raw);
    return Number.isNaN(parsed) ? null : Utils.formatDateInput(parsed);
  },

  _parseDimensions(value) {
    const raw = this._cleanText(value).replace(/,/g, '.');
    if (!raw) return { width: null, depth: null, height: null };
    const matches = raw.match(/-?\d+(?:\.\d+)?/g) || [];
    return {
      width : matches[0] ? parseFloat(matches[0]) : null,
      depth : matches[1] ? parseFloat(matches[1]) : null,
      height: matches[2] ? parseFloat(matches[2]) : null,
    };
  },

  _grossFromNet(value) {
    return value === null ? null : parseFloat((value * 1.19).toFixed(2));
  },

  _netFromGross(value) {
    return value === null ? null : parseFloat((value / 1.19).toFixed(2));
  },

  _parseLine(line, sep = ';') {
    const result = [];
    let cur = '', inQ = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i], nx = line[i + 1];
      if (ch === '"') {
        if (inQ && nx === '"') { cur += '"'; i++; }
        else { inQ = !inQ; }
      } else if (ch === sep && !inQ) {
        result.push(cur); cur = '';
      } else {
        cur += ch;
      }
    }
    result.push(cur);
    return result;
  },

  exportJSON() {
    const data = {
      exportedAt: new Date().toISOString(),
      version:    '1.4',
      articles:   DB.getArticles(),
      groups:     DB.getGroups(),
    };
    this._download(
      new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' }),
      `MÃ¶belWawi_Backup_${this._stamp()}.json`
    );
    Toast.success('JSON-Backup erstellt.');
  },

  confirmResetDB() {
    Modal.open(`
      <h2 class="modal-title" style="color:var(--color-danger);">
        <i class="fa-solid fa-triangle-exclamation"></i>
        Datenbank zurÃ¼cksetzen
      </h2>
      <p>LÃ¶scht <strong>alle Artikel und Gruppen unwiderruflich</strong> aus dem lokalen Speicher.</p>
      <div style="margin-top:16px;padding:14px;background:var(--color-danger-light);border-radius:var(--border-radius-sm);border-left:4px solid var(--color-danger);">
        <strong>Diese Aktion kann nicht rÃ¼ckgÃ¤ngig gemacht werden!</strong>
      </div>
      <div class="modal-actions">
        <button class="btn btn-ghost" onclick="Modal.close()">Abbrechen</button>
        <button class="btn btn-outline" id="backup-then-reset">
          <i class="fa-solid fa-download"></i> Backup &amp; Reset
        </button>
        <button class="btn btn-danger" id="confirm-reset-btn">
          <i class="fa-solid fa-trash-can"></i> Jetzt lÃ¶schen
        </button>
      </div>`, content => {
      content.querySelector('#backup-then-reset').addEventListener('click', () => {
        this.exportJSON();
        setTimeout(() => {
          DB.resetAll();
          Modal.close();
          Dashboard.renderStats();
          Toast.success('Backup erstellt & Datenbank zurÃ¼ckgesetzt.');
        }, 600);
      });
      content.querySelector('#confirm-reset-btn').addEventListener('click', () => {
        DB.resetAll();
        Modal.close();
        Dashboard.renderStats();
        Toast.warning('Datenbank zurÃ¼ckgesetzt.');
      });
    });
  },

  handleScan(rawValue) {
    const input   = document.getElementById('scanner-input');
    if (!rawValue) return;
    const resolved = ScanResolver.resolve(rawValue);
    if (resolved.type === 'location') {
      Toast.warning(`Standort-QR erkannt: ${resolved.location}. Bitte dafür die Umlagerung verwenden.`);
      input.value = ''; input.focus();
      return;
    }
    if (resolved.type === 'unknown') {
      Toast.error(ScanResolver.unknownMessage(rawValue, resolved, 'tools'));
      input.value = ''; input.focus();
      return;
    }
    const article = resolved.article;
    input.value = ''; input.focus();
    Modal.open(`
      <h2 class="modal-title">
        <i class="fa-solid fa-qrcode"></i>
        Aktion fÃ¼r ${Utils.escHtml(article.id)}
      </h2>
      <div style="padding:12px;background:var(--color-bg);border-radius:var(--border-radius-sm);margin-bottom:20px;display:flex;align-items:center;gap:14px;">
        ${article.photos && article.photos[0]
          ? `<img src="${article.photos[0]}" style="width:60px;height:60px;object-fit:cover;border-radius:var(--border-radius-sm);" alt="Foto"/>`
          : `<div style="width:60px;height:60px;background:var(--color-border);border-radius:var(--border-radius-sm);display:flex;align-items:center;justify-content:center;color:var(--color-muted);">
               <i class="fa-solid fa-couch"></i>
             </div>`}
        <div>
          <div style="font-weight:700;">${Utils.escHtml(Utils.articleDisplayName(article, article.id))}</div>
          <div style="margin-top:4px;font-size:var(--font-size-xs);color:var(--color-muted);">
            ${ScanResolver.articleSourceLabel(resolved.type)}
          </div>
          <div style="margin-top:4px;">${Utils.statusBadge(article.status)}</div>
          ${article.groupId
            ? (() => {
                const g = DB.getGroupById(article.groupId);
                return `<div style="font-size:var(--font-size-xs);color:var(--color-primary);margin-top:4px;">
                          <i class="fa-solid fa-layer-group"></i> ${Utils.escHtml(g?.name || article.groupId)}
                        </div>`;
              })()
            : ''}
        </div>
      </div>
      <div style="margin-bottom:16px;">
        <label style="font-weight:600;display:block;margin-bottom:8px;">
          <i class="fa-solid fa-arrow-right-arrow-left"></i> Status Ã¤ndern
        </label>
        <div style="display:flex;flex-wrap:wrap;gap:8px;" id="scanner-status-btns">
          ${['Verf\u00fcgbar', 'Reserviert', 'Verkauft', 'Entsorgt'].map(s =>
            `<button class="btn ${s === article.status ? 'btn-primary' : 'btn-ghost'} btn-sm scanner-status-opt"
                     data-status="${s}">${s}</button>`
          ).join('')}
        </div>
      </div>
      <div style="margin-bottom:16px;">
        <label for="scanner-location-input" style="font-weight:600;display:block;margin-bottom:8px;">
          <i class="fa-solid fa-location-dot"></i> Standort Ã¤ndern
        </label>
        <input type="text" id="scanner-location-input"
               value="${Utils.escHtml(article.location || '')}"
               placeholder="Neuer Standort â€¦" style="width:100%;"/>
      </div>
      <div class="modal-actions">
        <button class="btn btn-ghost" onclick="Modal.close()">Abbrechen</button>
        <button class="btn btn-primary" id="scanner-apply-btn">
          <i class="fa-solid fa-floppy-disk"></i> Ãœbernehmen
        </button>
      </div>`, content => {
      let selectedStatus = article.status;
      content.querySelectorAll('.scanner-status-opt').forEach(btn => {
        btn.addEventListener('click', () => {
          content.querySelectorAll('.scanner-status-opt').forEach(b => {
            b.classList.remove('btn-primary'); b.classList.add('btn-ghost');
          });
          btn.classList.add('btn-primary'); btn.classList.remove('btn-ghost');
          selectedStatus = btn.dataset.status;
        });
      });
      content.querySelector('#scanner-apply-btn').addEventListener('click', () => {
        const newLocation = content.querySelector('#scanner-location-input').value.trim();
        if (selectedStatus === 'Verkauft' && article.status !== 'Verkauft') {
          Modal.close();
          this._promptSoldData(article.id, { status: selectedStatus, location: newLocation });
          return;
        }
        DB.updateArticle(article.id, { status: selectedStatus, location: newLocation });
        Modal.close();
        Toast.success(`${article.id}: Status â†’ ${selectedStatus}.`);
        Dashboard.renderStats();
        if (State.currentView === 'inventory')    Inventory.render();
        if (State.currentView === 'encyclopedia') Encyclopedia.render();
      });
    });
  },

  _promptSoldData(articleId, baseUpdates) {
    Modal.open(`
      <h2 class="modal-title">
        <i class="fa-solid fa-handshake"></i>
        Verkaufsdaten â€“ ${Utils.escHtml(articleId)}
      </h2>
      <div class="form-group" style="margin-bottom:14px;">
        <label for="prompt-sold-price">Verkaufspreis (â‚¬) <span class="required">*</span></label>
        <input type="number" id="prompt-sold-price" placeholder="0,00" min="0" step="0.01" style="width:100%;"/>
      </div>
      <div class="form-group">
        <label for="prompt-sold-date">Verkaufsdatum <span class="required">*</span></label>
        <input type="date" id="prompt-sold-date" value="${new Date().toISOString().split('T')[0]}" style="width:100%;"/>
      </div>
      <div class="modal-actions">
        <button class="btn btn-ghost" onclick="Modal.close()">Abbrechen</button>
        <button class="btn btn-success" id="prompt-sold-confirm">
          <i class="fa-solid fa-check"></i> Speichern
        </button>
      </div>`, content => {
      content.querySelector('#prompt-sold-confirm').addEventListener('click', () => {
        const soldPrice = parseFloat(content.querySelector('#prompt-sold-price').value);
        const soldDate  = content.querySelector('#prompt-sold-date').value;
        if (!soldPrice || !soldDate) { Toast.error('Bitte Preis und Datum angeben.'); return; }
        DB.updateArticle(articleId, { ...baseUpdates, soldPrice, soldDate });
        Modal.close();
        Toast.success(`${articleId} als verkauft gespeichert.`);
        Dashboard.renderStats();
        if (State.currentView === 'inventory')    Inventory.render();
        if (State.currentView === 'encyclopedia') Encyclopedia.render();
        if (State.currentView === 'sold')         Sold.render();
      });
    });
  },

  _download(blob, filename) {
    const url = URL.createObjectURL(blob);
    const a   = document.createElement('a');
    a.href = url; a.download = filename;
    document.body.appendChild(a); a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  },

  _stamp() {
    const d = new Date();
    return [
      d.getFullYear(),
      String(d.getMonth() + 1).padStart(2, '0'),
      String(d.getDate()).padStart(2, '0'),
    ].join('-');
  },
};

/* ============================================================
   15. QR-SCANNER â€” Kamera-basierter QR-Code-Leser
============================================================ */
const QRScanner = {

  _scanner            : null,
  _running            : false,
  _cameras            : [],
  _camIndex           : 0,
  _mode               : 'single',
  _relocateArticleIds : [],
  _relocateAwaitingLocationScan: false,
  _activeResultId     : null,
  _lastScanValue      : '',
  _lastScanAt         : 0,
  _warehouseOrderId   : null,

  init() {
    document.getElementById('scanner-rescan')
      .addEventListener('click', () => this.start());
    document.getElementById('scanner-switch-cam')
      .addEventListener('click', () => {
        this._camIndex = (this._camIndex + 1) % this._cameras.length;
        this._startWithCamera(this._cameras[this._camIndex].id);
      });
    document.querySelectorAll('[data-scanner-mode]').forEach(btn => {
      btn.addEventListener('click', () => this.setMode(btn.dataset.scannerMode));
    });
    document.getElementById('scanner-relocate-reset')
      .addEventListener('click', () => this.resetRelocation());
    document.getElementById('scanner-relocate-remove-last')
      .addEventListener('click', () => this.removeLastRelocationArticle());
    document.getElementById('scanner-relocate-confirm-location-scan')
      .addEventListener('click', () => this.confirmLocationScan());
    document.getElementById('scanner-relocate-apply')
      .addEventListener('click', () => this.applyRelocation());
    document.getElementById('scanner-relocate-location')
      .addEventListener('input', () => {
        if (document.getElementById('scanner-relocate-location').value.trim()) {
          this._setRelocationAwaitingLocationScan(false);
        }
      });
    document.getElementById('scanner-relocate-location')
      .addEventListener('keydown', e => {
        if (e.key === 'Enter') this.applyRelocation();
      });
    document.getElementById('btn-scanner-context-back')
      .addEventListener('click', () => {
        if (this.hasWarehouseSession()) {
          this.returnToWarehouse();
          return;
        }
        const groupId = Groups._currentGroupId;
        if (!groupId) return;
        Router.navigate('groups');
        window.setTimeout(() => Groups.openDetail(groupId), 80);
      });
    document.getElementById('btn-scanner-context-end')
      .addEventListener('click', () => {
        if (this.hasWarehouseSession()) {
          this.returnToWarehouse(true);
          return;
        }
        Groups.stopExternalQrMode();
        this._renderExternalQrContext();
      });
    this.setMode('single');
    this._renderRelocationList();
    this._renderExternalQrContext();
  },

  setMode(mode) {
    this._mode = mode === 'relocate' ? 'relocate' : 'single';
    document.querySelectorAll('[data-scanner-mode]').forEach(btn => {
      btn.classList.toggle('is-active', btn.dataset.scannerMode === this._mode);
    });
    document.getElementById('scanner-relocation-panel')
      .classList.toggle('hidden', this._mode !== 'relocate');
    document.getElementById('scanner-result')
      .classList.toggle('hidden', this._mode !== 'single');
    document.getElementById('scanner-rescan').style.display =
      this._mode === 'single' && this._activeResultId && !Groups.isExternalQrModeActive() ? 'inline-flex' : 'none';

    if (this._mode === 'single') {
      this._setRelocationAwaitingLocationScan(false);
      this.refreshActiveResult();
    } else {
      this._setResult(null);
      this._renderRelocationList();
      this._setRelocationAwaitingLocationScan(false);
      this._setBadge(this._running ? 'scanning' : 'idle');
    }
    this._renderExternalQrContext();
  },

  resetRelocation() {
    this._relocateArticleIds = [];
    document.getElementById('scanner-relocate-location').value = '';
    this._setRelocationAwaitingLocationScan(false);
    this._renderRelocationList();
  },

  removeLastRelocationArticle() {
    this._relocateArticleIds.pop();
    this._renderRelocationList();
  },

  confirmLocationScan() {
    this._setRelocationAwaitingLocationScan(true);
    Toast.success('Standort-Scan aktiviert. Scanne jetzt den Ziel-Standort.');
  },

  applyRelocation() {
    const location = document.getElementById('scanner-relocate-location').value.trim();
    if (!Tools.relocateArticles(this._relocateArticleIds, location)) return;
    this.resetRelocation();
  },

  _setRelocationAwaitingLocationScan(isAwaiting) {
    this._relocateAwaitingLocationScan = !!isAwaiting;
    const btn = document.getElementById('scanner-relocate-confirm-location-scan');
    if (!btn) return;
    btn.classList.toggle('is-active', this._relocateAwaitingLocationScan);
    btn.innerHTML = this._relocateAwaitingLocationScan
      ? '<i class="fa-solid fa-location-crosshairs"></i> Standort-Scan aktiv'
      : '<i class="fa-solid fa-location-crosshairs"></i> Jetzt Standort scannen';
  },

  refreshActiveResult() {
    if (this._mode !== 'single') return;
    if (Groups.isExternalQrModeActive()) {
      this._setResult(null);
      this._renderExternalQrContext();
      return;
    }
    if (!this._activeResultId) {
      this._setResult(null);
      return;
    }
    const article = DB.getArticleById(this._activeResultId);
    if (!article) {
      this._setResult(null);
      return;
    }
    this._setResult(article);
  },

  _renderRelocationList() {
    const listEl  = document.getElementById('scanner-relocate-list');
    const countEl = document.getElementById('scanner-relocate-count');
    if (!listEl || !countEl) return;

    const articles = this._relocateArticleIds
      .map(id => DB.getArticleById(id))
      .filter(Boolean);

    countEl.textContent = `${articles.length} Artikel`;

    if (!articles.length) {
      listEl.innerHTML = `
        <div class="scanner-relocate-empty">
          <i class="fa-solid fa-qrcode"></i>
          <p>Noch keine Artikel gescannt.</p>
          <small>Scanne mehrere Artikel und danach den Standort-QR oder gib den Standort manuell ein.</small>
        </div>`;
      return;
    }

    listEl.innerHTML = articles.map(article => {
    const name = Utils.escHtml(Utils.articleDisplayName(article, article.id));
      return `
        <div class="scanner-relocate-item">
          <div class="scanner-relocate-item__meta">
            <strong>${Utils.escHtml(article.id)}</strong>
            <span>${name}</span>
            <small>${Utils.escHtml(article.location || 'Kein Standort')}</small>
          </div>
          <button type="button" class="btn btn-ghost btn-sm scanner-relocate-remove" data-id="${Utils.escHtml(article.id)}">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>`;
    }).join('');

    listEl.querySelectorAll('.scanner-relocate-remove').forEach(btn => {
      btn.addEventListener('click', () => {
        this._relocateArticleIds = this._relocateArticleIds.filter(id => id !== btn.dataset.id);
        this._renderRelocationList();
      });
    });
  },

  async start() {
    this._renderExternalQrContext();
    if (this._mode === 'single') this._setResult(null);
    this._setBadge('scanning');
    if (this._mode === 'single') {
      document.getElementById('scanner-rescan').style.display = 'none';
    }
    if (!this._scanner) {
      this._scanner = new Html5Qrcode('qr-reader');
    }
    try {
      const devices = await Html5Qrcode.getCameras();
      if (!devices || !devices.length) {
        Toast.error('Kein KameragerÃ¤t gefunden.');
        this._setBadge('idle');
        return;
      }
      this._cameras  = devices;
      const switchBtn = document.getElementById('scanner-switch-cam');
      switchBtn.style.display = devices.length > 1 ? 'inline-flex' : 'none';
      await this._startPreferredCamera(devices);
    } catch (err) {
      Toast.error('Kamera konnte nicht geÃ¶ffnet werden: ' + err);
      this._setBadge('idle');
    }
  },

  async _startPreferredCamera(devices) {
    const backIdx = devices.findIndex(device => /back|rear|environment|rück|ruck|haupt/i.test(device.label));
    this._camIndex = backIdx >= 0 ? backIdx : 0;

    const preferredSources = [
      { facingMode: { exact: 'environment' } },
      { facingMode: 'environment' },
    ];

    for (const source of preferredSources) {
      try {
        await this._startWithCamera(source);
        return;
      } catch (_) {}
    }

    await this._startWithCamera(devices[this._camIndex].id);
  },

  async _startWithCamera(cameraConfig) {
    if (this._running) {
      try { await this._scanner.stop(); } catch (_) {}
      this._running = false;
    }
    await this._scanner.start(
      cameraConfig,
      { fps: 10, qrbox: { width: 250, height: 250 } },
      (text) => this._onScan(text),
      () => {}
    );
    this._running = true;
    this._setBadge('scanning');
  },

  async stop() {
    if (this._scanner && this._running) {
      try { await this._scanner.stop(); } catch (_) {}
      this._running = false;
    }
    if (State.currentView !== 'scanner' && State.currentView !== 'warehouse') {
      this._warehouseOrderId = null;
    }
    this._setBadge('idle');
    document.getElementById('scanner-rescan').style.display = 'none';
    if (this._mode === 'single') this._setResult(null);
    this._renderExternalQrContext();
  },

  hasWarehouseSession() {
    return !!this._warehouseOrderId;
  },

  startWarehouseSession(orderId) {
    this._warehouseOrderId = orderId;
    this.setMode('single');
    Router.navigate('scanner');
    this._renderExternalQrContext();
  },

  returnToWarehouse(clearOnly = false) {
    const orderId = this._warehouseOrderId;
    this._warehouseOrderId = null;
    if (clearOnly) {
      Router.navigate('warehouse');
      if (orderId) State.selectedWarehouseOrderId = orderId;
      return;
    }
    Router.navigate('warehouse');
    if (orderId) {
      State.selectedWarehouseOrderId = orderId;
      window.setTimeout(() => {
        Warehouse.render();
        document.getElementById('warehouse-scan-input')?.focus();
      }, 80);
    }
  },

  async _onScan(text) {
    const value = text.trim();
    if (!value) return;
    if (this._lastScanValue === value && Date.now() - this._lastScanAt < 1200) return;
    this._lastScanValue = value;
    this._lastScanAt    = Date.now();

    if (this._mode === 'relocate') {
      await this._handleRelocationScan(value);
      return;
    }

    if (this.hasWarehouseSession()) {
      if (this._scanner && this._running) {
        try { await this._scanner.pause(true); } catch (_) {}
      }
      this._setBadge('found');
      const resolved = ScanResolver.resolve(value);
      if (![
        'article-internal',
        'article-external',
        'article-public-url',
        'article-listing',
      ].includes(resolved.type)) {
        Toast.error('Im Warenausgang können nur Artikelcodes gescannt werden.');
        try { this._scanner.resume(); } catch (_) {}
        this._setBadge('scanning');
        return;
      }
      const result = Warehouse.processScannedArticle(resolved.article, this._warehouseOrderId);
      if (!result.ok) {
        try { this._scanner.resume(); } catch (_) {}
        this._setBadge('scanning');
        return;
      }
      this.returnToWarehouse();
      return;
    }

    if (Groups.isExternalQrModeActive()) {
      if (this._scanner && this._running) {
        try { await this._scanner.pause(true); } catch (_) {}
      }
      this._setBadge('found');
      try {
        await Groups.assignExternalQrToCurrentArticle(value);
      } catch (err) {
        console.error('scanner external qr assignment failed:', err);
        Toast.error('Fremd-QR-Zuordnung im Scanner fehlgeschlagen.');
      }
      this._renderExternalQrContext();
      window.setTimeout(() => {
        if (this._scanner && this._running) {
          try { this._scanner.resume(); } catch (_) {}
        }
        this._setBadge('scanning');
      }, 280);
      return;
    }

    if (this._scanner && this._running) {
      try { await this._scanner.pause(true); } catch (_) {}
    }
    this._setBadge('found');
    const resolved = ScanResolver.resolve(value);
    if (resolved.type === 'location') {
      Toast.warning('Standort-QR erkannt. Bitte dafÃ¼r den Umlagerungsmodus verwenden.');
      try { this._scanner.resume(); } catch (_) {}
      this._setBadge('scanning');
      return;
    }
    if (resolved.type === 'unknown') {
      Toast.error(ScanResolver.unknownMessage(value, resolved, 'scanner'));
      try { this._scanner.resume(); } catch (_) {}
      this._setBadge('scanning');
      return;
    }
    const article = resolved.article;
    document.getElementById('scanner-rescan').style.display = 'inline-flex';
    this._setResult(article);
  },

  async _handleRelocationScan(value) {
    if (this._scanner && this._running) {
      try { await this._scanner.pause(true); } catch (_) {}
    }

    const resolved = ScanResolver.resolve(value);
    if (resolved.type === 'location') {
      if (!this._relocateAwaitingLocationScan) {
        Toast.warning('Bitte zuerst auf â€žJetzt Standort scannenâ€œ klicken.');
      } else {
        document.getElementById('scanner-relocate-location').value = resolved.location;
        this._setRelocationAwaitingLocationScan(false);
        Toast.success(`Ziel-Standort erkannt: ${resolved.location}`);
      }
    } else if (
      resolved.type === 'article-internal'
      || resolved.type === 'article-external'
      || resolved.type === 'article-public-url'
      || resolved.type === 'article-listing'
    ) {
      const article = resolved.article;
      if (this._relocateAwaitingLocationScan) {
        Toast.warning('Standort-Scan ist aktiv. Bitte jetzt den Standort scannen.');
      } else if (!this._relocateArticleIds.includes(article.id)) {
        this._relocateArticleIds.push(article.id);
        this._renderRelocationList();
      } else {
        Toast.warning(`${article.id} wurde bereits gescannt.`);
      }
    } else {
      Toast.error(ScanResolver.unknownMessage(value, resolved, 'relocate'));
    }

    this._setBadge('found');
    window.setTimeout(() => {
      if (this._scanner && this._running) {
        try { this._scanner.resume(); } catch (_) {}
      }
      this._setBadge('scanning');
    }, 280);
  },

  _setResult(article) {
    const el = document.getElementById('scanner-result');
    if (!article) {
      this._activeResultId = null;
      el.style.display = 'none';
      el.innerHTML = '';
      return;
    }
    this._activeResultId = article.id;
    const thumb = (article.photos && article.photos[0])
      ? '<img src="' + article.photos[0] + '" alt="Foto" class="scanner-result__photo"/>'
      : '<div class="scanner-result__no-photo"><i class="fa-solid fa-couch"></i></div>';
    const name  = Utils.escHtml(Utils.articleDisplayName(article, '-'));
    const dims  = [article.width, article.depth, article.height].filter(Boolean).map(v => v + ' cm').join(' Ã— ');
    const group = article.groupId ? DB.getGroupById(article.groupId) : null;
    const row = (icon, label, val) => val
      ? '<div class="scanner-result__row"><i class="fa-solid ' + icon + '"></i>'
        + '<span class="scanner-result__label">' + label + '</span>'
        + '<span class="scanner-result__val">' + val + '</span></div>'
      : '';
    el.style.display = 'block';
    el.innerHTML =
      '<div class="scanner-result-card">'
      + '<div class="scanner-result__header">'
      +   thumb
      +   '<div class="scanner-result__header-info">'
      +     '<span class="scanner-result__id">' + Utils.escHtml(article.id) + '</span>'
      +     '<span class="scanner-result__name">' + name + '</span>'
      +     '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:4px;">'
      +       Utils.statusBadge(article.status)
      +       Utils.condBadge(article.condition)
      +       (Utils.isNewArticle(article) ? Utils.newBadge() : '')
      +     '</div>'
      +   '</div>'
      + '</div>'
      + '<div class="scanner-result__body">'
      +   row('tag',            'Kategorie',  Utils.escHtml(article.category  || ''))
      +   row('location-dot',   'Standort',   Utils.escHtml(article.location  || ''))
      +   row('layer-group',    'Gruppe',     group ? Utils.escHtml(group.name) : '')
      +   row('ruler-combined', 'MaÃŸe',       Utils.escHtml(dims))
      +   row('euro-sign',      'EK (Netto)', article.purchasePrice ? Utils.formatEuro(article.purchasePrice) : '')
      +   row('euro-sign',      'VK (Netto)', article.soldPrice     ? Utils.formatEuro(article.soldPrice)     : '')
      +   row('sticky-note',    'Notizen',    Utils.escHtml(article.notes || ''))
      + '</div>'
      + '<div class="scanner-result__actions">'
      +   '<button class="btn btn-primary btn-sm" id="scanner-btn-edit">'
      +     '<i class="fa-solid fa-pen-to-square"></i> Bearbeiten</button>'
      +   '<button class="btn btn-ghost btn-sm" id="scanner-btn-qr">'
      +     '<i class="fa-solid fa-print"></i> QR drucken</button>'
      + '</div>'
      + '</div>';
    el.querySelector('#scanner-btn-edit').addEventListener('click', () => {
      Dashboard.loadArticleIntoForm(article.id);
      Router.navigate('dashboard');
    });
    el.querySelector('#scanner-btn-qr').addEventListener('click', () => {
      QRManager.printQR(article.id);
    });
  },

  _setBadge(state) {
    const el = document.getElementById('scanner-status-badge');
    if (!el) return;
    el.className = 'scanner-badge scanner-badge--' + state;
    el.textContent = { idle: 'Bereit', scanning: 'Scannt â€¦', found: 'Gefunden âœ“' }[state] ?? state;
  },

  _renderExternalQrContext() {
    const panel = document.getElementById('scanner-context-panel');
    const subtitle = document.getElementById('scanner-context-subtitle');
    const progress = document.getElementById('scanner-context-progress');
    const body = document.getElementById('scanner-context-body');
    const rescanBtn = document.getElementById('scanner-rescan');
    const backBtn = document.getElementById('btn-scanner-context-back');
    const endBtn = document.getElementById('btn-scanner-context-end');
    if (!panel || !subtitle || !progress || !body || !rescanBtn) return;

    const warehouseOrder = this.hasWarehouseSession()
      ? OrderLogic.decorate(DB.getOrderById(this._warehouseOrderId))
      : null;
    if (warehouseOrder?.id) {
      panel.classList.remove('hidden');
      subtitle.textContent = `${warehouseOrder.id} · ${warehouseOrder.customerName || 'Ohne Namen'}`;
      progress.textContent = `${warehouseOrder.progress.picked} von ${warehouseOrder.progress.total} gepickt`;
      rescanBtn.style.display = 'none';
      if (backBtn) backBtn.innerHTML = '<i class="fa-solid fa-arrow-left"></i> Zum Auftrag';
      if (endBtn) endBtn.innerHTML = '<i class="fa-solid fa-xmark"></i> Scanner schließen';
      body.innerHTML = `
        <div class="scanner-context-panel__card">
          <span>${Utils.escHtml(warehouseOrder.fulfillmentType || 'Abholung')}</span>
          <strong>${Utils.escHtml(warehouseOrder.customerName || warehouseOrder.id)}</strong>
          <div class="scanner-context-panel__meta">
            ${OrderLogic.renderStatusPill(warehouseOrder.warehouseStatus)}
            ${OrderLogic.renderStatusPill(warehouseOrder.paymentStatus)}
          </div>
        </div>
        <div class="scanner-context-panel__hint">
          Scanne jetzt einen Artikel für diesen Auftrag. Nach einem erfolgreichen Scan springt das System direkt zurück in den Warenausgang.
        </div>`;
      return;
    }

    const context = Groups.getExternalQrScannerContext();
    if (!context) {
      panel.classList.add('hidden');
      subtitle.textContent = '';
      progress.textContent = '';
      body.innerHTML = '';
      if (backBtn) backBtn.innerHTML = '<i class="fa-solid fa-arrow-left"></i> Zur Gruppe';
      if (endBtn) endBtn.innerHTML = '<i class="fa-solid fa-xmark"></i> Serienzuordnung beenden';
      rescanBtn.style.display =
        this._mode === 'single' && this._activeResultId ? 'inline-flex' : 'none';
      return;
    }

    panel.classList.remove('hidden');
    subtitle.textContent = `${context.groupId} · ${context.groupName}`;
    progress.textContent = `${context.assignedCount} von ${context.totalCount} zugeordnet`;
    if (backBtn) backBtn.innerHTML = '<i class="fa-solid fa-arrow-left"></i> Zur Gruppe';
    if (endBtn) endBtn.innerHTML = '<i class="fa-solid fa-xmark"></i> Serienzuordnung beenden';
    rescanBtn.style.display = 'none';

    let html = '';
    if (context.targetArticle) {
      html += `
        <div class="scanner-context-panel__card">
          <span>${Utils.escHtml(context.targetArticle.id)}</span>
          <strong>${Utils.escHtml(Utils.articleDisplayName(context.targetArticle, context.targetArticle.id))}</strong>
          <div class="scanner-context-panel__meta">
            ${Utils.statusBadge(context.targetArticle.status)}
            <span class="external-qr-chip external-qr-chip--missing">
              <i class="fa-solid fa-qrcode"></i> Fremd-QR fehlt
            </span>
          </div>
        </div>`;
    } else if (context.totalCount) {
      html += `
        <div class="scanner-context-panel__empty">
          <i class="fa-solid fa-copy"></i>
          <span>Alle vorhandenen Artikel haben bereits einen Fremd-QR-Code. Der nächste Scan legt automatisch ein weiteres Duplikat in dieser Gruppe an.</span>
        </div>`;
    } else {
      html += `
        <div class="scanner-context-panel__empty">
          <i class="fa-solid fa-circle-xmark"></i>
          <span>In dieser Gruppe sind noch keine Artikel vorhanden.</span>
        </div>`;
    }

    if (this._mode !== 'single') {
      html += `
        <div class="scanner-context-panel__hint">
          Für die Fremd-QR-Serienzuordnung bitte den Modus "Einzel-Scan" aktiv lassen.
        </div>`;
    }

    body.innerHTML = html;
  },
};

const OrderLogic = {
  NEW_ORDER_ID: '__new_order__',

  parsePrice(value) {
    if (value === '' || value === null || value === undefined) return null;
    const parsed = parseFloat(value);
    if (!Number.isFinite(parsed) || parsed < 0) return null;
    return parsed;
  },

  createEmptyOrder() {
    return {
      customerName: '',
      customerPhone: '',
      orderDate: Utils.formatDateInput(Date.now()),
      fulfillmentType: 'Abholung',
      orderStatus: 'Angelegt',
      warehouseStatus: 'Nicht freigegeben',
      paymentStatus: 'Offen',
      paymentMethod: '',
      invoiceStatus: 'Nicht erstellt',
      pickupDate: '',
      completedAt: null,
      notes: '',
      positions: [],
    };
  },

  getOrderStatuses() {
    return ['Angelegt', 'Freigegeben', 'Abgeschlossen', 'Storniert'];
  },

  getWarehouseStatuses() {
    return ['Nicht freigegeben', 'Offen', 'In Bearbeitung', 'Vollständig', 'Bereit zur Abholung', 'Übergeben'];
  },

  getPaymentStatuses() {
    return ['Offen', 'Teilbezahlt', 'Bezahlt'];
  },

  getInvoiceStatuses() {
    return ['Nicht erstellt', 'Vorbereitet', 'Erstellt', 'Nicht nötig'];
  },

  getGroupLabel(groupId) {
    const group = DB.getGroupById(groupId);
    if (!group) return groupId;
    return Utils.groupDisplayName(group, DB.getArticlesByGroup(groupId), group.name || group.id) || group.id;
  },

  getGroupDefaultUnitPrice(groupId) {
    const group = DB.getGroupById(groupId);
    const price = parseFloat(group?.priceNet);
    return Number.isFinite(price) ? price : null;
  },

  getAvailableGroupQuantity(groupId) {
    return DB.getArticlesByGroup(groupId)
      .filter(article => !['Entsorgt', 'Verkauft'].includes(Utils.normalizeStatus(article.status)))
      .length;
  },

  getLocationBreakdown(groupId, excludedArticleIds = []) {
    const excludedIds = new Set(
      (excludedArticleIds ?? []).map(articleId => String(articleId ?? '').trim()).filter(Boolean)
    );
    const locationBuckets = new Map();

    DB.getArticlesByGroup(groupId)
      .filter(article =>
        !excludedIds.has(String(article.id ?? '').trim())
        && !['Entsorgt', 'Verkauft'].includes(Utils.normalizeStatus(article.status))
      )
      .forEach(article => {
        const location = String(article.location ?? '').trim() || 'Kein Standort';
        const quantity = parseInt(article.quantity, 10) || 1;
        locationBuckets.set(location, (locationBuckets.get(location) || 0) + quantity);
      });

    return Array.from(locationBuckets.entries())
      .map(([location, quantity]) => ({ location, quantity }))
      .sort((left, right) => left.location.localeCompare(right.location));
  },

  normalizePositions(positions = []) {
    return positions.map((position, index) => {
      const groupId = String(position?.groupId ?? '').trim();
      const quantity = Math.max(1, parseInt(position?.quantity, 10) || 1);
      if (!groupId) return;

      const scannedArticleIds = Array.from(new Set(
        (position?.scannedArticleIds ?? [])
          .map(value => String(value ?? '').trim())
          .filter(Boolean)
      )).slice(0, quantity);

      const saleUnitPrice = this.parsePrice(position?.saleUnitPrice);
      const defaultUnitPrice = this.getGroupDefaultUnitPrice(groupId);
      const effectiveUnitPrice = saleUnitPrice ?? defaultUnitPrice;

      return {
        positionId: String(position?.positionId ?? `POS-${index + 1}`),
        groupId,
        quantity,
        scannedArticleIds,
        saleUnitPrice,
        defaultUnitPrice,
        effectiveUnitPrice,
        lineTotal: effectiveUnitPrice !== null ? effectiveUnitPrice * quantity : 0,
        hasCustomPrice: saleUnitPrice !== null,
      };
    }).filter(Boolean);
  },

  getProgress(order) {
    const positions = this.normalizePositions(order?.positions ?? []);
    const total = positions.reduce((sum, position) => sum + (parseInt(position.quantity, 10) || 0), 0);
    const picked = positions.reduce(
      (sum, position) => sum + Math.min(position.scannedArticleIds?.length ?? 0, parseInt(position.quantity, 10) || 0),
      0
    );
    const percent = total ? Math.round((picked / total) * 100) : 0;
    return { total, picked, percent };
  },

  getPricing(order) {
    const positions = Array.isArray(order)
      ? this.normalizePositions(order)
      : this.normalizePositions(order?.positions ?? []);
    const listTotal = positions.reduce(
      (sum, position) => sum + ((position.defaultUnitPrice ?? 0) * (parseInt(position.quantity, 10) || 0)),
      0
    );
    const total = positions.reduce(
      (sum, position) => sum + ((position.effectiveUnitPrice ?? 0) * (parseInt(position.quantity, 10) || 0)),
      0
    );
    const discount = listTotal - total;
    const unpricedPositions = positions.filter(position => position.effectiveUnitPrice === null).length;
    return { listTotal, total, discount, unpricedPositions };
  },

  getComputedWarehouseStatus(order) {
    const { total, picked } = this.getProgress(order);
    const orderStatus = String(order?.orderStatus ?? 'Angelegt');
    const currentStatus = String(order?.warehouseStatus ?? 'Nicht freigegeben');

    if (orderStatus === 'Storniert') return 'Nicht freigegeben';
    if (orderStatus === 'Angelegt') return 'Nicht freigegeben';
    if (currentStatus === 'Übergeben') return 'Übergeben';
    if (currentStatus === 'Bereit zur Abholung' && total > 0 && picked >= total) return 'Bereit zur Abholung';
    if (!total) return orderStatus === 'Freigegeben' ? 'Offen' : 'Nicht freigegeben';
    if (picked >= total) return 'Vollständig';
    if (picked > 0) return 'In Bearbeitung';
    return orderStatus === 'Freigegeben' ? 'Offen' : 'Nicht freigegeben';
  },

  decorate(order) {
    const positions = this.normalizePositions(order?.positions ?? []);
    const paymentStatus = this.getPaymentStatuses().includes(order?.paymentStatus) ? order.paymentStatus : 'Offen';
    const invoiceStatus = this.getInvoiceStatuses().includes(order?.invoiceStatus) ? order.invoiceStatus : 'Nicht erstellt';
    let orderStatus = this.getOrderStatuses().includes(order?.orderStatus) ? order.orderStatus : 'Angelegt';
    let warehouseStatus = this.getWarehouseStatuses().includes(order?.warehouseStatus)
      ? order.warehouseStatus
      : 'Nicht freigegeben';

    warehouseStatus = this.getComputedWarehouseStatus({ ...order, positions, warehouseStatus, orderStatus });
    if (warehouseStatus === 'Übergeben' && paymentStatus === 'Bezahlt' && orderStatus !== 'Storniert') {
      orderStatus = 'Abgeschlossen';
    }
    const isCompleted = orderStatus === 'Abgeschlossen' || warehouseStatus === 'Übergeben';
    const completedAt = isCompleted
      ? (order?.completedAt || order?.updatedAt || Date.now())
      : null;

    return {
      ...this.createEmptyOrder(),
      ...order,
      orderStatus,
      warehouseStatus,
      paymentStatus,
      paymentMethod: String(order?.paymentMethod ?? '').trim(),
      invoiceStatus,
      completedAt,
      positions,
      progress: this.getProgress({ positions }),
      pricing: this.getPricing({ positions }),
    };
  },

  prepareForSave(order) {
    const decorated = this.decorate(order);
    return {
      customerName: String(decorated.customerName ?? '').trim(),
      customerPhone: String(decorated.customerPhone ?? '').trim(),
      orderDate: decorated.orderDate || Utils.formatDateInput(Date.now()),
      fulfillmentType: decorated.fulfillmentType === 'Lieferung' ? 'Lieferung' : 'Abholung',
      orderStatus: decorated.orderStatus,
      warehouseStatus: decorated.warehouseStatus,
      paymentStatus: decorated.paymentStatus,
      paymentMethod: String(decorated.paymentMethod ?? '').trim(),
      invoiceStatus: decorated.invoiceStatus,
      pickupDate: decorated.pickupDate || '',
      completedAt: decorated.completedAt || null,
      notes: String(decorated.notes ?? '').trim(),
      positions: decorated.positions.map((position, index) => ({
        positionId: String(position.positionId ?? `POS-${index + 1}`),
        groupId: String(position.groupId ?? '').trim(),
        quantity: Math.max(1, parseInt(position.quantity, 10) || 1),
        scannedArticleIds: Array.from(new Set(
          (position.scannedArticleIds ?? []).map(value => String(value ?? '').trim()).filter(Boolean)
        )).slice(0, Math.max(1, parseInt(position.quantity, 10) || 1)),
        saleUnitPrice: this.parsePrice(position.saleUnitPrice) ?? position.defaultUnitPrice ?? null,
      })),
    };
  },

  matchesSearch(order, query) {
    if (!query) return true;
    const lowerQuery = query.toLowerCase();
    return [
      order.id,
      order.customerName,
      order.customerPhone,
      order.paymentMethod,
      order.notes,
    ].some(value => String(value ?? '').toLowerCase().includes(lowerQuery));
  },

  isVisibleInWarehouse(order) {
    if (order.orderStatus === 'Storniert') return false;
    return order.orderStatus === 'Freigegeben'
      || ['Offen', 'In Bearbeitung', 'Vollständig', 'Bereit zur Abholung', 'Übergeben'].includes(order.warehouseStatus);
  },

  isVisibleInSold(order) {
    return order.orderStatus === 'Abgeschlossen' || order.warehouseStatus === 'Übergeben';
  },

  getCompletionTimestamp(order) {
    const source = order?.progress ? order : this.decorate(order);
    if (!this.isVisibleInSold(source)) return null;
    const numericCompletedAt = Number(source.completedAt);
    if (Number.isFinite(numericCompletedAt) && numericCompletedAt > 0) return numericCompletedAt;
    if (typeof source.completedAt === 'string') {
      const parsedDate = Date.parse(source.completedAt);
      if (Number.isFinite(parsedDate)) return parsedDate;
    }
    return Number(source.updatedAt) || null;
  },

  getStatusTone(status) {
    if (['Abgeschlossen', 'Vollständig', 'Bereit zur Abholung', 'Bezahlt', 'Übergeben', 'Aktiv', 'Erstellt'].includes(status)) return 'success';
    if (['Freigegeben', 'In Bearbeitung', 'Teilbezahlt', 'Lieferung', 'Abholung', 'Vorbereitet'].includes(status)) return 'info';
    if (['Offen', 'Nicht freigegeben', 'Angelegt', 'Geschützt', 'Inaktiv', 'Nicht erstellt'].includes(status)) return 'warning';
    if (status === 'Nicht nötig') return 'neutral';
    if (status === 'Storniert') return 'danger';
    return 'neutral';
  },

  getStatusIcon(status) {
    return ({
      Angelegt: 'fa-file-circle-plus',
      Freigegeben: 'fa-share',
      Abgeschlossen: 'fa-circle-check',
      Storniert: 'fa-ban',
      'Nicht freigegeben': 'fa-lock',
      Offen: 'fa-hourglass-half',
      'In Bearbeitung': 'fa-bars-progress',
      Vollständig: 'fa-box-open',
      'Bereit zur Abholung': 'fa-truck-ramp-box',
      Übergeben: 'fa-handshake',
      Teilbezahlt: 'fa-money-bill-wave',
      Bezahlt: 'fa-credit-card',
      'Nicht erstellt': 'fa-file-circle-xmark',
      Vorbereitet: 'fa-file-circle-plus',
      Erstellt: 'fa-file-invoice',
      'Nicht nötig': 'fa-file-circle-minus',
      Abholung: 'fa-truck-ramp-box',
      Lieferung: 'fa-truck',
      Aktiv: 'fa-user-check',
      Inaktiv: 'fa-user-slash',
      Geschützt: 'fa-shield-halved',
    })[status] ?? 'fa-circle';
  },

  renderStatusPill(label) {
    const tone = this.getStatusTone(label);
    const icon = this.getStatusIcon(label);
    return `<span class="status-pill status-pill--${tone}">
      <i class="fa-solid ${icon}"></i>
      ${Utils.escHtml(label)}
    </span>`;
  },
};

const Orders = {
  NEW_ID: OrderLogic.NEW_ORDER_ID,

  init() {
    document.getElementById('orders-search')
      .addEventListener('input', () => this.render());
    document.getElementById('orders-filter-order-status')
      .addEventListener('change', () => this.render());
    document.getElementById('orders-filter-warehouse-status')
      .addEventListener('change', () => this.render());
    document.getElementById('orders-filter-payment-status')
      .addEventListener('change', () => this.render());
    document.getElementById('btn-order-new')
      .addEventListener('click', () => this.openNew());
    document.getElementById('btn-order-add-position')
      .addEventListener('click', () => {
        this.appendPositionRow();
        this.updatePositionSummary();
      });
    document.getElementById('btn-cancel-order')
      .addEventListener('click', () => this.resetDetailState());
    document.getElementById('btn-order-back')
      .addEventListener('click', () => this.closeDetail());
    document.getElementById('btn-order-release')
      .addEventListener('click', () => this.releaseSelectedOrder());
    document.getElementById('btn-order-set-ready')
      .addEventListener('click', () => this.markReady());
    document.getElementById('btn-order-set-handed-over')
      .addEventListener('click', () => this.markHandedOver());
    document.getElementById('order-form')
      .addEventListener('submit', event => {
        event.preventDefault();
        this.save();
      });

    const positionsEditor = document.getElementById('order-positions-editor');
    positionsEditor.addEventListener('click', event => {
      const removeButton = event.target.closest('[data-remove-order-position]');
      if (!removeButton) return;
      removeButton.closest('.order-position-row')?.remove();
      if (!positionsEditor.children.length) this.appendPositionRow();
      this.updatePositionSummary();
    });
    positionsEditor.addEventListener('change', event => {
      const row = event.target.closest('.order-position-row');
      if (!row) return;
      if (event.target.classList.contains('order-position-group-search')) {
        this.syncGroupSearchField(row, true);
        this.updatePositionSummary();
        return;
      }
      this.updatePositionRowMeta(row);
      this.updatePositionSummary();
    });
    positionsEditor.addEventListener('input', event => {
      const row = event.target.closest('.order-position-row');
      if (!row) return;
      if (event.target.classList.contains('order-position-group-search')) {
        this.syncGroupSearchField(row, false);
        this.updatePositionSummary();
        return;
      }
      if (!event.target.classList.contains('order-position-quantity')
        && !event.target.classList.contains('order-position-price')) return;
      this.updatePositionRowMeta(row);
      this.updatePositionSummary();
    });
  },

  getAllOrders() {
    return DB.getOrders()
      .map(order => OrderLogic.decorate(order))
      .sort((left, right) => (right.updatedAt ?? 0) - (left.updatedAt ?? 0));
  },

  getFilteredOrders() {
    const search = document.getElementById('orders-search').value.trim();
    const orderStatus = document.getElementById('orders-filter-order-status').value;
    const warehouseStatus = document.getElementById('orders-filter-warehouse-status').value;
    const paymentStatus = document.getElementById('orders-filter-payment-status').value;

    return this.getAllOrders().filter(order =>
      OrderLogic.matchesSearch(order, search)
      && (!orderStatus || order.orderStatus === orderStatus)
      && (!warehouseStatus || order.warehouseStatus === warehouseStatus)
      && (!paymentStatus || order.paymentStatus === paymentStatus)
    );
  },

  render() {
    if (!AccessControl.can('orders.view')) return;
    document.getElementById('btn-order-new').classList.toggle('hidden', !AccessControl.can('orders.create'));
    this.renderStats();
    this.renderList();
    this.renderDetail();
    this.updateLayoutState();
  },

  updateLayoutState() {
    const layout = document.querySelector('#view-orders .orders-layout');
    if (!layout) return;
    layout.classList.toggle(
      'show-detail-mobile',
      !!State.selectedOrderId
    );
  },

  renderStats() {
    const orders = this.getAllOrders();
    document.getElementById('orders-stat-total').textContent = String(orders.length);
    document.getElementById('orders-stat-open').textContent = String(
      orders.filter(order => ['Offen', 'In Bearbeitung', 'Vollständig', 'Bereit zur Abholung'].includes(order.warehouseStatus)).length
    );
    document.getElementById('orders-stat-paid').textContent = String(
      orders.filter(order => order.paymentStatus === 'Bezahlt').length
    );
  },

  renderList() {
    const container = document.getElementById('orders-list');
    const orders = this.getFilteredOrders();

    if (!orders.length) {
      State.selectedOrderId = null;
      container.innerHTML = `<div class="empty-state">
        <i class="fa-solid fa-clipboard-list"></i>
        <p>Noch keine passenden Aufträge vorhanden.</p>
      </div>`;
      this.updateLayoutState();
      return;
    }

    container.className = 'order-card-list';
    container.innerHTML = orders.map(order => {
      const isActive = State.selectedOrderId === order.id ? ' is-active' : '';
      return `<article class="order-card${isActive}" data-order-id="${Utils.escHtml(order.id)}">
        <div class="order-card__top">
          <div>
            <span class="order-card__eyebrow">${Utils.escHtml(order.id)}</span>
            <div class="order-card__title">${Utils.escHtml(order.customerName || 'Ohne Namen')}</div>
            <div class="order-card__meta">
              <span><i class="fa-solid fa-calendar-days"></i> ${Utils.escHtml(order.orderDate || '-')}</span>
              <span><i class="fa-solid fa-truck-ramp-box"></i> ${Utils.escHtml(order.fulfillmentType || 'Abholung')}</span>
            </div>
          </div>
        </div>
        <div class="order-card__footer">
          <div class="order-card__badges">
            ${OrderLogic.renderStatusPill(order.orderStatus)}
            ${OrderLogic.renderStatusPill(order.warehouseStatus)}
            ${OrderLogic.renderStatusPill(order.paymentStatus)}
          </div>
          <div class="order-card__progress">${order.progress.picked} von ${order.progress.total}</div>
        </div>
      </article>`;
    }).join('');

    container.querySelectorAll('[data-order-id]').forEach(card => {
      card.addEventListener('click', () => this.open(card.dataset.orderId));
    });
  },

  open(orderId) {
    State.selectedOrderId = orderId;
    this.renderDetail();
    this.renderList();
    this.updateLayoutState();
  },

  openNew() {
    if (!AccessControl.can('orders.create')) {
      Toast.warning('Für neue Aufträge fehlt die Berechtigung.');
      return;
    }
    State.selectedOrderId = this.NEW_ID;
    this.renderDetail();
    this.renderList();
    this.updateLayoutState();
  },

  getSelectedOrder() {
    if (!State.selectedOrderId || State.selectedOrderId === this.NEW_ID) return null;
    return OrderLogic.decorate(DB.getOrderById(State.selectedOrderId));
  },

  renderDetail() {
    const emptyState = document.getElementById('orders-empty-state');
    const shell = document.getElementById('orders-detail-shell');
    const selectedOrder = this.getSelectedOrder();
    const isNewOrder = State.selectedOrderId === this.NEW_ID;

    if (!selectedOrder && !isNewOrder) {
      emptyState.classList.remove('hidden');
      shell.classList.add('hidden');
      this.updateLayoutState();
      return;
    }

    emptyState.classList.add('hidden');
    shell.classList.remove('hidden');

    const order = selectedOrder ?? OrderLogic.createEmptyOrder();
    document.getElementById('order-detail-id').textContent = selectedOrder?.id ?? 'Neuer Auftrag';
    document.getElementById('order-detail-customer-display').textContent = order.customerName || 'Neuer Auftrag';
    document.getElementById('order-detail-status-badges').innerHTML = [
      OrderLogic.renderStatusPill(order.orderStatus),
      OrderLogic.renderStatusPill(order.warehouseStatus),
      OrderLogic.renderStatusPill(order.paymentStatus),
      OrderLogic.renderStatusPill(order.invoiceStatus),
    ].join('');

    document.getElementById('order-edit-id').value = selectedOrder?.id ?? '';
    document.getElementById('order-customer-name').value = order.customerName || '';
    document.getElementById('order-customer-phone').value = order.customerPhone || '';
    document.getElementById('order-date').value = order.orderDate || Utils.formatDateInput(Date.now());
    document.getElementById('order-fulfillment-type').value = order.fulfillmentType || 'Abholung';
    document.getElementById('order-status').value = order.orderStatus || 'Angelegt';
    document.getElementById('order-warehouse-status').value = order.warehouseStatus || 'Nicht freigegeben';
    document.getElementById('order-payment-status').value = order.paymentStatus || 'Offen';
    document.getElementById('order-pickup-date').value = order.pickupDate || '';
    document.getElementById('order-payment-method').value = order.paymentMethod || '';
    document.getElementById('order-invoice-status').value = order.invoiceStatus || 'Nicht erstellt';
    document.getElementById('order-notes').value = order.notes || '';

    this.renderPositionRows(order.positions);
    this.updatePositionSummary();
    this.updateActionButtons(order);

    const canModify = selectedOrder ? AccessControl.can('orders.edit') : AccessControl.can('orders.create');
    document.querySelectorAll('#order-form input, #order-form select, #order-form textarea').forEach(field => {
      if (field.id === 'order-edit-id') return;
      field.disabled = !canModify;
    });
    document.querySelectorAll('#order-form [data-remove-order-position], #btn-order-add-position').forEach(button => {
      button.disabled = !canModify;
    });
    this.updateLayoutState();
  },

  updateActionButtons(order) {
    const canRelease = AccessControl.can('orders.release');
    const canEditOrders = AccessControl.can('orders.edit');
    const canPayment = AccessControl.can('orders.payment');
    const isPersistedOrder = !!document.getElementById('order-edit-id').value.trim();
    document.getElementById('btn-order-release').disabled = !canRelease || !isPersistedOrder || !order || order.orderStatus === 'Freigegeben';
    document.getElementById('btn-order-set-ready').disabled = !canEditOrders || !isPersistedOrder || !order || order.progress.total === 0 || order.progress.picked < order.progress.total;
    document.getElementById('btn-order-set-handed-over').disabled = !canPayment || !isPersistedOrder || !order || order.paymentStatus !== 'Bezahlt';
  },

  getSelectableGroups() {
    return DB.getGroups()
      .filter(group => group.status !== 'Entsorgt')
      .sort((left, right) => String(left.name ?? '').localeCompare(String(right.name ?? '')));
  },

  getGroupSearchLabel(groupId = '') {
    if (!groupId) return '';
    return `${groupId} · ${OrderLogic.getGroupLabel(groupId)}`;
  },

  getGroupOptionsHtml() {
    return this.getSelectableGroups().map(group => `
      <option value="${Utils.escHtml(this.getGroupSearchLabel(group.id))}"></option>`).join('');
  },

  resolveGroupSearchValue(value = '') {
    const query = String(value ?? '').trim().toLowerCase();
    if (!query) return null;
    const groups = this.getSelectableGroups().map(group => ({
      id: group.id,
      label: this.getGroupSearchLabel(group.id),
      groupLabel: OrderLogic.getGroupLabel(group.id),
    }));

    const exactMatch = groups.find(group =>
      group.id.toLowerCase() === query
      || group.label.toLowerCase() === query
      || group.groupLabel.toLowerCase() === query
    );
    if (exactMatch) return exactMatch;

    const partialMatches = groups.filter(group =>
      group.id.toLowerCase().includes(query)
      || group.label.toLowerCase().includes(query)
      || group.groupLabel.toLowerCase().includes(query)
    );
    return partialMatches.length === 1 ? partialMatches[0] : null;
  },

  refreshGroupOptions() {
    const datalist = document.getElementById('order-group-options');
    if (datalist) datalist.innerHTML = this.getGroupOptionsHtml();
  },

  syncGroupSearchField(row, normalizeValue = false) {
    if (!row) return;
    const searchField = row.querySelector('.order-position-group-search');
    const hiddenField = row.querySelector('.order-position-group');
    if (!searchField || !hiddenField) return;

    const previousGroupId = hiddenField.value || '';
    const match = this.resolveGroupSearchValue(searchField.value);
    const nextGroupId = match?.id || '';

    if (nextGroupId !== previousGroupId) {
      row.dataset.scannedIds = '[]';
    }

    hiddenField.value = nextGroupId;
    row.dataset.originalGroupId = nextGroupId;

    if (normalizeValue && match) {
      searchField.value = match.label;
    }
  },

  positionRowHtml(position = {}) {
    const quantity = Math.max(1, parseInt(position.quantity, 10) || 1);
    const scannedArticleIds = Array.isArray(position.scannedArticleIds) ? position.scannedArticleIds : [];
    const groupId = String(position.groupId ?? '').trim();
    const available = groupId ? OrderLogic.getAvailableGroupQuantity(groupId) : 0;
    const defaultUnitPrice = OrderLogic.getGroupDefaultUnitPrice(groupId);
    const saleUnitPrice = OrderLogic.parsePrice(position.saleUnitPrice);
    const effectiveUnitPrice = saleUnitPrice ?? defaultUnitPrice;
    const lineTotal = effectiveUnitPrice !== null ? effectiveUnitPrice * quantity : 0;
    const groupSearchValue = groupId ? this.getGroupSearchLabel(groupId) : '';

    return `<div class="order-position-row" data-scanned-ids="${Utils.escHtml(JSON.stringify(scannedArticleIds))}" data-original-group-id="${Utils.escHtml(groupId)}">
      <div class="order-position-row__main">
        <div class="form-group order-position-row__group" style="margin-bottom:0;">
          <label>Artikelgruppe <span class="required">*</span></label>
          <input type="hidden" class="order-position-group" value="${Utils.escHtml(groupId)}"/>
          <input type="text" class="order-position-group-search" list="order-group-options" value="${Utils.escHtml(groupSearchValue)}" placeholder="Artikelgruppe suchen oder auswählen"/>
          <div class="order-position-meta">Verfügbar im Bestand: ${available}</div>
        </div>
        <button class="btn btn-ghost btn-sm order-position-row__remove" type="button" data-remove-order-position>
          <i class="fa-solid fa-trash-can"></i>
          Entfernen
        </button>
      </div>
      <div class="order-position-row__stats">
        <div class="form-group" style="margin-bottom:0;">
          <label>Stückzahl</label>
          <input type="number" class="order-position-quantity" value="${quantity}" min="1" max="999"/>
        </div>
        <div class="form-group" style="margin-bottom:0;">
          <label>VK pro Stück</label>
          <input type="number" class="order-position-price" value="${Utils.escHtml(String(saleUnitPrice ?? ''))}" min="0" step="0.01" placeholder="${defaultUnitPrice !== null ? Utils.escHtml(String(defaultUnitPrice.toFixed(2))) : '0,00'}"/>
          <div class="order-position-meta">Standardpreis: ${defaultUnitPrice !== null ? Utils.formatEuro(defaultUnitPrice) : '–'}</div>
        </div>
        <div class="form-group" style="margin-bottom:0;">
          <label>Positionssumme</label>
          <div class="order-position-total">${effectiveUnitPrice !== null ? Utils.formatEuro(lineTotal) : '–'}</div>
        </div>
        <div class="form-group" style="margin-bottom:0;">
          <label>Fortschritt</label>
          <div class="order-position-meta">${Math.min(scannedArticleIds.length, quantity)} von ${quantity} gescannt</div>
        </div>
      </div>
    </div>`;
  },

  renderPositionRows(positions = []) {
    const container = document.getElementById('order-positions-editor');
    const safePositions = positions.length ? positions : [{}];
    this.refreshGroupOptions();
    container.innerHTML = safePositions.map(position => this.positionRowHtml(position)).join('');
  },

  appendPositionRow(position = {}) {
    const container = document.getElementById('order-positions-editor');
    this.refreshGroupOptions();
    container.insertAdjacentHTML('beforeend', this.positionRowHtml(position));
  },

  updatePositionRowMeta(row) {
    if (!row) return;
    const groupId = row.querySelector('.order-position-group')?.value || '';
    const quantity = Math.max(1, parseInt(row.querySelector('.order-position-quantity')?.value, 10) || 1);
    const scannedIds = JSON.parse(row.dataset.scannedIds || '[]');
    const defaultUnitPrice = OrderLogic.getGroupDefaultUnitPrice(groupId);
    const saleUnitPrice = OrderLogic.parsePrice(row.querySelector('.order-position-price')?.value);
    const effectiveUnitPrice = saleUnitPrice ?? defaultUnitPrice;
    const metaNodes = row.querySelectorAll('.order-position-meta');
    if (metaNodes[0]) metaNodes[0].textContent = `Verfügbar im Bestand: ${groupId ? OrderLogic.getAvailableGroupQuantity(groupId) : 0}`;
    if (metaNodes[1]) metaNodes[1].textContent = `Standardpreis: ${defaultUnitPrice !== null ? Utils.formatEuro(defaultUnitPrice) : '–'}`;
    if (metaNodes[2]) metaNodes[2].textContent = `${Math.min(scannedIds.length, quantity)} von ${quantity} gescannt`;
    const totalNode = row.querySelector('.order-position-total');
    if (totalNode) totalNode.textContent = effectiveUnitPrice !== null ? Utils.formatEuro(effectiveUnitPrice * quantity) : '–';
  },

  collectPositionsFromEditor() {
    return Array.from(document.querySelectorAll('#order-positions-editor .order-position-row'))
      .map(row => {
        const groupId = row.querySelector('.order-position-group')?.value || '';
        const quantity = Math.max(1, parseInt(row.querySelector('.order-position-quantity')?.value, 10) || 1);
        const scannedArticleIds = JSON.parse(row.dataset.scannedIds || '[]');
        return {
          groupId,
          quantity,
          scannedArticleIds: Array.isArray(scannedArticleIds) ? scannedArticleIds : [],
          saleUnitPrice: OrderLogic.parsePrice(row.querySelector('.order-position-price')?.value),
        };
      })
      .filter(position => position.groupId);
  },

  updatePositionSummary() {
    const positions = OrderLogic.normalizePositions(this.collectPositionsFromEditor());
    const progress = OrderLogic.getProgress({ positions });
    const pricing = OrderLogic.getPricing({ positions });
    document.getElementById('order-position-summary').textContent = positions.length
      ? `${positions.length} Positionen · ${progress.picked} von ${progress.total} Stück aktuell erfasst`
      : 'Noch keine Positionen gewählt.';
    document.getElementById('order-pricing-summary').innerHTML = positions.length ? [
      `<article class="order-pricing-card">
        <span class="order-pricing-card__label">Listenwert</span>
        <div class="order-pricing-card__value">${Utils.formatEuro(pricing.listTotal)}</div>
        <div class="order-pricing-card__meta">Aus den hinterlegten Gruppenpreisen berechnet.</div>
      </article>`,
      `<article class="order-pricing-card">
        <span class="order-pricing-card__label">Verkauf gesamt</span>
        <div class="order-pricing-card__value">${Utils.formatEuro(pricing.total)}</div>
        <div class="order-pricing-card__meta">Tatsächlicher Auftragswert aus den Positionspreisen.</div>
      </article>`,
      `<article class="order-pricing-card">
        <span class="order-pricing-card__label">Rabatt / Abweichung</span>
        <div class="order-pricing-card__value">${Utils.formatEuro(pricing.discount)}</div>
        <div class="order-pricing-card__meta">${pricing.discount > 0 ? 'Unter dem Listenwert verkauft.' : pricing.discount < 0 ? 'Über dem Listenwert verkauft.' : 'Entspricht dem Listenwert.'}</div>
      </article>`,
      `<article class="order-pricing-card">
        <span class="order-pricing-card__label">Ohne Preis</span>
        <div class="order-pricing-card__value">${pricing.unpricedPositions}</div>
        <div class="order-pricing-card__meta">Positionen ohne Gruppen- oder Verkaufspreis.</div>
      </article>`,
    ].join('') : '';
  },

  async save() {
    const orderId = document.getElementById('order-edit-id').value.trim();
    const isExisting = !!orderId;
    const permission = isExisting ? 'orders.edit' : 'orders.create';
    if (!AccessControl.can(permission)) {
      Toast.warning('Für diese Aktion fehlt die Berechtigung.');
      return;
    }

    const positions = this.collectPositionsFromEditor();
    if (!document.getElementById('order-customer-name').value.trim()) {
      Toast.error('Bitte einen Kundennamen eingeben.');
      document.getElementById('order-customer-name').focus();
      return;
    }
    if (!positions.length) {
      Toast.error('Bitte mindestens eine Auftragsposition anlegen.');
      return;
    }

    const payload = OrderLogic.prepareForSave({
      customerName: document.getElementById('order-customer-name').value,
      customerPhone: document.getElementById('order-customer-phone').value,
      orderDate: document.getElementById('order-date').value,
      fulfillmentType: document.getElementById('order-fulfillment-type').value,
      orderStatus: document.getElementById('order-status').value,
      warehouseStatus: document.getElementById('order-warehouse-status').value,
      paymentStatus: document.getElementById('order-payment-status').value,
      paymentMethod: document.getElementById('order-payment-method').value,
      invoiceStatus: document.getElementById('order-invoice-status').value,
      pickupDate: document.getElementById('order-pickup-date').value,
      notes: document.getElementById('order-notes').value,
      positions,
    });

    if (isExisting) {
      DB.updateOrder(orderId, payload);
      State.selectedOrderId = orderId;
      Toast.success(`Auftrag ${orderId} wurde aktualisiert.`);
    } else {
      const savedOrder = await DB.saveOrder(payload);
      State.selectedOrderId = savedOrder.id;
      Toast.success(`Auftrag ${savedOrder.id} wurde gespeichert.`);
    }

    this.render();
    Warehouse.render();
  },

  resetDetailState() {
    this.closeDetail();
  },

  closeDetail() {
    State.selectedOrderId = null;
    document.getElementById('order-form').reset();
    this.render();
  },

  releaseSelectedOrder() {
    const selectedOrder = this.getSelectedOrder();
    if (!selectedOrder) return;
    if (!AccessControl.can('orders.release')) {
      Toast.warning('Für die Freigabe fehlt die Berechtigung.');
      return;
    }

    const updated = OrderLogic.prepareForSave({
      ...selectedOrder,
      orderStatus: 'Freigegeben',
      warehouseStatus: 'Offen',
    });
    DB.updateOrder(selectedOrder.id, updated);
    Toast.success(`Auftrag ${selectedOrder.id} wurde für den Warenausgang freigegeben.`);
    this.render();
    Warehouse.render();
  },

  markReady() {
    const selectedOrder = this.getSelectedOrder();
    if (!selectedOrder) return;
    if (!AccessControl.can('orders.edit')) {
      Toast.warning('Für diese Aktion fehlt die Berechtigung.');
      return;
    }
    if (selectedOrder.progress.total === 0 || selectedOrder.progress.picked < selectedOrder.progress.total) {
      Toast.warning('Der Auftrag ist noch nicht vollständig gepickt.');
      return;
    }
    DB.updateOrder(selectedOrder.id, OrderLogic.prepareForSave({
      ...selectedOrder,
      warehouseStatus: 'Bereit zur Abholung',
      orderStatus: 'Freigegeben',
    }));
    Toast.success(`Auftrag ${selectedOrder.id} wurde als bereit markiert.`);
    this.render();
    Warehouse.render();
  },

  markHandedOver() {
    const selectedOrder = this.getSelectedOrder();
    if (!selectedOrder) return;
    if (!AccessControl.can('orders.payment')) {
      Toast.warning('Für diese Aktion fehlt die Berechtigung.');
      return;
    }
    if (selectedOrder.paymentStatus !== 'Bezahlt') {
      Toast.warning('Vor der Übergabe muss der Zahlungsstatus auf "Bezahlt" stehen.');
      return;
    }
    DB.updateOrder(selectedOrder.id, OrderLogic.prepareForSave({
      ...selectedOrder,
      warehouseStatus: 'Übergeben',
      orderStatus: 'Abgeschlossen',
      completedAt: Date.now(),
    }));
    Toast.success(`Auftrag ${selectedOrder.id} wurde als übergeben abgeschlossen.`);
    this.render();
    Warehouse.render();
  },
};

const Warehouse = {
  init() {
    document.getElementById('warehouse-search')
      .addEventListener('input', () => this.render());
    document.getElementById('warehouse-filter-status')
      .addEventListener('change', () => this.render());
    document.getElementById('warehouse-show-finished')
      .addEventListener('change', () => this.render());
    document.getElementById('btn-warehouse-back')
      .addEventListener('click', () => this.closeDetail());
    document.getElementById('btn-warehouse-undo')
      .addEventListener('click', () => this.undoLastScan());
    document.getElementById('btn-warehouse-open-scanner')
      .addEventListener('click', () => this.openCameraScanner());
    document.getElementById('btn-warehouse-scan')
      .addEventListener('click', () => this.handleScan());
    document.getElementById('warehouse-scan-input')
      .addEventListener('keydown', event => {
        if (event.key === 'Enter') {
          event.preventDefault();
          this.handleScan();
        }
      });
    document.getElementById('btn-warehouse-mark-ready')
      .addEventListener('click', () => this.markReady());
    document.getElementById('btn-warehouse-mark-handed-over')
      .addEventListener('click', () => this.markHandedOver());
  },

  getOrders() {
    return DB.getOrders()
      .map(order => OrderLogic.decorate(order))
      .filter(order => OrderLogic.isVisibleInWarehouse(order))
      .sort((left, right) => (right.updatedAt ?? 0) - (left.updatedAt ?? 0));
  },

  getFilteredOrders() {
    const query = document.getElementById('warehouse-search').value.trim();
    const status = document.getElementById('warehouse-filter-status').value;
    const showFinished = document.getElementById('warehouse-show-finished').checked;

    return this.getOrders().filter(order => {
      if (!showFinished && order.warehouseStatus === 'Übergeben') return false;
      return OrderLogic.matchesSearch(order, query)
        && (!status || order.warehouseStatus === status);
    });
  },

  render() {
    if (!AccessControl.can('warehouse.view')) return;
    this.renderStats();
    this.renderList();
    this.renderDetail();
    this.updateLayoutState();
  },

  updateLayoutState() {
    const layout = document.querySelector('#view-warehouse .warehouse-layout');
    if (!layout) return;
    layout.classList.toggle(
      'show-detail-mobile',
      !!State.selectedWarehouseOrderId
    );
  },

  renderStats() {
    const orders = this.getOrders();
    document.getElementById('warehouse-stat-open').textContent = String(
      orders.filter(order => order.warehouseStatus === 'Offen').length
    );
    document.getElementById('warehouse-stat-progress').textContent = String(
      orders.filter(order => order.warehouseStatus === 'In Bearbeitung').length
    );
    document.getElementById('warehouse-stat-complete').textContent = String(
      orders.filter(order => ['Vollständig', 'Bereit zur Abholung'].includes(order.warehouseStatus)).length
    );
  },

  renderList() {
    const container = document.getElementById('warehouse-orders-list');
    const orders = this.getFilteredOrders();

    if (!orders.length) {
      State.selectedWarehouseOrderId = null;
      container.innerHTML = `<div class="empty-state">
        <i class="fa-solid fa-warehouse"></i>
        <p>Aktuell gibt es keine passenden Aufträge im Warenausgang.</p>
      </div>`;
      this.updateLayoutState();
      return;
    }

    if (!orders.some(order => order.id === State.selectedWarehouseOrderId)) {
      State.selectedWarehouseOrderId = null;
    }

    container.className = 'warehouse-order-list';
    container.innerHTML = orders.map(order => {
      const isActive = State.selectedWarehouseOrderId === order.id ? ' is-active' : '';
      return `<article class="warehouse-order-card${isActive}" data-warehouse-order-id="${Utils.escHtml(order.id)}">
        <div class="warehouse-order-card__top">
          <div>
            <span class="order-card__eyebrow">${Utils.escHtml(order.id)}</span>
            <div class="warehouse-order-card__title">${Utils.escHtml(order.customerName || 'Ohne Namen')}</div>
            <div class="warehouse-order-card__meta">
              <span><i class="fa-solid fa-truck-ramp-box"></i> ${Utils.escHtml(order.fulfillmentType || 'Abholung')}</span>
              <span><i class="fa-solid fa-calendar-days"></i> ${Utils.escHtml(order.pickupDate || order.orderDate || '-')}</span>
            </div>
          </div>
        </div>
        <div class="warehouse-order-card__footer">
          <div class="order-card__badges">
            ${OrderLogic.renderStatusPill(order.warehouseStatus)}
          </div>
          <div class="warehouse-order-card__progress">${order.progress.picked} von ${order.progress.total}</div>
        </div>
      </article>`;
    }).join('');

    container.querySelectorAll('[data-warehouse-order-id]').forEach(card => {
      card.addEventListener('click', () => {
        State.selectedWarehouseOrderId = card.dataset.warehouseOrderId;
        this.render();
      });
    });
    this.updateLayoutState();
  },

  getSelectedOrder() {
    return OrderLogic.decorate(DB.getOrderById(State.selectedWarehouseOrderId));
  },

  renderDetail() {
    const emptyState = document.getElementById('warehouse-empty-state');
    const shell = document.getElementById('warehouse-detail-shell');
    const order = this.getSelectedOrder();

    if (!order?.id) {
      emptyState.classList.remove('hidden');
      shell.classList.add('hidden');
      this.updateLayoutState();
      return;
    }

    emptyState.classList.add('hidden');
    shell.classList.remove('hidden');

    document.getElementById('warehouse-detail-order-id').textContent = order.id;
    document.getElementById('warehouse-detail-customer').textContent = order.customerName || 'Ohne Namen';
    document.getElementById('warehouse-detail-statuses').innerHTML = [
      OrderLogic.renderStatusPill(order.warehouseStatus),
      OrderLogic.renderStatusPill(order.paymentStatus),
      OrderLogic.renderStatusPill(order.fulfillmentType),
    ].join('');
    document.getElementById('warehouse-detail-progress-label').textContent = `${order.progress.picked} von ${order.progress.total}`;
    document.getElementById('warehouse-detail-progress-fill').style.width = `${order.progress.percent}%`;

    document.getElementById('warehouse-positions-list').innerHTML = order.positions.map(position => {
      const picked = Math.min(position.scannedArticleIds?.length ?? 0, parseInt(position.quantity, 10) || 0);
      const quantity = parseInt(position.quantity, 10) || 0;
      const percent = quantity ? Math.round((picked / quantity) * 100) : 0;
      const isComplete = picked >= quantity;
      const locationBreakdown = OrderLogic.getLocationBreakdown(position.groupId, position.scannedArticleIds ?? []);
      const locationSummary = locationBreakdown.length
        ? `Standorte anzeigen (${locationBreakdown.reduce((sum, item) => sum + item.quantity, 0)} Stück an ${locationBreakdown.length} Ort${locationBreakdown.length === 1 ? '' : 'en'})`
        : 'Keine offenen Standorte verfügbar';
      return `<article class="warehouse-position-card${isComplete ? ' is-complete' : ''}">
        <div class="warehouse-position-card__header">
          <div>
            <div class="warehouse-position-card__title">${Utils.escHtml(OrderLogic.getGroupLabel(position.groupId))}</div>
            <div class="warehouse-position-card__qty">${Utils.escHtml(position.groupId)} · ${quantity} Stück gesucht</div>
          </div>
          ${OrderLogic.renderStatusPill(isComplete ? 'Vollständig' : (picked > 0 ? 'In Bearbeitung' : 'Offen'))}
        </div>
        <div class="warehouse-position-card__progress">
          <div class="warehouse-position-card__progress-label">
            <span>${picked} von ${quantity}</span>
            <span>${percent}%</span>
          </div>
          <div class="warehouse-position-card__progress-bar">
            <span style="width:${percent}%"></span>
          </div>
        </div>
        ${locationBreakdown.length
          ? `<details class="warehouse-position-card__locations">
              <summary>${Utils.escHtml(locationSummary)}</summary>
              <div class="warehouse-location-list">
                ${locationBreakdown.map(item => `
                  <div class="warehouse-location-item">
                    <span>${item.quantity} Stück</span>
                    <strong>${Utils.escHtml(item.location)}</strong>
                  </div>`).join('')}
              </div>
            </details>`
          : `<div class="warehouse-position-card__locations-empty">Für diese Position sind aktuell keine offenen Standortbestände mehr vorhanden.</div>`}
      </article>`;
    }).join('');

    document.getElementById('btn-warehouse-mark-ready').disabled =
      !AccessControl.can('warehouse.ready')
      || order.progress.total === 0
      || order.progress.picked < order.progress.total;
    document.getElementById('btn-warehouse-mark-handed-over').disabled =
      !AccessControl.can('warehouse.handover')
      || order.paymentStatus !== 'Bezahlt';
    document.getElementById('btn-warehouse-undo').disabled =
      !AccessControl.can('warehouse.scan')
      || !State.warehouseLastScan
      || State.warehouseLastScan.orderId !== order.id;
    document.getElementById('btn-warehouse-open-scanner').disabled = !AccessControl.can('warehouse.scan');
    document.getElementById('warehouse-scan-input').disabled = !AccessControl.can('warehouse.scan');
    document.getElementById('btn-warehouse-scan').disabled = !AccessControl.can('warehouse.scan');
    this.updateLayoutState();
  },

  resolveArticle(scanValue) {
    const value = String(scanValue ?? '').trim();
    if (!value) return null;
    const resolved = ScanResolver.resolve(value);
    if ([
      'article-internal',
      'article-external',
      'article-public-url',
      'article-listing',
    ].includes(resolved.type)) {
      return resolved.article;
    }
    return null;
  },

  processScannedArticle(article, orderId = State.selectedWarehouseOrderId, options = {}) {
    const { showToast = true } = options;
    const order = OrderLogic.decorate(DB.getOrderById(orderId));
    if (!order?.id) {
      if (showToast) Toast.error('Kein Auftrag für den Scan ausgewählt.');
      return { ok: false };
    }
    if (!article) {
      if (showToast) Toast.error('Der Scan konnte keinem vorhandenen Artikel zugeordnet werden.');
      return { ok: false };
    }
    if (!article.groupId) {
      if (showToast) Toast.error(`Artikel ${article.id} ist keiner Artikelgruppe zugeordnet.`);
      return { ok: false };
    }
    if (['Verkauft', 'Entsorgt'].includes(Utils.normalizeStatus(article.status))) {
      if (showToast) Toast.error(`Artikel ${article.id} kann mit dem Status ${Utils.normalizeStatus(article.status)} nicht gepickt werden.`);
      return { ok: false };
    }

    const positions = OrderLogic.normalizePositions(order.positions).map(position => ({
      ...position,
      scannedArticleIds: [...(position.scannedArticleIds ?? [])],
    }));

    if (positions.some(position => position.scannedArticleIds.includes(article.id))) {
      if (showToast) Toast.warning(`Artikel ${article.id} wurde in diesem Auftrag bereits gescannt.`);
      return { ok: false };
    }

    const targetPosition = positions.find(position =>
      position.groupId === article.groupId
      && (position.scannedArticleIds?.length ?? 0) < (parseInt(position.quantity, 10) || 0)
    );

    if (!targetPosition) {
      if (showToast) Toast.error(`Artikel ${article.id} gehört zu keiner offenen Position dieses Auftrags.`);
      return { ok: false };
    }

    targetPosition.scannedArticleIds.push(article.id);
    DB.updateOrder(order.id, OrderLogic.prepareForSave({
      ...order,
      positions,
      orderStatus: 'Freigegeben',
    }));

    if (showToast) {
      Toast.success(`Artikel ${article.id} wurde dem Auftrag ${order.id} zugeordnet.`);
    }
    State.warehouseLastScan = {
      orderId: order.id,
      articleId: article.id,
      scannedAt: Date.now(),
    };
    return { ok: true, orderId: order.id, articleId: article.id };
  },

  openCameraScanner() {
    const order = this.getSelectedOrder();
    if (!order?.id) {
      Toast.warning('Bitte zuerst einen Auftrag im Warenausgang auswählen.');
      return;
    }
    if (!AccessControl.can('warehouse.scan')) {
      Toast.warning('Für das Scannen fehlt die Berechtigung.');
      return;
    }
    QRScanner.startWarehouseSession(order.id);
  },

  handleScan() {
    const order = this.getSelectedOrder();
    if (!order?.id) return;
    if (!AccessControl.can('warehouse.scan')) {
      Toast.warning('Für das Scannen fehlt die Berechtigung.');
      return;
    }

    const input = document.getElementById('warehouse-scan-input');
    const article = this.resolveArticle(input.value);
    if (!article) {
      Toast.error('Der Scan konnte keinem vorhandenen Artikel zugeordnet werden.');
      input.focus();
      input.select();
      return;
    }
    const result = this.processScannedArticle(article, order.id);
    if (!result.ok) {
      input.focus();
      input.select();
      return;
    }
    input.value = '';
    input.focus();
    this.render();
    Orders.render();
  },

  undoLastScan() {
    const lastScan = State.warehouseLastScan;
    if (!lastScan?.orderId || !lastScan.articleId) {
      Toast.warning('Es gibt keinen letzten Scan zum Rückgängig machen.');
      return;
    }

    const order = OrderLogic.decorate(DB.getOrderById(lastScan.orderId));
    if (!order?.id) {
      State.warehouseLastScan = null;
      Toast.error('Der Auftrag zum letzten Scan wurde nicht gefunden.');
      this.render();
      return;
    }

    const positions = OrderLogic.normalizePositions(order.positions).map(position => ({
      ...position,
      scannedArticleIds: [...(position.scannedArticleIds ?? [])],
    }));

    const targetPosition = positions.find(position => position.scannedArticleIds.includes(lastScan.articleId));
    if (!targetPosition) {
      State.warehouseLastScan = null;
      Toast.warning('Der letzte Scan ist in diesem Auftrag nicht mehr vorhanden.');
      this.render();
      return;
    }

    targetPosition.scannedArticleIds = targetPosition.scannedArticleIds.filter(articleId => articleId !== lastScan.articleId);

    DB.updateOrder(order.id, OrderLogic.prepareForSave({
      ...order,
      positions,
      orderStatus: 'Freigegeben',
    }));

    State.selectedWarehouseOrderId = order.id;
    State.warehouseLastScan = null;
    Toast.success(`Der letzte Scan (${lastScan.articleId}) wurde rückgängig gemacht.`);
    this.render();
    Orders.render();
  },

  closeDetail() {
    State.selectedWarehouseOrderId = null;
    document.getElementById('warehouse-scan-input').value = '';
    this.render();
  },

  markReady() {
    const order = this.getSelectedOrder();
    if (!order?.id) return;
    if (!AccessControl.can('warehouse.ready')) {
      Toast.warning('Für diese Aktion fehlt die Berechtigung.');
      return;
    }
    if (order.progress.total === 0 || order.progress.picked < order.progress.total) {
      Toast.warning('Der Auftrag ist noch nicht vollständig gescannt.');
      return;
    }
    DB.updateOrder(order.id, OrderLogic.prepareForSave({
      ...order,
      warehouseStatus: 'Bereit zur Abholung',
      orderStatus: 'Freigegeben',
    }));
    Toast.success(`Auftrag ${order.id} ist jetzt bereit zur Abholung.`);
    this.render();
    Orders.render();
  },

  markHandedOver() {
    const order = this.getSelectedOrder();
    if (!order?.id) return;
    if (!AccessControl.can('warehouse.handover')) {
      Toast.warning('Für diese Aktion fehlt die Berechtigung.');
      return;
    }
    if (order.paymentStatus !== 'Bezahlt') {
      Toast.warning('Vor der Übergabe muss der Auftrag als bezahlt markiert sein.');
      return;
    }
    DB.updateOrder(order.id, OrderLogic.prepareForSave({
      ...order,
      warehouseStatus: 'Übergeben',
      orderStatus: 'Abgeschlossen',
      completedAt: Date.now(),
    }));
    Toast.success(`Auftrag ${order.id} wurde als übergeben abgeschlossen.`);
    this.render();
    Orders.render();
  },
};

const AdminPanel = {
  _expandedPermissionGroups: new Set(),

  init() {
    document.querySelectorAll('[data-admin-tab]').forEach(button => {
      button.addEventListener('click', () => {
        State.adminTab = button.dataset.adminTab;
        this.render();
      });
    });

    document.getElementById('admin-user-form').addEventListener('submit', event => {
      event.preventDefault();
      this.saveUser();
    });
    document.getElementById('btn-cancel-admin-user')
      .addEventListener('click', () => this.resetUserForm());

    document.getElementById('admin-role-form').addEventListener('submit', event => {
      event.preventDefault();
      this.saveRole();
    });
    document.getElementById('btn-cancel-admin-role')
      .addEventListener('click', () => this.resetRoleForm());

    document.getElementById('admin-users-list').addEventListener('click', event => {
      const button = event.target.closest('[data-edit-user]');
      if (!button) return;
      this.openUser(button.dataset.editUser);
    });

    document.getElementById('admin-roles-list').addEventListener('click', event => {
      const button = event.target.closest('[data-edit-role]');
      if (!button) return;
      this.openRole(button.dataset.editRole);
    });

    const permissionsContainer = document.getElementById('admin-role-permissions');
    permissionsContainer.addEventListener('change', event => {
      if (event.target.classList.contains('permission-group-master')) {
        const groupId = event.target.dataset.groupId;
        permissionsContainer
          .querySelectorAll(`.permission-item-checkbox[data-group-id="${groupId}"]`)
          .forEach(checkbox => { checkbox.checked = event.target.checked; });
      }
      this.syncPermissionMasterCheckboxes();
    });
    permissionsContainer.addEventListener('click', event => {
      const button = event.target.closest('[data-toggle-permission-group]');
      if (!button) return;
      const groupId = button.dataset.togglePermissionGroup;
      if (this._expandedPermissionGroups.has(groupId)) {
        this._expandedPermissionGroups.delete(groupId);
      } else {
        this._expandedPermissionGroups.add(groupId);
      }
      this.renderPermissionGroups(this.getSelectedPermissionIds());
    });

    this.renderPermissionGroups([]);
  },

  getSelectedPermissionIds() {
    return Array.from(document.querySelectorAll('#admin-role-permissions .permission-item-checkbox:checked'))
      .map(checkbox => checkbox.value);
  },

  render() {
    if (!AccessControl.canAny(['admin.users.view', 'admin.roles.view'])) return;

    const canUsers = AccessControl.can('admin.users.view');
    const canRoles = AccessControl.can('admin.roles.view');
    const canManageUsers = AccessControl.can('admin.users.manage');
    const canManageRoles = AccessControl.can('admin.roles.manage');

    if (State.adminTab === 'users' && !canUsers) State.adminTab = canRoles ? 'roles' : 'users';
    if (State.adminTab === 'roles' && !canRoles) State.adminTab = canUsers ? 'users' : 'roles';

    document.querySelectorAll('[data-admin-tab]').forEach(button => {
      const isActive = button.dataset.adminTab === State.adminTab;
      button.classList.toggle('active', isActive);
      button.classList.toggle('hidden', (button.dataset.adminTab === 'users' && !canUsers) || (button.dataset.adminTab === 'roles' && !canRoles));
    });

    document.getElementById('admin-tab-users').classList.toggle('hidden', State.adminTab !== 'users');
    document.getElementById('admin-tab-roles').classList.toggle('hidden', State.adminTab !== 'roles');

    document.querySelectorAll('#admin-user-form input, #admin-user-form select').forEach(field => {
      if (field.id === 'admin-user-original-id') return;
      field.disabled = !canManageUsers;
    });
    document.querySelectorAll('#admin-role-form input, #admin-role-form textarea, #admin-role-form .permission-item-checkbox, #admin-role-form .permission-group-master').forEach(field => {
      if (field.id === 'admin-role-edit-id') return;
      field.disabled = !canManageRoles;
    });
    document.getElementById('btn-save-admin-user').disabled = !canManageUsers;
    document.getElementById('btn-save-admin-role').disabled = !canManageRoles;

    this.renderRoleOptions();
    this.renderUsersList();
    this.renderRolesList();
    this.syncPermissionMasterCheckboxes();
    document.querySelectorAll('#admin-role-form [data-toggle-permission-group]').forEach(button => {
      button.disabled = !canManageRoles;
    });
  },

  renderRoleOptions() {
    const select = document.getElementById('admin-user-role');
    const roles = DB.getRoles().sort((left, right) => String(left.name ?? '').localeCompare(String(right.name ?? '')));
    const currentValue = select.value;
    select.innerHTML = [
      '<option value="">Bitte Rolle wählen</option>',
      ...roles.map(role => `<option value="${Utils.escHtml(role.id)}">${Utils.escHtml(role.name || role.id)}</option>`),
    ].join('');
    select.value = roles.some(role => role.id === currentValue) ? currentValue : '';
  },

  renderUsersList() {
    const container = document.getElementById('admin-users-list');
    const users = DB.getUsers().sort((left, right) => {
      const leftLabel = String(left.displayName ?? left.email ?? '');
      const rightLabel = String(right.displayName ?? right.email ?? '');
      return leftLabel.localeCompare(rightLabel);
    });

    if (!users.length) {
      container.innerHTML = `<div class="empty-state">
        <i class="fa-solid fa-users"></i>
        <p>Noch keine Nutzer angelegt.</p>
      </div>`;
      return;
    }

    container.innerHTML = users.map(user => {
      const role = DB.getRoleById(user.roleId);
      return `<article class="admin-item">
        <div class="admin-item__top">
          <div>
            <div class="admin-item__title">${Utils.escHtml(user.displayName || user.email || user.id)}</div>
            <div class="admin-item__meta">
              <span><i class="fa-solid fa-envelope"></i> ${Utils.escHtml(user.email || '-')}</span>
              <span><i class="fa-solid fa-user-lock"></i> ${Utils.escHtml(role?.name || 'Ohne Rolle')}</span>
            </div>
          </div>
          <div class="admin-item__actions">
            ${user.active === false ? OrderLogic.renderStatusPill('Inaktiv') : OrderLogic.renderStatusPill('Aktiv')}
            ${AccessControl.can('admin.users.manage') ? `<button class="btn btn-outline btn-sm" type="button" data-edit-user="${Utils.escHtml(user.id)}">
              <i class="fa-solid fa-pen-to-square"></i> Bearbeiten
            </button>` : ''}
          </div>
        </div>
      </article>`;
    }).join('');
  },

  renderRolesList() {
    const container = document.getElementById('admin-roles-list');
    const roles = DB.getRoles().sort((left, right) => {
      if (!!left.isSystemRole !== !!right.isSystemRole) return left.isSystemRole ? -1 : 1;
      return String(left.name ?? '').localeCompare(String(right.name ?? ''));
    });

    if (!roles.length) {
      container.innerHTML = `<div class="empty-state">
        <i class="fa-solid fa-user-lock"></i>
        <p>Noch keine Rollen vorhanden.</p>
      </div>`;
      return;
    }

    container.innerHTML = roles.map(role => `
      <article class="admin-item">
        <div class="admin-item__top">
          <div>
            <div class="admin-item__title">${Utils.escHtml(role.name || role.id)}</div>
            <div class="admin-item__meta">
              <span><i class="fa-solid fa-key"></i> ${role.permissions?.length ?? 0} Berechtigungen</span>
              ${role.isSystemRole ? '<span><i class="fa-solid fa-shield-halved"></i> Systemrolle</span>' : ''}
            </div>
          </div>
          <div class="admin-item__actions">
            ${role.locked ? OrderLogic.renderStatusPill('Geschützt') : ''}
            ${AccessControl.can('admin.roles.manage') && !role.locked ? `<button class="btn btn-outline btn-sm" type="button" data-edit-role="${Utils.escHtml(role.id)}">
              <i class="fa-solid fa-pen-to-square"></i> Bearbeiten
            </button>` : ''}
          </div>
        </div>
      </article>`).join('');
  },

  renderPermissionGroups(selectedPermissions = []) {
    const selected = new Set(selectedPermissions);
    const container = document.getElementById('admin-role-permissions');
    container.innerHTML = RoleSecurity.permissionGroups.map(group => {
      const checkedCount = group.permissions.filter(permission => selected.has(permission.id)).length;
      const expanded = this._expandedPermissionGroups.has(group.id);
      return `<section class="permission-group" data-permission-group="${Utils.escHtml(group.id)}">
        <div class="permission-group__header">
          <label class="permission-group__master">
            <input type="checkbox" class="permission-group-master" data-group-id="${Utils.escHtml(group.id)}">
            <div>
              ${Utils.escHtml(group.label)}
              <small>${Utils.escHtml(group.description)} · ${checkedCount}/${group.permissions.length}</small>
            </div>
          </label>
          <button class="permission-group__toggle" type="button" data-toggle-permission-group="${Utils.escHtml(group.id)}">
            ${expanded ? 'Details ausblenden' : 'Details anzeigen'}
          </button>
        </div>
        <div class="permission-group__body${expanded ? '' : ' hidden'}">
          ${group.permissions.map(permission => `
            <label class="permission-item">
              <input type="checkbox"
                     class="permission-item-checkbox"
                     data-group-id="${Utils.escHtml(group.id)}"
                     value="${Utils.escHtml(permission.id)}"${selected.has(permission.id) ? ' checked' : ''}>
              <div>
                <strong>${Utils.escHtml(permission.label)}</strong>
                <span>${Utils.escHtml(permission.description)}</span>
              </div>
            </label>`).join('')}
        </div>
      </section>`;
    }).join('');
    this.syncPermissionMasterCheckboxes();
  },

  syncPermissionMasterCheckboxes() {
    document.querySelectorAll('.permission-group').forEach(groupElement => {
      const itemCheckboxes = Array.from(groupElement.querySelectorAll('.permission-item-checkbox'));
      const checkedCount = itemCheckboxes.filter(checkbox => checkbox.checked).length;
      const master = groupElement.querySelector('.permission-group-master');
      if (!master) return;
      master.checked = checkedCount === itemCheckboxes.length && itemCheckboxes.length > 0;
      master.indeterminate = checkedCount > 0 && checkedCount < itemCheckboxes.length;
    });
  },

  resetUserForm() {
    document.getElementById('admin-user-form').reset();
    document.getElementById('admin-user-original-id').value = '';
    const emailInput = document.getElementById('admin-user-email');
    emailInput.disabled = false;
    document.getElementById('admin-user-active').checked = true;
  },

  openUser(userId) {
    const user = DB.getUserById(userId);
    if (!user) return;
    document.getElementById('admin-user-original-id').value = user.id;
    document.getElementById('admin-user-name').value = user.displayName || '';
    document.getElementById('admin-user-email').value = user.email || '';
    document.getElementById('admin-user-email').disabled = true;
    document.getElementById('admin-user-role').value = user.roleId || '';
    document.getElementById('admin-user-active').checked = user.active !== false;
    State.adminTab = 'users';
    this.render();
  },

  async saveUser() {
    if (!AccessControl.can('admin.users.manage')) {
      Toast.warning('Für die Nutzerverwaltung fehlt die Berechtigung.');
      return;
    }

    const originalId = document.getElementById('admin-user-original-id').value.trim();
    const email = RoleSecurity.normalizeEmail(document.getElementById('admin-user-email').value);
    const roleId = document.getElementById('admin-user-role').value;
    const displayName = document.getElementById('admin-user-name').value.trim();

    if (!displayName) {
      Toast.error('Bitte einen Namen eingeben.');
      return;
    }
    if (!email) {
      Toast.error('Bitte eine gültige E-Mail-Adresse eingeben.');
      return;
    }
    if (!roleId) {
      Toast.error('Bitte eine Rolle auswählen.');
      return;
    }

    await DB.saveUser({
      ...(DB.getUserById(originalId || email) ?? {}),
      email,
      displayName,
      roleId,
      active: document.getElementById('admin-user-active').checked,
    });

    Toast.success(originalId ? 'Nutzer wurde aktualisiert.' : 'Nutzer wurde angelegt.');
    this.resetUserForm();
    this.render();
  },

  resetRoleForm() {
    document.getElementById('admin-role-form').reset();
    document.getElementById('admin-role-edit-id').value = '';
    this.renderPermissionGroups([]);
  },

  openRole(roleId) {
    const role = DB.getRoleById(roleId);
    if (!role) return;
    document.getElementById('admin-role-edit-id').value = role.id;
    document.getElementById('admin-role-name').value = role.name || '';
    document.getElementById('admin-role-description').value = role.description || '';
    this.renderPermissionGroups(role.permissions ?? []);
    State.adminTab = 'roles';
    this.render();
  },

  async saveRole() {
    if (!AccessControl.can('admin.roles.manage')) {
      Toast.warning('Für die Rollenverwaltung fehlt die Berechtigung.');
      return;
    }

    const roleId = document.getElementById('admin-role-edit-id').value.trim();
    const existingRole = roleId ? DB.getRoleById(roleId) : null;
    if (existingRole?.locked) {
      Toast.warning('Geschützte Systemrollen können nicht bearbeitet werden.');
      return;
    }

    const name = document.getElementById('admin-role-name').value.trim();
    const description = document.getElementById('admin-role-description').value.trim();
    const permissions = this.getSelectedPermissionIds();

    if (!name) {
      Toast.error('Bitte einen Rollennamen eingeben.');
      return;
    }
    if (!permissions.length) {
      Toast.error('Bitte mindestens eine Berechtigung auswählen.');
      return;
    }

    if (existingRole) {
      DB.updateRole(roleId, {
        ...existingRole,
        name,
        description,
        permissions,
      });
      Toast.success('Rolle wurde aktualisiert.');
    } else {
      await DB.saveRole({
        name,
        description,
        permissions,
        isSystemRole: false,
        locked: false,
      });
      Toast.success('Rolle wurde angelegt.');
    }

    this.resetRoleForm();
    this.render();
  },
};

/* ============================================================
   16. PUBLIC QR ROUTER
============================================================ */
const PublicQrRouter = {
  isPublicRoute() {
    return INITIAL_PUBLIC_QR_ROUTE;
  },

  setScreenState(title, message, linkHref = '') {
    const screen = document.getElementById('public-qr-screen');
    const titleEl = document.getElementById('public-qr-title');
    const messageEl = document.getElementById('public-qr-message');
    const linkEl = document.getElementById('public-qr-link');
    if (!screen || !titleEl || !messageEl || !linkEl) return;
    titleEl.textContent = title;
    messageEl.textContent = message;
    if (linkHref) {
      linkEl.href = linkHref;
      linkEl.classList.remove('hidden');
    } else {
      linkEl.href = '#';
      linkEl.classList.add('hidden');
    }
  },

  normalizeRedirectUrl(rawValue) {
    const value = String(rawValue ?? '').trim();
    if (!value) return '';
    try {
      const url = new URL(value);
      return /^https?:$/i.test(url.protocol) ? url.toString() : '';
    } catch (_) {
      return '';
    }
  },

  async init() {
    document.getElementById('login-screen')?.classList.add('hidden');
    document.getElementById('app')?.classList.add('hidden');
    document.getElementById('public-qr-screen')?.classList.remove('hidden');

    const token = PublicQr.getCurrentRouteToken();
    if (!token) {
      this.setScreenState(
        'Ungültiger QR-Code',
        'Dieser öffentliche QR-Code ist unvollständig oder nicht mehr gültig.'
      );
      return;
    }

    this.setScreenState(
      'Weiterleitung läuft',
      'Die passende Kleinanzeige wird geöffnet. Falls nichts passiert, nutze den Button unten.'
    );

    try {
      const snap = await firebase.firestore()
        .collection('articles')
        .where('publicQrToken', '==', token)
        .limit(1)
        .get();

      if (snap.empty) {
        this.setScreenState(
          'QR-Code nicht gefunden',
          'Zu diesem öffentlichen QR-Code wurde kein Artikel gefunden.'
        );
        return;
      }

      const article = snap.docs[0].data() ?? {};
      const redirectUrl = this.normalizeRedirectUrl(article.listingLink);
      if (!redirectUrl) {
        this.setScreenState(
          'Kein öffentlicher Link hinterlegt',
          'Für diesen Artikel ist derzeit kein öffentlicher Link hinterlegt.'
        );
        return;
      }

      this.setScreenState(
        'Weiterleitung läuft',
        'Die passende Kleinanzeige wird geöffnet. Falls nichts passiert, nutze den Button unten.',
        redirectUrl
      );
      window.location.replace(redirectUrl);
    } catch (err) {
      console.error('PublicQrRouter.init failed:', err);
      this.setScreenState(
        'Weiterleitung derzeit nicht verfügbar',
        'Die öffentliche Weiterleitung konnte im Moment nicht geladen werden. Bitte später erneut versuchen.'
      );
    }
  },
};

/* ============================================================
   16. APP â€” Initialisierung
============================================================ */
const App = {
  _syncingFromRealtime: false,
  _realtimeSyncQueued : false,

  handleRealtimeSync() {
    if (this._syncingFromRealtime) return;
    this._syncingFromRealtime = true;
    try {
      AccessControl.refreshSessionFromStore();
      AccessControl.refreshNavigation();
      AppChrome.update();
      if (State.appUser && !AccessControl.canAccessView(State.currentView)) {
        const fallbackView = AccessControl.getFirstAvailableView();
        if (fallbackView) {
          Router.navigate(fallbackView);
          return;
        }
      }
      Dashboard.renderStats();

      if (State.currentView === 'inventory') {
        Inventory.queueRender();
      } else if (State.currentView === 'orders') {
        Orders.render();
      } else if (State.currentView === 'warehouse') {
        Warehouse.render();
      } else if (State.currentView === 'sold') {
        Sold.queueRender();
      } else if (State.currentView === 'encyclopedia') {
        Encyclopedia.queueRender();
      } else if (State.currentView === 'admin') {
        AdminPanel.render();
      } else if (State.currentView === 'scanner') {
        QRScanner.refreshActiveResult();
        QRScanner._renderRelocationList();
        QRScanner._renderExternalQrContext();
      } else if (State.currentView === 'groups') {
        const detailVisible = !document.getElementById('group-detail-view').classList.contains('hidden');
        if (detailVisible && Groups._currentGroupId) {
          const group = DB.getGroupById(Groups._currentGroupId);
          if (group) {
            Groups._renderDetailMeta(group);
            Groups._renderGroupInfoCard(group);
            Groups.queueRenderGroupArticles();
          } else {
            GroupSelection.leave();
            Groups._showOverview();
            Groups.queueRender();
          }
        } else {
          Groups.queueRender();
        }
      }
    } finally {
      this._syncingFromRealtime = false;
    }
  },

  queueRealtimeSync() {
    if (this._realtimeSyncQueued) return;
    this._realtimeSyncQueued = true;
    requestAnimationFrame(() => {
      this._realtimeSyncQueued = false;
      this.handleRealtimeSync();
    });
  },

  async init() {
    await DB._ready;
    await DB.ensureSystemRoles();
    DB.ensurePublicQrTokens();
    Utils.observeVisibleTextRepair();
    Modal.init();
    Sidebar.init();
    AppChrome.init();
    Router.init();
    Dashboard.init();
    Inventory.init();
    Orders.init();
    Warehouse.init();
    Sold.init();
    Encyclopedia.init();
    AdminPanel.init();
    Tools.init();
    Groups.init();
    QRScanner.init();
    DymoManager.init();
    DB.onChange(() => this.queueRealtimeSync());

    Router.navigate(AccessControl.getFirstAvailableView() || 'dashboard');

    document.addEventListener('keydown', e => {
      if ((e.ctrlKey || e.metaKey) && e.key === 's') {
        e.preventDefault();
        if (State.currentView === 'dashboard') {
          const activeTab = document.querySelector('.form-tab.active');
          if (activeTab?.dataset.tab === 'tab-article')
            document.getElementById('btn-save-article').click();
          else if (activeTab?.dataset.tab === 'tab-group')
            document.getElementById('btn-save-group').click();
        }
      }
      if ((e.ctrlKey || e.metaKey) && e.key === 'n') {
        e.preventDefault();
        Router.navigate('dashboard');
        Dashboard.resetArticleForm();
      }
      if (e.key === 'Escape') {
        const modalVisible = !document.getElementById('modal-overlay').classList.contains('hidden');
        if (!modalVisible) {
          if (State.currentView === 'scanner' && QRScanner.hasWarehouseSession()) {
            QRScanner.returnToWarehouse(true);
          } else if (State.currentView === 'orders' && State.selectedOrderId) {
            Orders.closeDetail();
          } else if (State.currentView === 'warehouse' && State.selectedWarehouseOrderId) {
            Warehouse.closeDetail();
          } else if (State.currentView === 'sold' && State.selectedSoldOrderId) {
            Sold.closeDetail();
          } else if (GroupSelection._active) {
            GroupSelection.leave();
          } else if (InventorySelection._active) {
            InventorySelection.leave();
          } else if (State.currentView === 'dashboard') {
            const activeTab = document.querySelector('.form-tab.active');
            if (activeTab?.dataset.tab === 'tab-article' && State.editingArticleId) {
              const returnGroupId = State.articleReturnGroupId;
              Dashboard.resetArticleForm();
              if (returnGroupId) {
                Router.navigate('groups');
                setTimeout(() => Groups.openDetail(returnGroupId), 80);
              } else {
                Router.navigate('inventory');
              }
            } else if (activeTab?.dataset.tab === 'tab-group' && State.editingGroupId) {
              const groupId = State.editingGroupId;
              Dashboard.resetGroupForm();
              Router.navigate('groups');
              setTimeout(() => Groups.openDetail(groupId), 80);
            }
          } else if (State.currentView === 'groups' &&
                     !document.getElementById('group-detail-view').classList.contains('hidden')) {
            Groups._showOverview();
          }
        }
      }
      if (e.key === 'Control' && !e.repeat) {
        const modalVisible = !document.getElementById('modal-overlay').classList.contains('hidden');
        if (!modalVisible) {
          if (State.currentView === 'groups' &&
              !document.getElementById('group-detail-view').classList.contains('hidden')) {
            GroupSelection._active ? GroupSelection.leave() : GroupSelection.enter(Groups._currentGroupId);
          } else if (State.currentView === 'inventory') {
            InventorySelection.toggleMode();
          }
        }
      }
    });

    console.info(
      '%cðŸª‘ MÃ¶belWawi v1.4 bereit',
      'color:#2563eb;font-weight:bold;font-size:14px;'
    );
  },
};

document.addEventListener('DOMContentLoaded', () => {
  if (PublicQrRouter.isPublicRoute()) {
    PublicQrRouter.init();
    return;
  }

  let appInitialized = false;
  Utils.observeVisibleTextRepair();
  const googleLoginBtn = document.getElementById('google-login-btn');
  const googleLoginBtnLabel = document.getElementById('google-login-btn-label');
  const googleLoginSpinner = document.getElementById('google-login-spinner');
  const setLoginLoading = isLoading => {
    if (googleLoginBtn) {
      googleLoginBtn.disabled = isLoading;
      googleLoginBtn.classList.toggle('is-loading', isLoading);
    }
    if (googleLoginSpinner) {
      googleLoginSpinner.classList.toggle('hidden', !isLoading);
    }
    if (googleLoginBtnLabel) {
      googleLoginBtnLabel.textContent = isLoading ? 'Anmeldung läuft ...' : 'Mit Google anmelden';
    }
  };

  document.getElementById('google-login-btn').addEventListener('click', () => {
    setLoginLoading(true);
    _auth.signInWithPopup(_googleProvider).catch(e => {
      setLoginLoading(false);
      const el = document.getElementById('login-error');
      if (el) { el.textContent = 'Login fehlgeschlagen: ' + e.message; el.style.display = 'block'; }
    });
  });

  document.getElementById('logout-btn').addEventListener('click', () => {
    AccessControl.clearSession();
    _auth.signOut();
  });

  _auth.onAuthStateChanged(async user => {
    const errorEl = document.getElementById('login-error');
    if (!user) {
      setLoginLoading(false);
      AccessControl.clearSession();
      document.getElementById('login-screen').classList.remove('hidden');
      document.getElementById('app').classList.add('hidden');
      return;
    }
    if (!appInitialized) {
      appInitialized = true;
      await App.init();
    }

    const access = await AccessControl.syncAuthUser(user);
    if (!access.allowed) {
      setLoginLoading(false);
      document.getElementById('login-screen').classList.remove('hidden');
      document.getElementById('app').classList.add('hidden');
      if (errorEl) {
        errorEl.textContent = access.message;
        errorEl.style.display = 'block';
      }
      AccessControl.clearSession();
      await _auth.signOut();
      return;
    }

    if (errorEl) {
      errorEl.style.display = 'none';
      errorEl.textContent = '';
    }
    setLoginLoading(false);
    document.getElementById('login-screen').classList.add('hidden');
    document.getElementById('app').classList.remove('hidden');
    AccessControl.refreshNavigation();
    AppChrome.update();
    if (!AccessControl.canAccessView(State.currentView)) {
      const fallbackView = AccessControl.getFirstAvailableView();
      if (fallbackView) Router.navigate(fallbackView);
    } else {
      Router.navigate(State.currentView);
    }
  });
});

