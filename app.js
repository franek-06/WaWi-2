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

/* ============================================================
 1. WAWIDB â€” Datenbankschicht (Firestore)
============================================================ */
class WawiDB {
  constructor() {
    this._articles  = [];
    this._groups    = [];
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
      let pendingInitialSnapshots = 2;
      let settled = false;

      const handleSnapshot = (type, snap) => {
        if (type === 'articles') this._articles = snap.docs.map(d => d.data());
        if (type === 'groups')   this._groups   = snap.docs.map(d => d.data());

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
    const article = {
      ...data,
      id,
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
    for (let i = 0; i < quantity; i++) {
      const article = {
        ...data,
        quantity : 1,
        id       : ids[i],
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
    this._articles[idx] = { ...this._articles[idx], ...data, updatedAt: Date.now() };
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
      this._articles[existing] = { ...this._articles[existing], ...data, updatedAt: now };
      this._fsSet('articles', id, this._articles[existing]);
    } else {
      const article = { ...data, id, createdAt: now, updatedAt: now };
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
}

const DB = new WawiDB();

/* ============================================================
   2. GLOBALER STATE
============================================================ */
const State = {
  currentView      : 'dashboard',
  editingArticleId : null,
  editingGroupId   : null,
  articlePhotos    : [],
  groupImageBase64 : null,
  inventoryViewMode: 'grid',
  encSortKey       : 'updatedAt',
  encSortDir       : 'desc',
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
    if (!/[ÃÂâð�]/.test(input)) return input;
    let repaired = input
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
    const textTargets = [];
    if (root.nodeType === Node.TEXT_NODE) {
      textTargets.push(root);
    } else if (root.querySelectorAll) {
      textTargets.push(...root.querySelectorAll('*'));
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

  observeVisibleTextRepair() {
    if (this._visibleTextRepairObserver) return;
    this.repairVisibleDom(document.body);
    this._visibleTextRepairObserver = new MutationObserver(mutations => {
      mutations.forEach(mutation => {
        if (mutation.type === 'characterData') {
          this.repairVisibleDom(mutation.target);
          return;
        }
        mutation.addedNodes.forEach(node => this.repairVisibleDom(node));
        if (mutation.type === 'attributes' && mutation.target) {
          this.repairVisibleDom(mutation.target);
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
    const icons = {
      success: 'fa-circle-check',
      error  : 'fa-circle-xmark',
      warning: 'fa-triangle-exclamation',
      default: 'fa-circle-info',
    };
    t.className = `toast toast-${type}`;
    t.innerHTML = `<i class="fa-solid ${icons[type] ?? icons.default}"></i>
                   ${Utils.escHtml(message)}`;
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
    this.content.innerHTML = html;
    this.overlay.classList.remove('hidden');
    document.body.style.overflow = 'hidden';
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

  views: ['dashboard', 'groups', 'inventory', 'sold', 'encyclopedia', 'tools', 'scanner'],

  titles: {
    dashboard  : 'Dashboard &amp; Erfassung',
    groups     : 'Artikelgruppen',
    inventory  : 'Bestandsübersicht',
    sold       : 'Verkaufte Artikel',
    encyclopedia: 'Enzyklopädie',
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
      sold       : () => Sold.render(),
      encyclopedia: () => Encyclopedia.render(),
      tools      : () => {},
      scanner    : () => QRScanner.start(),
    };
    if (State.currentView !== 'scanner') QRScanner.stop();
    renderers[viewId]?.();
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
  </style>
</head>
<body>
  <div id="qr"></div>
  <p>${Utils.escHtml(label)}</p>
  ${subtitle ? `<small>${Utils.escHtml(subtitle)}</small>` : ''}
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
    window.addEventListener('load', function () {
      setTimeout(function () { window.print(); }, 250);
    });
  <\/script>
</body>
</html>`);
    w.document.close();
    w.focus();
  },

  printQR(articleId) {
    const a = DB.getArticleById(articleId);
    if (!a) return;
    this._openPrintWindow({
      title   : `QR ${a.id}`,
      label   : a.id,
      qrText  : a.id,
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

  populateGroupDropdown() {
    const sel    = document.getElementById('art-group-assign');
    const groups = DB.getGroups().filter(g => g.status !== 'Entsorgt');
    sel.innerHTML =
      `<option value="">â€“ Automatisch zuordnen â€“</option>` +
      groups.map(g => {
        const label = g.name
          ? `${g.id} Â· ${g.name.substring(0, 35)}${g.name.length > 35 ? 'â€¦' : ''}`
          : g.id;
        return `<option value="${g.id}">${Utils.escHtml(label)}</option>`;
      }).join('');
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

    document.getElementById('art-photos')
      .addEventListener('change', e => this.handlePhotoUpload(e.target.files));

    document.getElementById('article-form')
      .addEventListener('submit', e => { e.preventDefault(); this.saveArticle(); });

    document.getElementById('btn-reset-article')
      .addEventListener('click', () => this.resetArticleForm());

    document.getElementById('btn-print-qr')
      .addEventListener('click', () => {
        if (State.editingArticleId) QRManager.printQR(State.editingArticleId);
      });
    const cancelArtBtn = document.getElementById('btn-cancel-article');
    if (cancelArtBtn) {
      cancelArtBtn.addEventListener('click', () => {
        const articleId = State.editingArticleId;
        this.resetArticleForm();
        if (articleId) Router.navigate('inventory');
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

  async saveArticle() {
    if (!this.validateArticle()) return;
    try {
    const condEl = document.querySelector('input[name="art-condition"]:checked');
    const editId = State.editingArticleId;
    const qty    = parseInt(document.getElementById('art-quantity').value) || 1;
    const data = {
      status        : document.getElementById('art-status').value,
      category      : document.getElementById('art-category').value,
      manufacturer  : document.getElementById('art-manufacturer').value.trim(),
      model         : document.getElementById('art-model').value.trim(),
      location      : document.getElementById('art-location').value.trim(),
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
      listingLink   : document.getElementById('art-listing-link').value.trim(),
      pickupZip     : document.getElementById('art-pickup-zip').value.trim(),
      shipping      : document.getElementById('art-shipping').checked,
      shippingCost  : parseFloat(document.getElementById('art-shipping-cost').value)  || null,
      photos        : [...State.articlePhotos],
      notes         : document.getElementById('art-notes').value.trim(),
      soldPrice     : parseFloat(document.getElementById('art-sold-price').value)          || null,
      soldPriceGross: parseFloat(document.getElementById('art-sold-price-gross').value)    || null,
      soldDate      : document.getElementById('art-sold-date').value                  || null,
      groupId       : document.getElementById('art-group-assign').value
                      || document.getElementById('article-edit-group-id').value
                      || null,
    };

    if (editId) {
      const dataForOriginal = { ...data, quantity: 1 };
      const saved = DB.updateArticle(editId, dataForOriginal);
      if (!saved) return;

      let dupIds = [];
      if (qty > 1) {
        const dupData = { ...dataForOriginal, groupId: saved.groupId };
        const dups = await DB.saveBulkArticles(dupData, qty - 1);
        dupIds = dups.map(d => d.id);
      }

      const allIds = [editId, ...dupIds];
      if (!saved.groupId) {
        const group = await DB.autoAssignGroup(allIds, saved);
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
      document.getElementById('art-id-display').value         = saved.id;
      document.getElementById('art-qr-section').style.display = 'block';
      State.editingArticleId = saved.id;
      QRManager.generate('art-qr-preview', saved.id, 128);
      this.renderStats();
      const savedId = saved.id;
      setTimeout(() => {
        Router.navigate('inventory');
        setTimeout(() => {
          const card = document.querySelector('[data-id="' + CSS.escape(savedId) + '"]');
          if (card) {
            card.scrollIntoView({ behavior: 'smooth', block: 'center' });
            card.classList.add('highlight-new');
            setTimeout(() => card.classList.remove('highlight-new'), 1800);
          }
        }, 200);
      }, 400);
      return;
    }

    const savedArticles = qty > 1
      ? await DB.saveBulkArticles(data, qty)
      : [await DB.saveArticle({ ...data, quantity: 1 })];
    const articleIds = savedArticles.map(a => a.id);
    const group      = await DB.autoAssignGroup(articleIds, data);
    const firstId    = articleIds[0];
    document.getElementById('art-id-display').value         = firstId;
    document.getElementById('art-qr-section').style.display = 'block';
    State.editingArticleId = firstId;
    QRManager.generate('art-qr-preview', firstId, 128);
    document.getElementById('qty-hint-banner')?.remove();
    if (qty > 1) {
      Toast.success(qty + ' Artikel angelegt (' + articleIds[0] + '-' + articleIds[articleIds.length - 1] + ') Â· Gruppe "' + Utils.escHtml(group.name) + '".');
    } else {
      Toast.success('Artikel ' + firstId + ' gespeichert Â· Gruppe "' + Utils.escHtml(group.name) + '".');
    }
    this.renderStats();
    } catch (err) {
      console.error('saveArticle failed:', err);
      Toast.error('Speichern fehlgeschlagen. Bitte erneut versuchen.');
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
    document.getElementById('article-edit-group-id').value  = '';
    document.querySelectorAll('.condition-btn')
      .forEach(l => l.classList.remove('selected'));
    document.getElementById('qty-hint-banner')?.remove();
    State.editingArticleId = null;
    State.articlePhotos    = [];
  },

  loadArticleIntoForm(id) {
    const a = DB.getArticleById(id);
    if (!a) return;

    this.resetArticleForm();
    this.populateArticleCategoryDropdown(a.category);
    State.editingArticleId = id;

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
    QRManager.generate('art-qr-preview', a.id, 128);

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
    document.getElementById('sold-period-preset')
      .addEventListener('change', () => {
        const val     = document.getElementById('sold-period-preset').value;
        const wrapper = document.getElementById('sold-date-range-wrapper');
        if (val === 'custom') {
          wrapper.style.display = 'flex';
        } else {
          wrapper.style.display = 'none';
          const { from, to } = this._presetRange(val);
          document.getElementById('sold-date-from').value = from;
          document.getElementById('sold-date-to').value   = to;
        }
        this.queueRender();
      });
    document.getElementById('sold-date-from')
      .addEventListener('change', () => this.queueRender());
    document.getElementById('sold-date-to')
      .addEventListener('change', () => this.queueRender());
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
    const now   = new Date();
    const pad   = n => String(n).padStart(2, '0');
    const fmt   = d => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
    const today = fmt(now);
    if (preset === 'week') {
      const day = now.getDay() || 7;
      const mon = new Date(now);
      mon.setDate(now.getDate() - day + 1);
      return { from: fmt(mon), to: today };
    }
    if (preset === 'month')   return { from: `${now.getFullYear()}-${pad(now.getMonth() + 1)}-01`, to: today };
    if (preset === 'quarter') {
      const q    = Math.floor(now.getMonth() / 3);
      const from = new Date(now.getFullYear(), q * 3, 1);
      return { from: fmt(from), to: today };
    }
    if (preset === 'year') return { from: `${now.getFullYear()}-01-01`, to: today };
    return { from: '', to: '' };
  },

  render() {
    const container = document.getElementById('sold-container');
    const search    = document.getElementById('sold-search').value.trim();
    const dateFrom  = document.getElementById('sold-date-from').value;
    const dateTo    = document.getElementById('sold-date-to').value;

    let articles = DB.getArticles().filter(a => a.status === 'Verkauft');
    if (search)   articles = articles.filter(a => Utils.articleMatchesSearch(a, search));
    if (dateFrom) articles = articles.filter(a => a.soldDate && a.soldDate >= dateFrom);
    if (dateTo)   articles = articles.filter(a => a.soldDate && a.soldDate <= dateTo);
    articles.sort((a, b) => b.updatedAt - a.updatedAt);

    const totalRevenue  = articles.reduce((s, a) => s + (parseFloat(a.soldPrice)     || 0), 0);
    const totalPurchase = articles.reduce((s, a) => s + (parseFloat(a.purchasePrice) || 0), 0);
    const totalProfit   = totalRevenue - totalPurchase;

    document.getElementById('sum-revenue').textContent  = Utils.formatEuro(totalRevenue);
    document.getElementById('sum-purchase').textContent = Utils.formatEuro(totalPurchase);
    const profitEl       = document.getElementById('sum-profit');
    profitEl.textContent = Utils.formatEuro(totalProfit);
    profitEl.className   = `profit-value ${totalProfit >= 0 ? 'profit-positive' : 'profit-negative'}`;

    if (!articles.length) {
      container.innerHTML = `
        <div class="empty-state">
          <i class="fa-solid fa-handshake"></i>
          <p>Noch keine verkauften Artikel.</p>
        </div>`;
      return;
    }

    container.className = 'cards-grid';
    container.innerHTML = articles.map(a => {
      const img = a.photos?.[0]
        ? `<img src="${a.photos[0]}" alt="${Utils.escHtml(a.model || a.category)}" loading="lazy"/>`
        : `<div class="card-image-placeholder"><i class="fa-solid fa-couch"></i></div>`;
      const sold     = parseFloat(a.soldPrice)     || 0;
      const purchase = parseFloat(a.purchasePrice) || 0;
      const profit   = sold - purchase;
      const cls      = profit >= 0 ? 'profit-positive' : 'profit-negative';
      const trendIcon = profit >= 0 ? 'fa-arrow-trend-up' : 'fa-arrow-trend-down';
      const soldDate  = a.soldDate
        ? new Date(a.soldDate).toLocaleDateString('de-DE')
        : 'â€“';
      const groupBadge = a.groupId
        ? (() => {
            const g     = DB.getGroupById(a.groupId);
            const label = g?.name
              ? `${Utils.escHtml(a.groupId)} Â· ${Utils.escHtml(g.name.substring(0, 22))}${g.name.length > 22 ? 'â€¦' : ''}`
              : Utils.escHtml(a.groupId);
            return `<div style="margin-top:6px;">
                      <span style="font-size:var(--font-size-xs);color:var(--color-primary);font-weight:600;">
                        <i class="fa-solid fa-layer-group"></i> ${label}
                      </span>
                    </div>`;
          })()
        : '';
      return `
        <article class="article-card">
          <div class="card-image">${img}<div class="card-badges">${Utils.statusBadge(a.status)}</div></div>
          <div class="card-body">
            <span class="card-id">${Utils.escHtml(a.id)}</span>
        <div class="card-title">${Utils.escHtml(Utils.articleDisplayName(a))}</div>
            <div class="card-meta">${Utils.condBadge(a.condition)}</div>
            ${groupBadge}
            <div style="margin-top:12px;padding:12px;background:var(--color-bg);border-radius:var(--border-radius-sm);">
              <div style="display:flex;justify-content:space-between;font-size:var(--font-size-sm);margin-bottom:4px;">
                <span style="color:var(--color-muted);">Einkaufspreis</span>
                <span>${purchase ? Utils.formatEuro(purchase) : 'â€“'}</span>
              </div>
              <div style="display:flex;justify-content:space-between;font-size:var(--font-size-sm);margin-bottom:4px;">
                <span style="color:var(--color-muted);">Verkaufspreis</span>
                <span style="font-weight:600;">${sold ? Utils.formatEuro(sold) : 'â€“'}</span>
              </div>
              <div style="display:flex;justify-content:space-between;font-size:var(--font-size-sm);border-top:1px solid var(--color-border);padding-top:6px;">
                <span style="color:var(--color-muted);">Gewinn / Verlust</span>
                <span class="${cls}"><i class="fa-solid ${trendIcon}"></i> ${Utils.formatEuro(profit)}</span>
              </div>
            </div>
            <div style="font-size:var(--font-size-xs);color:var(--color-muted);margin-top:8px;">
              <i class="fa-solid fa-calendar-check"></i> Verkauft am ${soldDate}
            </div>
          </div>
          <div class="card-footer">
            <button class="btn btn-ghost btn-sm" data-action="edit" data-id="${a.id}">
              <i class="fa-solid fa-pen-to-square"></i> Bearbeiten
            </button>
          </div>
        </article>`;
    }).join('');

    container.querySelectorAll('[data-action]').forEach(btn => {
      btn.addEventListener('click', () => {
        if (btn.dataset.action === 'edit')
          Dashboard.loadArticleIntoForm(btn.dataset.id);
      });
    });
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
    this._currentGroupId = null;
    document.getElementById('groups-overview').classList.remove('hidden');
    document.getElementById('group-detail-view').classList.add('hidden');
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
      </div>` : ''}`;
    document.getElementById('detail-price-history-btn').addEventListener('click', () => Dashboard.showPriceHistoryModal(group));
    document.getElementById('detail-edit-group-btn').addEventListener('click', () => this._editGroup(group.id));
    document.getElementById('detail-delete-group-btn').addEventListener('click', () => this._confirmDeleteGroup(group.id));
    const applyImgBtn = document.getElementById('detail-apply-group-image-btn');
    if (applyImgBtn) { applyImgBtn.addEventListener('click', () => this._applyGroupImageToArticles(group.id)); }
  },

  _renderGroupArticles(groupId) {
    const container   = document.getElementById('group-articles-container');
    const toolbar     = document.getElementById('group-articles-toolbar');
    const allArticles = DB.getArticlesByGroup(groupId);
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
      const thumb = (a.photos && a.photos[0])
        ? '<div class="group-article-thumb"><img src="' + a.photos[0] + '" alt="Foto" loading="lazy"/></div>'
        : '<div class="group-article-thumb"><i class="fa-solid fa-couch"></i></div>';
    const name = Utils.escHtml(Utils.articleDisplayName(a, '-'));
      const dims = [a.width, a.depth, a.height].filter(Boolean).map(v => v + 'cm').join(' x ');
      let meta = Utils.statusBadge(a.status) + Utils.condBadge(a.condition);
      if (a.category) meta += '<span style="font-size:var(--font-size-xs);color:var(--color-muted);">' + Utils.escHtml(a.category) + '</span>';
      if (dims)       meta += '<span style="font-size:var(--font-size-xs);color:var(--color-muted);"><i class="fa-solid fa-ruler-combined"></i> ' + dims + '</span>';
      if (a.location) meta += '<span style="font-size:var(--font-size-xs);color:var(--color-muted);"><i class="fa-solid fa-location-dot"></i> ' + Utils.escHtml(a.location) + '</span>';
      if (a.color)    meta += '<span style="font-size:var(--font-size-xs);color:var(--color-muted);"><i class="fa-solid fa-palette"></i> ' + Utils.escHtml(a.color) + '</span>';
      let price = '';
      if (a.purchasePrice) price += '<span style="font-size:var(--font-size-sm);font-weight:600;color:var(--color-primary);">EK: ' + Utils.formatEuro(a.purchasePrice) + '</span>';
      if (a.soldPrice)     price += '<span style="font-size:var(--font-size-sm);font-weight:600;color:var(--color-success);">VK: ' + Utils.formatEuro(a.soldPrice) + '</span>';
      html += '<div class="group-article-card" data-article-id="' + Utils.escHtml(a.id) + '">'
        + '<div class="article-select-checkbox-wrap"><input type="checkbox" class="article-select-cb" data-article-id="' + Utils.escHtml(a.id) + '"/></div>'
        + thumb + '<div class="group-article-info">'
        + '<span class="group-article-id">' + Utils.escHtml(a.id) + '</span>'
        + '<span class="group-article-name">' + name + '</span>'
        + '<div class="group-article-meta">' + meta + '</div>'
        + '<div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:4px;">' + price + '</div>'
        + '</div><div class="group-article-actions">'
        + '<button class="btn btn-outline btn-sm" data-action="edit-article" data-id="' + a.id + '"><i class="fa-solid fa-pen-to-square"></i> Bearbeiten</button>'
        + '<button class="btn btn-ghost btn-sm" data-action="qr-article" data-id="' + a.id + '"><i class="fa-solid fa-qrcode"></i> QR</button>'
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
    'Versand', 'Versandkosten_EUR', 'Abhol_PLZ', 'Inserat_Link',
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
      a.pickupZip, a.listingLink, a.groupId || '',
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
    const article = DB.getArticleById(rawValue);
    if (!article) {
      Toast.error(`Artikel â€ž${Utils.escHtml(rawValue)}" nicht gefunden.`);
      input.value = ''; input.focus();
      return;
    }
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
    this.setMode('single');
    this._renderRelocationList();
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
      this._mode === 'single' && this._activeResultId ? 'inline-flex' : 'none';

    if (this._mode === 'single') {
      this._setRelocationAwaitingLocationScan(false);
      this.refreshActiveResult();
    } else {
      this._setResult(null);
      this._renderRelocationList();
      this._setRelocationAwaitingLocationScan(false);
      this._setBadge(this._running ? 'scanning' : 'idle');
    }
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
      const backIdx  = devices.findIndex(d => /back|rear|environment/i.test(d.label));
      this._camIndex = backIdx >= 0 ? backIdx : 0;
      const switchBtn = document.getElementById('scanner-switch-cam');
      switchBtn.style.display = devices.length > 1 ? 'inline-flex' : 'none';
      await this._startWithCamera(devices[this._camIndex].id);
    } catch (err) {
      Toast.error('Kamera konnte nicht geÃ¶ffnet werden: ' + err);
      this._setBadge('idle');
    }
  },

  async _startWithCamera(cameraId) {
    if (this._running) {
      try { await this._scanner.stop(); } catch (_) {}
      this._running = false;
    }
    await this._scanner.start(
      cameraId,
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
    this._setBadge('idle');
    document.getElementById('scanner-rescan').style.display = 'none';
    if (this._mode === 'single') this._setResult(null);
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

    if (this._scanner && this._running) {
      try { await this._scanner.pause(true); } catch (_) {}
    }
    this._setBadge('found');
    const article = DB.getArticleById(value);
    if (!article) {
      Toast.error('Artikel â€ž' + Utils.escHtml(value) + '" nicht gefunden.');
      try { this._scanner.resume(); } catch (_) {}
      this._setBadge('scanning');
      return;
    }
    document.getElementById('scanner-rescan').style.display = 'inline-flex';
    this._setResult(article);
  },

  async _handleRelocationScan(value) {
    if (this._scanner && this._running) {
      try { await this._scanner.pause(true); } catch (_) {}
    }

    const location = QRManager.parseLocationCode(value);
    if (location) {
      if (!this._relocateAwaitingLocationScan) {
        Toast.warning('Bitte zuerst auf â€žJetzt Standort scannenâ€œ klicken.');
      } else {
        document.getElementById('scanner-relocate-location').value = location;
        this._setRelocationAwaitingLocationScan(false);
        Toast.success(`Ziel-Standort erkannt: ${location}`);
      }
    } else {
      const article = DB.getArticleById(value);
      if (!article) {
        Toast.error('Code â€ž' + Utils.escHtml(value) + '" ist weder ein Artikel noch ein Standort-QR.');
      } else if (this._relocateAwaitingLocationScan) {
        Toast.warning('Standort-Scan ist aktiv. Bitte jetzt den Standort scannen.');
      } else if (!this._relocateArticleIds.includes(article.id)) {
        this._relocateArticleIds.push(article.id);
        this._renderRelocationList();
      } else {
        Toast.warning(`${article.id} wurde bereits gescannt.`);
      }
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
      Dashboard.renderStats();

      if (State.currentView === 'inventory') {
        Inventory.queueRender();
      } else if (State.currentView === 'sold') {
        Sold.queueRender();
      } else if (State.currentView === 'encyclopedia') {
        Encyclopedia.queueRender();
      } else if (State.currentView === 'scanner') {
        QRScanner.refreshActiveResult();
        QRScanner._renderRelocationList();
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
    Utils.observeVisibleTextRepair();
    Modal.init();
    Sidebar.init();
    Router.init();
    Dashboard.init();
    Inventory.init();
    Sold.init();
    Encyclopedia.init();
    Tools.init();
    Groups.init();
    QRScanner.init();
    DB.onChange(() => this.queueRealtimeSync());

    Router.navigate('dashboard');

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
          if (GroupSelection._active) {
            GroupSelection.leave();
          } else if (InventorySelection._active) {
            InventorySelection.leave();
          } else if (State.currentView === 'dashboard') {
            const activeTab = document.querySelector('.form-tab.active');
            if (activeTab?.dataset.tab === 'tab-article' && State.editingArticleId) {
              Dashboard.resetArticleForm();
              Router.navigate('inventory');
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
  let appInitialized = false;
  Utils.observeVisibleTextRepair();

  document.getElementById('google-login-btn').addEventListener('click', () => {
    _auth.signInWithPopup(_googleProvider).catch(e => {
      const el = document.getElementById('login-error');
      if (el) { el.textContent = 'Login fehlgeschlagen: ' + e.message; el.style.display = 'block'; }
    });
  });

  document.getElementById('logout-btn').addEventListener('click', () => {
    _auth.signOut();
  });

  _auth.onAuthStateChanged(async user => {
    if (!user) {
      document.getElementById('login-screen').classList.remove('hidden');
      document.getElementById('app').classList.add('hidden');
      return;
    }
    document.getElementById('login-screen').classList.add('hidden');
    document.getElementById('app').classList.remove('hidden');
    if (!appInitialized) {
  appInitialized = true;
  await App.init();
}
  });
});

