// Firebase backend for Campus by Unrest multiplayer. Load with <script type="module" src="campus-firebase.js">.
// Resolves window.__fbReady with { uid, staff, db, tr, ... } (or null if Firebase can't start; the game then falls back).
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.14.1/firebase-app.js";
import { getAuth, onAuthStateChanged, signInAnonymously, signInWithPopup, GoogleAuthProvider, signOut } from "https://www.gstatic.com/firebasejs/10.14.1/firebase-auth.js";
import { getFirestore, doc, collection, getDoc, setDoc, deleteDoc, onSnapshot } from "https://www.gstatic.com/firebasejs/10.14.1/firebase-firestore.js";
import { getDatabase, ref, set as rset, remove as rremove, onChildAdded, onChildChanged, onChildRemoved, onValue, onDisconnect } from "https://www.gstatic.com/firebasejs/10.14.1/firebase-database.js";

const ADMIN_EMAIL = "manaspandeya@gmail.com";
const cfg = {
  apiKey: "AIzaSyDBJ0hHjVyEnwg7mV7ECeG163K7fs1fsiE", authDomain: "du-verse-e75db.firebaseapp.com",
  databaseURL: "https://du-verse-e75db-default-rtdb.asia-southeast1.firebasedatabase.app", projectId: "du-verse-e75db",
  storageBucket: "du-verse-e75db.firebasestorage.app", messagingSenderId: "84735746102", appId: "1:84735746102:web:6fe5353a0586db71323897"
};

try {
  const app = initializeApp(cfg), auth = getAuth(app), fs = getFirestore(app), rtdb = getDatabase(app);
  const user = await new Promise(res => {
    let asked = false;
    onAuthStateChanged(auth, u => { if (u) res(u); else if (!asked) { asked = true; signInAnonymously(auth).catch(() => res(null)); } });
  });
  if (!user) throw new Error("no auth");
  const uid = user.uid, staff = user.email === ADMIN_EMAIL && user.emailVerified === true;

  /* ---- db adapter: the game's MP.db API on top of Firestore + RTDB ----
     verify/{uid}  -> Firestore campus_verify   (ID-card requests, staff review)
     status/{uid}  -> RTDB campus/ok/{uid} (the approval the rules enforce) + Firestore campus_status (rejection reasons)
     reports/{uid} -> Firestore campus_reports */
  const fsPath = p => { const [a, ...r] = p.split("/"); return ["campus_" + a, ...r]; };
  const snap = s => ({ exists: s.exists(), data: () => s.data() });
  const db = {
    doc: p => {
      const [c, id] = fsPath(p), r = doc(fs, c, id);
      return {
        set: async d => {
          if (c === "campus_status") {
            if (d.s === "approved") { await rset(ref(rtdb, "campus/ok/" + id), { m: d.method === "un" ? "un" : "du" }); return; }
            await rremove(ref(rtdb, "campus/ok/" + id));
          }
          return setDoc(r, d);
        },
        delete: () => deleteDoc(r),
        onSnapshot: (cb, er) => {
          if (c !== "campus_status") return onSnapshot(r, s => cb(snap(s)), er);
          let ok, st; const emit = () => {
            if (ok === undefined || st === undefined) return;               // wait for both first answers
            if (ok.exists()) cb({ exists: true, data: () => ({ s: "approved", method: ok.val().m }) });
            else if (st.exists()) cb(snap(st)); else cb({ exists: false, data: () => ({}) });
          };
          const u1 = onValue(ref(rtdb, "campus/ok/" + id), s => { ok = s; emit(); }, er);
          const u2 = onSnapshot(r, s => { st = s; emit(); }, er);
          return () => { u1(); u2(); };
        }
      };
    },
    collection: p => { const [c] = fsPath(p); return { onSnapshot: (cb, er) => onSnapshot(collection(fs, c), q => cb({ docs: q.docs.map(d => ({ id: d.id, data: () => d.data() })) }), er) }; }
  };

  /* ---- realtime transport (RTDB) ----
     campus/pr/{uid}        profile + queue entry: name, look, ok, jt   (everyone reads: this is the queue)
     campus/p/{scene}/{uid} position only, written while admitted; clients listen to their own scene only */
  function makeTr() {
    const prof = new Map(), dyn = new Map();
    let cb = null, qcb = null, sc = null, un = [], timer = 0, minePr = "", myDyn = null;
    const flush = () => {
      timer = 0;
      if (qcb) qcb([...prof].map(([id, p]) => ({ id, jt: p.jt })));
      if (cb) cb(sc ? [...dyn].filter(([id]) => id !== uid && prof.has(id)).map(([id, d]) => { const p = prof.get(id);
        return { id, s: { v: 1, u: id, n: p.n, lk: Object.values(p.lk || {}), x: d.x, y: d.y, f: d.f, m: d.m, sc, jt: p.jt, w: 1, ok: p.ok, sy: d.sy || null, em: d.em || null } }; }) : []);
    };
    const kick = () => { if (!timer) timer = setTimeout(flush, 90); };
    const watch = (path, map) => {
      const r = ref(rtdb, path), f = s => { map.set(s.key, s.val()); kick(); };
      const a = [onChildAdded(r, f), onChildChanged(r, f), onChildRemoved(r, s => { map.delete(s.key); kick(); })];
      return () => a.forEach(x => x());
    };
    const clear = () => {
      un.forEach(x => x()); un = []; dyn.clear(); sc = null; minePr = "";
      rremove(ref(rtdb, "campus/pr/" + uid)).catch(() => {});
      if (myDyn) rremove(ref(rtdb, myDyn)).catch(() => {}); myDyn = null;
    };
    return {
      kind: "fb",
      on: (f, q) => { cb = f; qcb = q; watch("campus/pr", prof); },
      set: (s, live) => {
        if (!s) { clear(); return; }
        const want = live ? s.sc : null;
        if (want !== sc) {
          un.forEach(x => x()); un = []; dyn.clear();
          if (myDyn) { rremove(ref(rtdb, myDyn)).catch(() => {}); myDyn = null; }
          sc = want; if (sc) un.push(watch("campus/p/" + sc, dyn)); kick();
        }
        const key = JSON.stringify([s.n, s.lk, s.ok, s.jt]);
        if (key !== minePr) { minePr = key; const r = ref(rtdb, "campus/pr/" + uid); rset(r, { n: s.n, lk: s.lk, ok: s.ok || "du", jt: s.jt }).catch(() => {}); onDisconnect(r).remove(); }
        if (live) {
          const path = "campus/p/" + s.sc + "/" + uid, r = ref(rtdb, path);
          rset(r, { x: s.x, y: s.y, f: s.f, m: s.m, sy: s.sy || null, em: s.em || null }).catch(() => {});
          if (myDyn !== path) { myDyn = path; onDisconnect(r).remove(); }
        }
      },
      leave: clear
    };
  }

  window.__fbResolve({
    uid, staff, db, tr: makeTr(),
    // Soft gate: confirms the username exists in username_slots. It does not prove the player owns it.
    checkUsername: async h => (await getDoc(doc(fs, "username_slots", h.toLowerCase()))).exists(),
    approveUsername: h => rset(ref(rtdb, "campus/ok/" + uid), { m: "un", h: h.toLowerCase() }),
    staffSignIn: () => signInWithPopup(auth, new GoogleAuthProvider()).then(r => { if (r.user.email !== ADMIN_EMAIL) return signOut(auth); }).then(() => location.reload()).catch(() => {}),
    staffSignOut: () => signOut(auth).then(() => location.reload())
  });
} catch (e) { console.warn("Campus multiplayer: Firebase unavailable", e); window.__fbResolve(null); }
