
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.14.1/firebase-app.js";

import {
  getAuth,
  onAuthStateChanged,
  signInAnonymously,
  signInWithEmailAndPassword,
  signOut
} from "https://www.gstatic.com/firebasejs/10.14.1/firebase-auth.js";

import {
  getFirestore,
  doc,
  collection,
  getDoc,
  setDoc,
  deleteDoc,
  onSnapshot
} from "https://www.gstatic.com/firebasejs/10.14.1/firebase-firestore.js";

import {
  getDatabase,
  ref,
  set,
  remove,
  onValue,
  onChildAdded,
  onChildChanged,
  onChildRemoved,
  onDisconnect
} from "https://www.gstatic.com/firebasejs/10.14.1/firebase-database.js";

const ADMIN_EMAIL = "manaspandeya@gmail.com";

const firebaseConfig = {
  apiKey: "AIzaSyDBJ0hHjVyEnwg7mV7ECeG163K7fs1fsiE",
  authDomain: "du-verse-e75db.firebaseapp.com",
  databaseURL:
    "https://du-verse-e75db-default-rtdb.asia-southeast1.firebasedatabase.app",
  projectId: "du-verse-e75db",
  storageBucket: "du-verse-e75db.firebasestorage.app",
  messagingSenderId: "84735746102",
  appId: "1:84735746102:web:6fe5353a0586db71323897"
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const firestore = getFirestore(app);
const realtimeDB = getDatabase(app);

const rtRef = path => ref(realtimeDB, path);

function reportError(error) {
  console.error("[Campus Firebase]", error);

  window.dispatchEvent(
    new CustomEvent("campus-network-error", {
      detail: error?.code || error?.message || "Firebase operation failed"
    })
  );
}

function watchRealtime(path, callback, errorCallback = reportError) {
  return onValue(rtRef(path), callback, errorCallback);
}

function watchChildren(path, map, refresh) {
  const r = rtRef(path);

  const addOrChange = snapshot => {
    map.set(snapshot.key, snapshot.val());
    refresh();
  };

  const unsubscribers = [
    onChildAdded(r, addOrChange, reportError),
    onChildChanged(r, addOrChange, reportError),
    onChildRemoved(
      r,
      snapshot => {
        map.delete(snapshot.key);
        refresh();
      },
      reportError
    )
  ];

  return () => unsubscribers.forEach(unsubscribe => unsubscribe());
}

function firestorePath(path) {
  const [collectionName, ...rest] = path.split("/");
  return ["campus_" + collectionName, ...rest];
}

function createDatabaseAdapter(uid, staff) {
  const document = path => {
    const [collectionName, documentId] = firestorePath(path);
    const documentRef = doc(firestore, collectionName, documentId);

    return {
      set: async data => {
        if (collectionName === "campus_status") {
          if (data.s === "approved") {
            await set(rtRef("campus/ok/" + documentId), {
              m: data.method || "un",
              h: String(data.handle || "").toLowerCase()
            });
            return;
          }

          await remove(rtRef("campus/ok/" + documentId));
        }

        return setDoc(documentRef, data);
      },

      delete: () => deleteDoc(documentRef),

      onSnapshot: (callback, errorCallback) => {
        if (collectionName !== "campus_status") {
          return onSnapshot(
            documentRef,
            snapshot =>
              callback({
                exists: () => snapshot.exists(),
                data: () => snapshot.data()
              }),
            errorCallback || reportError
          );
        }

        let approval;
        let status;
        let approvalLoaded = false;
        let statusLoaded = false;

        const emit = () => {
          if (!approvalLoaded || !statusLoaded) return;

          if (approval?.exists()) {
            const value = approval.val();

            callback({
              exists: () => true,
              data: () => ({
                s: "approved",
                method: value.m,
                handle: value.h || ""
              })
            });
          } else if (status?.exists()) {
            callback({
              exists: () => true,
              data: () => status.data()
            });
          } else {
            callback({
              exists: () => false,
              data: () => ({})
            });
          }
        };

        const stopApproval = onValue(
          rtRef("campus/ok/" + documentId),
          snapshot => {
            approval = snapshot;
            approvalLoaded = true;
            emit();
          },
          errorCallback || reportError
        );

        const stopStatus = onSnapshot(
          documentRef,
          snapshot => {
            status = {
              exists: () => snapshot.exists(),
              data: () => snapshot.data()
            };
            statusLoaded = true;
            emit();
          },
          errorCallback || reportError
        );

        return () => {
          stopApproval();
          stopStatus();
        };
      }
    };
  };

  return {
    doc: document,

    collection: path => {
      const [collectionName] = firestorePath(path);

      return {
        onSnapshot: (callback, errorCallback) =>
          onSnapshot(
            collection(firestore, collectionName),
            snapshot =>
              callback({
                docs: snapshot.docs.map(item => ({
                  id: item.id,
                  data: () => item.data()
                }))
              }),
            errorCallback || reportError
          )
      };
    }
  };
}

function createMultiplayerTransport(uid) {
  const profiles = new Map();
  const positions = new Map();

  let playerCallback = null;
  let queueCallback = null;
  let currentScene = null;
  let stopProfiles = null;
  let stopPositions = null;
  let refreshTimer = null;
  let lastProfile = "";
  let currentPositionPath = null;

  function refresh() {
    if (refreshTimer) return;

    refreshTimer = setTimeout(() => {
      refreshTimer = null;

      if (queueCallback) {
        queueCallback(
          [...profiles].map(([id, profile]) => ({
            id,
            jt: profile.jt,
            n: profile.n,
            ok: profile.ok
          }))
        );
      }

      if (playerCallback) {
        const players = [];

        for (const [id, position] of positions) {
          if (id === uid) continue;

          const profile = profiles.get(id);
          if (!profile || !position) continue;

          players.push({
            id,
            s: {
              v: 1,
              u: id,
              n: profile.n || "Student",
              lk: Object.values(profile.lk || {}),
              x: position.x,
              y: position.y,
              f: position.f,
              m: position.m,
              sc: currentScene,
              jt: profile.jt,
              w: 1,
              ok: profile.ok,
              sy: position.sy || null,
              em: position.em || null
            }
          });
        }

        playerCallback(currentScene ? players : []);
      }
    }, 100);
  }

  function stopSceneListeners() {
    if (stopPositions) stopPositions();
    stopPositions = null;
    positions.clear();
    currentScene = null;
  }

  async function leave() {
    if (stopProfiles) stopProfiles();
    stopProfiles = null;

    stopSceneListeners();

    try {
      await remove(rtRef("campus/pr/" + uid));

      if (currentPositionPath) {
        await remove(rtRef(currentPositionPath));
      }
    } catch (error) {
      reportError(error);
    }

    currentPositionPath = null;
    lastProfile = "";
  }

  return {
    kind: "fb",

    on: (onPlayers, onQueue) => {
      playerCallback = onPlayers;
      queueCallback = onQueue;

      if (stopProfiles) stopProfiles();

      stopProfiles = watchChildren(
        "campus/pr",
        profiles,
        refresh
      );
    },

    set: (state, isLive) => {
      if (!state) {
        void leave();
        return;
      }

      const desiredScene = isLive ? state.sc : null;

      if (desiredScene !== currentScene) {
        stopSceneListeners();

        if (currentPositionPath) {
          remove(rtRef(currentPositionPath)).catch(reportError);
          currentPositionPath = null;
        }

        currentScene = desiredScene;

        if (currentScene) {
          stopPositions = watchChildren(
            "campus/p/" + currentScene,
            positions,
            refresh
          );
        }
      }

      const profileSignature = JSON.stringify([
        state.n,
        state.lk,
        state.ok,
        state.jt
      ]);

      if (profileSignature !== lastProfile) {
        lastProfile = profileSignature;

        const profileRef = rtRef("campus/pr/" + uid);

        set(profileRef, {
          n: state.n || "Student",
          lk: state.lk || {},
          ok: state.ok || "un",
          jt: state.jt || Date.now()
        }).catch(reportError);

        onDisconnect(profileRef).remove().catch(reportError);
      }

      if (isLive && state.sc) {
        const path = "campus/p/" + state.sc + "/" + uid;
        const positionRef = rtRef(path);

        set(positionRef, {
          x: state.x,
          y: state.y,
          f: state.f,
          m: state.m,
          sy: state.sy || null,
          em: state.em || null
        }).catch(reportError);

        if (currentPositionPath !== path) {
          currentPositionPath = path;
          onDisconnect(positionRef).remove().catch(reportError);
        }
      }
    },

    leave
  };
}

async function verifyUsername(uid, username) {
  const handle = String(username || "")
    .trim()
    .replace(/^@/, "")
    .toLowerCase();

  if (!handle) return false;

  const profileSnapshot = await getDoc(doc(firestore, "users", uid));

  if (!profileSnapshot.exists()) return false;

  const profile = profileSnapshot.data();
  const profileUsername = String(profile.username || "")
    .trim()
    .replace(/^@/, "")
    .toLowerCase();

  if (profileUsername !== handle) return false;

  const slotSnapshot = await getDoc(
    doc(firestore, "username_slots", handle)
  );

  if (!slotSnapshot.exists()) return false;

  return slotSnapshot.data().uid === uid ? handle : false;
}

async function initializeCampus(user) {
  if (!user) throw new Error("Firebase authentication unavailable.");

  const staff =
    user.email === ADMIN_EMAIL &&
    user.emailVerified === true;

  const db = createDatabaseAdapter(user.uid, staff);

  window.__fbResolve({
    uid: user.uid,
    email: user.email || "",
    account: !user.isAnonymous && !!user.email,
    staff,
    db,
    tr: createMultiplayerTransport(user.uid),

    checkUsername: username => {
      if (user.isAnonymous) {
        throw new Error("Sign in with your Unrest account first.");
      }

      return verifyUsername(user.uid, username);
    },

    approveUsername: async username => {
      if (user.isAnonymous) {
        throw new Error("Sign in with your Unrest account first.");
      }

      const handle = await verifyUsername(user.uid, username);

      if (!handle) {
        throw new Error("nouser");
      }

      // The database rules must authorize this approval write.
      await set(rtRef("campus/ok/" + user.uid), {
        m: "un",
        h: handle
      });

      return handle;
    },

    watchApprovedUsers: (callback, errorCallback) => {
      if (!staff) throw new Error("Team access required.");

      return watchRealtime(
        "campus/ok",
        snapshot => {
          const data = snapshot.val() || {};

          callback(
            Object.entries(data).map(([userId, value]) => ({
              uid: userId,
              handle: value?.h || "",
              method: value?.m || ""
            }))
          );
        },
        errorCallback || reportError
      );
    },

    watchPlayers: (callback, errorCallback) => {
      if (!staff) throw new Error("Team access required.");

      let profiles = {};
      let scenes = {};

      const emit = () => {
        const players = [];

        for (const [scene, users] of Object.entries(scenes)) {
          for (const [playerId, position] of Object.entries(users || {})) {
            const profile = profiles[playerId];

            if (!profile || !position) continue;

            players.push({
              uid: playerId,
              name: profile.n || "Student",
              handle: "",
              scene,
              x: position.x,
              y: position.y,
              joinedAt: profile.jt || 0,
              moving: !!position.m
            });
          }
        }

        callback(players);
      };

      const stopProfiles = watchRealtime(
        "campus/pr",
        snapshot => {
          profiles = snapshot.val() || {};
          emit();
        },
        errorCallback || reportError
      );

      const stopPositions = watchRealtime(
        "campus/p",
        snapshot => {
          scenes = snapshot.val() || {};
          emit();
        },
        errorCallback || reportError
      );

      return () => {
        stopProfiles();
        stopPositions();
      };
    },

    studentSignIn: (email, password) =>
      signInWithEmailAndPassword(
        auth,
        String(email || "").trim(),
        String(password || "")
      ),

    staffSignIn: async (email, password) => {
      const result = await signInWithEmailAndPassword(
        auth,
        String(email || "").trim(),
        String(password || "")
      );

      if (
        result.user.email !== ADMIN_EMAIL ||
        !result.user.emailVerified
      ) {
        await signOut(auth);
        throw new Error("not-staff");
      }

      location.reload();
      return result;
    },

    staffSignOut: () =>
      signOut(auth).then(() => location.reload())
  });
}

try {
  let initialUser = await new Promise((resolve, reject) => {
    let unsubscribe = () => {};

    unsubscribe = onAuthStateChanged(
      auth,
      user => {
        unsubscribe();
        resolve(user);
      },
      reject
    );
  });

  if (!initialUser) {
    try {
      const result = await signInAnonymously(auth);
      initialUser = result.user;
    } catch (error) {
      reportError(error);
      throw error;
    }
  }

  await initializeCampus(initialUser);
} catch (error) {
  reportError(error);
  window.__fbResolve(null);
}
