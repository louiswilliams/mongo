/**
 * Tests increasing and decreasing the WiredTiger session cursor cache.
 * @tags: [requires_wiredtiger]
 */

(function() {
    'use strict';

    function getOpenCursorCount() {
        let c = db.serverStatus().wiredTiger.session["open cursor count"];
        print("Cursor count: ", c);
        return c;
    }

    let coll1 = db.wt_session_cursors_cache1;
    let coll2 = db.wt_session_cursors_cache2;

    // Set the starting cache size to be high (default).
    db.adminCommand({setParameter: 1, "WTSessionCursorCacheSize": 10000});

    // Get the starting cursor count.
    let startingCount = getOpenCursorCount();
    assert.gt(startingCount, 0);

    // Create two collections, which should implicity create more cached cursors.
    coll1.insert({a: 1});
    coll2.insert({b: 1});
    let cursorsAfterCreate = getOpenCursorCount();
    assert.gt(cursorsAfterCreate, startingCount);

    // Dropping a collection should close a cursor, even if it was cached.
    coll1.drop();
    let cursorsAfterDrop = getOpenCursorCount();
    assert.lt(cursorsAfterDrop, cursorsAfterCreate);

    // Set the cache size to zero. No extra cursors will remain open, and old ones will be closed.
    db.adminCommand({setParameter: 1, "WTSessionCursorCacheSize": 0});

    // Nothing should have changed yet.
    assert.eq(getOpenCursorCount(), cursorsAfterDrop);

    // Doing a database operation on an existing collection should trigger the cache to get purged.
    coll2.update({b: 1}, {b: 2});
    let cursorsAfterPurge = getOpenCursorCount();
    assert.lt(cursorsAfterPurge, cursorsAfterDrop);

    // The total numrer of cursors after this point should not change because each cursor is closed
    // once it is done being used.
    coll1.insert({a: 2});
    assert.eq(getOpenCursorCount(), cursorsAfterPurge);

    coll2.insert({b: 2});
    assert.eq(getOpenCursorCount(), cursorsAfterPurge);

    coll2.insert({b: 2}, {b: 3});
    assert.eq(getOpenCursorCount(), cursorsAfterPurge);

    coll1.drop();
    assert.eq(getOpenCursorCount(), cursorsAfterPurge);

    coll2.drop();
    assert.eq(getOpenCursorCount(), cursorsAfterPurge);

    // Set the starting cache size to be high.
    db.adminCommand({setParameter: 1, "WTSessionCursorCacheSize": 10000});

    // More cursors should be open.
    coll1.insert({a: 3});
    assert.gt(getOpenCursorCount(), cursorsAfterPurge);

    coll2.insert({b: 3});
    assert.gt(getOpenCursorCount(), cursorsAfterPurge);
})();
