use super::*;
use crate::meta::config::{CacheConfig, ClientOptions, DatabaseConfig};
use crate::meta::file_lock::{FileLockQuery, FileLockRange, FileLockType};
use tokio::time;

fn test_config() -> Config {
    Config {
        database: DatabaseConfig {
            db_config: DatabaseType::Sqlite {
                url: "sqlite:file::memory:".to_string(),
            },
        },
        cache: CacheConfig::default(),
        client: ClientOptions::default(),
    }
}

fn file_db_config(path: &std::path::Path) -> Config {
    Config {
        database: DatabaseConfig {
            db_config: DatabaseType::Sqlite {
                url: format!("sqlite://{}?mode=rwc", path.display()),
            },
        },
        cache: CacheConfig::default(),
        client: ClientOptions::default(),
    }
}

/// Configuration for shared database testing (multi-session)
fn shared_db_config() -> Config {
    Config {
        database: DatabaseConfig {
            db_config: DatabaseType::Postgres {
                url: "postgres://slayerfs:slayerfs@127.0.0.1:15432/database".to_string(),
            },
        },
        cache: CacheConfig::default(),
        client: ClientOptions::default(),
    }
}

async fn new_test_store() -> DatabaseMetaStore {
    DatabaseMetaStore::from_config(test_config())
        .await
        .expect("Failed to create test database store")
}

/// Create a new test store with pre-configured session ID
async fn new_test_store_with_session(session_id: Uuid) -> DatabaseMetaStore {
    let store = new_test_store().await;
    store.set_sid(session_id).expect("Failed to set session ID");
    store
}

#[tokio::test]
async fn test_next_id_unique_across_store_instances() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db_path = temp_dir.path().join("counter-unique.db");
    let config = file_db_config(&db_path);

    let store1 = DatabaseMetaStore::from_config(config.clone())
        .await
        .expect("create store1");
    let store2 = DatabaseMetaStore::from_config(config)
        .await
        .expect("create store2");

    let parent = store1.root_ino();
    let ino1 = store1
        .create_file(parent, "counter_a".to_string())
        .await
        .expect("create file on store1");
    let ino2 = store2
        .create_file(parent, "counter_b".to_string())
        .await
        .expect("create file on store2");

    assert_ne!(ino1, ino2, "inode ids must be unique across stores");
    assert!(ino1 > 1);
    assert!(ino2 > 1);
}

/// Helper struct to manage multiple test sessions
struct TestSessionManager {
    stores: Vec<DatabaseMetaStore>,
}

use std::sync::LazyLock;
use tokio::sync::Mutex;

// 静态初始化，确保只执行一次
static SHARED_DB_INIT: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

impl TestSessionManager {
    async fn new(session_count: usize) -> Self {
        // 获取锁，确保串行初始化
        let _guard = SHARED_DB_INIT.lock().await;

        use std::env;
        // Clean up existing shared test database
        let temp_dir = env::temp_dir();
        let db_path = temp_dir.join("slayerfs_shared_test.db");

        // 只在第一次初始化时清理
        static FIRST_INIT: std::sync::Once = std::sync::Once::new();
        FIRST_INIT.call_once(|| {
            let _ = std::fs::remove_file(&db_path);
        });

        let mut stores = Vec::with_capacity(session_count);
        let mut session_ids = Vec::with_capacity(session_count);

        // 创建第一个 store（会初始化数据库）
        let config = shared_db_config();
        let first_store = DatabaseMetaStore::from_config(config.clone())
            .await
            .expect("Failed to create shared test database store");

        let first_session_id = Uuid::now_v7();
        first_store
            .set_sid(first_session_id)
            .expect("Failed to set session ID");

        stores.push(first_store);
        session_ids.push(first_session_id);

        // 后续的 store 复用已初始化的数据库
        for _ in 1..session_count {
            let store = DatabaseMetaStore::from_config(config.clone())
                .await
                .expect("Failed to create shared test database store");

            let session_id = Uuid::now_v7();
            store.set_sid(session_id).expect("Failed to set session ID");

            stores.push(store);
            session_ids.push(session_id);

            time::sleep(time::Duration::from_millis(5)).await;
        }

        Self { stores }
    }

    fn get_store(&self, index: usize) -> &DatabaseMetaStore {
        &self.stores[index]
    }
}

#[tokio::test]
#[ignore]
async fn test_hardlink_parent_field_single_link() {
    // Test that single-link files use parent field for O(1) lookup
    let store = new_test_store().await;
    let parent = store.root_ino();

    // Create a file
    let file_ino = store
        .create_file(parent, "single_link_file.txt".to_string())
        .await
        .unwrap();

    // Verify file has nlink=1
    let file_meta = FileMeta::find_by_id(file_ino)
        .one(&store.db)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(file_meta.nlink, 1);
    assert_eq!(
        file_meta.parent, parent,
        "Parent field should be set for single-link files"
    );

    // Verify no LinkParent entries exist
    let link_parents = LinkParentMeta::find()
        .filter(link_parent_meta::Column::Inode.eq(file_ino))
        .all(&store.db)
        .await
        .unwrap();

    assert!(
        link_parents.is_empty(),
        "No LinkParent entries should exist for single-link files"
    );
}

#[tokio::test]
#[ignore]
async fn test_hardlink_transition_to_linkparent() {
    // Test transition from parent field to LinkParent when creating first hardlink
    let store = new_test_store().await;
    let parent = store.root_ino();

    // Create a file
    let file_ino = store
        .create_file(parent, "original_file.txt".to_string())
        .await
        .unwrap();

    // Verify initial state
    let file_before = FileMeta::find_by_id(file_ino)
        .one(&store.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(file_before.nlink, 1);
    assert_eq!(file_before.parent, parent);

    // Create a hardlink
    let _attr = store.link(file_ino, parent, "hardlink.txt").await.unwrap();

    // Verify transition to LinkParent mode
    let file_after = FileMeta::find_by_id(file_ino)
        .one(&store.db)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        file_after.nlink, 2,
        "nlink should be 2 after creating hardlink"
    );
    assert_eq!(
        file_after.parent, 0,
        "Parent field should be 0 after transition to LinkParent mode"
    );

    // Verify LinkParent entries for both links
    let link_parents = LinkParentMeta::find()
        .filter(link_parent_meta::Column::Inode.eq(file_ino))
        .all(&store.db)
        .await
        .unwrap();

    assert_eq!(link_parents.len(), 2, "Should have 2 LinkParent entries");

    // Verify both links are tracked
    let names: Vec<String> = link_parents
        .iter()
        .map(|lp| lp.entry_name.clone())
        .collect();
    assert!(names.contains(&"original_file.txt".to_string()));
    assert!(names.contains(&"hardlink.txt".to_string()));
}

#[tokio::test]
#[ignore]
async fn test_hardlink_no_reversion_to_parent() {
    // When nlink drops from 2 to 1, parent field is restored (optimization)
    let store = new_test_store().await;
    let parent = store.root_ino();

    let file_ino = store
        .create_file(parent, "file1.txt".to_string())
        .await
        .unwrap();

    // Create hardlink: nlink 1 -> 2, parent becomes 0 (LinkParent mode)
    store.link(file_ino, parent, "file2.txt").await.unwrap();

    let file = FileMeta::find_by_id(file_ino)
        .one(&store.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(file.nlink, 2);
    assert_eq!(file.parent, 0);

    // Unlink: nlink 2 -> 1, parent restored for O(1) lookup
    store.unlink(parent, "file2.txt").await.unwrap();

    let file = FileMeta::find_by_id(file_ino)
        .one(&store.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(file.nlink, 1);
    assert_eq!(file.parent, parent);

    // LinkParent entries should be removed
    let count = LinkParentMeta::find()
        .filter(link_parent_meta::Column::Inode.eq(file_ino))
        .count(&store.db)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore]
async fn test_hardlink_multiple_links() {
    // Test LinkParent with multiple hardlinks
    let store = new_test_store().await;
    let parent = store.root_ino();

    // Create original file
    let file_ino = store
        .create_file(parent, "link1.txt".to_string())
        .await
        .unwrap();

    // Create multiple hardlinks
    store.link(file_ino, parent, "link2.txt").await.unwrap();
    store.link(file_ino, parent, "link3.txt").await.unwrap();
    store.link(file_ino, parent, "link4.txt").await.unwrap();

    // Verify nlink count
    let file_meta = FileMeta::find_by_id(file_ino)
        .one(&store.db)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(file_meta.nlink, 4, "Should have 4 links");
    assert_eq!(
        file_meta.parent, 0,
        "Parent field should be 0 for multi-link files"
    );

    // Verify all LinkParent entries
    let link_parents = LinkParentMeta::find()
        .filter(link_parent_meta::Column::Inode.eq(file_ino))
        .all(&store.db)
        .await
        .unwrap();

    assert_eq!(link_parents.len(), 4, "Should have 4 LinkParent entries");

    let names: Vec<String> = link_parents
        .iter()
        .map(|lp| lp.entry_name.clone())
        .collect();
    assert!(names.contains(&"link1.txt".to_string()));
    assert!(names.contains(&"link2.txt".to_string()));
    assert!(names.contains(&"link3.txt".to_string()));
    assert!(names.contains(&"link4.txt".to_string()));
}

#[tokio::test]
async fn test_hardlink_last_unlink_cleanup() {
    // Test that last unlink marks file as deleted and cleans up LinkParent entries
    let store = new_test_store().await;
    let parent = store.root_ino();

    // Create file with hardlink
    let file_ino = store
        .create_file(parent, "fileA.txt".to_string())
        .await
        .unwrap();
    store.link(file_ino, parent, "fileB.txt").await.unwrap();

    // Unlink both files
    store.unlink(parent, "fileB.txt").await.unwrap();
    store.unlink(parent, "fileA.txt").await.unwrap();

    // Verify file is marked as deleted
    let file_meta = FileMeta::find_by_id(file_ino)
        .one(&store.db)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(file_meta.nlink, 0, "nlink should be 0");
    assert_eq!(file_meta.parent, 0, "parent should be 0");
    assert!(file_meta.deleted, "File should be marked as deleted");

    // Verify all LinkParent entries are cleaned up
    let link_parents = LinkParentMeta::find()
        .filter(link_parent_meta::Column::Inode.eq(file_ino))
        .all(&store.db)
        .await
        .unwrap();

    assert!(
        link_parents.is_empty(),
        "All LinkParent entries should be cleaned up"
    );
}

#[tokio::test]
async fn test_hardlink_dentry_binding_cross_dir_rename_unlink() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let dir_a = store.mkdir(root, "a".to_string()).await.unwrap();
    let dir_b = store.mkdir(root, "b".to_string()).await.unwrap();

    let ino = store.create_file(dir_a, "x".to_string()).await.unwrap();
    store.link(ino, dir_b, "y").await.unwrap();

    let names = store.get_names(ino).await.unwrap();
    assert!(names.contains(&(Some(dir_a), "x".to_string())));
    assert!(names.contains(&(Some(dir_b), "y".to_string())));

    assert_eq!(store.lookup(dir_a, "x").await.unwrap(), Some(ino));
    assert_eq!(store.lookup(dir_b, "y").await.unwrap(), Some(ino));

    store
        .rename(dir_b, "y", dir_b, "z".to_string())
        .await
        .unwrap();

    let names = store.get_names(ino).await.unwrap();
    assert!(names.contains(&(Some(dir_a), "x".to_string())));
    assert!(names.contains(&(Some(dir_b), "z".to_string())));
    assert!(!names.contains(&(Some(dir_b), "y".to_string())));

    assert_eq!(store.lookup(dir_b, "y").await.unwrap(), None);
    assert_eq!(store.lookup(dir_b, "z").await.unwrap(), Some(ino));
    assert_eq!(store.lookup(dir_a, "x").await.unwrap(), Some(ino));

    store.unlink(dir_a, "x").await.unwrap();

    let names = store.get_names(ino).await.unwrap();
    assert_eq!(names, vec![(Some(dir_b), "z".to_string())]);
    assert_eq!(store.lookup(dir_b, "z").await.unwrap(), Some(ino));
}

#[tokio::test]
async fn test_hardlink_dentry_binding_cross_dir_move_rename() {
    let store = new_test_store().await;
    let root = store.root_ino();

    let dir_a = store.mkdir(root, "a".to_string()).await.unwrap();
    let dir_b = store.mkdir(root, "b".to_string()).await.unwrap();
    let dir_c = store.mkdir(root, "c".to_string()).await.unwrap();

    let ino = store.create_file(dir_a, "x".to_string()).await.unwrap();
    store.link(ino, dir_b, "y").await.unwrap();

    assert_eq!(store.lookup(dir_a, "x").await.unwrap(), Some(ino));
    assert_eq!(store.lookup(dir_b, "y").await.unwrap(), Some(ino));

    store
        .rename(dir_b, "y", dir_c, "z".to_string())
        .await
        .unwrap();

    let names = store.get_names(ino).await.unwrap();
    assert!(names.contains(&(Some(dir_a), "x".to_string())));
    assert!(names.contains(&(Some(dir_c), "z".to_string())));
    assert!(!names.contains(&(Some(dir_b), "y".to_string())));

    assert_eq!(store.lookup(dir_b, "y").await.unwrap(), None);
    assert_eq!(store.lookup(dir_c, "z").await.unwrap(), Some(ino));
    assert_eq!(store.lookup(dir_a, "x").await.unwrap(), Some(ino));
}

#[tokio::test]
async fn test_symlink_uses_parent_field() {
    // Test that symlinks use parent field (they always have nlink=1)
    let store = new_test_store().await;
    let parent = store.root_ino();

    // Create a symlink
    let (symlink_ino, _attr) = store
        .symlink(parent, "my_symlink", "/target/path")
        .await
        .unwrap();

    // Verify symlink has nlink=1 and uses parent field
    let file_meta = FileMeta::find_by_id(symlink_ino)
        .one(&store.db)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(file_meta.nlink, 1, "Symlink should have nlink=1");
    assert_eq!(file_meta.parent, parent, "Symlink should use parent field");
    assert_eq!(file_meta.symlink_target, Some("/target/path".to_string()));

    // Verify no LinkParent entries
    let link_parents = LinkParentMeta::find()
        .filter(link_parent_meta::Column::Inode.eq(symlink_ino))
        .all(&store.db)
        .await
        .unwrap();

    assert!(
        link_parents.is_empty(),
        "Symlinks should not have LinkParent entries"
    );
}

#[tokio::test]
#[ignore]
async fn test_basic_read_lock() {
    let store = new_test_store().await;
    let session_id = Uuid::now_v7();
    let owner: u64 = 1001;

    // Set session
    store.set_sid(session_id).unwrap();

    // Create a file first
    let parent = store.root_ino();
    let file_ino = store
        .create_file(parent, "test_file.txt".to_string())
        .await
        .unwrap();

    // Acquire read lock
    store
        .set_plock(
            file_ino,
            owner as i64,
            false,
            FileLockType::Read,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await
        .unwrap();

    // Verify lock exists
    let query = FileLockQuery {
        owner: owner as i64,
        lock_type: FileLockType::Read,
        range: FileLockRange { start: 0, end: 100 },
    };

    let lock_info = store.get_plock(file_ino, &query).await.unwrap();
    assert_eq!(lock_info.lock_type, FileLockType::UnLock);
}

#[tokio::test]
#[ignore]
async fn test_multiple_read_locks() {
    // Create session manager with 2 sessions
    let session_mgr = TestSessionManager::new(2).await;

    let owner1: i64 = 1001;
    let owner2: i64 = 1002;

    // Create a file first using the first session
    let store1 = session_mgr.get_store(0);
    let parent = store1.root_ino();
    let file_ino = store1
        .create_file(
            parent,
            format!("test_multiple_read_locks_{}.txt", Uuid::now_v7()),
        )
        .await
        .unwrap();

    // First session acquires read lock
    store1
        .set_plock(
            file_ino,
            owner1,
            false,
            FileLockType::Read,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await
        .unwrap();

    // Second session should be able to acquire read lock on same range
    let store2 = session_mgr.get_store(1);
    store2
        .set_plock(
            file_ino,
            owner2,
            false,
            FileLockType::Read,
            FileLockRange { start: 0, end: 100 },
            5678,
        )
        .await
        .unwrap();

    // Verify both locks exist by querying each session
    let query1 = FileLockQuery {
        owner: owner1,
        lock_type: FileLockType::Read,
        range: FileLockRange { start: 0, end: 100 },
    };

    let query2 = FileLockQuery {
        owner: owner2,
        lock_type: FileLockType::Write,
        range: FileLockRange { start: 0, end: 100 },
    };

    let lock_info1 = store1.get_plock(file_ino, &query1).await.unwrap();
    assert_eq!(lock_info1.lock_type, FileLockType::UnLock);

    let lock_info2 = store2.get_plock(file_ino, &query2).await.unwrap();
    assert_eq!(lock_info2.lock_type, FileLockType::Read);
    assert_eq!(lock_info2.range.start, 0);
    assert_eq!(lock_info2.range.end, 100);
    assert_eq!(
        lock_info2.pid, 0,
        "pid should be 0 for cross-session queries (security feature)"
    );
}

#[tokio::test]
#[ignore]
async fn test_write_lock_conflict() {
    // Create session manager with 2 sessions
    let session_mgr = TestSessionManager::new(2).await;

    let owner1: u64 = 1001;
    let owner2: u64 = 1002;

    // Create a file first using the first session
    let store1 = session_mgr.get_store(0);
    let parent = store1.root_ino();
    let file_ino = store1
        .create_file(
            parent,
            format!("test_write_lock_conflict_{}.txt", Uuid::now_v7()),
        )
        .await
        .unwrap();

    // First session acquires read lock
    store1
        .set_plock(
            file_ino,
            owner1 as i64,
            false,
            FileLockType::Read,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await
        .unwrap();

    // Second session should not be able to acquire write lock on overlapping range
    let store2 = session_mgr.get_store(1);
    let result = store2
        .set_plock(
            file_ino,
            owner2 as i64,
            false, // non-blocking
            FileLockType::Write,
            FileLockRange {
                start: 50,
                end: 150,
            }, // Overlapping range
            5678,
        )
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::LockConflict {
            inode: err_inode,
            owner: err_owner,
            range: err_range,
        } => {
            assert_eq!(err_inode, file_ino);
            assert_eq!(err_owner, owner2 as i64);
            assert_eq!(err_range.start, 50);
            assert_eq!(err_range.end, 150);
        }
        _ => panic!("Expected LockConflict error"),
    }
}

#[tokio::test]
#[ignore]
async fn test_lock_release() {
    let session_id = Uuid::now_v7();
    let owner = 1001;

    // Create a store with pre-configured session
    let store = new_test_store_with_session(session_id).await;

    // Create a file first
    let parent = store.root_ino();
    let file_ino = store
        .create_file(parent, "test_file.txt".to_string())
        .await
        .unwrap();

    // Acquire lock
    store
        .set_plock(
            file_ino,
            owner,
            false,
            FileLockType::Write,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await
        .unwrap();

    // Verify lock exists
    let query = FileLockQuery {
        owner,
        lock_type: FileLockType::Write,
        range: FileLockRange { start: 0, end: 100 },
    };

    let lock_info = store.get_plock(file_ino, &query).await.unwrap();
    assert_eq!(lock_info.lock_type, FileLockType::Write);

    // Release lock
    store
        .set_plock(
            file_ino,
            owner,
            false,
            FileLockType::UnLock,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await
        .unwrap();

    // Verify lock is released
    let lock_info = store.get_plock(file_ino, &query).await.unwrap();
    assert_eq!(lock_info.lock_type, FileLockType::UnLock);
}

#[tokio::test]
#[ignore]
async fn test_non_overlapping_locks() {
    // Create session manager with 2 sessions
    let session_mgr = TestSessionManager::new(2).await;

    let owner1: i64 = 1001;
    let owner2: i64 = 1002;

    // Create a file first using the first session
    let store1 = session_mgr.get_store(0);
    let parent = store1.root_ino();
    let file_ino = store1
        .create_file(
            parent,
            format!("test_non_overlapping_locks_{}.txt", Uuid::now_v7()),
        )
        .await
        .unwrap();

    // First session acquires lock on range 0-100
    store1
        .set_plock(
            file_ino,
            owner1,
            false,
            FileLockType::Write,
            FileLockRange { start: 0, end: 100 },
            1234,
        )
        .await
        .unwrap();

    // Second session should be able to acquire lock on non-overlapping range 200-300
    let store2 = session_mgr.get_store(1);
    store2
        .set_plock(
            file_ino,
            owner2,
            false,
            FileLockType::Write,
            FileLockRange {
                start: 200,
                end: 300,
            },
            5678,
        )
        .await
        .unwrap();

    // Verify both locks exist
    let query1 = FileLockQuery {
        owner: owner1,
        lock_type: FileLockType::Write,
        range: FileLockRange { start: 0, end: 100 },
    };

    let query2 = FileLockQuery {
        owner: owner2,
        lock_type: FileLockType::Write,
        range: FileLockRange {
            start: 200,
            end: 300,
        },
    };

    let lock_info1 = store1.get_plock(file_ino, &query1).await.unwrap();
    assert_eq!(lock_info1.lock_type, FileLockType::Write);
    assert_eq!(lock_info1.range.start, 0);
    assert_eq!(lock_info1.range.end, 100);
    assert_eq!(lock_info1.pid, 1234);

    let lock_info2 = store2.get_plock(file_ino, &query2).await.unwrap();
    assert_eq!(lock_info2.lock_type, FileLockType::Write);
    assert_eq!(lock_info2.range.start, 200);
    assert_eq!(lock_info2.range.end, 300);
    assert_eq!(lock_info2.pid, 5678);
}

#[tokio::test]
#[ignore]
async fn test_concurrent_read_write_locks() {
    // Test multiple sessions acquiring different types of locks
    let session_mgr = TestSessionManager::new(3).await;

    // Create a file
    let store0 = session_mgr.get_store(0);
    let parent = store0.root_ino();
    let file_ino = store0
        .create_file(parent, format!("concurrent_test_{}.txt", Uuid::now_v7()))
        .await
        .unwrap();

    let owner1: i64 = 1001;
    let owner2: i64 = 1002;
    let owner3: i64 = 1003;

    // Session 1: Acquire write lock on range 0-100
    {
        let store1 = session_mgr.get_store(0);
        store1
            .set_plock(
                file_ino,
                owner1,
                false,
                FileLockType::Write,
                FileLockRange { start: 0, end: 100 },
                1111,
            )
            .await
            .expect("Failed to acquire write lock");
    }

    // Session 2: Acquire read lock on range 200-300 (should succeed)
    {
        let store2 = session_mgr.get_store(1);
        store2
            .set_plock(
                file_ino,
                owner2,
                false,
                FileLockType::Read,
                FileLockRange {
                    start: 200,
                    end: 300,
                },
                2222,
            )
            .await
            .expect("Failed to acquire read lock");
    }

    // Session 3: Try to acquire write lock on overlapping range 50-150 (should fail)
    {
        let store3 = session_mgr.get_store(2);
        let result = store3
            .set_plock(
                file_ino,
                owner3,
                false,
                FileLockType::Write,
                FileLockRange {
                    start: 50,
                    end: 150,
                },
                3333,
            )
            .await;

        // Verify it fails with LockConflict
        assert!(result.is_err());
        match result.unwrap_err() {
            MetaError::LockConflict { .. } => {}
            _ => panic!("Expected LockConflict error"),
        }
    }

    // Verify successful locks exist
    let query1 = FileLockQuery {
        owner: owner1,
        lock_type: FileLockType::Write,
        range: FileLockRange { start: 0, end: 100 },
    };

    let query2 = FileLockQuery {
        owner: owner2,
        lock_type: FileLockType::Read,
        range: FileLockRange {
            start: 200,
            end: 300,
        },
    };

    // Check locks from different sessions
    {
        let store1 = session_mgr.get_store(0);
        let lock_info1 = store1.get_plock(file_ino, &query1).await.unwrap();
        assert_eq!(lock_info1.lock_type, FileLockType::Write);
    }

    {
        let store2 = session_mgr.get_store(1);
        let lock_info2 = store2.get_plock(file_ino, &query2).await.unwrap();
        assert_eq!(lock_info2.lock_type, FileLockType::UnLock);
    }
}

#[tokio::test]
#[ignore]
async fn test_cross_session_lock_visibility() {
    // Test that locks set by one session are visible to another session
    let session_mgr = TestSessionManager::new(2).await;

    let owner1: u64 = 1001;

    // Create a file
    let store1 = session_mgr.get_store(0);
    let parent = store1.root_ino();
    let file_ino = store1
        .create_file(parent, format!("visibility_test_{}.txt", Uuid::now_v7()))
        .await
        .unwrap();

    // Session 1 acquires a write lock
    store1
        .set_plock(
            file_ino,
            owner1 as i64,
            false,
            FileLockType::Write,
            FileLockRange {
                start: 0,
                end: 1000,
            },
            4444,
        )
        .await
        .unwrap();

    // Session 2 should be able to see the lock (and respect it)
    let store2 = session_mgr.get_store(1);
    let conflict_result = store2
        .set_plock(
            file_ino,
            2002, // different owner
            false,
            FileLockType::Write,
            FileLockRange {
                start: 500,
                end: 600,
            }, // overlapping range
            5555,
        )
        .await;

    // Should fail due to lock conflict
    assert!(conflict_result.is_err());
    match conflict_result.unwrap_err() {
        MetaError::LockConflict { .. } => {}
        _ => panic!("Expected LockConflict error"),
    }

    // Session 1 releases the lock
    store1
        .set_plock(
            file_ino,
            owner1 as i64,
            false,
            FileLockType::UnLock,
            FileLockRange {
                start: 0,
                end: 1000,
            },
            4444,
        )
        .await
        .unwrap();

    // Now Session 2 should be able to acquire the lock
    store2
        .set_plock(
            file_ino,
            2002,
            false,
            FileLockType::Write,
            FileLockRange {
                start: 500,
                end: 600,
            },
            5555,
        )
        .await
        .unwrap();

    // Verify the lock exists
    let query = FileLockQuery {
        owner: 2002,
        lock_type: FileLockType::Write,
        range: FileLockRange {
            start: 500,
            end: 600,
        },
    };

    let lock_info = store2.get_plock(file_ino, &query).await.unwrap();
    assert_eq!(lock_info.lock_type, FileLockType::Write);
    assert_eq!(lock_info.pid, 5555);
}

// -------------------------------------------------------------------
// Permission / chmod tests
// -------------------------------------------------------------------

#[tokio::test]
async fn test_file_default_mode() {
    let store = new_test_store().await;
    let parent = store.root_ino();
    let ino = store
        .create_file(parent, "perm_file.txt".to_string())
        .await
        .unwrap();

    let attr = store.stat(ino).await.unwrap().unwrap();
    // Default file mode: permission bits should be 0o644.
    assert_eq!(
        attr.mode & 0o777,
        0o644,
        "newly created file should have default permission 0644"
    );
}

#[tokio::test]
async fn test_directory_default_mode() {
    let store = new_test_store().await;
    let parent = store.root_ino();
    let ino = store.mkdir(parent, "perm_dir".to_string()).await.unwrap();

    let attr = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(
        attr.mode & 0o7777,
        0o755,
        "newly created directory should have default permission 0755"
    );
}

#[tokio::test]
async fn test_chmod_updates_mode() {
    let store = new_test_store().await;
    let parent = store.root_ino();
    let ino = store
        .create_file(parent, "chmod_test.txt".to_string())
        .await
        .unwrap();

    let attr = store.chmod(ino, 0o755).await.unwrap();
    assert_eq!(attr.mode & 0o777, 0o755);

    // Verify via stat
    let stat = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(stat.mode & 0o777, 0o755);
}

#[tokio::test]
async fn test_chmod_strips_special_bits() {
    let store = new_test_store().await;
    let parent = store.root_ino();
    let ino = store
        .create_file(parent, "special_bits.txt".to_string())
        .await
        .unwrap();

    // MetaStore::chmod strips setuid/setgid/sticky (masks to 0o777).
    let attr = store.chmod(ino, 0o7755).await.unwrap();
    assert_eq!(
        attr.mode & 0o7777,
        0o755,
        "setuid/setgid/sticky should be stripped"
    );
}

#[tokio::test]
async fn test_chmod_nonexistent_inode() {
    let store = new_test_store().await;
    let result = store.chmod(999999, 0o644).await;
    assert!(result.is_err(), "chmod on nonexistent inode should fail");
}

// -------------------------------------------------------------------
// chown tests
// -------------------------------------------------------------------

#[tokio::test]
async fn test_chown_updates_uid_and_gid() {
    let store = new_test_store().await;
    let parent = store.root_ino();
    let ino = store
        .create_file(parent, "chown_test.txt".to_string())
        .await
        .unwrap();

    let attr = store.chown(ino, Some(1000), Some(1000)).await.unwrap();
    assert_eq!(attr.uid, 1000);
    assert_eq!(attr.gid, 1000);

    // Verify via stat
    let stat = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(stat.uid, 1000);
    assert_eq!(stat.gid, 1000);
}

#[tokio::test]
async fn test_chown_uid_only() {
    let store = new_test_store().await;
    let parent = store.root_ino();
    let ino = store
        .create_file(parent, "chown_uid.txt".to_string())
        .await
        .unwrap();

    let before = store.stat(ino).await.unwrap().unwrap();
    let original_gid = before.gid;

    let attr = store.chown(ino, Some(2000), None).await.unwrap();
    assert_eq!(attr.uid, 2000);
    assert_eq!(attr.gid, original_gid, "gid should remain unchanged");
}

#[tokio::test]
async fn test_chown_gid_only() {
    let store = new_test_store().await;
    let parent = store.root_ino();
    let ino = store
        .create_file(parent, "chown_gid.txt".to_string())
        .await
        .unwrap();

    let before = store.stat(ino).await.unwrap().unwrap();
    let original_uid = before.uid;

    let attr = store.chown(ino, None, Some(3000)).await.unwrap();
    assert_eq!(attr.uid, original_uid, "uid should remain unchanged");
    assert_eq!(attr.gid, 3000);
}

#[tokio::test]
async fn test_chown_preserves_mode() {
    let store = new_test_store().await;
    let parent = store.root_ino();
    let ino = store
        .create_file(parent, "chown_mode.txt".to_string())
        .await
        .unwrap();

    // Change mode first
    store.chmod(ino, 0o755).await.unwrap();

    // Then change owner
    let attr = store.chown(ino, Some(1000), Some(1000)).await.unwrap();
    assert_eq!(
        attr.mode & 0o777,
        0o755,
        "chown should not alter permission bits"
    );
}

#[tokio::test]
async fn test_chown_nonexistent_inode() {
    let store = new_test_store().await;
    let result = store.chown(999999, Some(1000), Some(1000)).await;
    assert!(result.is_err(), "chown on nonexistent inode should fail");
}

#[tokio::test]
async fn test_chown_directory() {
    let store = new_test_store().await;
    let parent = store.root_ino();
    let ino = store.mkdir(parent, "chown_dir".to_string()).await.unwrap();

    let attr = store.chown(ino, Some(500), Some(500)).await.unwrap();
    assert_eq!(attr.uid, 500);
    assert_eq!(attr.gid, 500);

    let stat = store.stat(ino).await.unwrap().unwrap();
    assert_eq!(stat.uid, 500);
    assert_eq!(stat.gid, 500);
}


// -------------------------------------------------------------------
// Compact and GC Tests
// -------------------------------------------------------------------

#[tokio::test]
async fn test_compact_threshold_trigger() {
    let store = new_test_store().await;

    // initial state: 0 slices
    let (should_compact, _is_sync) = store.should_compact_chunk(chunk_id).await.unwrap();
    assert!(!should_compact, "should not compact with 0 slices");

    // add 3 slices (less than threshold 5)
    let txn = store.db.begin().await.unwrap();
    for i in 1..=3 {
        let model = slice_meta::ActiveModel {
            slice_id: Set(i),
            chunk_id: Set(chunk_id as i64),
            offset: Set((i * 100) as i64),
            length: Set(100),
            ..Default::default()
        };
        model.insert(&txn).await.unwrap();
    }
    txn.commit().await.unwrap();

    // verify: should_compact_chunk returns false
    let (should_compact, _) = store.should_compact_chunk(chunk_id).await.unwrap();
    assert!(!should_compact, "should not compact with only 3 slices");

    // add 3 more slices (total 6, exceeds threshold 5)
    let txn = store.db.begin().await.unwrap();
    for i in 4..=6 {
        let model = slice_meta::ActiveModel {
            slice_id: Set(i),
            chunk_id: Set(chunk_id as i64),
            offset: Set((i * 100) as i64),
            length: Set(100),
            ..Default::default()
        };
        model.insert(&txn).await.unwrap();
    }
    txn.commit().await.unwrap();

    // verify: should_compact_chunk returns true (depending on fragmentation ratio)
    let (should_compact, is_sync) = store.should_compact_chunk(chunk_id).await.unwrap();
    info!(
        "threshold test: 6 slices, should_compact={}, is_sync={}",
        should_compact, is_sync
    );

    // verify: can get statistics
    let (slice_count, total_size, fragment_ratio) =
        store.get_chunk_compact_stats(chunk_id).await.unwrap();
    assert_eq!(slice_count, 6, "should have 6 slices");
    assert_eq!(total_size, 600, "total size should be 600");
    info!(
        "compact stats: {} slices, {} bytes, {:.2} fragmentation ratio",
        slice_count, total_size, fragment_ratio
    );

    info!("threshold trigger test passed");
}

#[tokio::test]
async fn test_merge_slices_functionality() {
    let store = new_test_store().await;

    // test case 1: non-overlapping slices should all be kept
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 0,
            length: 100,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 200,
            length: 100,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();
    assert_eq!(result.len(), 2, "non-overlapping slices should all be kept");
    assert_eq!(result[0].offset, 0);
    assert_eq!(result[0].length, 100);
    assert_eq!(result[1].offset, 200);
    assert_eq!(result[1].length, 100);

    // test case 2: partially overlapping slices — both kept intact
    // slice 1 (older): 0-100, slice 2 (newer): 50-150
    // slice 1 is NOT fully covered (only [50,100) is overlapped),
    // so it must be kept with original offset/length to preserve
    // block data addressing correctness.
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 0,
            length: 100,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 50,
            length: 100,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();
    assert_eq!(
        result.len(),
        2,
        "partially overlapping slices should both be kept (no splitting)"
    );
    // result is sorted by offset
    assert_eq!(result[0].slice_id, 1, "older slice kept intact");
    assert_eq!(result[0].offset, 0);
    assert_eq!(
        result[0].length, 100,
        "slice 1 must keep original length (can't trim without rewriting block data)"
    );
    assert_eq!(result[1].slice_id, 2, "newer slice fully kept");
    assert_eq!(result[1].offset, 50);
    assert_eq!(result[1].length, 100);

    // test case 3: fully covered slice should be removed
    // slice 1 (older): 0-100, slice 2 (newer): 0-150 (fully covers slice 1)
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 0,
            length: 100,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 0,
            length: 150,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();
    assert_eq!(result.len(), 1, "fully covered slice should be removed");
    assert_eq!(result[0].slice_id, 2, "newer slice should be kept");
    assert_eq!(result[0].offset, 0);
    assert_eq!(result[0].length, 150);

    // test case 4: multiple slices with some fully covered
    // slice 1: 0-50, slice 2: 0-100 (fully covers 1), slice 3: 150-200 (separate)
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 0,
            length: 50,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 0,
            length: 100,
        },
        SliceDesc {
            slice_id: 3,
            chunk_id: 1,
            offset: 150,
            length: 50,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();
    assert_eq!(
        result.len(),
        2,
        "slice 1 fully covered by 2, slices 2 and 3 kept"
    );
    assert_eq!(result[0].slice_id, 2);
    assert_eq!(result[1].slice_id, 3);

    // test case 5: empty input
    let slices: Vec<SliceDesc> = vec![];
    let result = store.merge_slices(&slices).await.unwrap();
    assert!(result.is_empty(), "empty input should return empty");

    // test case 6: single slice
    let slices = vec![SliceDesc {
        slice_id: 1,
        chunk_id: 1,
        offset: 0,
        length: 100,
    }];
    let result = store.merge_slices(&slices).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].offset, 0);
    assert_eq!(result[0].length, 100);

    info!("merge slices functionality test passed");
}

/// Test merge_slices with non-zero offset slices
/// This test verifies that partially covered slices with non-zero offset
/// are handled correctly (should NOT change slice offset to avoid breaking
/// block data addressing which uses slice-relative offsets)
#[tokio::test]
async fn test_merge_slices_nonzero_offset() {
    let store = new_test_store().await;

    // Test case: partially overlapping slices with non-zero offset
    // slice 1 (older): offset=100, length=100 -> [100, 200)
    // slice 2 (newer): offset=150, length=100 -> [150, 250)
    // slice 1's [150-200) is covered by slice 2, but [100-150) is still needed
    // Since we cannot split slice 1 without rewriting block data,
    // the entire slice 1 should be kept with its original offset
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 100,
            length: 100,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 150,
            length: 100,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();

    // Both slices should be kept (cannot split slice 1)
    assert_eq!(
        result.len(),
        2,
        "partially covered slice with non-zero offset should be kept entirely"
    );

    // Verify slice 1 keeps its original offset (100), not changed to uncovered portion
    let slice_1 = result.iter().find(|s| s.slice_id == 1).unwrap();
    assert_eq!(
        slice_1.offset, 100,
        "slice offset should NOT be changed - would break block data addressing"
    );
    assert_eq!(slice_1.length, 100, "slice length should remain unchanged");

    // Verify slice 2 is also kept
    let slice_2 = result.iter().find(|s| s.slice_id == 2).unwrap();
    assert_eq!(slice_2.offset, 150);
    assert_eq!(slice_2.length, 100);

    info!("merge slices with non-zero offset test passed");
}

/// Test that delayed_slice records preserve offset field correctly
/// This test verifies the 20-byte format (slice_id + offset + size) is
/// correctly parsed and the offset field is persisted in the database
#[tokio::test]
async fn test_delayed_slice_offset_persistence() {
    let store = new_test_store().await;
    let chunk_id = 1u64;
    let old_slice_id = 42u64;
    let old_slice_offset = 1234u64; // Non-zero offset to verify persistence
    let old_slice_size = 5678u32;

    // Create delayed data in 20-byte format:
    // slice_id (8 bytes) + offset (8 bytes) + size (4 bytes)
    let mut delayed_data = Vec::new();
    delayed_data.extend_from_slice(&old_slice_id.to_le_bytes());
    delayed_data.extend_from_slice(&old_slice_offset.to_le_bytes());
    delayed_data.extend_from_slice(&old_slice_size.to_le_bytes());
    assert_eq!(delayed_data.len(), 20, "delayed data should be 20 bytes");

    // Insert delayed slice record
    let txn = store.db.begin().await.unwrap();
    store
        .cleanup_delayed_slices(chunk_id, &delayed_data, &txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Verify the delayed slice record was inserted with correct offset
    let delayed_records: Vec<delayed_slice::Model> = DelayedSlice::find()
        .filter(delayed_slice::Column::SliceId.eq(old_slice_id as i64))
        .all(&store.db)
        .await
        .unwrap();

    assert_eq!(
        delayed_records.len(),
        1,
        "should have exactly one delayed slice record"
    );

    let record = &delayed_records[0];
    assert_eq!(
        record.slice_id, old_slice_id as i64,
        "slice_id should match"
    );
    assert_eq!(
        record.offset, old_slice_offset as i64,
        "offset should be correctly persisted (was missing in bug)"
    );
    assert_eq!(record.size, old_slice_size as i64, "size should match");
    assert_eq!(record.chunk_id, chunk_id as i64, "chunk_id should match");

    info!("delayed slice offset persistence test passed");
}

// ==================== Comprehensive merge_slices tests ====================

/// Verify that merge_slices enforces all slices belong to the same chunk.
#[tokio::test]
async fn test_merge_slices_different_chunk_ids_rejected() {
    let store = new_test_store().await;
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 0,
            length: 100,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 2,
            offset: 0,
            length: 100,
        },
    ];
    let result = store.merge_slices(&slices).await;
    assert!(
        result.is_err(),
        "should reject slices from different chunks"
    );
}

/// Partial overlap where newer covers the HEAD of older slice.
/// Must keep both slices intact (cannot trim head without breaking addressing).
#[tokio::test]
async fn test_merge_slices_partial_head_overlap() {
    let store = new_test_store().await;
    // Slice A (older): [100, 200), Slice B (newer): [50, 150)
    // B covers A's head [100, 150), A's tail [150, 200) is uncovered
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 100,
            length: 100,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 50,
            length: 100,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();
    assert_eq!(result.len(), 2, "both kept since A is not fully covered");
    let a = result.iter().find(|s| s.slice_id == 1).unwrap();
    assert_eq!(a.offset, 100, "slice A offset must not change");
    assert_eq!(a.length, 100, "slice A length must not change");
}

/// Partial overlap where newer covers the TAIL of older slice.
#[tokio::test]
async fn test_merge_slices_partial_tail_overlap() {
    let store = new_test_store().await;
    // Slice A (older): [0, 100), Slice B (newer): [80, 200)
    // B covers A's tail [80, 100), A's head [0, 80) is uncovered
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 0,
            length: 100,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 80,
            length: 120,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();
    assert_eq!(result.len(), 2);
    let a = result.iter().find(|s| s.slice_id == 1).unwrap();
    assert_eq!(a.offset, 0);
    assert_eq!(a.length, 100, "slice A kept intact, not trimmed");
}

/// Newer slice covers the MIDDLE of older slice (sandwich).
#[tokio::test]
async fn test_merge_slices_middle_overlap() {
    let store = new_test_store().await;
    // A (older): [0, 200), B (newer): [50, 150)
    // B covers [50, 150) of A, but A's [0, 50) and [150, 200) are uncovered
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 0,
            length: 200,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 50,
            length: 100,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();
    assert_eq!(result.len(), 2, "A is NOT fully covered, must keep both");
    let a = result.iter().find(|s| s.slice_id == 1).unwrap();
    assert_eq!(a.offset, 0);
    assert_eq!(a.length, 200);
}

/// Non-zero offset scenario (the original bug scenario from the discussion).
/// Slice A: [100, 200), Slice B: [150, 250) — partial overlap at tail,
/// A is not fully covered, must keep intact.
#[tokio::test]
async fn test_merge_slices_nonzero_offset_tail_overlap() {
    let store = new_test_store().await;
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 100,
            length: 100,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 150,
            length: 100,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();
    assert_eq!(result.len(), 2);
    let a = result.iter().find(|s| s.slice_id == 1).unwrap();
    assert_eq!(a.offset, 100, "offset must stay at 100");
    assert_eq!(a.length, 100, "length must stay at 100");
}

/// Three slices where slice 1 is covered by the UNION of slices 2+3,
/// but NOT by any single slice. Slice 1 must be kept.
#[tokio::test]
async fn test_merge_slices_union_coverage_not_removed() {
    let store = new_test_store().await;
    // A (oldest): [0, 100), B: [0, 60), C: [50, 100)
    // B∪C covers [0, 100) = A's range, but neither B nor C alone fully covers A.
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 0,
            length: 100,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 0,
            length: 60,
        },
        SliceDesc {
            slice_id: 3,
            chunk_id: 1,
            offset: 50,
            length: 50,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();
    // Current implementation checks single-range coverage, not union.
    // A is NOT fully covered by any single covered_range entry.
    assert_eq!(
        result.len(),
        3,
        "none is fully covered by a single newer slice"
    );
}

/// Exact same range: newer fully covers older.
#[tokio::test]
async fn test_merge_slices_exact_same_range() {
    let store = new_test_store().await;
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 50,
            length: 100,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 50,
            length: 100,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();
    assert_eq!(
        result.len(),
        1,
        "older slice fully covered by exact same range"
    );
    assert_eq!(result[0].slice_id, 2);
}

/// Chain coverage: A fully covered by B, B fully covered by C.
#[tokio::test]
async fn test_merge_slices_chain_coverage() {
    let store = new_test_store().await;
    // A: [0, 50), B: [0, 80), C: [0, 100)
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 0,
            length: 50,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 0,
            length: 80,
        },
        SliceDesc {
            slice_id: 3,
            chunk_id: 1,
            offset: 0,
            length: 100,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();
    assert_eq!(result.len(), 1, "only the newest slice survives");
    assert_eq!(result[0].slice_id, 3);
}

/// Adjacent but non-overlapping slices: both kept.
#[tokio::test]
async fn test_merge_slices_adjacent_no_overlap() {
    let store = new_test_store().await;
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 0,
            length: 100,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 100,
            length: 100,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();
    assert_eq!(result.len(), 2, "adjacent slices are non-overlapping");
}

/// Many slices, some fully covered and some not.
#[tokio::test]
async fn test_merge_slices_complex_scenario() {
    let store = new_test_store().await;
    // Timeline (oldest→newest): 1, 2, 3, 4, 5
    // 1: [0, 50)   — covered by 2 AND 5
    // 2: [0, 100)  — covered by 5
    // 3: [200, 300) — not covered by any single newer slice
    // 4: [250, 280) — newer than 3 but smaller
    // 5: [0, 100)
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 0,
            length: 50,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 0,
            length: 100,
        },
        SliceDesc {
            slice_id: 3,
            chunk_id: 1,
            offset: 200,
            length: 100,
        },
        SliceDesc {
            slice_id: 4,
            chunk_id: 1,
            offset: 250,
            length: 30,
        },
        SliceDesc {
            slice_id: 5,
            chunk_id: 1,
            offset: 0,
            length: 100,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();
    let ids: Vec<u64> = result.iter().map(|s| s.slice_id).collect();
    // 1: fully covered by 2 AND 5 → removed
    assert!(!ids.contains(&1), "slice 1 fully covered by 2");
    // 2: fully covered by 5 → removed
    assert!(!ids.contains(&2), "slice 2 fully covered by 5");
    // 3: NOT fully covered (only [250, 280) covered by 4) → kept
    assert!(ids.contains(&3), "slice 3 not fully covered");
    // 4: newer, not covered → kept
    assert!(ids.contains(&4), "slice 4 newer, kept");
    // 5: newest in [0,100), not covered → kept
    assert!(ids.contains(&5), "slice 5 newest, kept");
}

/// Zero-length slice should be handled (edge case).
#[tokio::test]
async fn test_merge_slices_zero_length() {
    let store = new_test_store().await;
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 0,
            length: 0,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 0,
            length: 100,
        },
    ];
    let result = store.merge_slices(&slices).await.unwrap();
    // A zero-length slice at offset 0: start=0, end=0.
    // Coverage check: 0 >= 0 && 0 <= 100 → fully covered → removed
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].slice_id, 2);
}

// ==================== Comprehensive compact_chunk tests ====================

/// Compact with partially overlapping slices — no slices removed (both kept).
#[tokio::test]
async fn test_compact_chunk_partial_overlap_no_change() {
    let store = new_test_store().await;
    let chunk_id = 10u64;
    // Slice 1: [0, 100), Slice 2: [50, 150) — partial overlap, neither fully covered
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id,
            offset: 0,
            length: 100,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id,
            offset: 50,
            length: 100,
        },
    ];
    let txn = store.db.begin().await.unwrap();
    for s in &slices {
        let model = slice_meta::ActiveModel {
            slice_id: Set(s.slice_id as i64),
            chunk_id: Set(chunk_id as i64),
            offset: Set(s.offset as i64),
            length: Set(s.length as i64),
            ..Default::default()
        };
        model.insert(&txn).await.unwrap();
    }
    txn.commit().await.unwrap();

    let result = store
        .compact_chunk(1, 0, &[], &slices, 0, 0, chunk_id, 150, &[])
        .await;
    assert!(result.is_ok());

    let final_slices = store.get_slices(chunk_id).await.unwrap();
    assert_eq!(
        final_slices.len(),
        2,
        "partial overlap: no slices can be removed, count unchanged"
    );
}

/// Compact with chain of fully-covered slices → cascading removal.
#[tokio::test]
async fn test_compact_chunk_cascading_full_coverage() {
    let store = new_test_store().await;
    let chunk_id = 20u64;
    // 1: [0,50), 2: [0,80), 3: [0,100) — 1 covered by 2, 2 covered by 3
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id,
            offset: 0,
            length: 50,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id,
            offset: 0,
            length: 80,
        },
        SliceDesc {
            slice_id: 3,
            chunk_id,
            offset: 0,
            length: 100,
        },
    ];
    let txn = store.db.begin().await.unwrap();
    for s in &slices {
        let model = slice_meta::ActiveModel {
            slice_id: Set(s.slice_id as i64),
            chunk_id: Set(chunk_id as i64),
            offset: Set(s.offset as i64),
            length: Set(s.length as i64),
            ..Default::default()
        };
        model.insert(&txn).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Prepare delayed data for slices 1 and 2 (format: slice_id + offset + size = 20 bytes each)
    let mut delayed_data = Vec::new();
    // Slice 1: offset=0, length=50
    delayed_data.extend_from_slice(&1u64.to_le_bytes());
    delayed_data.extend_from_slice(&0u64.to_le_bytes());
    delayed_data.extend_from_slice(&50u32.to_le_bytes());
    // Slice 2: offset=0, length=80
    delayed_data.extend_from_slice(&2u64.to_le_bytes());
    delayed_data.extend_from_slice(&0u64.to_le_bytes());
    delayed_data.extend_from_slice(&80u32.to_le_bytes());

    let result = store
        .compact_chunk(1, 0, &[], &slices, 0, 0, chunk_id, 100, &delayed_data)
        .await;
    assert!(result.is_ok());

    let final_slices = store.get_slices(chunk_id).await.unwrap();
    assert_eq!(final_slices.len(), 1, "only slice 3 should remain");
    assert_eq!(final_slices[0].slice_id, 3);

    // Verify delayed slices were created for removed slices
    let delayed: Vec<delayed_slice::Model> = DelayedSlice::find()
        .filter(delayed_slice::Column::ChunkId.eq(chunk_id as i64))
        .all(&store.db)
        .await
        .unwrap();
    assert_eq!(
        delayed.len(),
        2,
        "slices 1 and 2 should be in delayed table"
    );
    let delayed_ids: Vec<i64> = delayed.iter().map(|d| d.slice_id).collect();
    assert!(delayed_ids.contains(&1));
    assert!(delayed_ids.contains(&2));
}

/// compact_chunk with invalid chunk_id=0 returns error.
#[tokio::test]
async fn test_compact_chunk_invalid_chunk_id() {
    let store = new_test_store().await;
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 0,
            offset: 0,
            length: 50,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 0,
            offset: 0,
            length: 100,
        },
    ];
    let result = store
        .compact_chunk(1, 0, &[], &slices, 0, 0, 0, 100, &[])
        .await;
    assert!(result.is_err(), "chunk_id=0 should be rejected");
}

/// compact_chunk with fewer than 2 slices is a no-op.
#[tokio::test]
async fn test_compact_chunk_single_slice_noop() {
    let store = new_test_store().await;
    let chunk_id = 30u64;
    let slices = vec![SliceDesc {
        slice_id: 1,
        chunk_id,
        offset: 0,
        length: 100,
    }];
    let txn = store.db.begin().await.unwrap();
    let model = slice_meta::ActiveModel {
        slice_id: Set(1),
        chunk_id: Set(chunk_id as i64),
        offset: Set(0),
        length: Set(100),
        ..Default::default()
    };
    model.insert(&txn).await.unwrap();
    txn.commit().await.unwrap();

    let result = store
        .compact_chunk(1, 0, &[], &slices, 0, 0, chunk_id, 100, &[])
        .await;
    assert!(result.is_ok());

    let final_slices = store.get_slices(chunk_id).await.unwrap();
    assert_eq!(final_slices.len(), 1, "single slice is unchanged");
}

/// compact_chunk with size=0 returns error.
#[tokio::test]
async fn test_compact_chunk_zero_size() {
    let store = new_test_store().await;
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 0,
            length: 50,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 0,
            length: 100,
        },
    ];
    let result = store
        .compact_chunk(1, 0, &[], &slices, 0, 0, 1, 0, &[])
        .await;
    assert!(result.is_err(), "size=0 should be rejected");
}

/// Slices that exceed chunk bounds are filtered out.
#[tokio::test]
async fn test_compact_chunk_out_of_bounds_slices_filtered() {
    let store = new_test_store().await;
    let chunk_id = 40u64;
    // Slice 1: [0, 50) — within bounds
    // Slice 2: [0, 100) — within bounds, covers slice 1
    // Slice 3: [90, 200) — exceeds size=150 → filtered out
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id,
            offset: 0,
            length: 50,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id,
            offset: 0,
            length: 100,
        },
        SliceDesc {
            slice_id: 3,
            chunk_id,
            offset: 90,
            length: 110,
        },
    ];
    let txn = store.db.begin().await.unwrap();
    for s in &slices {
        let model = slice_meta::ActiveModel {
            slice_id: Set(s.slice_id as i64),
            chunk_id: Set(chunk_id as i64),
            offset: Set(s.offset as i64),
            length: Set(s.length as i64),
            ..Default::default()
        };
        model.insert(&txn).await.unwrap();
    }
    txn.commit().await.unwrap();

    // size=150, pos=0 → chunk_end=150. Slice 3 end=200 > 150 → filtered.
    let result = store
        .compact_chunk(1, 0, &[], &slices, 0, 0, chunk_id, 150, &[])
        .await;
    assert!(result.is_ok());

    let final_slices = store.get_slices(chunk_id).await.unwrap();
    // Valid slices [1, 2]: 1 fully covered by 2 → removed.
    // Slice 3 is out of bounds → also removed.
    // Only slice 2 remains.
    assert_eq!(final_slices.len(), 1);
    assert_eq!(final_slices[0].slice_id, 2);
}

/// Delayed data with invalid length (not multiple of 20) is rejected.
#[tokio::test]
async fn test_compact_chunk_invalid_delayed_data() {
    let store = new_test_store().await;
    let slices = vec![
        SliceDesc {
            slice_id: 1,
            chunk_id: 1,
            offset: 0,
            length: 50,
        },
        SliceDesc {
            slice_id: 2,
            chunk_id: 1,
            offset: 0,
            length: 100,
        },
    ];
    // 15 bytes — not a multiple of 20
    let bad_delayed = vec![0u8; 15];
    let result = store
        .compact_chunk(1, 0, &[], &slices, 0, 0, 1, 100, &bad_delayed)
        .await;
    assert!(
        result.is_err(),
        "invalid delayed data length should be rejected"
    );
}
