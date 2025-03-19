#[test]
fn rclone_ls() {
    if std::env::var("RUN_RCLONE").is_ok() {
        let output = std::process::Command::new("rclone")
            .args([
                "ls",
                "data:/",
                "--s3-access-key-id",
                "access-key-id",
                "--s3-secret-access-key",
                "secret-access-key",
                "--s3-endpoint",
                "http://localhost:8080",
            ])
            .output()
            .unwrap();

        let output = String::from_utf8(output.stdout).unwrap();
        let expected = "        0 adata/data-a.txt\n        0 bdata/data-b.txt\n";

        assert_eq!(output, expected.to_string());
    }
}

#[test]
fn test_recursive_copy() {
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    // Skip test if RCLONE env var not set
    if std::env::var("RUN_RCLONE").is_err() {
        return;
    }

    // Generate unique directory names based on timestamp
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Prepare test directories
    let test_dir_path = PathBuf::from(format!("./test_rclone_src_{}", timestamp));
    let download_path = PathBuf::from(format!("./test_rclone_dest_{}", timestamp));

    // Create directories and cleanup any existing ones
    if test_dir_path.exists() {
        fs::remove_dir_all(&test_dir_path).unwrap();
    }
    if download_path.exists() {
        fs::remove_dir_all(&download_path).unwrap();
    }

    fs::create_dir(&test_dir_path).unwrap();
    fs::create_dir(&download_path).unwrap();

    // Create nested directory structure
    let nested_dir1 = test_dir_path.join("nested1");
    let nested_dir2 = nested_dir1.join("nested2");
    fs::create_dir_all(&nested_dir2).unwrap();

    // Create test files
    let file1_path = test_dir_path.join("file1.txt");
    let file2_path = nested_dir1.join("file2.txt");
    let file3_path = nested_dir2.join("file3.txt");

    let mut file1 = fs::File::create(&file1_path).unwrap();
    let mut file2 = fs::File::create(&file2_path).unwrap();
    let mut file3 = fs::File::create(&file3_path).unwrap();

    file1.write_all(b"root file content").unwrap();
    file2.write_all(b"level 1 content").unwrap();
    file3.write_all(b"level 2 content").unwrap();

    // 1. Copy to S3 using rclone
    let output = Command::new("rclone")
        .args([
            "copy",
            &test_dir_path.to_string_lossy(),
            "data:test-recursive/",
            "--s3-access-key-id",
            "access-key-id",
            "--s3-secret-access-key",
            "secret-access-key",
            "--s3-endpoint",
            "http://localhost:8080",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "Failed to copy to S3: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // 2. List bucket contents to verify all files were uploaded
    let output = Command::new("rclone")
        .args([
            "ls",
            "data:test-recursive/",
            "--s3-access-key-id",
            "access-key-id",
            "--s3-secret-access-key",
            "secret-access-key",
            "--s3-endpoint",
            "http://localhost:8080",
        ])
        .output()
        .unwrap();

    let listing = String::from_utf8(output.stdout).unwrap();

    // Verify all files are listed
    assert!(listing.contains("file1.txt"), "file1.txt not listed");
    assert!(
        listing.contains("nested1/file2.txt"),
        "nested1/file2.txt not listed"
    );
    assert!(
        listing.contains("nested1/nested2/file3.txt"),
        "nested1/nested2/file3.txt not listed"
    );

    // 3. Test downloading the entire directory structure
    let output = Command::new("rclone")
        .args([
            "copy",
            "data:test-recursive/",
            &download_path.to_string_lossy(),
            "--s3-access-key-id",
            "access-key-id",
            "--s3-secret-access-key",
            "secret-access-key",
            "--s3-endpoint",
            "http://localhost:8080",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "Failed to download from S3: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // 4. Verify downloaded files match original structure
    let downloaded_file1 = download_path.join("file1.txt");
    let downloaded_file2 = download_path.join("nested1").join("file2.txt");
    let downloaded_file3 = download_path
        .join("nested1")
        .join("nested2")
        .join("file3.txt");

    assert!(downloaded_file1.exists(), "file1.txt not downloaded");
    assert!(
        downloaded_file2.exists(),
        "nested1/file2.txt not downloaded"
    );
    assert!(
        downloaded_file3.exists(),
        "nested1/nested2/file3.txt not downloaded"
    );

    // 5. Verify file contents
    let content1 = fs::read_to_string(&downloaded_file1).unwrap();
    let content2 = fs::read_to_string(&downloaded_file2).unwrap();
    let content3 = fs::read_to_string(&downloaded_file3).unwrap();

    assert_eq!(content1, "root file content");
    assert_eq!(content2, "level 1 content");
    assert_eq!(content3, "level 2 content");

    // Cleanup
    fs::remove_dir_all(&test_dir_path).unwrap_or(());
    fs::remove_dir_all(&download_path).unwrap_or(());
}
