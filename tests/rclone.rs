#[test]
fn rclone_ls() {
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
