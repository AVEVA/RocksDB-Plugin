# BlobFilesystemImpl Integration Tests

## Overview
This file contains comprehensive integration tests for the `BlobFilesystemImpl` class. These tests verify real interactions with Azure Blob Storage using the integration test helpers framework.

## Test Categories

### 1. File Creation and Existence Tests
- **CreateWriteableFile_CreatesNewFile**: Verifies that creating a writeable file actually creates it in Azure
- **CreateReadableFile_FromExistingBlob_Succeeds**: Tests reading from an existing blob
- **CreateReadWriteFile_CreatesNewFile**: Validates read-write file creation
- **FileExists_NonExistentFile_ReturnsFalse**: Confirms non-existent file detection

### 2. File Size and Metadata Tests
- **GetFileSize_AfterWriting_ReturnsCorrectSize**: Validates file size reporting
- **GetFileModificationTime_ReturnsValidTimestamp**: Tests modification time retrieval

### 3. File Deletion Tests
- **DeleteFile_ExistingFile_ReturnsTrue**: Confirms file deletion works
- **DeleteFile_NonExistentFile_ReturnsFalse**: Tests deletion of non-existent files

### 4. Directory Operations Tests
- **GetChildren_EmptyDirectory_ReturnsEmpty**: Tests listing empty directories
- **GetChildren_WithFiles_ReturnsFileNames**: Validates directory listing with multiple files
- **GetChildrenFileAttributes_ReturnsCorrectSizes**: Tests retrieving file attributes
- **DeleteDir_RemovesAllFilesInDirectory**: Validates recursive directory deletion

### 5. File Rename Tests
- **RenameFile_MovesDataCorrectly**: Tests file renaming with data preservation
- **RenameFile_LargeFile_HandlesCorrectly**: Tests renaming files larger than 4MB (chunked operations)

### 6. File Truncate Tests
- **Truncate_ReducesFileSize**: Validates file truncation
- **Truncate_ToLargerSize_DoesNothing**: Confirms truncate doesn't increase size

### 7. Lock File Tests
- **LockFile_SuccessfullyAcquiresLock**: Tests lock acquisition
- **UnlockFile_ReleasesLock**: Validates lock release
- **UnlockFile_InvalidLock_ReturnsFalse**: Tests unlocking non-existent locks

### 8. Reopen and Reuse Tests
- **ReopenWriteableFile_PreservesExistingData**: Tests reopening files without data loss
- **ReuseWritableFile_CreatesNewFile**: Validates file reuse (delete and recreate)

### 9. Logger Tests
- **CreateLogger_CreatesLogFile**: Tests log file creation

### 10. Directory Creation Tests
- **CreateDirectory_ReturnsDirectoryImpl**: Validates directory creation

### 11. Edge Cases and Error Handling
- **CreateWriteableFile_SameFileTwice_TruncatesExisting**: Tests overwriting existing files
- **GetChildren_WithSizeHint_ReturnsResults**: Tests directory listing with size hints

## Prerequisites

### Environment Variables
These tests require the following environment variables to be set:

- `AZURE_SERVICE_PRINCIPAL_ID`: Service principal client ID
- `AZURE_SERVICE_PRINCIPAL_SECRET`: Service principal client secret
- `AZURE_STORAGE_ACCOUNT_NAME`: Azure storage account name
- `AZURE_TENANT_ID` (optional): Azure tenant ID
- `AZURE_TEST_CONTAINER` (optional): Container name (defaults to "aveva-rocksdb-plugin-integration-tests")

### Skipping Tests
If credentials are not configured, tests will be automatically skipped with an informative message. This allows the test suite to run in environments without Azure access.

## Test Structure

All tests inherit from `AzureIntegrationTestBase` which provides:
- Automatic Azure resource setup/teardown
- Blob name generation for test isolation
- Helper methods for creating test blobs
- Authentication error handling

## Usage

Run the tests using:
```bash
ctest -R aveva-rocksdb-plugin-azure-impl-tests
```

Or run specifically the BlobFilesystem integration tests:
```bash
ctest -R BlobFilesystemIntegrationTests
```

## Implementation Notes

1. **Test Isolation**: Each test generates a unique blob name to avoid interference
2. **Cleanup**: TearDown() ensures test blobs are cleaned up
3. **Authentication**: Tests gracefully skip on authentication failures
4. **Real Azure Operations**: These are true integration tests hitting real Azure services
5. **Data Verification**: Tests verify both operations succeed and data integrity is maintained

## Adding New Tests

When adding new tests:
1. Inherit from `AzureIntegrationTestBase`
2. Use `m_filesystem` to access the BlobFilesystemImpl instance
3. Use `m_blobName` for unique test file names
4. Clean up any additional resources created
5. Follow the naming pattern: `MethodName_Scenario_ExpectedResult`
